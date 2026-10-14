// 并发竞速与节点健康度 —— 移植自 vertex-master 的对冲竞速引擎（race_engine.go）
// 与节点健康存储（nodes/store.go 健康评分/冷却/衰减 + sticky.go 粘性池）。
//
// Workers 化裁剪：
//   - 原项目 SQLite 持久化 → KV 快照（isolate 内存累积 + 20 秒批量刷盘，规避免费计划 1000 写/天）；
//   - 原项目 goroutine 竞速 → Promise 竞速（编排见 proxy/racefetch.ts，本文件只放纯逻辑）；
//   - 健康语义与原项目对齐：
//       * 连败指数冷却 30s * 2^(n-1)，封顶 30 分钟（RecordTest）；
//       * 429 固定 30 秒冷却并递增计次，重复 429 自然降权（RecordRateLimit）；
//       * 计数衰减防溢出（decayHealthCounters）；
//       * 粘性池：竞速胜出节点优先复用，触发限流/连败即驱逐（StickyNodePool）；
//       * 未测试节点只给少量探索名额，不能压过已验证节点（SelectForParallel）；
//       * 冷却节点垫底兜底，仅在无可用节点时启用。
//
// ⚠️ 本文件不 import "cloudflare:sockets"，可在 Node 单测中直接运行；
//    KV 仅为鸭子类型读写（测试可注入假 KV）。
import type { Env } from "./config.ts";

// ---------- 竞速配置 ----------

export interface RacingConfig {
  /** 是否启用并发竞速；关闭时退回「健康排序 + 轮换 + 故障接力」模式 */
  enabled: boolean;
  /** 参与竞速的已验证候选上限（按健康分取前 K） */
  top_k: number;
  /** 同时在飞的竞速请求数上限 */
  max_concurrent: number;
  /** 对冲延迟（毫秒）：首个节点发出后，每隔该时间追加下一个候选 */
  hedge_delay_ms: number;
  /** 动态对冲：用全体健康节点的平均延迟作为对冲间隔 */
  dynamic_delay: boolean;
  /** 单个请求最多尝试的节点数（Workers 子请求限额保护） */
  max_attempts: number;
  /** 节点内重试（原项目 parallel_pool_retry_enabled）：网络/5xx 错误时同一节点立即重试一次；429 仍直接冷却换节点 */
  node_retry: boolean;
  /** v1.4.0：整轮 429 重试次数 —— 一轮候选全部撞 per-IP 窗口限流后，等待最早恢复
   *  窗口（钳 250ms~8s）并重新选点再战一轮（每轮重新 selectCandidates，被冷却
   *  节点自然让位 → 天然换 IP 轮转）。0 = 关闭（耗尽即兜底）；project 级每日
   *  配额仍硬透传不受此影响。总尝试数受 TOTAL_ATTEMPT_CAP 保护。 */
  rate_limit_retries: number;
}

export const DEFAULT_RACING: RacingConfig = {
  enabled: true,
  top_k: 6,
  max_concurrent: 3,
  hedge_delay_ms: 1000,
  dynamic_delay: false,
  max_attempts: 8,
  node_retry: true,
  rate_limit_retries: 1,
};

// ---------- 定时健康巡检（原项目 proxy_health_check_*，Workers 用 Cron Triggers 驱动） ----------

export interface HealthCheckConfig {
  /** 是否启用定时健康巡检（需在 wrangler.jsonc 配置 triggers.crons） */
  enabled: boolean;
  /** 巡检间隔（分钟，对齐原项目 interval_minutes 默认 15） */
  interval_minutes: number;
  /** 每轮巡检最多测试的代理数（免费计划单次调用 50 子请求上限，默认 40 留余量） */
  batch_size: number;
  /** 巡检并发数 */
  concurrency: number;
  /** 单个代理巡检超时（秒） */
  timeout_seconds: number;
}

export const DEFAULT_HEALTH_CHECK: HealthCheckConfig = {
  enabled: true,
  interval_minutes: 15,
  batch_size: 40,
  concurrency: 5,
  timeout_seconds: 8,
};

export function sanitizeHealthCheckConfig(raw: unknown): HealthCheckConfig {
  const d = raw && typeof raw === "object" ? (raw as Partial<HealthCheckConfig>) : {};
  return {
    enabled: d.enabled === undefined ? DEFAULT_HEALTH_CHECK.enabled : !!d.enabled,
    interval_minutes: clampInt(d.interval_minutes, DEFAULT_HEALTH_CHECK.interval_minutes, 5, 1440),
    batch_size: clampInt(d.batch_size, DEFAULT_HEALTH_CHECK.batch_size, 1, 40),
    concurrency: clampInt(d.concurrency, DEFAULT_HEALTH_CHECK.concurrency, 1, 10),
    timeout_seconds: clampInt(d.timeout_seconds, DEFAULT_HEALTH_CHECK.timeout_seconds, 2, 30),
  };
}

function clamp(v: number, lo: number, hi: number): number {
  return Math.min(hi, Math.max(lo, v));
}

function clampInt(v: unknown, def: number, lo: number, hi: number): number {
  const n = Number(v);
  return Number.isFinite(n) ? Math.floor(clamp(n, lo, hi)) : def;
}

export function sanitizeRacingConfig(raw: unknown): RacingConfig {
  const d = raw && typeof raw === "object" ? (raw as Partial<RacingConfig>) : {};
  return {
    enabled: d.enabled === undefined ? DEFAULT_RACING.enabled : !!d.enabled,
    top_k: clampInt(d.top_k, DEFAULT_RACING.top_k, 1, 16),
    max_concurrent: clampInt(d.max_concurrent, DEFAULT_RACING.max_concurrent, 1, 6),
    hedge_delay_ms: clampInt(d.hedge_delay_ms, DEFAULT_RACING.hedge_delay_ms, 100, 10_000),
    dynamic_delay: !!d.dynamic_delay,
    max_attempts: clampInt(d.max_attempts, DEFAULT_RACING.max_attempts, 1, 20),
    node_retry: d.node_retry === undefined ? DEFAULT_RACING.node_retry : !!d.node_retry,
    rate_limit_retries: clampInt(d.rate_limit_retries, DEFAULT_RACING.rate_limit_retries, 0, 3),
  };
}

// ---------- 定时任务纯逻辑判定（供 cron.ts 与单测使用；racing.ts 无 Workers 依赖） ----------

/** 是否该跑健康巡检：开关开启 且（从未巡检过 或 距上次 ≥ interval 分钟） */
export function sweepDue(
  cfg: { health_check: { enabled: boolean; interval_minutes: number } },
  lastSweepAt: number,
  now: number,
): boolean {
  if (!cfg.health_check.enabled) return false;
  if (!lastSweepAt) return true;
  return now - lastSweepAt >= cfg.health_check.interval_minutes * 60_000;
}

/** 是否该发保活：URL 非空 且（从未发过（首次立即发，对齐原项目）或 距上次 ≥ interval 秒） */
export function keepaliveDue(keepaliveUrl: string, keepaliveIntervalSec: number, lastAt: number, now: number): boolean {
  if (!keepaliveUrl) return false;
  const interval = Math.max(5, keepaliveIntervalSec) * 1000;
  if (!lastAt) return true;
  return now - lastAt >= interval;
}

/** 是否该主动刷新订阅（v1.8.0）：从未拉过 或 缓存距上次拉取 ≥ refresh 分钟。
 *  此前 cron 每跳（15 分钟）都无条件拉订阅 + 写 KV —— 无视 subscription_refresh_minutes，
 *  免费计划每天白耗 ~96 次 KV 写与 96 次订阅出站请求。 */
export function subscriptionDue(cacheAt: number, refreshMinutes: number, now: number): boolean {
  if (!cacheAt) return true;
  const interval = Math.max(5, refreshMinutes) * 60_000;
  return now - cacheAt >= interval;
}

// ---------- 健康记录 ----------

export interface ProxyHealth {
  success: number;
  fail: number;
  consec_fail: number;
  /** 最近一次成功请求延迟（毫秒） */
  last_ms: number;
  /** 成功延迟的指数移动平均 */
  avg_ms: number;
  last_error: string;
  last_success_at: number; // epoch 秒
  last_fail_at: number; // epoch 秒
  cooldown_until: number; // epoch 秒
  rate_limit_count: number;
  /** 粘性标记：近期竞速胜出节点 */
  sticky: boolean;
}

export function emptyHealth(): ProxyHealth {
  return {
    success: 0,
    fail: 0,
    consec_fail: 0,
    last_ms: 0,
    avg_ms: 0,
    last_error: "",
    last_success_at: 0,
    last_fail_at: 0,
    cooldown_until: 0,
    rate_limit_count: 0,
    sticky: false,
  };
}

/** 连败指数冷却：30s * 2^(n-1)，封顶 30 分钟（对齐原项目 RecordTest） */
export function cooldownSecondsFor(consecFail: number): number {
  const n = Math.max(1, consecFail);
  return Math.min(1800, 30 * Math.pow(2, Math.min(n - 1, 6)));
}

export const RATE_LIMIT_COOLDOWN_SEC = 30;

/** 计数衰减：量大时对半，防止长期运行后成功率失真（对齐原项目 decayHealthCounters）
 *  v2.5.1：rate_limit_count 一并衰减 —— 否则 429 惩罚会永久压低节点分数。 */
function decayCounters(h: ProxyHealth): void {
  if (h.success <= 1000 && h.fail <= 200 && h.rate_limit_count <= 8) return;
  h.success = Math.floor(h.success / 2);
  h.fail = Math.floor(h.fail / 2);
  h.rate_limit_count = Math.floor(h.rate_limit_count / 2);
}

/**
 * 节点健康分（越高越优先）：
 *   - 未测试节点固定 80 分（探索档，永远排在已验证节点之后）；
 *   - 已验证节点 = 60 基础分 + 20*成功率 + 延迟加分(0~20) - 8*连败(封顶5) + 粘性加分15
 *     - v2.5.1 429 惩罚：- 5*min(rate_limit_count, 6)（最多 -30）。
 *       单 IP 滑动窗口限流（每 IP 独立 20 次/窗口）下，top_k 竞速总挑「最近成功」的
 *       节点 —— 恰是窗口刚耗尽的节点：撞 429 → 冷却 ~20s → 恢复 → 又被选中 → 循环，
 *       池里其余几百个节点永远轮不到。计次惩罚让频繁撞墙的节点短期让位，流量
 *       分散到全池（375+ 节点轮换 ≈ 接近无限额度）；计数由 decayCounters 周期减半
 *       清理，节点长期正常服务后自动回到高位。
 */
/** v1.4.2：成功记录新鲜度 —— 6h 宽限期内全额计分，6h~24h 线性衰减到 0 */
const STALE_GRACE_SEC = 6 * 3600;
const STALE_ZERO_SEC = 24 * 3600; // 与 HEALTH_TTL_SEC 对齐：超过一天的快照本就不加载

export function healthScore(h: ProxyHealth | undefined, now: number): number {
  if (isCooling(h, now)) return 0; // 冷却中（防御：优先于未测试判断）
  if (!h || (h.last_success_at === 0 && h.last_fail_at === 0)) return 80; // 未测试
  const total = h.success + h.fail;
  const rate = total > 0 ? h.success / total : 0;
  const latencyBonus = clamp(20 - h.avg_ms / 50, 0, 20); // 1000ms+ → 0 分，0ms → 20 分
  const failPenalty = 8 * Math.min(h.consec_fail, 5);
  const rlPenalty = 5 * Math.min(h.rate_limit_count, 6);
  const stickyBonus = h.sticky ? 15 : 0;
  // v1.4.2：成功侧加分（成功率/延迟/粘性）按最近一次成功的新鲜度衰减 ——
  // 数天前巡检残留的陈旧成功不再把早已死掉的节点顶到未测试节点（80 分）之前；
  // 基础分 60 与失败/限流惩罚不衰减：已知坏节点依旧垫底。举例：16 天前
  // success=2 的节点从 ~86 分降到 60 分（未测试档之下），接力顺序自动让位给
  // 新鲜血液，新 isolate 冷启动不再优先撞死节点。
  const ageSec = h.last_success_at > 0 ? now / 1000 - h.last_success_at : Infinity;
  const ageFactor =
    ageSec <= STALE_GRACE_SEC
      ? 1
      : ageSec >= STALE_ZERO_SEC
        ? 0
        : 1 - (ageSec - STALE_GRACE_SEC) / (STALE_ZERO_SEC - STALE_GRACE_SEC);
  return 60 + (20 * rate + latencyBonus + stickyBonus) * ageFactor - failPenalty - rlPenalty;
}

export function isCooling(h: ProxyHealth | undefined, now: number): boolean {
  return !!h && h.cooldown_until * 1000 > now;
}

// ---------- 竞速候选选择（纯函数，供单测与 racefetch 使用） ----------

export interface CandidateLike {
  raw: string;
}

/**
 * 选择竞速候选（对齐原项目 SelectForParallel 语义）：
 *   1. 健康分 ≥ 未测试档（80）的已验证节点按降序取前 top_k；
 *   2. 未测试节点最多给 2 个探索名额；
 *   3. v2.5.1 降级让位：被 429 计次/连败惩罚压到 80 分以下的已验证节点，排在
 *      未测试节点**之后**（原实现已验证节点无条件占 top_k 坑 —— 单 IP 窗口限流下
 *      top_k 竞速反复撞同一批「窗口刚耗尽」的节点，池里几百个新鲜 IP 永远轮不到；
 *      降级让位 + 未测试探索 = 全池轮换 ≈ 接近无限额度）。降级节点仍排在冷却
 *      节点之前垫底（比冷却节点先恢复可用）；
 *   4. 冷却节点按「最早恢复」排序垫底兜底；
 *   5. 总数不超过 max_attempts。
 */
export function selectCandidates<T extends CandidateLike>(
  entries: T[],
  healthMap: Map<string, ProxyHealth>,
  cfg: RacingConfig,
  now: number,
): T[] {
  const verified: Array<{ e: T; score: number }> = [];
  const untested: T[] = [];
  const cooling: Array<{ e: T; recoverAt: number }> = [];
  const seen = new Set<string>();
  for (const e of entries) {
    if (seen.has(e.raw)) continue;
    seen.add(e.raw);
    const h = healthMap.get(e.raw);
    if (isCooling(h, now)) {
      cooling.push({ e, recoverAt: h!.cooldown_until });
      continue;
    }
    if (!h || (h.last_success_at === 0 && h.last_fail_at === 0)) {
      untested.push(e);
      continue;
    }
    verified.push({ e, score: healthScore(h, now) });
  }
  verified.sort((a, b) => b.score - a.score);
  cooling.sort((a, b) => a.recoverAt - b.recoverAt);

  // v2.5.1：健康档（≥80，与未测试同档）优先占 top_k；降级档（<80）让位给未测试节点
  const strong = verified.filter((v) => v.score >= 80);
  const demoted = verified.filter((v) => v.score < 80);

  const out: T[] = [];
  const cap = Math.max(1, cfg.max_attempts);
  for (const v of strong) {
    if (out.length >= Math.min(cap, Math.max(1, cfg.top_k))) break;
    out.push(v.e);
  }
  for (const u of untested) {
    if (out.length >= Math.min(cap, Math.max(1, cfg.top_k) + 2)) break; // 最多 2 个探索名额
    out.push(u);
  }
  for (const v of demoted) {
    if (out.length >= cap) break;
    out.push(v.e);
  }
  if (out.length === 0) {
    // 全部冷却：让最早恢复的节点兜底，避免直接放弃
    for (const c of cooling) {
      if (out.length >= cap) break;
      out.push(c.e);
    }
  }
  return out.slice(0, cap);
}

/** 全体健康节点平均延迟（动态对冲用）；无数据时 500ms（对齐原项目 GetAverageLatency） */
export function averageLatency(healthMap: Map<string, ProxyHealth>, now: number): number {
  let sum = 0;
  let count = 0;
  for (const h of healthMap.values()) {
    if (h.avg_ms > 0 && !isCooling(h, now)) {
      sum += h.avg_ms;
      count++;
    }
  }
  return count === 0 ? 500 : sum / count;
}

// ---------- v1.4.0：整轮 429 重试（纯函数，Node 可单测） ----------

/** 跨轮总尝试预算：免费计划单请求子请求上限 50，预留直连兜底/保活余量。
 *  防御 max_attempts=20 × (1+rate_limit_retries=3) = 80 打爆限额。 */
export const TOTAL_ATTEMPT_CAP = 40;

/** 429 重试轮的等待上限（毫秒）：per-IP 窗口典型 ~20s 即恢复，等待过久不如
 *  尽早把 429 透传给客户端（调用方自带直连兜底）。 */
export const RATE_LIMIT_ROUND_WAIT_MAX_MS = 8_000;

/** 429 重试轮等待时长：传入本轮仍处于冷却的候选节点的 cooldown_until（epoch 秒）。
 *  - 空列表（有非冷却新候选可换）→ 0，立即换点再战；
 *  - 有冷却候选 → 等到最早恢复者，钳位 [250ms, 8s]（等待中不耗 CPU，只耗墙钟）。
 *  全部候选冷却且最早恢复仍很远时，只等 8s 就带上「最早恢复」的节点再战 ——
 *  宁可重试可能仍限流的节点，也不让客户端干等分钟级。 */
export function rateLimitRoundWaitMs(cooldownUntilSecs: number[], nowMs: number): number {
  const future = cooldownUntilSecs.filter((s) => Number.isFinite(s) && s * 1000 > nowMs);
  if (future.length === 0) return 0;
  const earliest = Math.min(...future) * 1000;
  return Math.min(Math.max(earliest - nowMs, 250), RATE_LIMIT_ROUND_WAIT_MAX_MS);
}

// ---------- v1.6.0：anon PVP 竞速轮次/截止（纯函数，Node 可单测） ----------

/**
 * v1.6.0：anon 整轮 429 重试轮数（anon/upstream.ts 调用）。
 * 复用 racing.rate_limit_retries 配置，钳在 1~4 轮：
 *   - 默认（未配置）1 次重试 → 2 轮；
 *   - PVP 推荐配置 rate_limit_retries=3 → 4 轮 × 3 并发 = 12 发候选。
 * PVP 语义（用户线上实测确认）：匿名端点 429 是 per-IP 窗口抢占而非每日配额，
 * 窗口秒级释放 —— 每轮重选候选（被 429 冷却的节点自然沉底 → 天然换 IP），
 * 多轮换 IP 轮转即更多抢占机会。
 */
export function anonRoundCount(rateLimitRetries: number | undefined): number {
  const n = typeof rateLimitRetries === "number" && Number.isFinite(rateLimitRetries) ? Math.trunc(rateLimitRetries) : 1;
  return 1 + Math.min(Math.max(n, 0), 3);
}

/**
 * v1.6.0：anon 单轮「产出内容前」截止 = min(单轮上限 22s, 整场剩余墙钟预算)，
 * ≥3s 保底（剩余再少也至少给在飞轮 3s，而非立即全灭）。
 * 整场墙钟 60s 硬限（ANON_TOTAL_RACE_DEADLINE_MS）：4 轮 × 22s 最坏 ~88s 会
 * 逼近 CF 代理 ~100s 首字节超时；钳入剩余预算后内容前阶段总时长 ≤ 60s。
 * 产出内容后截止解除（胜者的长生成不受限）。
 */
export function anonRoundDeadlineMs(remainingTotalMs: number, perRoundCapMs: number): number {
  return Math.min(perRoundCapMs, Math.max(3_000, remainingTotalMs));
}

/**
 * v1.6.1：巡检批量上限 —— anon 模式双主机探测每节点最多 2 子请求
 * （anchor + batchGraphql 主机；阶段 1 失败短路后死节点只耗 1），钳 20
 * 保免费计划单次调用 50 子请求限额（20×2 + KV 读写 ≈ 44）；official 模式
 * 单探测维持 40。探测目标失真的教训见 anon/probe.ts 头注释。
 */
export function sweepBatchCap(apimode: string): number {
  return apimode === "anon" ? 20 : 40;
}

// ---------- v1.6.2：anon PVP 候选排序（纯函数，Node 可单测） ----------

/**
 * v1.6.2：anon 模式专用候选排序 —— PVP 语义下「429 = 活着但在竞争」。
 *
 * 背景（线上实测）：通用 healthScore 的 429 计次降权（-5×count，v2.5.1 为
 * official 375 节点池设计）把反复撞 429 的活节点压到未测试节点（80 分）
 * 之下 —— 在 anon 现实（几百节点仅 ~5% 活着）下反效果：竞速宁可去撞
 * 95% 死亡率的未测试节点（recaptcha 阶段即死，非 429 relay → 整轮 break
 * → 客户端拿 502），也不复用已知活节点。429 本身就是节点与 Google 全链路
 * 可用的证明，远比未测试节点有价值。
 *
 * 分层（anon 专用，不改动 official 路径的 selectCandidates）：
 *   1. 已验证且未冷却 —— 按 healthScore 降序（429 计次只在层内微调次序）；
 *   2. 已验证但冷却中（近 6h 内成功过，含 429 冷却/瞬时失败）—— 按最早恢复
 *      排序：冷却只是排序概念而非连接阻断，PVP 下直接复打（每次 ~2-3 子请求，
 *      窗口是否释放一试便知）；跨轮天然轮转（最早恢复者优先）；
 *   3. 未测试探索档（发现新节点；cron 巡检为主、竞速兜底）；
 *   4. 其余冷却（连败死节点）。
 */
export function anonPvpCandidateOrder<T extends CandidateLike>(
  entries: T[],
  healthMap: Map<string, ProxyHealth>,
  now: number,
  limit: number,
): T[] {
  const seen = new Set<string>();
  const live: Array<{ e: T; score: number }> = [];
  const coolingLive: Array<{ e: T; recoverAt: number }> = [];
  const untested: T[] = [];
  const rest: Array<{ e: T; recoverAt: number }> = [];
  for (const e of entries) {
    if (seen.has(e.raw)) continue;
    seen.add(e.raw);
    const h = healthMap.get(e.raw);
    if (!h || (h.last_success_at === 0 && h.last_fail_at === 0)) {
      untested.push(e);
      continue;
    }
    if (isCooling(h, now)) {
      const freshSuccess = h.last_success_at > 0 && now / 1000 - h.last_success_at < 6 * 3600;
      (freshSuccess ? coolingLive : rest).push({ e, recoverAt: h.cooldown_until });
      continue;
    }
    live.push({ e, score: healthScore(h, now) });
  }
  live.sort((a, b) => b.score - a.score);
  coolingLive.sort((a, b) => a.recoverAt - b.recoverAt);
  rest.sort((a, b) => a.recoverAt - b.recoverAt);
  const out: T[] = [];
  for (const v of live) {
    if (out.length >= limit) break;
    out.push(v.e);
  }
  for (const v of coolingLive) {
    if (out.length >= limit) break;
    out.push(v.e);
  }
  for (const u of untested) {
    if (out.length >= limit) break;
    out.push(u);
  }
  for (const v of rest) {
    if (out.length >= limit) break;
    out.push(v.e);
  }
  return out;
}

// ---------- v2.5.0：429 分类（纯函数，Node 可单测） ----------

/**
 * v2.5.0：429 分类（修正 v2.4.1 的语义误判）。
 *
 * 实测教训：v2.4.1 用 /exceeded your current quota|PerProject|FreeTier/ 把一切配额类
 * 429 判成「Key 级每日配额」硬失败 —— 但 "You exceeded your current quota" 文案同时
 * 出现在**单 IP 滑动窗口限流**（metric generate_content_free_tier_requests，
 * "Please retry in 20.46s"，等 ~20s 即恢复）与每日配额两类错误里；且 RPM 型
 * quotaId（GenerateRequestsPerMinute**PerProject**PerModel）本身就含 "PerProject"。
 * 结果：配额无限的 Key + 每 IP 20 次/窗口的限流被误判为 Key 级，不换节点直接
 * 把 429 透传客户端 —— 而 375 个代理节点每个 IP 都有独立窗口，本可轮转绕过。
 *
 * 现行语义（可用性优先）：
 *   - per-ip  ：单 IP 滑动窗口限流（秒级 retry 提示 / PerMinute / per-IP / 裸文案 /
 *               代理自制 429 页）→ 冷却该节点（用上游明示时长）并换节点重试；
 *   - project ：项目级**每日**配额（PerDay 等特征，换节点无法绕过）→ 请求级
 *               硬错误：立即透传客户端并中止在飞候选（保留 v2.4.1 保护）。
 *   裸 "exceeded your current quota"（无 PerDay、无秒级提示）按 per-ip 处理：
 *   最坏情况多试几个节点后仍把 429 透传（浪费 ≤max_attempts 次上游请求），
 *   好过把可轮转绕过的限流误判成硬失败。
 */
export type Quota429Level = "per-ip" | "project";

export interface Quota429Class {
  level: Quota429Level;
  /** 上游明示的滑动窗口重试等待（秒），用于该节点冷却时长 */
  retryAfterSecs?: number;
}

/** 解析上游 429 体里的秒级重试提示：
 *  "Please retry in 20.464172338s" / "try again in 30 seconds" / "retryDelay":"25s"。
 *  只认 ≤300s 的值 —— 每日配额的 reset 提示是小时级，不应被当成窗口限流。 */
export function parse429RetryAfterSecs(bodyText: string): number | null {
  let m = bodyText.match(/retry[^0-9\n]{0,16}(\d+(?:\.\d+)?)\s*s\b/i); // Please retry in 20.46s
  if (!m) m = bodyText.match(/try\s+again[^0-9\n]{0,16}(\d+(?:\.\d+)?)\s*(?:s\b|sec|seconds)/i);
  if (!m) m = bodyText.match(/"retryDelay"\s*:\s*"(\d+(?:\.\d+)?)s/i);
  if (!m) return null;
  const v = Number(m[1]);
  return Number.isFinite(v) && v > 0 && v <= 300 ? v : null;
}

export function classify429Body(bodyText: string): Quota429Class {
  // 项目级每日配额特征：换节点无法绕过（quotaId PerDay / daily limit / resets daily）
  if (/PerDay|per[_\s-]?day(?:[_\s]?limit)?\b|daily[_\s-]?limit|resets?\s+daily/i.test(bodyText)) {
    return { level: "project" };
  }
  const retryAfterSecs = parse429RetryAfterSecs(bodyText);
  if (retryAfterSecs != null) return { level: "per-ip", retryAfterSecs };
  // 每分钟 / 单 IP 指标名 → 窗口限流
  if (/PerMinute|per[_\s-]?minute|per[_\s-]?ip/i.test(bodyText)) return { level: "per-ip" };
  // 其余一律按节点级（可用性优先，见函数头注释）
  return { level: "per-ip" };
}

// ---------- v2.5.3：可疑上游鉴权错误（公共代理池劫持节点伪造检测） ----------

/**
 * 密钥/账号级错误特征 —— 公共代理池中的劫持节点最爱伪造这类错误（30ms 即回，
 * 骗竞速「首胜即停」直接透传，让客户端误以为密钥失效而放弃或换 Key）。
 *
 * 判定依据（线上实测案例）：同一密钥几分钟内先收到标准 429（RESOURCE_EXHAUSTED
 * 含 free_tier 计量 —— 限流中但密钥有效），随后却收到 "The bound service account
 * is deleted or disabled"。密钥经用户在 Google 后台确认一直有效 → 后者只能来自
 * 代理伪造。
 *
 * 策略：首个命中不做硬错误立即透传，而是记失败（连败冷却让伪造者出局）并接力
 * 下一节点验证 —— 第二个节点仍返回同类错误（两个独立出口一致）才视为真实错误
 * 透传；全部候选失败时上层熔断兜底直连（直连返回的才是可信结论）。
 */
export const SUSPICIOUS_AUTH_ERROR_SIGNATURE =
  /service account[^.\n]{0,60}(deleted|disabled)|(?:API[_ ]?key)[^.\n]{0,30}(not valid|invalid|expired)|API_KEY_INVALID|permission[_\s-]?denied[^.\n]{0,60}service account/i;

export function isSuspiciousAuthError(bodyText: string): boolean {
  return SUSPICIOUS_AUTH_ERROR_SIGNATURE.test(bodyText);
}

/** v1.9.0：巡检选点 —— 「最久未测优先」轮转。
 * 旧版固定 `slice(0, batch)` 只反复测池子头部节点：200 节点的池子后 160 个
 * 永远不会被巡检探索（若头部 40 个全坏，巡检就永远因在死节点上）。
 * 现在按 max(last_success_at, last_fail_at) 升序排（未测试 = 0 最优先），
 * 数轮巡检即可全池轮转覆盖；同时间戳保持池子原顺序（稳定排序）。 */
export function leastRecentlyTestedOrder<T extends CandidateLike>(
  entries: T[],
  healthMap: Map<string, ProxyHealth>,
): T[] {
  const lastTested = (e: T): number => {
    const h = healthMap.get(e.raw);
    if (!h) return 0;
    return Math.max(h.last_success_at, h.last_fail_at);
  };
  return [...entries].sort((a, b) => lastTested(a) - lastTested(b));
}

// ---------- 代理池平台级熔断（v1.9.1，纯逻辑） ----------

/**
 * Workers 平台适配：cloudflare:sockets 的 startTls() 无法在已承载代理握手流量的
 * socket 上完成 TLS 升级（workerd runtime 限制，100% 复现 "TLS Handshake Failed."，
 * 与代理质量/协议实现无关 —— 隧道双向透传原始 TLS 字节正常）。因此代理池在
 * Workers 上可能「整体不可用」：若沿用原项目语义（全池失败 → 直接报错），
 * 配置了订阅的部署会全站瘫痪。熔断器在全池连续失败 N 次后打开，后续请求
 * 直接走直连 fetch（保命），冷却窗口过后自动半开重试代理池 —— 若平台修复
 * 或用户换到可用代理，行为自动恢复。
 */
export interface PoolBreakerState {
  /** 全池连续失败次数（任一代理成功即清零） */
  consecutivePoolFailures: number;
  /** 熔断打开至（epoch ms）；0 = 关闭 */
  openUntil: number;
}

export const POOL_BREAKER_THRESHOLD = 3;
export const POOL_BREAKER_COOLDOWN_MS = 10 * 60_000;

export const CLOSED_POOL_BREAKER: PoolBreakerState = { consecutivePoolFailures: 0, openUntil: 0 };

/** 熔断器当前是否处于打开（跳过代理池）状态 */
export function poolBreakerIsOpen(b: PoolBreakerState, now: number): boolean {
  return b.openUntil > now;
}

/** 一次「全池尝试」的结果回写：成功清零；失败累计并在达到阈值时打开熔断。
 * v1.9.1：platformTlsSignature=true（本次全池失败的错误中含 workerd 平台签名
 * 「TLS Handshake Failed」，即 startTls 无法在代理隧道上完成 TLS 升级）时
 * 立即打开熔断，不等阈值 —— 冷 isolate 的首个请求就跳过代理池，免烧 2–5 秒。 */
export function poolBreakerOnOutcome(
  b: PoolBreakerState,
  poolSucceeded: boolean,
  now: number,
  platformTlsSignature = false,
): PoolBreakerState {
  if (poolSucceeded) return CLOSED_POOL_BREAKER;
  if (platformTlsSignature) {
    return { consecutivePoolFailures: Math.max(b.consecutivePoolFailures + 1, POOL_BREAKER_THRESHOLD), openUntil: now + POOL_BREAKER_COOLDOWN_MS };
  }
  const failures = b.consecutivePoolFailures + 1;
  const openUntil = failures >= POOL_BREAKER_THRESHOLD ? now + POOL_BREAKER_COOLDOWN_MS : b.openUntil;
  return { consecutivePoolFailures: failures, openUntil };
}

/** 错误聚合串中是否命中 workerd 平台级代理隧道 TLS 失败签名 */
export const PLATFORM_TLS_FAILURE_SIGNATURE = /TLS Handshake Failed/i;

// 熔断运行时状态（isolate 内存 + KV 快照同步，跨 isolate 共享；详见下方 KV 快照段）
let breakerMem: PoolBreakerState = CLOSED_POOL_BREAKER;

/** 面板/管理端读取熔断器当前状态（/admin/health 暴露） */
export function poolBreakerSnapshot(): PoolBreakerState & { open: boolean } {
  return { ...breakerMem, open: poolBreakerIsOpen(breakerMem, Date.now()) };
}

/** 全池成功：清零并标记待刷盘 */
export function breakerOnPoolSuccess(): void {
  breakerMem = CLOSED_POOL_BREAKER;
  touchPending();
}

/** 全池失败：按结果/签名更新熔断器；返回是否处于「打开」状态 */
export function breakerOnPoolFailure(platformTlsSignature: boolean): boolean {
  breakerMem = poolBreakerOnOutcome(breakerMem, false, Date.now(), platformTlsSignature);
  return poolBreakerIsOpen(breakerMem, Date.now());
}

// ---------- 直连（无代理池）模式退避重试（v1.6.0） ----------

/**
 * 直连模式下是否值得单次重试：
 * 429 / 408 / 425 / 5xx 属于瞬时故障（上游限流或抖动），退避后重发一次价值高；
 * 2xx / 3xx 成功与其余 4xx（请求级错误，重发必然同样失败）不重试。
 */
export function shouldRetryDirect(status: number): boolean {
  return status === 429 || status === 408 || status === 425 || status >= 500;
}

/**
 * 重试退避时长（毫秒）：优先遵循上游 Retry-After 头（秒数或 HTTP 日期格式），
 * 钳位 250ms–4s（避免请求长时间挂起），无头时默认 500ms。
 */
export function retryDelayMs(retryAfterHeader: string | null | undefined, nowMs: number = Date.now()): number {
  const clamp = (v: number) => Math.min(Math.max(v, 250), 4_000);
  if (typeof retryAfterHeader === "string" && retryAfterHeader.trim()) {
    const sec = Number(retryAfterHeader.trim());
    if (Number.isFinite(sec) && sec >= 0) return clamp(sec * 1000);
    const date = Date.parse(retryAfterHeader);
    if (Number.isFinite(date)) return clamp(date - nowMs);
  }
  return 500;
}

// ---------- isolate 全局健康存储 + KV 快照同步 ----------

const HEALTH_KEY = "proxy_health";
const HEALTH_FLUSH_MS = 20_000;
const HEALTH_TTL_SEC = 24 * 3600; // 快照新鲜度：超过一天的记录直接丢弃
const HEALTH_MAX_NODES = 600; // v1.6.3：200 → 600 —— 531 节点订阅池此前只加载
// 前 200 个（28 个双主机验证活节点每个 isolate 仅见 ~10 个，轮转池被砍半再
// 砍半）；600 覆盖全池，内存成本 ~100KB（Workers 128MB 限额内可忽略）。

const healthMem = new Map<string, ProxyHealth>();
let healthLoaded = false;
let oldestPendingAt = 0;
let healthFlushing = false;

function touchPending(): void {
  if (oldestPendingAt === 0) oldestPendingAt = Date.now();
}

function healthOf(uri: string): ProxyHealth {
  let h = healthMem.get(uri);
  if (!h) {
    h = emptyHealth();
    healthMem.set(uri, h);
  }
  return h;
}

/** 记录一次成功（真实请求或测速）：清冷却、清连败、更新延迟 EMA、加入粘性池 */
export function recordProxySuccess(uri: string, ms: number, now: number = Date.now()): void {
  if (!uri) return;
  const h = healthOf(uri);
  h.success += 1;
  h.consec_fail = 0;
  h.last_ms = Math.max(0, ms);
  h.avg_ms = h.avg_ms > 0 ? h.avg_ms * 0.7 + Math.max(0, ms) * 0.3 : Math.max(0, ms);
  h.last_success_at = Math.floor(now / 1000);
  h.cooldown_until = 0;
  h.last_error = "";
  h.sticky = true;
  decayCounters(h);
  touchPending();
}

/** 记录一次失败（连接/握手/5xx）：连败冷却 + 驱逐粘性（对齐原项目 recordProxyAttempt） */
export function recordProxyFailure(uri: string, error: string, now: number = Date.now()): void {
  if (!uri) return;
  const h = healthOf(uri);
  h.fail += 1;
  h.consec_fail += 1;
  h.last_fail_at = Math.floor(now / 1000);
  h.last_error = String(error).slice(0, 200);
  h.cooldown_until = Math.floor(now / 1000) + cooldownSecondsFor(h.consec_fail);
  h.sticky = false;
  decayCounters(h);
  touchPending();
}

/** 记录 429 限流：驱逐粘性 + 计次降权 + 节点冷却。
 *  v2.5.0：冷却时长优先用上游明示的窗口重试秒数（+2s 缓冲，clamp 5~120s）——
 *  单 IP 滑动窗口限流 ~20s 即恢复，固定 30s 冷却会白白浪费节点；
 *  无提示时维持 30s 默认。 */
export function recordProxyRateLimit(uri: string, now: number = Date.now(), retryAfterSecs?: number): void {
  if (!uri) return;
  const h = healthOf(uri);
  h.rate_limit_count += 1;
  h.last_error = retryAfterSecs != null ? "429 Rate Limit (retry in " + Math.ceil(retryAfterSecs) + "s)" : "429 Rate Limit";
  h.last_fail_at = Math.floor(now / 1000);
  const cooldown = retryAfterSecs != null ? Math.min(Math.max(Math.ceil(retryAfterSecs) + 2, 5), 120) : RATE_LIMIT_COOLDOWN_SEC;
  h.cooldown_until = Math.floor(now / 1000) + cooldown;
  h.sticky = false;
  touchPending();
}

export function proxyHealth(uri: string): ProxyHealth | undefined {
  return healthMem.get(uri);
}

export function healthMapSnapshot(): Map<string, ProxyHealth> {
  return healthMem;
}

/** 面板用：健康表（raw -> health）转普通对象 */
export function allHealthRecords(): Record<string, ProxyHealth> {
  const out: Record<string, ProxyHealth> = {};
  for (const [k, v] of healthMem.entries()) out[k] = structuredClone(v);
  return out;
}

// ---------- KV 快照 ----------

interface HealthSnapshot {
  at: number; // epoch 秒
  nodes: Record<string, ProxyHealth>;
  /** v1.9.1：代理池熔断器（跨 isolate 共享；缺失时视为关闭，首个请求重新学习） */
  breaker?: PoolBreakerState;
}

/** 冷启动恢复：从 KV 读取快照（24 小时内有效；内存已有数据时以内存为准） */
export async function ensureHealthLoaded(env: Env): Promise<void> {
  if (healthLoaded) return;
  healthLoaded = true;
  try {
    const snap = (await env.VPROXY_KV.get(HEALTH_KEY, "json")) as HealthSnapshot | null;
    if (!snap || typeof snap !== "object" || !snap.nodes) return;
    const now = Date.now();
    if (snap.at && now / 1000 - snap.at > HEALTH_TTL_SEC) return;
    for (const [uri, h] of Object.entries(snap.nodes)) {
      if (!h || typeof h !== "object" || healthMem.has(uri)) continue;
      if (healthMem.size >= HEALTH_MAX_NODES) break;
      healthMem.set(uri, { ...emptyHealth(), ...h });
    }
    const b = snap.breaker;
    if (b && typeof b === "object" && Number.isFinite(b.consecutivePoolFailures) && Number.isFinite(b.openUntil)) {
      // v1.9.1：恢复其他 isolate 写入的熔断状态（冷 isolate 首个请求即跳过死代理池）
      breakerMem = { consecutivePoolFailures: Math.max(0, b.consecutivePoolFailures), openUntil: Math.max(0, b.openUntil) };
    }
  } catch {
    // KV 读失败不影响请求
  }
}

/** 定期刷盘：距首条待刷记录超过间隔才真正写 KV（waitUntil 调用） */
export async function flushHealthIfDue(env: Env): Promise<void> {
  if (healthMem.size === 0 || oldestPendingAt === 0) return;
  if (Date.now() - oldestPendingAt < HEALTH_FLUSH_MS) return;
  await flushHealthNow(env);
}

/** 立即刷盘（管理端点与 schedule 场景用） */
export async function flushHealthNow(env: Env): Promise<void> {
  if (healthFlushing) return;
  healthFlushing = true;
  try {
    const breakerClosed = breakerMem.consecutivePoolFailures === 0 && breakerMem.openUntil === 0;
    if (healthMem.size === 0 && breakerClosed) return;
    const snap: HealthSnapshot = { at: Math.floor(Date.now() / 1000), nodes: {}, breaker: breakerMem };
    for (const [uri, h] of healthMem.entries()) snap.nodes[uri] = h;
    await env.VPROXY_KV.put(HEALTH_KEY, JSON.stringify(snap));
    oldestPendingAt = 0;
  } catch {
    // 写失败保留内存，下个窗口重试
  } finally {
    healthFlushing = false;
  }
}

/** 清空健康度（管理端点用） */
export async function resetHealth(env: Env): Promise<void> {
  healthMem.clear();
  oldestPendingAt = 0;
  try {
    await env.VPROXY_KV.delete(HEALTH_KEY);
  } catch {
    // ignore
  }
}
