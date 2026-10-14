// v1.4.0 回归：整轮 429 重试（多轮竞速）—— 纯逻辑部分。
// raceUpstream/rotateUpstream 的运行时编排依赖 cloudflare:sockets（Workers 专属），
// 此处验证其决策依据的纯函数与状态联动：配置钳位 / 轮间等待 / 429 冷却 × 重选联动 /
// 跨轮预算。模块内存健康表（recordProxyRateLimit 直写）使用 v140- 前缀独占 URI，
// 避免与同文件其他用例互相污染。
import { test } from "node:test";
import assert from "node:assert/strict";
import {
  DEFAULT_RACING,
  RATE_LIMIT_ROUND_WAIT_MAX_MS,
  TOTAL_ATTEMPT_CAP,
  classify429Body,
  emptyHealth,
  healthMapSnapshot,
  healthScore,
  isCooling,
  proxyHealth,
  rateLimitRoundWaitMs,
  recordProxyRateLimit,
  sanitizeRacingConfig,
  selectCandidates,
  type ProxyHealth,
  type RacingConfig,
} from "../src/racing.ts";

const NOW = Math.floor(Date.now() / 1000) * 1000; // 整秒对齐，避免边界抖动

function mkHealth(p: Partial<ProxyHealth>): ProxyHealth {
  return { ...emptyHealth(), ...p };
}

function rc(p: Partial<RacingConfig> = {}): RacingConfig {
  return { ...DEFAULT_RACING, ...p };
}

/** 合并模块内存健康快照与本地构造记录（不污染模块内存） */
function mergedHealth(local: Map<string, ProxyHealth>): Map<string, ProxyHealth> {
  const merged = new Map(healthMapSnapshot());
  for (const [k, v] of local) merged.set(k, v);
  return merged;
}

// ---------- sanitizeRacingConfig：rate_limit_retries ----------

test("v1.4.0 sanitize: rate_limit_retries 默认 1；钳位 0~3；非法值回默认", () => {
  assert.equal(DEFAULT_RACING.rate_limit_retries, 1);
  assert.equal(sanitizeRacingConfig(undefined).rate_limit_retries, 1);

  assert.equal(sanitizeRacingConfig({ rate_limit_retries: 0 }).rate_limit_retries, 0); // 0 合法（关闭整轮重试）
  assert.equal(sanitizeRacingConfig({ rate_limit_retries: 99 }).rate_limit_retries, 3);
  assert.equal(sanitizeRacingConfig({ rate_limit_retries: -5 }).rate_limit_retries, 0);
  assert.equal(sanitizeRacingConfig({ rate_limit_retries: "abc" }).rate_limit_retries, 1); // 非数字回默认
});

// ---------- rateLimitRoundWaitMs：轮间等待 ----------

test("v1.4.0 wait: 无冷却候选（有活节点可换）→ 0ms 立即换点再战", () => {
  assert.equal(rateLimitRoundWaitMs([], NOW), 0);
  assert.equal(rateLimitRoundWaitMs([0, 0], NOW), 0); // cooldown_until=0（从未冷却）
  assert.equal(rateLimitRoundWaitMs([NaN, Infinity, -1], NOW), 0); // 非法输入不计入
});

test("v1.4.0 wait: 冷却已过期 → 0ms", () => {
  assert.equal(rateLimitRoundWaitMs([Math.floor(NOW / 1000) - 10], NOW), 0);
});

test("v1.4.0 wait: 等到最早恢复者（多候选取最小）", () => {
  const a = Math.floor(NOW / 1000) + 30; // 30s 后恢复
  const b = Math.floor(NOW / 1000) + 5; // 5s 后恢复（最早）
  const wait = rateLimitRoundWaitMs([a, b], NOW);
  assert.ok(Math.abs(wait - 5000) < 25, "应等待约 5000ms，实际 " + wait);
});

test("v1.4.0 wait: 下限 250ms（距恢复只剩一瞬也至少等一小段，避开窗口边界）", () => {
  const soon = Math.floor(NOW / 1000) + 1; // NOW+1s 恢复
  assert.equal(rateLimitRoundWaitMs([soon], NOW + 950), 250); // 距恢复仅 50ms → 钳到 250
});

test("v1.4.0 wait: 上限 8s（最早恢复很远时不再干等）", () => {
  const far = Math.floor(NOW / 1000) + 120; // retryAfter 钳位上限 120s
  assert.equal(rateLimitRoundWaitMs([far], NOW), RATE_LIMIT_ROUND_WAIT_MAX_MS);
});

// ---------- 429 冷却 × 重选联动（多轮换 IP 的根基） ----------

test("v1.4.0 联动: 撞 429 后节点冷却 → selectCandidates 让位，窗口恢复后回归", () => {
  const limited = "socks5://v140-limited:1080";
  const fresh = "socks5://v140-fresh:1080";
  const healthy = "socks5://v140-healthy:1080";

  // 本地构造：healthy 满血（不写模块内存）
  const local = new Map<string, ProxyHealth>([
    [healthy, mkHealth({ success: 50, fail: 0, avg_ms: 150, last_ms: 150, last_success_at: Math.floor(NOW / 1000), sticky: true })],
  ]);

  // limited 撞 429（上游明示 20.46s 窗口）→ 模块内存：冷却 ceil(20.46)+2 = 23s
  recordProxyRateLimit(limited, NOW, 20.464172338);
  const h = proxyHealth(limited)!;
  assert.equal(h.cooldown_until - Math.floor(NOW / 1000), 23);
  assert.ok(isCooling(h, NOW));
  assert.equal(healthScore(h, NOW), 0, "冷却中 0 分");

  const entries = [limited, healthy, fresh].map((raw) => ({ raw }));
  const cfg = rc({ top_k: 3, max_attempts: 8 });

  // 第一轮选点：limited 冷却让位（fresh 探索名额顶上）
  const round1 = selectCandidates(entries, mergedHealth(local), cfg, NOW).map((e) => e.raw);
  assert.ok(!round1.includes(limited), "冷却节点应让位，实际: " + round1.join(","));
  assert.ok(round1.includes(healthy) && round1.includes(fresh));

  // 23s 后窗口恢复：limited 回归（带 429 计次惩罚 -5，排位下降但仍可用）
  const later = NOW + 24_000;
  const round2 = selectCandidates(entries, mergedHealth(local), cfg, later).map((e) => e.raw);
  assert.ok(round2.includes(limited), "恢复后应回归候选");
  assert.ok(round2.indexOf(healthy) < round2.indexOf(limited), "健康满血节点仍优先于 429 惩罚节点");
});

test("v1.4.0 联动: 全池撞 429 冷却 → selectCandidates 兜底按「最早恢复」排序", () => {
  const a = "socks5://v140-pool-a:1080";
  const b = "socks5://v140-pool-b:1080";
  const now = Date.now();
  recordProxyRateLimit(a, now, 20); // 冷却 ~22s
  recordProxyRateLimit(b, now, 5); // 冷却 ~7s（最早恢复）
  const entries = [a, b].map((raw) => ({ raw }));

  const picked = selectCandidates(entries, mergedHealth(new Map()), rc({ top_k: 4, max_attempts: 8 }), now);
  assert.equal(picked.length, 2, "全冷却时兜底返回全部候选");
  assert.equal(picked[0].raw, b, "最早恢复的 b 应排首位（等待后优先重试对象）");
});

// ---------- 跨轮预算保护 ----------

test("v1.4.0 预算: TOTAL_ATTEMPT_CAP=40，极端配置按剩余预算收敛每轮 max_attempts", () => {
  assert.equal(TOTAL_ATTEMPT_CAP, 40); // 免费计划 50 子请求/请求，留直连+KV 余量
  const cfg = sanitizeRacingConfig({ max_attempts: 20, rate_limit_retries: 3 });
  assert.equal(cfg.max_attempts, 20);
  assert.equal(cfg.rate_limit_retries, 3);
  // raceUpstream 每轮的预算计算逻辑（镜像实现）：
  const budgetFor = (used: number) => Math.min(cfg.max_attempts, TOTAL_ATTEMPT_CAP - used);
  assert.equal(budgetFor(0), 20);
  assert.equal(budgetFor(20), 20);
  assert.equal(budgetFor(36), 4); // 收敛
  assert.ok(budgetFor(40) <= 0, "预算耗尽必须停止发起新轮");
});

// ---------- 429 分类在整轮重试语境下的关键回归 ----------

test("v1.4.0 分类: project 级每日配额硬失败（不进整轮重试）；per-ip 带窗口秒数（等待依据）", () => {
  const perDay = classify429Body(
    JSON.stringify({ error: { code: 429, message: "quota exceeded", details: [{ violations: [{ quotaId: "GenerateRequestsPerDayPerProjectPerModel-FreeTier" }] }] } }),
  );
  assert.equal(perDay.level, "project");

  const perIp = classify429Body("Quota exceeded for metric: generate_content_free_tier_requests, limit: 20. Please retry in 20.46s.");
  assert.equal(perIp.level, "per-ip");
  assert.ok(perIp.retryAfterSecs != null); // 整轮等待时长的依据
});
