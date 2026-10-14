// racefetch.ts —— 出站请求编排：单代理隧道 + 对冲竞速 + 健康接力 + 直连兜底。
//
// 借鉴 cf-vproxy proxyfetch.ts（隧道层）与 racefetch.ts（编排层）的设计，重新实现：
//   - viaProxy(p, url, init, signal)：暖连接优先（跳过 TCP+代理+TLS 全部握手），
//     冷路径 = tcpConnect → 代理握手 → TLS 1.3（纯 JS，防 MITM）→ HTTP/1.1；
//     「请求已完整写出但未收到任何响应字节」的暖连接失败自动冷路径重试一次
//     （浏览器同款安全重试规则：请求大概率未被上游处理）；
//   - 竞速编排（移植 vertex-master race_engine.go 语义）：
//       * 首个候选立即发出，之后每隔 hedge_delay_ms（或动态平均延迟）追加候选，
//         在飞数不超过 max_concurrent；任一候选拿到可用响应即胜出并中止其余候选；
//       * 429 按体分类：单 IP 滑动窗口限流 → 冷却节点 + 接力；项目级每日配额（PerDay）
//         → 请求级硬错误立即透传；
//       * v1.4.0 整轮 429 重试：一轮候选全部撞 per-IP 窗口限流后，等待最早恢复窗口
//         （钳 250ms~8s）并重新 selectCandidates 再战一轮（被冷却节点自然让位 →
//         天然换 IP 轮转）；轮次由 rate_limit_retries 控制（0~3），跨轮总尝试数
//         钳 TOTAL_ATTEMPT_CAP=40 保护免费计划 50 子请求/请求限额；全轮耗尽仍 429
//         → PoolRateLimitedError → 直连兜底（独立出口 IP，不计熔断）；
//       * 5xx/连接失败 → 记失败冷却 + 极速接力；其它 4xx → 请求级硬错误（不惩罚节点）；
//       * v2.5.3 伪造密钥错误接力验证：可疑 4xx 首中记失败并接力，第二个节点同类
//         错误才判真实；
//   - dispatchUpstream：无池/熔断打开 → 直连（429/408/425/5xx 退避重试一次）；
//     竞速关闭或单节点 → 健康排序轮换；竞速开启 → 对冲竞速；全池失败 → 熔断计数
//     + 直连兜底。
//
// ⚠️ import 链含 "cloudflare:sockets"（经 sockets.ts），只能在 Workers 运行时加载。
import type { Env } from "../config.ts";
import {
  DEFAULT_RACING,
  PLATFORM_TLS_FAILURE_SIGNATURE,
  breakerOnPoolFailure,
  breakerOnPoolSuccess,
  poolBreakerIsOpen,
  poolBreakerSnapshot,
  retryDelayMs,
  shouldRetryDirect,
  type RacingConfig,
} from "../racing.ts";
import {
  averageLatency,
  ensureHealthLoaded,
  flushHealthIfDue,
  flushHealthNow,
  healthMapSnapshot,
  healthScore,
  isCooling,
  classify429Body,
  isSuspiciousAuthError,
  rateLimitRoundWaitMs,
  TOTAL_ATTEMPT_CAP,
  type Quota429Class,
  proxyHealth,
  recordProxyFailure,
  recordProxyRateLimit,
  recordProxySuccess,
  selectCandidates,
} from "../racing.ts";
import { ByteBufReader } from "./byteio.ts";
import { acquireWithSweep as acquireWarm, discard, isWarm, release, type PooledConn } from "./connpool.ts";
import { proxyHandshake, type ProxyEndpoint, type Tunnel } from "./handshake.ts";
import {
  determineBodyShape,
  isBodylessStatus,
  makeBodyStream,
  parseResponseHeadBlock,
  readFullBody,
  responseAllowsReuse,
  writeTunnelRequest,
  type BodyShape,
  type ResponseHead,
} from "./httpio.ts";
import { parseProxyUrl, ProxyPool, type ProxyEntry } from "./pool.ts";
import { closeQuietly, sleep, tcpConnect, withTimeout } from "./sockets.ts";
import { Tls13Client, invalidateSessionTicket } from "./tls13.ts";

export type { ProxyPool, ProxyEntry };

const CONNECT_TIMEOUT_MS = 15_000;
const HANDSHAKE_TIMEOUT_MS = 15_000;
const TLS13_HANDSHAKE_TIMEOUT_MS = 12_000;
// 响应头等待上限：代理静默黑洞（TCP 活着但不转发）在 120s 内必失败并进入
// 健康度/冷却，而不是把请求与并发闸门名额永久占死。
const RESPONSE_HEAD_TIMEOUT_MS = 120_000;
const WRITE_TIMEOUT_MS = 15_000;

const clampNum = (v: number, lo: number, hi: number) => Math.min(hi, Math.max(lo, v));

// ---------- 单代理隧道 ----------

/** 在已建立的 TLS 隧道上发送 HTTP/1.1 请求并构造 Response（响应体流透传） */
async function sendRequestOnConn(
  conn: TunnelConn,
  url: string,
  init: RequestInit,
  signal: AbortSignal | undefined,
  warm: boolean,
  markBytes?: () => void,
): Promise<Response> {
  const { tls, reader, writer } = conn;
  const onTlsAbort = () => tls.close();
  signal?.addEventListener("abort", onTlsAbort, { once: true });
  try {
    const headers: Record<string, string> = {};
    const inHeaders = new Headers(init.headers ?? {});
    inHeaders.forEach((v, k) => {
      const lk = k.toLowerCase();
      if (["host", "content-length", "transfer-encoding", "connection", "keep-alive"].includes(lk)) return;
      headers[k] = v;
    });

    const bodyBytes = await encodeBody(init.body);
    await withTimeout(
      writeTunnelRequest(writer, { method: init.method ?? "GET", url, headers, body: bodyBytes }),
      WRITE_TIMEOUT_MS,
      "write request timeout",
    );
    let head: ResponseHead;
    for (;;) {
      const headBlock = await withTimeout(
        reader.readHeaderBlock(),
        RESPONSE_HEAD_TIMEOUT_MS,
        "no response head within " + RESPONSE_HEAD_TIMEOUT_MS / 1000 + "s (dead tunnel?)",
      );
      markBytes?.();
      head = parseResponseHeadBlock(headBlock);
      if (head.status < 100 || head.status >= 200) break; // 1xx 过渡响应跳过
    }
    const method = init.method ?? "GET";
    const shape: BodyShape = isBodylessStatus(method, head.status)
      ? { chunked: false, contentLength: 0 }
      : determineBodyShape(head.headers);
    const allowsReuse = responseAllowsReuse(head) && (shape.chunked || shape.contentLength !== null);
    const outHeaders = new Headers();
    for (const [k, v] of Object.entries(head.headers)) {
      if (["content-length", "transfer-encoding", "connection", "keep-alive", "proxy-connection"].includes(k)) continue;
      outHeaders.set(k, v);
    }
    outHeaders.set("x-vproxy-conn", warm ? "warm" : "cold");

    let settled = false; // finish/cancel 双重触发防护
    const cleanup = (bodyReusable: boolean) => {
      if (settled) return;
      settled = true;
      signal?.removeEventListener("abort", onTlsAbort);
      const reusable = bodyReusable && allowsReuse && !signal?.aborted;
      release(conn, reusable);
    };
    const body = makeBodyStream(reader, shape, cleanup);
    return new Response(body, { status: head.status, statusText: head.reason, headers: outHeaders });
  } catch (err) {
    signal?.removeEventListener("abort", onTlsAbort);
    discard(conn);
    throw err;
  }
}

/** 隧道连接：TLS 客户端 + 缓冲读取器 + 写适配器（连接池持有） */
interface TunnelConn extends PooledConn {
  tls: Tls13Client;
  reader: ByteBufReader;
  writer: { write(chunk: Uint8Array): Promise<void> };
}

async function encodeBody(body: BodyInit | null | undefined): Promise<Uint8Array> {
  if (!body) return new Uint8Array(0);
  if (typeof body === "string") return new TextEncoder().encode(body);
  if (body instanceof Uint8Array) return body;
  if (body instanceof ArrayBuffer) return new Uint8Array(body);
  return new Uint8Array(await new Response(body).arrayBuffer());
}

/** 经单个代理发起 HTTPS 请求；signal 中止时立即关闭底层 socket（竞速败者清理） */
export async function viaProxy(p: ProxyEntry, url: string, init: RequestInit, signal?: AbortSignal): Promise<Response> {
  if (signal?.aborted) throw new Error("attempt aborted before connect");
  const u = new URL(url);
  if (u.protocol !== "https:") throw new Error("viaProxy: only https upstream is supported");
  const targetHost = u.hostname;
  const targetPort = Number(u.port) || 443;
  const target = targetHost + ":" + targetPort;

  // ---- 暖连接优先 ----（池内连接均为 TunnelConn，取出时还原具体类型）
  const warm = acquireWarm(p.raw, target) as TunnelConn | null;
  if (warm && !warm.tls.isClosed) {
    let sawResponseBytes = false;
    const markBytes = () => {
      sawResponseBytes = true;
    };
    try {
      return await sendRequestOnConn(warm, url, init, signal, true, markBytes);
    } catch (err) {
      discard(warm);
      // 安全重试：请求已完整写出但未收到任何响应字节 → 重试不产生重复副作用
      if (sawResponseBytes || warm.reader.buffered > 0 || signal?.aborted) throw err;
    }
  } else if (warm) {
    discard(warm);
  }
  if (signal?.aborted) throw new Error("attempt aborted before connect");
  return await viaProxyCold(p, url, init, targetHost, targetPort, target, signal);
}

/** 冷路径：新建 TCP + 代理握手 + TLS 1.3 握手，成功后交给连接池管理 */
async function viaProxyCold(
  p: ProxyEntry,
  url: string,
  init: RequestInit,
  targetHost: string,
  targetPort: number,
  target: string,
  signal?: AbortSignal,
): Promise<Response> {
  const sock = tcpConnect(p.host, p.port);
  const onAbort = () => closeQuietly(sock);
  signal?.addEventListener("abort", onAbort, { once: true });
  try {
    await withTimeout(sock.opened, CONNECT_TIMEOUT_MS, "proxy connect timeout (" + p.host + ":" + p.port + ")");

    let tunnel: Tunnel;
    try {
      const reader = new ByteBufReader(sock.readable.getReader());
      const writer = sock.writable.getWriter();
      tunnel = { reader, writer };
      await withTimeout(proxyHandshake(tunnel, p, targetHost, targetPort), HANDSHAKE_TIMEOUT_MS, p.kind + " handshake timeout");
    } catch (err) {
      closeQuietly(sock); // 握手失败/超时：立即释放底层 socket
      throw err;
    }

    // startTls 无法在承载过代理握手的 socket 上完成升级（workerd 平台限制），
    // 因此在隧道字节流上直接运行纯 JS TLS 1.3 客户端（证书链完整校验，MITM 拒绝）。
    const tls = new Tls13Client(tunnel.reader, tunnel.writer, {
      serverName: targetHost,
      onClose: () => closeQuietly(sock),
    });
    try {
      await withTimeout(tls.handshake(), TLS13_HANDSHAKE_TIMEOUT_MS, "tls13 handshake timeout (" + p.host + ":" + p.port + ")");
    } catch (err) {
      // PSK 握手遭遇 alert/解密/Finished 校验失败 → 票据大概率失效，清缓存回退完整握手
      if (tls.pskOffered && err instanceof Error && /alert|bad_record_mac|Finished verify|decrypt/i.test(err.message)) {
        invalidateSessionTicket(targetHost);
      }
      closeQuietly(sock);
      throw err;
    }
    const now = Date.now();
    const conn: TunnelConn = {
      proxyRaw: p.raw,
      target,
      tls,
      reader: tunnel.reader,
      writer: tunnel.writer,
      created: now,
      lastUsed: now,
      uses: 0,
      close: () => tls.close(),
    };
    return await sendRequestOnConn(conn, url, init, signal, false);
  } finally {
    signal?.removeEventListener("abort", onAbort);
  }
}

/** 管理端点/健康巡检用：测试单个代理连通性（对 Gemini API 发起 GET 探测） */
export async function testProxy(
  proxyUrl: string,
  timeoutMs = 10_000,
): Promise<{ ok: boolean; latency_ms: number; error?: string; body_bytes?: number }> {
  const p = parseProxyUrl(proxyUrl);
  if (!p) return { ok: false, latency_ms: 0, error: "invalid proxy url" };
  const started = Date.now();
  try {
    const resp = await viaProxy(
      p,
      "https://generativelanguage.googleapis.com/v1beta/models?pageSize=1",
      { method: "GET" },
      AbortSignal.timeout(timeoutMs),
    );
    const body = await resp.text(); // 完整消费 → 连接回池
    return {
      ok: resp.status < 500,
      latency_ms: Date.now() - started,
      error: resp.status >= 500 ? "HTTP " + resp.status : undefined,
      body_bytes: body.length,
    };
  } catch (err) {
    return { ok: false, latency_ms: Date.now() - started, error: err instanceof Error ? err.message : String(err) };
  }
}

// ---------- 竞速编排 ----------

export interface UpstreamResult {
  response: Response;
  via: string;
}

/** 读入 429 响应体并分类（per-ip 窗口限流 / project 每日配额），返回重建后的 Response */
async function absorb429(resp: Response): Promise<{ resp: Response; cls: Quota429Class }> {
  const cl = Number(resp.headers.get("content-length") ?? "0");
  if (Number.isFinite(cl) && cl > 65_536) return { resp, cls: { level: "per-ip" } };
  let body = "";
  try {
    body = await resp.text();
  } catch {
    return { resp, cls: { level: "per-ip" } };
  }
  const hdrRetry = Number(resp.headers.get("retry-after"));
  const cls = classify429Body(body);
  if (cls.level === "per-ip" && cls.retryAfterSecs == null && Number.isFinite(hdrRetry) && hdrRetry > 0 && hdrRetry <= 300) {
    cls.retryAfterSecs = hdrRetry;
  }
  return { resp: new Response(body, { status: resp.status, statusText: resp.statusText, headers: resp.headers }), cls };
}

/** 读入非 429 的 4xx 响应体，判断是否为代理伪造的密钥级错误（v2.5.3 语义） */
async function absorb4xx(resp: Response): Promise<{ resp: Response; suspect: boolean }> {
  const cl = Number(resp.headers.get("content-length") ?? "0");
  if (Number.isFinite(cl) && cl > 65_536) return { resp, suspect: false };
  let body = "";
  try {
    body = await resp.text();
  } catch {
    return { resp, suspect: false };
  }
  const suspect = isSuspiciousAuthError(body);
  return { resp: new Response(body, { status: resp.status, statusText: resp.statusText, headers: resp.headers }), suspect };
}

function warmFirst<T extends { raw: string }>(cands: T[]): T[] {
  const warm: T[] = [];
  const cold: T[] = [];
  for (const c of cands) (isWarm(c.raw) ? warm : cold).push(c);
  return warm.length > 0 ? [...warm, ...cold] : cands;
}

/** 健康分排序后的候选列表（冷却节点垫底、暖连接优先）—— anon 中继等调用方复用 */
export function orderedCandidates(pool: ProxyPool, limit?: number): ProxyEntry[] {
  const order = healthyOrder(pool.entries);
  return limit != null ? order.slice(0, Math.max(1, limit)) : order;
}

function healthyOrder(entries: ProxyEntry[]): ProxyEntry[] {
  const now = Date.now();
  const ok = entries.filter((e) => !isCooling(proxyHealth(e.raw), now));
  const cooling = entries.filter((e) => isCooling(proxyHealth(e.raw), now));
  const score = (e: { raw: string }) => healthScore(proxyHealth(e.raw), now);
  ok.sort((a, b) => score(b) - score(a));
  return warmFirst([...ok, ...cooling]);
}

interface NodeAttempt {
  resp?: Response;
  error?: string;
  ms: number;
  q429?: Quota429Class;
  suspect4xx?: boolean;
}

async function attemptOnce(p: ProxyEntry, url: string, init: RequestInit): Promise<NodeAttempt> {
  const started = Date.now();
  try {
    const resp = await viaProxy(p, url, init);
    if (resp.status === 429) {
      const r = await absorb429(resp);
      return { resp: r.resp, ms: Date.now() - started, q429: r.cls };
    }
    if (resp.status >= 400 && resp.status < 500) {
      const r = await absorb4xx(resp);
      return { resp: r.resp, ms: Date.now() - started, suspect4xx: r.suspect };
    }
    return { resp, ms: Date.now() - started };
  } catch (err) {
    return { error: err instanceof Error ? err.message : String(err), ms: Date.now() - started };
  }
}

/** 节点内重试：网络层错误或 5xx 时同一节点立即重试一次；429 不重试（直接冷却接力） */
async function retrySameNodeOnce(
  p: ProxyEntry,
  url: string,
  init: RequestInit,
  errors: string[],
  reason: string,
): Promise<Response | null> {
  const second = await attemptOnce(p, url, init);
  if (second.resp && second.resp.status < 400) {
    recordProxySuccess(p.raw, second.ms);
    return second.resp;
  }
  if (second.resp) {
    if (second.resp.status === 429) recordProxyRateLimit(p.raw, Date.now(), second.q429?.retryAfterSecs);
    else recordProxyFailure(p.raw, "HTTP " + second.resp.status);
    errors.push("[" + p.raw + "] retry(" + reason + ") HTTP " + second.resp.status);
    void second.resp.body?.cancel().catch(() => {});
  } else {
    recordProxyFailure(p.raw, second.error ?? "retry failed");
    errors.push("[" + p.raw + "] retry(" + reason + ") " + (second.error ?? "failed"));
  }
  return null;
}

let rrCursor = 0;

/** 丢弃一个不再需要的响应体（连接回池/释放资源） */
const dropResp = (resp: Response | null | undefined): void => {
  if (resp) void resp.body?.cancel().catch(() => {});
};

/** v1.4.0：全轮 429（per-IP 窗口限流）耗尽 —— 携带最后的 429 响应交由
 *  dispatchUpstream 直连兜底（Workers 自身出口 IP = 独立限流窗口）；
 *  直连也失败时透传该响应。不计入池熔断：per-IP 窗口秒级即恢复，
 *  10 分钟熔断过重（那是给「池整体坏了」准备的）。 */
export class PoolRateLimitedError extends Error {
  readonly resp: Response;
  readonly via: string;
  constructor(resp: Response, via: string) {
    super("proxy pool rate-limited: all rounds exhausted with per-IP 429");
    this.name = "PoolRateLimitedError";
    this.resp = resp;
    this.via = via;
  }
}

/** v1.4.0：整轮 429 重试前的等待 —— 池内还有可选节点（非冷却/未测试）时
 *  立即换点再战（0ms）；全池冷却时等到最早恢复者（钳 250ms~8s）。
 *  v1.5.0 起导出：anon 候选接力维度复用同一等待语义。 */
export function poolRoundWaitMs(pool: ProxyPool): number {
  const now = Date.now();
  const cooldowns: number[] = [];
  for (const e of pool.entries) {
    const h = proxyHealth(e.raw);
    if (!h) return 0; // 存在未测试节点：立即可换
    if (!isCooling(h, now)) return 0; // 有活节点：立即换点
    cooldowns.push(h.cooldown_until);
  }
  return rateLimitRoundWaitMs(cooldowns, now);
}

/** 顺序模式：按健康分排序后轮换，每轮最多接力 3 个；v1.4.0 整轮 429 重试 ——
 *  一轮撞过 per-IP 429 且还有轮次/预算时，等待最早恢复窗口后重新排序再战
 *  （被冷却节点自然沉底 → 天然换 IP；单节点池即等窗口恢复后重试同节点）。
 *  全轮耗尽仍以 429 收尾 → 抛 PoolRateLimitedError（上层直连兜底）。 */
export async function rotateUpstream(
  url: string,
  init: RequestInit,
  pool: ProxyPool,
  nodeRetry = false,
  rateLimitRounds = 0,
): Promise<UpstreamResult> {
  let attemptsUsed = 0;
  for (let round = 0; ; round++) {
    const order = healthyOrder(pool.entries);
    const maxTry = Math.min(order.length, 3, TOTAL_ATTEMPT_CAP - attemptsUsed);
    const errors: string[] = [];
    const retried = new Set<string>();
    let seenSuspect = false;
    let roundSaw429 = false;
    let lastWas429 = false;
    let live429: { resp: Response; via: string } | null = null;
    const start = order.length > 0 ? rrCursor % order.length : 0;
    rrCursor = (rrCursor + 1) % Math.max(1, order.length);
    for (let i = 0; i < maxTry; i++) {
      const p = order[(start + i) % order.length];
      attemptsUsed++;
      const r = await attemptOnce(p, url, init);
      lastWas429 = false;
      if (r.resp && r.resp.status < 400) {
        recordProxySuccess(p.raw, r.ms);
        dropResp(live429?.resp);
        return { response: r.resp, via: p.raw };
      }
      if (r.resp && r.resp.status === 429) {
        // 仅项目级每日配额硬透传；单 IP 窗口限流冷却该节点后接力（每 IP 独立窗口）
        if (r.q429?.level === "project") {
          dropResp(live429?.resp);
          return { response: r.resp, via: p.raw };
        }
        recordProxyRateLimit(p.raw, Date.now(), r.q429?.retryAfterSecs);
        errors.push("[" + p.raw + "] 429 Rate Limit" + (r.q429?.retryAfterSecs != null ? " (retry " + Math.ceil(r.q429.retryAfterSecs) + "s)" : ""));
        roundSaw429 = true;
        lastWas429 = true;
        dropResp(live429?.resp);
        live429 = { resp: r.resp, via: p.raw }; // 保活：轮末 429 透传/兜底要用，不能 cancel
        continue;
      }
      if (r.resp && r.resp.status >= 500) {
        recordProxyFailure(p.raw, "HTTP " + r.resp.status);
        errors.push("[" + p.raw + "] HTTP " + r.resp.status);
        void r.resp.body?.cancel().catch(() => {});
        if (nodeRetry && !retried.has(p.raw) && attemptsUsed < TOTAL_ATTEMPT_CAP) {
          retried.add(p.raw);
          attemptsUsed++;
          const again = await retrySameNodeOnce(p, url, init, errors, "5xx");
          if (again) {
            dropResp(live429?.resp);
            return { response: again, via: p.raw };
          }
        }
        continue;
      }
      if (r.resp) {
        // 请求级 4xx：与代理无关，直接返回（不惩罚节点、不接力）；可疑伪造错误例外
        if (r.suspect4xx && !seenSuspect) {
          seenSuspect = true;
          recordProxyFailure(p.raw, "suspicious auth error (likely forged by proxy)");
          errors.push("[" + p.raw + "] suspicious auth error → 接力验证");
          void r.resp.body?.cancel().catch(() => {});
          continue;
        }
        dropResp(live429?.resp);
        return { response: r.resp, via: p.raw };
      }
      recordProxyFailure(p.raw, r.error ?? "network error");
      errors.push("[" + p.raw + "] " + (r.error ?? "network error"));
      if (nodeRetry && !retried.has(p.raw) && attemptsUsed < TOTAL_ATTEMPT_CAP) {
        retried.add(p.raw);
        attemptsUsed++;
        const again = await retrySameNodeOnce(p, url, init, errors, "network");
        if (again) {
          dropResp(live429?.resp);
          return { response: again, via: p.raw };
        }
      }
    }
    // 轮末：本轮撞过 per-IP 429 且还有轮次与预算 → 等窗口恢复再战（v1.4.0）
    if (roundSaw429 && live429 && round < rateLimitRounds && attemptsUsed < TOTAL_ATTEMPT_CAP) {
      const wait = poolRoundWaitMs(pool);
      await sleep(wait > 0 ? wait + Math.random() * 250 : 0); // 抖动错峰并发重试端群
      continue;
    }
    if (live429 && lastWas429) {
      // 全轮耗尽仍以 429 收尾：交由上层直连兜底（独立出口 IP）
      throw new PoolRateLimitedError(live429.resp, live429.via);
    }
    throw new Error("all proxy attempts failed: " + errors.join(" | "));
  }
}

type AttemptKind = "ok" | "ratelimit" | "retryable" | "hard" | "hard-suspect" | "aborted";

interface AttemptResult {
  kind: AttemptKind;
  resp?: Response;
  ms: number;
  error?: string;
  retryAfterSecs?: number;
}

function classifyStatus(status: number): AttemptKind {
  if (status < 400) return "ok";
  if (status === 429) return "ratelimit";
  if (status >= 500 || status === 408 || status === 425) return "retryable";
  return "hard";
}

async function runAttempt(p: ProxyEntry, url: string, init: RequestInit, signal: AbortSignal): Promise<AttemptResult> {
  const started = Date.now();
  try {
    const resp = await viaProxy(p, url, init, signal);
    const ms = Date.now() - started;
    if (resp.status === 429) {
      const r = await absorb429(resp);
      if (r.cls.level === "project") return { kind: "hard", resp: r.resp, ms };
      return { kind: "ratelimit", resp: r.resp, ms, retryAfterSecs: r.cls.retryAfterSecs };
    }
    const kind = classifyStatus(resp.status);
    if (kind === "ok") {
      recordProxySuccess(p.raw, ms);
      return { kind, resp, ms };
    }
    if (kind === "hard") {
      const r = await absorb4xx(resp);
      if (r.suspect) return { kind: "hard-suspect", resp: r.resp, ms };
      return { kind, resp: r.resp, ms };
    }
    if (kind === "retryable") recordProxyFailure(p.raw, "HTTP " + resp.status);
    return { kind, resp, ms };
  } catch (err) {
    const ms = Date.now() - started;
    const msg = err instanceof Error ? err.message : String(err);
    // 竞速败者被主动中止不是节点故障：不计失败、不进冷却
    if (signal.aborted) return { kind: "aborted", ms, error: msg };
    recordProxyFailure(p.raw, msg);
    return { kind: "retryable", ms, error: msg };
  }
}

/** 单轮竞速的耗尽详情（未分胜负时返回，供外层决定是否整轮 429 重试） */
interface RaceExhaustion {
  /** 轮内最后一个拿到的响应（429/5xx 等；纯网络失败时为 null） */
  lastResp: Response | null;
  lastVia: string;
  /** 最后一次尝试结论是否为 per-IP 429 */
  lastWas429: boolean;
  /** 本轮是否撞过 429（触发整轮重试的依据） */
  any429: boolean;
  errors: string[];
  attempts: number;
}

type RaceRoundOutcome = { ok: true; response: Response; via: string } | { ok: false; exhaust: RaceExhaustion };

/** 单轮对冲竞速：首胜即停、败者中止；健康度记录随竞速自动进行。
 *  轮末未分胜负时返回耗尽详情（不抛错），由外层 raceUpstream 决定重试或收尾。 */
function raceOnce(
  url: string,
  init: RequestInit,
  candidates: ProxyEntry[],
  racing: RacingConfig,
  hedge: number,
): Promise<RaceRoundOutcome> {
  return new Promise<RaceRoundOutcome>((resolve) => {
    let nextIdx = 0;
    let active = 0;
    let settled = false;
    let lastResp: Response | null = null;
    let lastVia = "race";
    let lastWas429 = false;
    let any429 = false;
    let attempts = 0;
    const errors: string[] = [];
    const controllers = new Map<string, AbortController>();
    let timer: ReturnType<typeof setTimeout> | undefined;
    let suspectHits = 0;

    const cleanupTimer = () => {
      if (timer) {
        clearTimeout(timer);
        timer = undefined;
      }
    };
    const finish = (fn: () => void) => {
      if (settled) return;
      settled = true;
      cleanupTimer();
      fn();
    };
    const abortLosers = () => {
      for (const [, ac] of controllers) {
        try {
          ac.abort();
        } catch {
          // ignore
        }
      }
      controllers.clear();
    };
    const launch = (): boolean => {
      if (settled) return false;
      if (nextIdx >= candidates.length) return false;
      if (active >= racing.max_concurrent) return false;
      const p = candidates[nextIdx++];
      const ac = new AbortController();
      controllers.set(p.raw, ac);
      active++;
      attempts++;
      runAttempt(p, url, init, ac.signal)
        .then((res) => onResult(p, res))
        .catch((err) => onResult(p, { kind: "retryable", ms: 0, error: err instanceof Error ? err.message : String(err) }));
      return true;
    };
    const scheduleHedge = () => {
      cleanupTimer();
      if (nextIdx < candidates.length && active < racing.max_concurrent) {
        timer = setTimeout(() => {
          if (launch()) scheduleHedge();
        }, hedge);
      }
    };

    const onResult = (p: ProxyEntry, res: AttemptResult) => {
      active--;
      controllers.delete(p.raw);
      if (res.kind === "aborted") {
        dropResp(res.resp);
        if (settled) return;
      }
      if (settled) {
        dropResp(res.resp);
        return;
      }
      if (res.error) errors.push("[" + p.raw + "] " + res.error);

      if (res.kind === "ok") {
        abortLosers();
        finish(() => resolve({ ok: true, response: res.resp!, via: p.raw }));
        return;
      }
      if (res.kind === "hard") {
        abortLosers();
        finish(() => resolve({ ok: true, response: res.resp!, via: p.raw }));
        return;
      }
      if (res.kind === "hard-suspect") {
        // 可疑密钥错误首中接力验证；第二个节点同类错误才判真实透传
        suspectHits++;
        if (suspectHits >= 2) {
          abortLosers();
          finish(() => resolve({ ok: true, response: res.resp!, via: p.raw }));
          return;
        }
        recordProxyFailure(p.raw, "suspicious auth error (likely forged by proxy)");
        errors.push("[" + p.raw + "] suspicious auth error → relay to verify");
        if (nextIdx >= candidates.length && active === 0) {
          const resp = res.resp;
          const via = p.raw;
          finish(() => resolve({ ok: true, response: resp!, via }));
          return;
        }
        dropResp(res.resp);
        launch();
        scheduleHedge();
        return;
      }

      // ratelimit / retryable：极速接力
      lastWas429 = res.kind === "ratelimit";
      if (res.kind === "ratelimit") {
        any429 = true;
        recordProxyRateLimit(p.raw, Date.now(), res.retryAfterSecs);
      }
      if (res.resp) {
        dropResp(lastResp);
        lastResp = res.resp;
        lastVia = p.raw;
      }
      launch();
      scheduleHedge();
      if (active === 0 && nextIdx >= candidates.length) {
        const exhaust: RaceExhaustion = { lastResp, lastVia, lastWas429, any429, errors: errors.slice(), attempts };
        finish(() => resolve({ ok: false, exhaust }));
      }
    };

    launch();
    scheduleHedge();
  });
}

/** 对冲竞速（v1.4.0 多轮化）：每轮重新 selectCandidates（上轮 429 冷却节点自然
 *  让位 → 天然换 IP 轮转），单轮首胜即停；一轮撞过 per-IP 429 且还有轮次/预算时，
 *  等待最早恢复窗口（池内仍有活节点则立即换点，钳 250ms~8s）再战一轮。
 *  全轮耗尽仍以 429 收尾 → 抛 PoolRateLimitedError（上层直连兜底）。 */
export async function raceUpstream(
  url: string,
  init: RequestInit,
  pool: ProxyPool,
  racing: RacingConfig,
  waitUntil: (p: Promise<unknown>) => void,
): Promise<UpstreamResult> {
  void waitUntil;
  let attemptsUsed = 0;
  let finalLive429: { resp: Response; via: string } | null = null;
  let lastEx: RaceExhaustion | null = null;
  for (let round = 0; ; round++) {
    // 跨轮总尝试预算（免费计划 50 子请求/请求，预留直连兜底与 KV 余量）
    const budget = Math.min(racing.max_attempts, TOTAL_ATTEMPT_CAP - attemptsUsed);
    if (budget <= 0) break;
    const roundCfg: RacingConfig = budget >= racing.max_attempts ? racing : { ...racing, max_attempts: Math.max(1, budget) };
    const now = Date.now();
    const candidates = warmFirst(selectCandidates(pool.entries, healthMapSnapshot(), roundCfg, now));
    if (candidates.length === 0) {
      return await rotateUpstream(url, init, pool, racing.node_retry, racing.rate_limit_retries);
    }
    if (candidates.length === 1) {
      const only = candidates[0];
      attemptsUsed++;
      const r = await attemptOnce(only, url, init);
      if (r.resp) {
        const kind = classifyStatus(r.resp.status);
        if (kind === "ok") {
          recordProxySuccess(only.raw, r.ms);
          return { response: r.resp, via: only.raw };
        }
        if (kind === "ratelimit") {
          if (r.q429?.level === "project") return { response: r.resp, via: only.raw };
          recordProxyRateLimit(only.raw, Date.now(), r.q429?.retryAfterSecs);
          // v1.4.0：单候选撞 per-IP 窗口 → 等窗口恢复再战（重选后可能换出其他节点）
          if (round < racing.rate_limit_retries && attemptsUsed < TOTAL_ATTEMPT_CAP) {
            const wait = poolRoundWaitMs(pool);
            await sleep(wait > 0 ? wait + Math.random() * 250 : 0);
            continue;
          }
          throw new PoolRateLimitedError(r.resp, only.raw);
        }
        if (kind === "retryable") {
          recordProxyFailure(only.raw, "HTTP " + r.resp.status);
          if (racing.node_retry && attemptsUsed < TOTAL_ATTEMPT_CAP) {
            attemptsUsed++;
            const again = await retrySameNodeOnce(only, url, init, [], "single-candidate");
            if (again) return { response: again, via: only.raw };
          }
        } else if (kind === "hard" && r.suspect4xx) {
          recordProxyFailure(only.raw, "suspicious auth error (likely forged by proxy)");
        }
        return { response: r.resp, via: only.raw };
      }
      recordProxyFailure(only.raw, r.error ?? "network error");
      if (racing.node_retry && attemptsUsed < TOTAL_ATTEMPT_CAP) {
        attemptsUsed++;
        const again = await retrySameNodeOnce(only, url, init, [], "single-candidate");
        if (again) return { response: again, via: only.raw };
      }
      throw new Error("upstream attempt failed: " + (r.error ?? "network error"));
    }

    const hedge = racing.dynamic_delay ? clampNum(averageLatency(healthMapSnapshot(), now), 100, 10_000) : racing.hedge_delay_ms;
    const outcome = await raceOnce(url, init, candidates, roundCfg, hedge);
    if (outcome.ok) return { response: outcome.response, via: outcome.via };
    attemptsUsed += outcome.exhaust.attempts;
    lastEx = outcome.exhaust;
    // 本轮以 429 收尾 → 记为最终 429 候选（供直连兜底失败时透传）；非 429 收尾则作废旧 429
    dropResp(finalLive429?.resp);
    finalLive429 = lastEx.lastWas429 && lastEx.lastResp ? { resp: lastEx.lastResp, via: lastEx.lastVia } : null;
    // v1.4.0 整轮 429 重试：本轮撞过 per-IP 429 且还有轮次与预算 → 等最早恢复窗口再战
    if (lastEx.any429 && round < racing.rate_limit_retries && attemptsUsed < TOTAL_ATTEMPT_CAP) {
      const wait = poolRoundWaitMs(pool);
      await sleep(wait > 0 ? wait + Math.random() * 250 : 0); // 抖动错峰并发重试蜂拥
      continue;
    }
    break;
  }
  // 全轮耗尽
  if (finalLive429) throw new PoolRateLimitedError(finalLive429.resp, finalLive429.via);
  if (lastEx?.lastResp) return { response: lastEx.lastResp, via: lastEx.lastVia }; // 5xx 等真实上游响应仍透传（维持原语义）
  throw new Error("race: all candidates failed: " + (lastEx?.errors.join(" | ") ?? "no candidates"));
}

// ---------- 直连与统一入口 ----------

/** 直连 fetch（429/408/425/5xx 按 Retry-After 退避重试一次） */
async function directFetchWithRetry(url: string, init: RequestInit): Promise<Response> {
  let resp = await fetch(url, init);
  if (shouldRetryDirect(resp.status)) {
    const delay = retryDelayMs(resp.headers.get("retry-after"));
    void resp.body?.cancel().catch(() => {});
    await sleep(delay);
    resp = await fetch(url, init);
  }
  return resp;
}

export { poolBreakerSnapshot };

/**
 * 统一出站 fetch：
 *   - 无代理池 / 熔断打开 → 直连（429/408/425/5xx 退避重试一次）；
 *   - 竞速关闭或池中仅 1 节点 → 健康排序轮换 + 故障接力（v1.4.0 起含整轮 429 重试）；
 *   - 竞速开启 → 对冲竞速（v1.4.0 起含整轮 429 重试）；
 *   - 全轮 429 耗尽 → 直连兜底（不计熔断：per-IP 窗口秒级恢复，熔断 10 分钟过重）；
 *   - 其它全池失败 → 熔断计数 + 直连兜底。
 */
export async function dispatchUpstream(
  url: string,
  init: RequestInit,
  pool: ProxyPool | null,
  env: Env,
  waitUntil: (p: Promise<unknown>) => void,
  racing?: RacingConfig,
): Promise<UpstreamResult> {
  await ensureHealthLoaded(env);
  const rc = racing ?? DEFAULT_RACING;
  const now = Date.now();
  const hasPool = !!pool && pool.size > 0;
  const bypassPool = !hasPool || poolBreakerIsOpen(poolBreakerSnapshot(), now);
  if (bypassPool) {
    const via = hasPool ? "direct(pool-breaker-open)" : "direct";
    const resp = await directFetchWithRetry(url, init);
    return { response: resp, via };
  }
  let result: UpstreamResult;
  try {
    if (!rc.enabled || pool!.size === 1) {
      result = await rotateUpstream(url, init, pool!, rc.node_retry, rc.rate_limit_retries);
    } else {
      result = await raceUpstream(url, init, pool!, rc, waitUntil);
    }
  } catch (err) {
    if (err instanceof PoolRateLimitedError) {
      // v1.4.0：整轮 429 耗尽 —— per-IP 窗口限流是瞬态，不计熔断。直连走 Workers
      // 自身出口 IP（独立限流窗口，自带 429 退避重试一次）；直连网络失败才透传
      // 代理侧的 429 原响应。
      waitUntil(flushHealthIfDue(env));
      try {
        const resp = await directFetchWithRetry(url, init);
        dropResp(err.resp);
        return { response: resp, via: "direct(pool-rate-limited)" };
      } catch {
        return { response: err.resp, via: err.via };
      }
    }
    const msg = err instanceof Error ? err.message : String(err);
    const signature = PLATFORM_TLS_FAILURE_SIGNATURE.test(msg);
    const opened = breakerOnPoolFailure(signature);
    if (opened) {
      console.warn("[egress] proxy pool exhausted (" + (signature ? "platform TLS-over-tunnel signature" : "consecutive failures") + ") — circuit breaker OPEN, direct fallback for 10min. Last error: " + msg.slice(0, 200));
      waitUntil(flushHealthNow(env));
    }
    waitUntil(flushHealthIfDue(env));
    const resp = await directFetchWithRetry(url, init);
    return { response: resp, via: "direct(pool-fallback)" };
  }
  breakerOnPoolSuccess();
  waitUntil(flushHealthIfDue(env));
  return result;
}

// ---------- 全量测速（管理面板用） ----------

/** 并发测速全部节点（默认并发 6），结果按「可用优先 + 延迟升序」排序 */
export async function testAllProxies(
  pool: ProxyPool,
  concurrency = 6,
): Promise<Array<{ proxy: string; kind: string; ok: boolean; latency_ms: number; error?: string }>> {
  const entries = pool.entries;
  const out: Array<{ proxy: string; kind: string; ok: boolean; latency_ms: number; error?: string }> = [];
  let cursor = 0;
  const worker = async (): Promise<void> => {
    for (;;) {
      const idx = cursor++;
      if (idx >= entries.length) return;
      const e = entries[idx];
      const started = Date.now();
      let result: { ok: boolean; latency_ms: number; error?: string; body_bytes?: number };
      try {
        const resp = await viaProxy(e, "https://generativelanguage.googleapis.com/v1beta/models?pageSize=1", { method: "GET" }, AbortSignal.timeout(10_000));
        const body = await resp.text();
        result = { ok: resp.status < 500, latency_ms: Date.now() - started, error: resp.status >= 500 ? "HTTP " + resp.status : undefined, body_bytes: body.length };
      } catch (err) {
        result = { ok: false, latency_ms: Date.now() - started, error: err instanceof Error ? err.message : String(err) };
      }
      if (result.ok) recordProxySuccess(e.raw, result.latency_ms);
      else recordProxyFailure(e.raw, result.error ?? "unreachable");
      out.push({ proxy: e.raw, kind: e.kind, ok: result.ok, latency_ms: result.latency_ms, error: result.error });
    }
  };
  await Promise.all(Array.from({ length: Math.min(Math.max(1, concurrency), Math.max(1, entries.length)) }, worker));
  return out.sort((a, b) => (a.ok === b.ok ? a.latency_ms - b.latency_ms : a.ok ? -1 : 1));
}
