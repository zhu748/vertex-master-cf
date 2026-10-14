// probe.ts —— v1.6.1：anon 模式代理节点双主机健康探测。
//
// 背景（线上 tail 实测）：巡检 testProxy 探测 generativelanguage.googleapis.com
// （official 主机），对 anon 完全失真 —— 节点能过 official 探测，却在 anon 流程
// 第一跳 recaptcha（www.google.com）就死（socks5 CONNECT rep=5 / 隧道 TLS
// handshake_failure）。12 发竞速弹药大部分浪费在死节点上，健康表还被巡检
// 持续用错误目标覆写（种子数据数小时后失真）。
//
// 探测语义（与 scripts/seed_anon_health.py 完全一致）：
//   阶段 1：anchor GET https://www.google.com/recaptcha/enterprise/anchor?...
//           → 200 且含 recaptcha-token（隧道 + TLS + 真实 HTTP + Google 不拒）；
//   阶段 2：GET https://cloudconsole-pa.clients6.google.com/（batchGraphql 主机）
//           → 任一 1xx~4xx 状态即隧道可用（404/405 也算通，5xx 视为代理伪响应）。
// 阶段 1 失败短路（死节点只耗 1 子请求）。
//
// ⚠️ 依赖 egress（cloudflare:sockets），只能在 Workers 运行时加载。
import { viaProxy } from "../egress/racefetch.ts";
import { parseProxyUrl } from "../egress/pool.ts";
import { ANON_BASE_URL, anchorHeaders, anchorProbeUrl, randomString } from "./payload.ts";


export interface AnonProbeResult {
  ok: boolean;
  latency_ms: number;
  error?: string;
  /** 失败阶段：anchor / graphql-host（诊断用） */
  stage?: string;
}

/** 探测响应体读取上限（anchor HTML ~10KB，防异常大响应） */
const PROBE_MAX_BYTES = 256 * 1024;

async function readCapped(resp: Response): Promise<string> {
  const buf = await resp.arrayBuffer();
  if (buf.byteLength > PROBE_MAX_BYTES) throw new Error("probe: response too large");
  return new TextDecoder().decode(buf);
}

/** v1.6.1：anon 双主机探测（cron 健康巡检在 apimode=anon 时调用） */
export async function testProxyAnon(proxyUrl: string, timeoutMs = 10_000): Promise<AnonProbeResult> {
  const p = parseProxyUrl(proxyUrl);
  if (!p) return { ok: false, latency_ms: 0, error: "invalid proxy url" };
  const started = Date.now();

  // 阶段 1：anchor（recaptcha 首跳，www.google.com）
  try {
    const resp = await viaProxy(p, anchorProbeUrl(randomString(10)), { method: "GET", headers: anchorHeaders() }, AbortSignal.timeout(timeoutMs));
    const body = await readCapped(resp); // 完整消费 → 连接回池
    if (resp.status !== 200) {
      return { ok: false, latency_ms: Date.now() - started, error: "anchor HTTP " + resp.status, stage: "anchor" };
    }
    if (!body.includes("recaptcha-token")) {
      return { ok: false, latency_ms: Date.now() - started, error: "anchor response without recaptcha-token", stage: "anchor" };
    }
  } catch (err) {
    return { ok: false, latency_ms: Date.now() - started, error: (err instanceof Error ? err.message : String(err)).slice(0, 120), stage: "anchor" };
  }

  // 阶段 2：batchGraphql 主机根路径（任一 1xx~4xx = 隧道可用）
  try {
    const resp = await viaProxy(p, ANON_BASE_URL + "/", { method: "GET", headers: { "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/150.0.0.0 Safari/537.36" } }, AbortSignal.timeout(timeoutMs));
    await readCapped(resp);
    if (resp.status >= 500) {
      return { ok: false, latency_ms: Date.now() - started, error: "graphql host HTTP " + resp.status, stage: "graphql-host" };
    }
  } catch (err) {
    return { ok: false, latency_ms: Date.now() - started, error: (err instanceof Error ? err.message : String(err)).slice(0, 120), stage: "graphql-host" };
  }

  return { ok: true, latency_ms: Date.now() - started };
}
