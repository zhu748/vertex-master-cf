// upstream.ts —— 匿名端点上游（anon 模式核心，移植自 vertex-master 的
// VertexAIClient.CompleteChat / StreamChat / countTokens 链路）。
//
// 与 official 模式的关键差异：
//   - 上游不是 generativelanguage.googleapis.com，而是 Cloud Console 匿名 batchGraphql
//     端点，请求体带 recaptchaToken（现抓现用）；
//   - 响应是「拼接 JSON 对象流」而非 SSE/JSON，需要解析并转换为 Gemini 原生格式
//     （本模块对外产出的 Response 与 official 模式完全同构：非流式 JSON / SSE 流），
//     上层协议处理器零改动；
//   - 鉴权失败重试语义（verify-fail 首帧重试同 token、失败换 token）与 429 接力
//     从 Go 版 stream.go 移植；token 与 batchGraphql 请求走同一出口 IP（token 与
//     IP 绑定校验），因此 anon 模式按「候选接力」顺序执行而非对冲竞速。
//
// ⚠️ 传输层经 egress（cloudflare:sockets），只能在 Workers 运行时加载。
import type { VProxyConfig } from "../types.ts";
import type { HandlerCtx } from "../upstream.ts";
import { errGemini, json } from "../convert/common.ts";
import type { ProxyEntry, ProxyPool } from "../egress/pool.ts";
import { orderedCandidates } from "../egress/racefetch.ts";
import { closeQuietly, sleep, tcpConnect, withTimeout } from "../egress/sockets.ts";
import { ByteBufReader } from "../egress/byteio.ts";
import { proxyHandshake } from "../egress/handshake.ts";
import { determineBodyShape, isBodylessStatus, makeBodyStream, parseResponseHeadBlock, readFullBody } from "../egress/httpio.ts";
import { Tls13Client, invalidateSessionTicket } from "../egress/tls13.ts";
import { classify429Body, recordProxyFailure, recordProxyRateLimit, recordProxySuccess } from "../racing.ts";
import { fetchRecaptchaToken } from "./recaptcha.ts";
import {
  anonHeaders,
  batchGraphqlURL,
  buildChatPayload,
  buildCountTokensPayload,
  ensureBrowserValidation,
  type AnonOverrides,
} from "./payload.ts";
import {
  AnonAuthError,
  AnonUpstreamError,
  ChunkCollector,
  parseCountTokensResult,
  processStreamingObject,
  scanJsonObjects,
} from "./streamparse.ts";

const ANON_MAX_CANDIDATES = 3; // 单请求最多接力候选数（Workers 子请求预算内）
const ANON_MAX_ATTEMPTS = 4; // 单候选内最大尝试（首验重试 + 换 token 重试）
const ANON_CONNECT_TIMEOUT_MS = 15_000;
const ANON_HANDSHAKE_TIMEOUT_MS = 15_000;
const ANON_TLS_TIMEOUT_MS = 12_000;
const ANON_RESPONSE_HEAD_TIMEOUT_MS = 120_000;

function anonOverrides(cfg: VProxyConfig): AnonOverrides {
  return {
    anonApiKey: cfg.anon_api_key || undefined,
    anonBaseUrl: cfg.anon_base_url || undefined,
    countTokensQuerySignature: cfg.count_tokens_query_signature || undefined,
  };
}

// ---------- 单候选内的一次 HTTP 往返 ----------

/** 经单个代理（或直连）向目标发起请求，返回原始 Response（含 body 流） */
async function requestViaCandidate(
  p: ProxyEntry | null,
  url: string,
  method: string,
  headers: Record<string, string>,
  body: string | null,
  signal?: AbortSignal,
): Promise<Response> {
  if (!p) {
    return await fetch(url, { method, headers, body: body ?? undefined, signal });
  }
  const u = new URL(url);
  const targetHost = u.hostname;
  const targetPort = Number(u.port) || 443;
  const sock = tcpConnect(p.host, p.port);
  try {
    await withTimeout(sock.opened, ANON_CONNECT_TIMEOUT_MS, "anon proxy connect timeout (" + p.host + ":" + p.port + ")");
    const reader = new ByteBufReader(sock.readable.getReader());
    const writer = sock.writable.getWriter();
    await withTimeout(
      proxyHandshake({ reader, writer }, p, targetHost, targetPort),
      ANON_HANDSHAKE_TIMEOUT_MS,
      "anon " + p.kind + " handshake timeout",
    );
    const tls = new Tls13Client(reader, writer, { serverName: targetHost, onClose: () => closeQuietly(sock) });
    try {
      await withTimeout(tls.handshake(), ANON_TLS_TIMEOUT_MS, "anon tls13 handshake timeout (" + p.host + ":" + p.port + ")");
    } catch (err) {
      if (tls.pskOffered && err instanceof Error && /alert|bad_record_mac|Finished verify|decrypt/i.test(err.message)) {
        invalidateSessionTicket(targetHost);
      }
      throw err;
    }

    const bodyBytes = body ? new TextEncoder().encode(body) : new Uint8Array(0);
    const lines: string[] = [method + " " + u.pathname + u.search + " HTTP/1.1", "Host: " + u.host];
    for (const [k, v] of Object.entries(headers)) {
      const lk = k.toLowerCase();
      if (["host", "content-length", "transfer-encoding", "connection"].includes(lk)) continue;
      lines.push(k + ": " + v);
    }
    lines.push("Content-Length: " + bodyBytes.length, "Connection: close");
    const head = new TextEncoder().encode(lines.join("\r\n") + "\r\n\r\n");
    const out = new Uint8Array(head.length + bodyBytes.length);
    out.set(head, 0);
    out.set(bodyBytes, head.length);
    await withTimeout(writer.write(out), ANON_CONNECT_TIMEOUT_MS, "anon write request timeout");

    const headBlock = await withTimeout(
      reader.readHeaderBlock(),
      ANON_RESPONSE_HEAD_TIMEOUT_MS,
      "anon: no response head within " + ANON_RESPONSE_HEAD_TIMEOUT_MS / 1000 + "s (dead tunnel?)",
    );
    const rh = parseResponseHeadBlock(headBlock);
    const shape = isBodylessStatus(method.toUpperCase(), rh.status)
      ? { chunked: false, contentLength: 0 }
      : determineBodyShape(rh.headers);
    const outHeaders = new Headers();
    for (const [k, v] of Object.entries(rh.headers)) {
      if (["content-length", "transfer-encoding", "connection", "keep-alive", "proxy-connection"].includes(k)) continue;
      outHeaders.set(k, v);
    }
    if (shape.chunked || shape.contentLength !== null) {
      // 流式体：ReadableStream 透传（调用方整读或扫描）
      const bodyStream = makeBodyStream(reader, shape, () => {
        try {
          tls.close();
        } catch {
          // ignore
        }
      });
      return new Response(bodyStream, { status: rh.status, statusText: rh.reason, headers: outHeaders });
    }
    const full = await readFullBody(reader, shape);
    tls.close();
    return new Response(full, { status: rh.status, statusText: rh.reason, headers: outHeaders });
  } catch (err) {
    closeQuietly(sock);
    throw err;
  }
}

// ---------- chat 候选执行 ----------

/** 候选执行结果：
 *  - "ok"        正常完成（内容已全部 emit）
 *  - "relay"     本次尝试失败且未产出内容 → 接力下一候选
 *  - "terminal"  已产出内容后失败 / 不可接力错误（err 携带对外错误） */
type CandidateOutcome = { kind: "ok" } | { kind: "relay" } | { kind: "terminal"; err: AnonUpstreamError };

interface ChatParams {
  model: string;
  geminiPayload: Record<string, unknown>;
  dropMaxTokens: boolean;
  overrides: AnonOverrides;
  url: string;
}

/**
 * 单候选执行完整 chat 流程（token 抓取 → batchGraphql → 流解析）。
 * emit 返回 false 表示客户端断开（停止扫描，正常结束）。
 * 内部重试（对齐 Go 版 stream.go 语义）：
 *   - 首次 verify-fail（无内容产出）→ 同 token 500ms 后重试一次；
 *   - 再失败 → 换 token 重试（最多 ANON_MAX_ATTEMPTS）；
 *   - 429 → 记节点限流冷却，relay 下一候选；
 *   - 其它 4xx/网络错误 → 记节点失败，relay。
 */
async function runChatCandidate(
  p: ProxyEntry | null,
  params: ChatParams,
  emit: (chunk: Record<string, unknown>) => boolean,
  signal?: AbortSignal,
): Promise<CandidateOutcome> {
  let token = await fetchRecaptchaToken({ proxies: p ? [p] : [] }, signal);
  let isFirstAuth = true;
  let attempt = 0;
  let emitted = false;

  while (attempt < ANON_MAX_ATTEMPTS) {
    if (signal?.aborted) return { kind: "terminal", err: new AnonUpstreamError(499, "", "client disconnected") };
    if (!token) {
      token = await fetchRecaptchaToken({ proxies: p ? [p] : [] }, signal);
      if (!token) {
        attempt++;
        if (attempt >= ANON_MAX_ATTEMPTS) {
          return emitted ? { kind: "terminal", err: new AnonUpstreamError(502, "", "recaptcha token unavailable") } : { kind: "relay" };
        }
        continue;
      }
    }
    const payload = buildChatPayload(params.model, params.geminiPayload, token, params.overrides, {
      dropMaxTokens: params.dropMaxTokens,
    });
    let resp: Response;
    try {
      resp = await requestViaCandidate(p, params.url, "POST", anonHeaders(), JSON.stringify(payload), signal);
    } catch (err) {
      if (p) recordProxyFailure(p.raw, err instanceof Error ? err.message : String(err));
      if (emitted) return { kind: "terminal", err: new AnonUpstreamError(502, "", err instanceof Error ? err.message : String(err)) };
      return { kind: "relay" };
    }

    if (resp.status !== 200) {
      let errText = "";
      try {
        errText = (await resp.text()).slice(0, 4096);
      } catch {
        // ignore
      }
      const isAuthFail =
        resp.status === 401 ||
        resp.status === 403 ||
        errText.includes("Failed to verify action") ||
        errText.includes("The caller does not have permission");
      if (isAuthFail) {
        if (isFirstAuth && !emitted) {
          // 首次认证失败：同 token 再打一次（匿名端点首帧预期 verify-fail）
          isFirstAuth = false;
          attempt++;
          await sleep(500);
          continue;
        }
        token = null; // 换 token
        attempt++;
        if (attempt >= ANON_MAX_ATTEMPTS) {
          const err = new AnonUpstreamError(401, errText, "anon auth failed: " + (errText.slice(0, 200) || "HTTP " + resp.status));
          return emitted ? { kind: "terminal", err } : { kind: "relay" };
        }
        await sleep(1000);
        continue;
      }
      if (resp.status === 429) {
        const cls = classify429Body(errText);
        if (p) recordProxyRateLimit(p.raw, Date.now(), cls.retryAfterSecs);
        if (emitted) return { kind: "terminal", err: new AnonUpstreamError(429, errText, "anon rate limited (HTTP 429)") };
        return { kind: "relay" };
      }
      if (p) recordProxyFailure(p.raw, "anon HTTP " + resp.status);
      if (emitted) return { kind: "terminal", err: new AnonUpstreamError(resp.status, errText) };
      return { kind: "relay" };
    }

    // 200：扫描拼接 JSON 对象流，逐帧 emit
    try {
      const scanErrors: Error[] = [];
      await scanJsonObjects(resp.body!, (raw) => {
        let obj: unknown;
        try {
          obj = JSON.parse(new TextDecoder().decode(raw));
        } catch {
          return false; // 跳过无法解析的帧
        }
        if (!obj || typeof obj !== "object" || Array.isArray(obj)) return false;
        try {
          const keepGoing = processStreamingObject(obj as Record<string, unknown>, (chunk) => {
            emitted = true;
            return emit(chunk);
          });
          if (!keepGoing) return true; // 客户端断开：停止扫描
        } catch (e) {
          scanErrors.push(e instanceof Error ? e : new Error(String(e)));
          return true;
        }
        return false;
      });
      const scanError: Error | null = scanErrors[0] ?? null;
      if (scanError) {
        if (scanError instanceof AnonAuthError && !emitted) {
          token = null;
          attempt++;
          continue;
        }
        const err =
          scanError instanceof AnonUpstreamError
            ? scanError
            : new AnonUpstreamError(502, "", "anon stream scan failed: " + scanError.message);
        if (p) recordProxyFailure(p.raw, err.message);
        return emitted ? { kind: "terminal", err } : { kind: "relay" };
      }
      // 扫描正常结束：0 chunk + 首验 → 同 token 重试一次
      if (!emitted && isFirstAuth) {
        isFirstAuth = false;
        attempt++;
        await sleep(500);
        continue;
      }
      if (!emitted) return { kind: "relay" }; // 空响应：换候选
      if (p) recordProxySuccess(p.raw, 0);
      return { kind: "ok" };
    } catch (err) {
      // 流读取错误（截断/超时）
      const msg = err instanceof Error ? err.message : String(err);
      if (p) recordProxyFailure(p.raw, msg);
      return emitted ? { kind: "terminal", err: new AnonUpstreamError(502, "", msg) } : { kind: "relay" };
    }
  }
  const err = new AnonUpstreamError(502, "", "anon: all attempts exhausted");
  return emitted ? { kind: "terminal", err } : { kind: "relay" };
}

// ---------- 对外入口 ----------

/** 候选列表：代理池健康排序（无池 → 直连兜底） */
function anonCandidates(pool: ProxyPool | null): Array<ProxyEntry | null> {
  if (pool && pool.size > 0) return orderedCandidates(pool, ANON_MAX_CANDIDATES);
  return [null];
}

function terminalResponse(err: AnonUpstreamError): Response {
  const status = err.status >= 400 && err.status <= 599 ? err.status : 502;
  const geminiStatus =
    status === 401 || status === 403
      ? "UNAUTHENTICATED"
      : status === 429
        ? "RESOURCE_EXHAUSTED"
        : status >= 500
          ? "UNAVAILABLE"
          : "INVALID_ARGUMENT";
  return errGemini(status, err.message || "anon upstream error", geminiStatus);
}

/** anon 模式非流式 chat（generateContent / predict 类）：聚合为单个 Gemini 响应 */
export async function callAnonGenerate(
  ctx: HandlerCtx,
  model: string,
  geminiPayload: Record<string, unknown>,
  signal?: AbortSignal,
): Promise<Response> {
  const cfg = ctx.cfg;
  const params: ChatParams = {
    model,
    geminiPayload,
    dropMaxTokens: cfg.drop_max_tokens,
    overrides: anonOverrides(cfg),
    url: batchGraphqlURL(anonOverrides(cfg)),
  };
  let lastTerminal: AnonUpstreamError | null = null;
  for (const p of anonCandidates(ctx.pool)) {
    const collector = new ChunkCollector();
    const outcome = await runChatCandidate(
      p,
      params,
      (chunk) => {
        collector.add(chunk);
        return true;
      },
      signal,
    );
    if (outcome.kind === "ok") {
      const resp = collector.buildResponse();
      if (!resp) {
        continue; // 空响应：换候选
      }
      return json(resp);
    }
    if (outcome.kind === "terminal") lastTerminal = outcome.err;
  }
  if (lastTerminal) return terminalResponse(lastTerminal);
  return errGemini(502, "anon upstream failed: all candidates exhausted (no content)", "UNAVAILABLE");
}

/** anon 模式流式 chat（streamGenerateContent）：合成 Gemini 原生 SSE 流 */
export function callAnonStream(ctx: HandlerCtx, model: string, geminiPayload: Record<string, unknown>): Response {
  const cfg = ctx.cfg;
  const params: ChatParams = {
    model,
    geminiPayload,
    dropMaxTokens: cfg.drop_max_tokens,
    overrides: anonOverrides(cfg),
    url: batchGraphqlURL(anonOverrides(cfg)),
  };
  const candidates = anonCandidates(ctx.pool);
  const encoder = new TextEncoder();
  const abort = new AbortController();
  let candidateIdx = 0;
  let yieldedAny = false;

  const stream = new ReadableStream<Uint8Array>({
    async pull(controller) {
      try {
        for (;;) {
          if (candidateIdx >= candidates.length) {
            // 全部候选耗尽
            if (!yieldedAny) {
              controller.enqueue(
                encoder.encode(
                  "data: " +
                    JSON.stringify({ error: { code: 502, message: "anon upstream failed: all candidates exhausted", status: "UNAVAILABLE" } }) +
                    "\n\n",
                ),
              );
            }
            controller.close();
            return;
          }
          const p = candidates[candidateIdx++];
          const outcome = await runChatCandidate(
            p,
            params,
            (chunk) => {
              if (Object.keys(chunk).length === 0) return true;
              controller.enqueue(encoder.encode("data: " + JSON.stringify(chunk) + "\n\n"));
              yieldedAny = true;
              return true;
            },
            abort.signal,
          );
          if (outcome.kind === "ok") {
            controller.close();
            return;
          }
          if (outcome.kind === "terminal") {
            if (outcome.err.status === 499) {
              // 客户端已断开：直接收流，不接力
              controller.close();
              return;
            }
            if (!yieldedAny) {
              // 未产出任何内容：401/429 透传语义错误，其余接力语义
              const status = outcome.err.status;
              if (status === 401 || status === 403 || status === 429) {
                controller.enqueue(encoder.encode("data: " + JSON.stringify({ error: terminalErrorBody(outcome.err) }) + "\n\n"));
                controller.close();
                return;
              }
              continue; // 接力
            }
            // 已产出内容：错误作为终止帧
            controller.enqueue(encoder.encode("data: " + JSON.stringify({ error: terminalErrorBody(outcome.err) }) + "\n\n"));
            controller.close();
            return;
          }
          // relay：下一候选
        }
      } catch (err) {
        controller.error(err);
      }
    },
    cancel() {
      abort.abort();
    },
  });
  return new Response(stream, {
    status: 200,
    headers: { "content-type": "text/event-stream", "cache-control": "no-cache" },
  });
}

function terminalErrorBody(err: AnonUpstreamError): Record<string, unknown> {
  const status = err.status >= 400 && err.status <= 599 ? err.status : 502;
  const geminiStatus =
    status === 401 || status === 403
      ? "UNAUTHENTICATED"
      : status === 429
        ? "RESOURCE_EXHAUSTED"
        : status >= 500
          ? "UNAVAILABLE"
          : "INVALID_ARGUMENT";
  return { code: status, message: err.message || "anon upstream error", status: geminiStatus };
}

/** anon 模式 countTokens：CountTokens 操作 → {totalTokens} */
export async function callAnonCountTokens(
  ctx: HandlerCtx,
  model: string,
  contents: unknown,
  signal?: AbortSignal,
): Promise<Response> {
  const overrides = anonOverrides(ctx.cfg);
  await ensureBrowserValidation();
  const url = batchGraphqlURL(overrides);
  for (const p of anonCandidates(ctx.pool)) {
    const token = await fetchRecaptchaToken({ proxies: p ? [p] : [] }, signal);
    if (!token) continue;
    const payload = buildCountTokensPayload(model, contents, token, overrides);
    try {
      const resp = await requestViaCandidate(p, url, "POST", anonHeaders(), JSON.stringify(payload), signal);
      const text = await resp.text();
      if (resp.status !== 200) {
        if (p) recordProxyFailure(p.raw, "anon countTokens HTTP " + resp.status);
        continue;
      }
      const total = parseCountTokensResult(text);
      if (total > 0) {
        if (p) recordProxySuccess(p.raw, 0);
        return json({ totalTokens: total });
      }
    } catch (err) {
      if (p) recordProxyFailure(p.raw, err instanceof Error ? err.message : String(err));
    }
  }
  return errGemini(502, "anon countTokens failed: all candidates exhausted", "UNAVAILABLE");
}
