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
import { orderedCandidates, poolRoundWaitMs } from "../egress/racefetch.ts";
import { closeQuietly, sleep, tcpConnect, withTimeout } from "../egress/sockets.ts";
import { ByteBufReader } from "../egress/byteio.ts";
import { proxyHandshake } from "../egress/handshake.ts";
import { applyContentDecoding, decodeFullBodyBytes, determineBodyShape, isBodylessStatus, makeBodyStream, parseResponseHeadBlock, readFullBody } from "../egress/httpio.ts";
import { Tls13Client, invalidateSessionTicket } from "../egress/tls13.ts";
import { classify429Body, ensureHealthLoaded, recordProxyFailure, recordProxyRateLimit, recordProxySuccess } from "../racing.ts";
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

const ANON_MAX_CANDIDATES = 3; // 单轮最多接力候选数（Workers 子请求预算内）
const ANON_MAX_ATTEMPTS = 4; // 单候选内最大尝试（首验重试 + 换 token 重试）
/** v1.5.0：跨轮候选调用预算（token 抓取 + 多次 batchGraphql 均耗子请求）。
 *  每次候选调用约 3~5 个 socket（1 recaptcha 隧道 + ≤4 batchGraphql 隧道），
 *  预算 6 次 ≈ 30 子请求，加上 KV 读写仍在免费版 50 限额内。 */
const ANON_CANDIDATE_BUDGET = 6;
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

    // v1.5.2 根因修复：TLS 握手完成后的应用数据必须经 tls.write()/tls.read()
    // 加解密。旧版直接把请求写进裸隧道、从裸隧道读响应 —— 等于在已完成 TLS
    // 握手的连接上发明文 HTTP/1.1，服务端必然断连（"byteio: EOF while reading
    // headers"，Workers 上代理路径 100% 失败；本地 Python ssl 模块验证正常故
    // 一直未发现 —— byteio.ts v2.0 注释的「注入 tls13 客户端字节源」从未接上）。
    const tlsReader = new ByteBufReader({
      read: async () => {
        const chunk = await tls.read();
        return chunk ? { value: chunk, done: false } : { done: true };
      },
    });
    const tlsWriter = { write: (b: Uint8Array) => tls.write(b) };

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
    await withTimeout(tlsWriter.write(out), ANON_CONNECT_TIMEOUT_MS, "anon write request timeout");

    const headBlock = await withTimeout(
      tlsReader.readHeaderBlock(),
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
      // 流式体：ReadableStream 透传（调用方整读或扫描）；v1.5.3 Content-Encoding 解压
      let bodyStream = makeBodyStream(tlsReader, shape, () => {
        try {
          tls.close();
        } catch {
          // ignore
        }
      });
      bodyStream = applyContentDecoding(rh.headers, bodyStream);
      outHeaders.delete("content-encoding"); // outHeaders 已先于解码拷贝，同步移除避免矛盾头
      return new Response(bodyStream, { status: rh.status, statusText: rh.reason, headers: outHeaders });
    }
    let full = await readFullBody(tlsReader, shape);
    const contentEncoding = (rh.headers["content-encoding"] || "").trim().toLowerCase();
    if (contentEncoding === "gzip" || contentEncoding === "deflate") {
      delete rh.headers["content-encoding"];
      outHeaders.delete("content-encoding");
      full = await decodeFullBodyBytes(full, contentEncoding);
    }
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
 *  - "relay"     本次尝试失败且未产出内容 → 接力下一候选（err 携带 429 限流信息，
 *                供上层整轮重试决策；其它原因 relay 不携带）
 *  - "terminal"  已产出内容后失败 / 不可接力错误（err 携带对外错误） */
type CandidateOutcome = { kind: "ok" } | { kind: "relay"; err?: AnonUpstreamError } | { kind: "terminal"; err: AnonUpstreamError };

interface ChatParams {
  model: string;
  geminiPayload: Record<string, unknown>;
  dropMaxTokens: boolean;
  overrides: AnonOverrides;
  url: string;
  /** safety_settings 配置（逐类别阈值覆盖，Go 版 cfg.SafetySettings() 语义） */
  safetySettingsMap: Record<string, string>;
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
  // v1.5.0：token 抓不到（隧道死/被墙）→ 记节点失败并立即换候选。旧版在循环内
  // 反复重抓（fetchRecaptchaToken 内部 3 隧道尝试 × 循环 4 轮 = 单候选最多 12
  // 次隧道），既拖慢整体失败路径，又不记健康 → 死节点凭陈旧高分始终霸占候选
  // 头部（线上实测：早最探测存活的节点 4 小时后全死，但健康分未衰减完）。
  let token = await fetchRecaptchaToken({ proxies: p ? [p] : [] }, signal);
  if (!token) {
    if (p) recordProxyFailure(p.raw, "recaptcha token unavailable (tunnel dead or blocked)");
    return { kind: "relay" };
  }
  let isFirstAuth = true;
  let attempt = 0;
  let emitted = false;

  while (attempt < ANON_MAX_ATTEMPTS) {
    if (signal?.aborted) return { kind: "terminal", err: new AnonUpstreamError(499, "", "client disconnected") };
    const payload = buildChatPayload(params.model, params.geminiPayload, token, params.overrides, {
      dropMaxTokens: params.dropMaxTokens,
      safetySettingsMap: params.safetySettingsMap,
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
          // 首次认证失败：同 token 再打一次（匿名端点首帧预期 verify-fail，
          // v1.5.0 实测确认：同 token 第二次请求即可通过验证）
          isFirstAuth = false;
          attempt++;
          await sleep(500);
          continue;
        }
        token = null; // 换 token
        isFirstAuth = true; // 新 token 首帧仍会 verify-fail → 重新走首验同 token 重试
        attempt++;
        if (attempt >= ANON_MAX_ATTEMPTS) {
          const err = new AnonUpstreamError(401, errText, "anon auth failed: " + (errText.slice(0, 200) || "HTTP " + resp.status));
          return emitted ? { kind: "terminal", err } : { kind: "relay" };
        }
        await sleep(1000);
        token = await fetchRecaptchaToken({ proxies: p ? [p] : [] }, signal);
        if (!token) {
          if (p) recordProxyFailure(p.raw, "recaptcha token unavailable (auth refetch)");
          return { kind: "relay" };
        }
        continue;
      }
      if (resp.status === 429) {
        const cls = classify429Body(errText);
        if (p) recordProxyRateLimit(p.raw, Date.now(), cls.retryAfterSecs);
        if (emitted) return { kind: "terminal", err: new AnonUpstreamError(429, errText, "anon rate limited (HTTP 429)") };
        // v1.5.0：relay 携带 429 错误 —— 上层据此驱动整轮等待重选（换 IP）再战
        return { kind: "relay", err: new AnonUpstreamError(429, errText, "anon rate limited (HTTP 429)") };
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
        if (scanError instanceof AnonAuthError) {
          // v1.5.0 根因修复（对齐 Go stream.go auth 分支）：
          //   - 首验 verify-fail（未产出内容）→ 同 token 500ms 重试一次 ——
          //     匿名端点每个新 token 的首次调用必然 verify-fail，同 token 第二次
          //     即可通过；旧版在此直接丢弃 token 换新 token，导致永远卡在首帧
          //     verify-fail（每个新 token 又从首帧开始撞）；
          //   - 再失败 → 换 token，且重置首验标记（新 token 首帧同样预期 verify-fail）
          if (!emitted) {
            if (isFirstAuth) {
              isFirstAuth = false;
              attempt++;
              await sleep(500);
              continue;
            }
            token = null;
            isFirstAuth = true;
            attempt++;
            if (attempt >= ANON_MAX_ATTEMPTS) {
              return { kind: "relay" };
            }
            await sleep(1000);
            token = await fetchRecaptchaToken({ proxies: p ? [p] : [] }, signal);
            if (!token) {
              if (p) recordProxyFailure(p.raw, "recaptcha token unavailable (auth refetch)");
              return { kind: "relay" };
            }
            continue;
          }
          // 已产出内容后的 auth 错误：终止（语义错误透传）
          return { kind: "terminal", err: new AnonUpstreamError(401, "", scanError.message) };
        }
        // v1.5.0：in-band 限流（HTTP 200 + results[].errors 携 gRPC code 8
        // "Resource has been exhausted"，streamparse 已映射为 429）—— 记节点限流
        // 冷却 + relay 携带 429，上层整轮重试换 IP。旧版落入通用失败分支：
        // 节点不冷却、不换点，客户端拿到 502 而非 429。
        if (scanError instanceof AnonUpstreamError && scanError.status === 429 && !emitted) {
          if (p) recordProxyRateLimit(p.raw, Date.now(), classify429Body(scanError.body).retryAfterSecs);
          return { kind: "relay", err: scanError };
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

/** v1.5.0：anon 整轮 429 重试轮数（对齐 racefetch v1.4.0 语义，复用
 *  racing.rate_limit_retries 配置）。钳在 1~3 轮：每轮重新选候选（被 429
 *  冷却的节点自然沉底 → 天然换 IP），配合 ANON_CANDIDATE_BUDGET 保证
 *  Workers 免费版 50 子请求限额内。 */
function anonMaxRounds(cfg: VProxyConfig): number {
  const r = cfg.racing?.rate_limit_retries;
  const n = typeof r === "number" && Number.isFinite(r) ? Math.trunc(r) : 1;
  return 1 + Math.min(Math.max(n, 0), 2);
}

/** v1.5.0：429 换轮等待 —— 池内有活节点立即换点（0ms）；全池冷却则等到
 *  最早恢复者（钳 250ms~8s，与 racefetch 同语义）；直连短退避 1s。 */
async function anonRoundWait(pool: ProxyPool | null): Promise<void> {
  if (!pool || pool.size === 0) {
    await sleep(1000 + Math.random() * 250);
    return;
  }
  const wait = poolRoundWaitMs(pool);
  if (wait > 0) await sleep(wait + Math.random() * 250); // 抖动错峰并发重试蜂拥
}

/** 候选列表：代理池健康排序（无池 → 直连兜底） */
function anonCandidates(pool: ProxyPool | null): Array<ProxyEntry | null> {
  if (pool && pool.size > 0) return orderedCandidates(pool, ANON_MAX_CANDIDATES);
  return [null];
}

/** chat 竞速终局：ok = 已有内容经 emit 流出（lastTerminal 为内容后错误） */
interface ChatRaceResult {
  ok: boolean;
  lastTerminal: AnonUpstreamError | null;
  last429: AnonUpstreamError | null;
}

/**
 * v1.5.4：chat 候选并发竞速（Generate/Stream 共用骨架）。
 * 每轮最多 ANON_MAX_CANDIDATES 个候选**并行**执行（各自独立隧道 + token，
 * 互不干扰；Workers 同时连接数上限 6，3 候选 × 1 隧道安全）：
 *   - 首个产出内容的候选成为 owner，其 chunk 经 emit 流出；其余候选立即中止；
 *   - 全部 relay 且撞过 429 → 等待换轮重选候选（冷却让位 → 换 IP）再战；
 *   - 跨轮预算 ANON_CANDIDATE_BUDGET 总控。
 * 取代 v1.5.0 的顺序接力：全池限流场景下 429 返回时延从 ~48s 降至 ~15s，
 * 成功场景下首个响应即用（不再被前排慢节点拖住）。
 */
async function raceChatCandidates(
  ctx: HandlerCtx,
  params: ChatParams,
  emit: (chunk: Record<string, unknown>) => boolean,
  signal?: AbortSignal,
): Promise<ChatRaceResult> {
  const maxRounds = anonMaxRounds(ctx.cfg);
  let budget = ANON_CANDIDATE_BUDGET;
  let lastTerminal: AnonUpstreamError | null = null;
  let last429: AnonUpstreamError | null = null;

  for (let round = 0; round < maxRounds && budget > 0; round++) {
    if (round > 0) await anonRoundWait(ctx.pool);
    const pool = anonCandidates(ctx.pool);
    const cands = pool.filter(() => budget-- > 0);
    if (cands.length === 0) break;

    // 每候选独立 AbortController：owner 产生即中止其余候选（父 signal 中止全部）
    const aborts = cands.map(() => new AbortController());
    const onParentAbort = () => {
      for (const a of aborts) a.abort();
    };
    signal?.addEventListener("abort", onParentAbort, { once: true });
    let owner = -1; // 首个产出内容的候选
    const abortLosers = (idx: number) => {
      for (let j = 0; j < aborts.length; j++) {
        if (j !== idx) aborts[j].abort();
      }
    };

    const outcomes = await Promise.all(
      cands.map(async (p, idx): Promise<CandidateOutcome> => {
        if (aborts[idx].signal.aborted) return { kind: "relay" };
        return await runChatCandidate(
          p,
          params,
          (chunk) => {
            if (owner === -1) {
              owner = idx;
              abortLosers(idx);
            }
            if (owner !== idx) return true; // 迟到的非 owner chunk：丢弃（其候选随即被中止）
            return emit(chunk);
          },
          aborts[idx].signal,
        );
      }),
    );
    signal?.removeEventListener("abort", onParentAbort);

    if (owner >= 0) {
      // owner 完成：其 terminal（内容后错误）作为终局错误
      const ownerOutcome = outcomes[owner];
      return {
        ok: true,
        lastTerminal: ownerOutcome && ownerOutcome.kind === "terminal" ? ownerOutcome.err : null,
        last429: null,
      };
    }
    // 本轮无内容：汇总 relay/terminal 信息决定是否换轮
    let round429 = false;
    for (const out of outcomes) {
      if (out.kind === "terminal") lastTerminal = out.err;
      if (out.kind === "relay" && out.err?.status === 429) {
        round429 = true;
        last429 = out.err;
      }
    }
    if (!round429) break; // 非 429 原因失败：重试无意义
  }
  return { ok: false, lastTerminal, last429 };
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

/** anon 模式非流式 chat（generateContent / predict 类）：聚合为单个 Gemini 响应。
 *  v1.5.4：候选并发竞速（raceChatCandidates）—— 首个产出内容的候选胜出，
 *  其余立即中止；全池 429 时快速换轮，耗尽返回 429（而非 502）。 */
export async function callAnonGenerate(
  ctx: HandlerCtx,
  model: string,
  geminiPayload: Record<string, unknown>,
  signal?: AbortSignal,
): Promise<Response> {
  // v1.4.1：冷启动 isolate 必须先载入 KV 健康快照再选候选 —— 此前 anon 路径
  // 不加载，内存健康表为空 → 所有节点按「未测试 80 分」以订阅原始顺序接力，
  // 前部的坏代理（CONNECT 403/405/断连）全部撞墙也轮不到已验证好节点。
  await ensureHealthLoaded(ctx.env);
  const cfg = ctx.cfg;
  const params: ChatParams = {
    model,
    geminiPayload,
    dropMaxTokens: cfg.drop_max_tokens,
    overrides: anonOverrides(cfg),
    url: batchGraphqlURL(anonOverrides(cfg)),
    safetySettingsMap: cfg.safety_settings,
  };
  const collector = new ChunkCollector();
  const result = await raceChatCandidates(
    ctx,
    params,
    (chunk) => {
      collector.add(chunk);
      return true;
    },
    signal,
  );
  if (result.ok && !result.lastTerminal) {
    const resp = collector.buildResponse();
    if (resp) return json(resp);
  }
  if (result.lastTerminal) return terminalResponse(result.lastTerminal);
  if (result.last429) {
    return terminalResponse(new AnonUpstreamError(429, result.last429.body, "anon rate limited: all candidates exhausted (429)"));
  }
  return errGemini(502, "anon upstream failed: all candidates exhausted (no content)", "UNAVAILABLE");
}

/** anon 模式流式 chat（streamGenerateContent）：合成 Gemini 原生 SSE 流。
 *  v1.5.4：候选并发竞速 + 推送式 ReadableStream —— 首个产出内容的候选胜出
 *  （其 chunk 实时入队），其余候选立即中止；全池 429 快速换轮，耗尽发送
 *  429 错误帧（保留限流语义）后收流。 */
export async function callAnonStream(ctx: HandlerCtx, model: string, geminiPayload: Record<string, unknown>): Promise<Response> {
  // v1.4.1：同 callAnonGenerate —— 流式路径也先载入健康快照（含冷启动场景）
  await ensureHealthLoaded(ctx.env);
  const cfg = ctx.cfg;
  const params: ChatParams = {
    model,
    geminiPayload,
    dropMaxTokens: cfg.drop_max_tokens,
    overrides: anonOverrides(cfg),
    url: batchGraphqlURL(anonOverrides(cfg)),
    safetySettingsMap: cfg.safety_settings,
  };
  const encoder = new TextEncoder();
  const abort = new AbortController();

  const stream = new ReadableStream<Uint8Array>({
    async start(controller) {
      try {
        const result = await raceChatCandidates(
          ctx,
          params,
          (chunk) => {
            if (Object.keys(chunk).length === 0) return true;
            controller.enqueue(encoder.encode("data: " + JSON.stringify(chunk) + "\n\n"));
            return true;
          },
          abort.signal,
        );
        if (result.ok) {
          if (result.lastTerminal) {
            // 已产出内容后的错误：作为终止帧
            controller.enqueue(encoder.encode("data: " + JSON.stringify({ error: terminalErrorBody(result.lastTerminal) }) + "\n\n"));
          }
          controller.close();
          return;
        }
        // 无内容终局：401/403/429 语义透传，其余 502
        const terminal = result.lastTerminal;
        if (terminal && (terminal.status === 401 || terminal.status === 403 || terminal.status === 429)) {
          controller.enqueue(encoder.encode("data: " + JSON.stringify({ error: terminalErrorBody(terminal) }) + "\n\n"));
        } else if (result.last429) {
          const err = new AnonUpstreamError(429, result.last429.body, "anon rate limited: all candidates exhausted (429)");
          controller.enqueue(encoder.encode("data: " + JSON.stringify({ error: terminalErrorBody(err) }) + "\n\n"));
        } else {
          controller.enqueue(
            encoder.encode("data: " + JSON.stringify({ error: { code: 502, message: "anon upstream failed: all candidates exhausted", status: "UNAVAILABLE" } }) + "\n\n"),
          );
        }
        controller.close();
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

/** anon 模式 countTokens：CountTokens 操作 → {totalTokens}。
 *  v1.5.0（对齐 Go counttokens.go）：同 token verify-fail 首验重试（≤2 次）
 *  + in-band/HTTP 429 记节点限流冷却 + 整轮重试；耗尽仍 429 → 429 语义。 */
export async function callAnonCountTokens(
  ctx: HandlerCtx,
  model: string,
  contents: unknown,
  signal?: AbortSignal,
): Promise<Response> {
  // v1.4.1：同上 —— countTokens 接力也先载入健康快照
  await ensureHealthLoaded(ctx.env);
  const overrides = anonOverrides(ctx.cfg);
  await ensureBrowserValidation();
  const url = batchGraphqlURL(overrides);
  const maxRounds = anonMaxRounds(ctx.cfg);
  let budget = ANON_CANDIDATE_BUDGET;
  let saw429 = false;
  for (let round = 0; round < maxRounds && budget > 0; round++) {
    if (round > 0) await anonRoundWait(ctx.pool);
    let round429 = false;
    for (const p of anonCandidates(ctx.pool)) {
      if (budget <= 0) break;
      budget--;
      const token = await fetchRecaptchaToken({ proxies: p ? [p] : [] }, signal);
      if (!token) {
        // v1.5.0：token 抓不到同样记节点失败（死隧道不应凭陈旧高分霸占候选头）
        if (p) recordProxyFailure(p.raw, "recaptcha token unavailable");
        continue;
      }
      const payload = buildCountTokensPayload(model, contents, token, overrides);
      // Go counttokens.go：同一 token 允许一次首验重试（匿名端点首帧预期 verify-fail）
      for (let attempt = 0; attempt < 2; attempt++) {
        try {
          const resp = await requestViaCandidate(p, url, "POST", anonHeaders(), JSON.stringify(payload), signal);
          const text = await resp.text();
          if (resp.status === 200) {
            if (text.includes("Failed to verify action") || text.includes("The caller does not have permission")) {
              if (attempt === 0) continue; // 同 token 重试一次
              if (p) recordProxyFailure(p.raw, "anon countTokens verify-fail");
              break;
            }
            if (text.includes("Resource has been exhausted") || text.includes("RESOURCE_EXHAUSTED")) {
              // in-band 429（gRPC code 8）：记节点限流冷却，接力换点
              if (p) recordProxyRateLimit(p.raw, Date.now());
              round429 = true;
              break;
            }
            const total = parseCountTokensResult(text);
            if (total > 0) {
              if (p) recordProxySuccess(p.raw, 0);
              return json({ totalTokens: total });
            }
            if (p) recordProxyFailure(p.raw, "anon countTokens: no totalTokens");
            break;
          }
          if (resp.status === 429) {
            if (p) recordProxyRateLimit(p.raw, Date.now());
            round429 = true;
            break;
          }
          if (attempt === 0 && (resp.status === 401 || resp.status === 403)) continue;
          if (p) recordProxyFailure(p.raw, "anon countTokens HTTP " + resp.status);
          break;
        } catch (err) {
          if (p) recordProxyFailure(p.raw, err instanceof Error ? err.message : String(err));
          break;
        }
      }
    }
    if (!round429) break;
    saw429 = true;
  }
  if (saw429) return errGemini(429, "anon countTokens rate limited: all candidates exhausted", "RESOURCE_EXHAUSTED");
  return errGemini(502, "anon countTokens failed: all candidates exhausted", "UNAVAILABLE");
}
