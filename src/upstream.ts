// 上游调用 + 模型校验 —— 各协议处理器共用的胶水层。
//
// 上游通道分流（原项目 gemini_api_mode 语义）：
//   - anon（默认）：匿名 batchGraphql 端点，免 API Key（reCAPTCHA token 现抓现用）；
//   - official：单 Key 官方 Gemini API 直连。
// 两种模式对处理器完全透明：callGemini 恒返回 Gemini 原生格式 Response
// （非流式 JSON / 流式 SSE），anon 转换在 src/anon/ 内完成。
import type { VProxyConfig } from "./types.ts";
import type { GGenerationConfig } from "./types.ts";
import type { Env } from "./config.ts";
import { isAnonMode, pickGeminiKey } from "./config.ts";
import { dispatchUpstream, type ProxyPool } from "./egress/racefetch.ts";
import { callAnonGenerate, callAnonStream, callAnonCountTokens } from "./anon/upstream.ts";
import { unwrapCountTokensBody } from "./counttokens.ts";
import { errOpenAI, json } from "./convert/common.ts";
import { isGemini36OrLater, normalizeThinkingConfig } from "./thinking.ts";

// 模型解析已抽离为纯逻辑模块（Node 可单测）；此处 re-export 保持兼容
export { resolveModel, type ModelResolution } from "./modelresolve.ts";

export interface HandlerCtx {
  env: Env;
  cfg: VProxyConfig;
  pool: ProxyPool | null;
  waitUntil: (p: Promise<unknown>) => void;
  clientKey: string;
  /** v2.2：最近一次出站调用实际使用的出口（含 [warm] 复用标记）；
   * 处理器重建 Response 时会丢失 __via，route() 用此字段统一回填进请求日志 */
  lastVia?: string;
}

export const GEMINI_BASE = "https://generativelanguage.googleapis.com/v1beta";

/** 上游基地址（原项目 gemini_api_base_url 语义）：配置了镜像/中转则用之，否则官方地址 */
export function geminiBase(cfg: VProxyConfig): string {
  return cfg.gemini_base_url || GEMINI_BASE;
}

/**
 * 出站 generationConfig 收尾（移植原项目 buildGenerationConfig 阶段行为）：
 *   - drop_max_tokens 开启，或目标是 Gemini 3.6+ 时删除 maxOutputTokens（思考 token 不再挤占正文）；
 *   - thinkingConfig 按模型家族归一化（budget/level 只留一个，不支持则删除）。
 */
export function finalizeGenerationConfig(cfg: VProxyConfig, model: string, gc: GGenerationConfig | undefined): GGenerationConfig | undefined {
  if (!gc) return gc;
  const out: GGenerationConfig = { ...gc };
  if (out.maxOutputTokens !== undefined && (cfg.drop_max_tokens || isGemini36OrLater(model))) {
    delete out.maxOutputTokens;
  }
  if (out.thinkingConfig !== undefined) {
    const normalized = normalizeThinkingConfig(model, out.thinkingConfig);
    if (normalized) out.thinkingConfig = normalized;
    else delete out.thinkingConfig;
  }
  return Object.keys(out).length > 0 ? out : undefined;
}

/** 调用 Gemini 上游（anon/official 双通道自动分流；经代理池出口：竞速开启走对冲竞速，否则健康排序轮换） */
export async function callGemini(ctx: HandlerCtx, model: string, action: string, bodyText: string | null): Promise<Response> {
  // ---- anon 模式：匿名 batchGraphql 端点 ----
  if (isAnonMode(ctx.cfg)) {
    return await callAnon(ctx, model, action, bodyText);
  }
  // ---- official 模式：单 Key 官方直连 ----
  const key = pickGeminiKey(ctx.cfg);
  if (!key) {
    return json(
      {
        error: {
          code: 401,
          message: "official 模式未配置 Gemini 官方 API Key：请通过 wrangler secret（GEMINI_API_KEY）或 Web 面板 /admin 的配置页提供，或切换回 anon 模式（无需 Key）",
          status: "UNAUTHENTICATED",
        },
      },
      401,
    );
  }
  const url =
    geminiBase(ctx.cfg) + "/models/" + encodeURIComponent(model) + ":" + action + (action === "streamGenerateContent" ? "?alt=sse" : "");
  const { response, via } = await dispatchUpstream(
    url,
    {
      method: "POST",
      headers: {
        "content-type": "application/json",
        "x-goog-api-key": key,
        accept: "text/event-stream, application/json",
      },
      body: bodyText ?? "",
    },
    ctx.pool,
    ctx.env,
    ctx.waitUntil,
    ctx.cfg.racing,
  );
  // v2.2：via 携带连接复用标记（viaProxy 响应头 x-vproxy-conn），日志/面板可直接观测暖连接命中；
  // 同时写入 ctx.lastVia —— 处理器重建 Response 丢失 __via 时由 route() 统一回填
  const connKind = response.headers.get("x-vproxy-conn");
  const viaTagged = connKind === "warm" ? via + " [warm]" : via;
  (response as Response & { __via?: string }).__via = viaTagged;
  ctx.lastVia = viaTagged;
  return response;
}

/** anon 模式分流：generateContent/predict → 非流式；streamGenerateContent → SSE；countTokens → 精确计数 */
async function callAnon(ctx: HandlerCtx, model: string, action: string, bodyText: string | null): Promise<Response> {
  let payload: Record<string, unknown> = {};
  if (bodyText) {
    try {
      const parsed = JSON.parse(bodyText) as unknown;
      if (parsed && typeof parsed === "object" && !Array.isArray(parsed)) payload = parsed as Record<string, unknown>;
    } catch {
      return json({ error: { code: 400, message: "invalid JSON body", status: "INVALID_ARGUMENT" } }, 400);
    }
  }
  switch (action) {
    case "generateContent":
      return await callAnonGenerate(ctx, model, payload);
    case "streamGenerateContent":
      // v1.4.1：callAnonStream 已改为 async（先载入健康快照再选候选）
      return await callAnonStream(ctx, model, payload);
    case "countTokens": {
      const unwrapped = unwrapCountTokensBody(payload);
      return await callAnonCountTokens(ctx, model, unwrapped.contents);
    }
    case "predict":
    case "predictLongRunning":
      return json(
        {
          error: {
            code: 400,
            message: "anon 模式不支持 :predict / :predictLongRunning（imagen/veo 专用操作需官方 Key）；请切换 official 模式，或改用 generateContent + gemini-*-image 系列模型生图",
            status: "INVALID_ARGUMENT",
          },
        },
        400,
      );
    default:
      return json({ error: { code: 400, message: "unsupported action :" + action, status: "INVALID_ARGUMENT" } }, 400);
  }
}

/** 将上游错误响应转成对应协议格式的 Response（v1.6.0：透传上游 Retry-After 头） */
export async function mapUpstreamError(
  upstream: Response,
  protocol: "openai" | "anthropic" | "gemini",
): Promise<Response> {
  let message = "upstream error HTTP " + upstream.status;
  let status = upstream.status;
  try {
    const body = (await upstream.json()) as { error?: { code?: number; message?: string; status?: string } };
    if (body?.error?.message) message = body.error.message;
    if (typeof body?.error?.code === "number") status = body.error.code;
  } catch {
    // 保留默认 message
  }
  const retryAfter = upstream.headers.get("retry-after");
  const extraHeaders = retryAfter ? { "retry-after": retryAfter } : undefined;
  if (protocol === "openai") return errOpenAI(status, message, undefined, extraHeaders);
  if (protocol === "anthropic") {
    const t =
      status === 401 ? "authentication_error" : status === 429 ? "rate_limit_error" : status === 400 ? "invalid_request_error" : "api_error";
    return new Response(JSON.stringify({ type: "error", error: { type: t, message } }), {
      status,
      headers: { "content-type": "application/json", ...(extraHeaders ?? {}) },
    });
  }
  const s =
    status === 401
      ? "UNAUTHENTICATED"
      : status === 429
        ? "RESOURCE_EXHAUSTED"
        : status >= 500
          ? "UNAVAILABLE"
          : "INVALID_ARGUMENT";
  return json({ error: { code: status, message, status: s } }, status, extraHeaders);
}
