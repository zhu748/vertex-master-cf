// vertex-master-cf —— Vertex Master 全功能 Cloudflare Workers 版
//
// 路由总览：
//   GET  /                                          服务信息
//   GET  /healthz | /readyz                         健康检查
//   GET  /admin | /admin/panel                      Web 管理面板（登录后可管配置/代理/用量/日志）
//   GET  /v1/models                                 OpenAI 模型列表
//   GET  /v1/models/{model}                         OpenAI 单模型查询（SDK 兼容）
//   POST /v1/chat/completions                       OpenAI Chat Completions（流式/非流式）
//   POST /v1/responses                              OpenAI Responses API（流式/非流式）
//   POST /v1/audio/speech                           OpenAI TTS（Gemini TTS 出站）
//   POST /v1/images/generations|edits|variations    OpenAI Images（Gemini 图像模型出站）
//   POST /v1/messages                               Anthropic Messages（流式/非流式）
//   POST /v1/messages/count_tokens                  Anthropic Token 计数（带缓存）
//   GET  /v1beta/models                             Gemini 模型列表（当前生效表，带官方元数据）
//   POST /v1beta/models/{model}:{action}            Gemini 原生透传（generateContent/stream/countTokens/predict）
//   /admin/*                                        管理端点（ADMIN_TOKEN）
//
// 上游双通道（原项目 gemini_api_mode 语义，面板/配置可切换）：
//   anon     匿名 batchGraphql 端点（免 Key，reCAPTCHA token 现抓，出口 IP 建议代理池轮换）
//   official 单 Key 官方 Gemini API 直连（无需 reCAPTCHA）
//
// 闸门（v1.6.0）：max_concurrent_requests（全局在飞业务请求上限，超出 503 + Retry-After）
//              与 max_request_mb（请求体上限，超出 413）在此层统一执行。
//
// 鉴权：客户端携带的 Key（Bearer / x-api-key / x-goog-api-key / ?key=）必须命中
//       KV 配置中的 api_keys 列表；上游使用单个 Gemini 官方 API Key（单 Key 直连）。
// v1.1.0 —— Workers 平台专项优化：
//   - 并发闸门名额在读 body 之前获取：同时在飞的大体积缓冲受 max_concurrent_requests
//     约束，避免 128MB isolate 内存被并发大请求（max_request_mb 上限 64MB）打爆；
//   - 模型表（loadActiveModels）与代理池（resolveProxyPool）并行加载，冷缓存省一次
//     串行 KV 往返（两个模块各自带 isolate 缓存，热路径零额外开销）；
//   - wrangler minify 开启 + waitUntil 刷盘防御性 catch。
import { handleAdmin } from "./handlers/admin.ts";
import { handleAnthropicCountTokens, handleAnthropicMessages } from "./handlers/anthropic.ts";
import { handleGeminiListModels, handleGeminiNative } from "./handlers/gemini.ts";
import { handleChatCompletions, handleOpenAIModels } from "./handlers/openai.ts";
import { handleResponses } from "./handlers/responses.ts";
import { handleAudioSpeech } from "./handlers/audio.ts";
import { handleImagesGenerations, handleImagesEdits } from "./handlers/images.ts";
import { json, protocolErrorResponse, tokensEqual } from "./convert/common.ts";
import type { Env } from "./config.ts";
import { loadConfig } from "./config.ts";
import { resolveProxyPool, type ProxyPool } from "./egress/pool.ts";
import { loadActiveModels, listChatModels, geminiModelInfo, isKnownModelActive } from "./modellist.ts";
import { flushIfDue } from "./usage.ts";
import { flushHealthIfDue } from "./racing.ts";
import { flushMetricsIfDue, metricsBegin, metricsFinish } from "./metrics.ts";
import { runScheduledTasks } from "./cron.ts";
import { maskClientKey, protocolOf, pushLog } from "./logs.ts";
import type { HandlerCtx } from "./upstream.ts";
import { matchApiRoute, NEEDS_UPSTREAM, type ApiRoute } from "./router.ts";
import { acquireSlot, bodyLimitViolation } from "./gate.ts";
import { withFakeVariants } from "./fakestream.ts";

// v1.2.0 —— 补齐原项目遗漏端点（逐行比对 Go 版 server.go 路由表）：
//   - GET /health（/healthz 别名）、GET|HEAD /keepalive、GET|HEAD /api/hello（Claude Code
//     网关探测）、GET /favicon.ico —— 均为鉴权豁免（对齐 Go 版 withAPIKey excluded 表）；
//   - GET /v1beta/models/{model}（Gemini 格式模型详情，原项目 handleModelInfo）；
//   - safety_settings 配置 + normalizeSafetySettings 归一化（Go 版 BuildVertexVariables 语义）。
// v1.3.0 —— 第二轮逐行比对 Go 版后的补齐与 Workers 优化：
//   - Anthropic 流式改发协议级 event: ping（15s，对齐 Go emitPing，Claude Code SDK 依赖）；
//   - levelToBudget HIGH 统一 24576（对齐 Go budgetForThinkingLevel，原 pro 误为 32768）；
//   - 拒绝示例密钥 sk-your-key-here（对齐 Go withAPIKey）；
//   - 并发闸门仅作用于出站端点（对齐 Go isUpstreamWorkloadPath：模型列表/详情不占名额）；
//   - Images n 张有界并发生成（滑动窗口 concurrency=3，n=8 延迟从 8x 降至 ⌈8/3⌉x）。
const VERSION = "1.4.1";

function corsHeaders(): Record<string, string> {
  return {
    "access-control-allow-origin": "*",
    "access-control-allow-methods": "GET, POST, OPTIONS",
    "access-control-allow-headers": "authorization, content-type, x-api-key, x-goog-api-key, anthropic-version, x-requested-with",
    "access-control-max-age": "86400",
  };
}

function withCors(resp: Response): Response {
  const out = new Response(resp.body, resp);
  for (const [k, v] of Object.entries(corsHeaders())) out.headers.set(k, v);
  return out;
}

function extractClientKey(req: Request): string {
  const auth = req.headers.get("authorization") ?? "";
  if (auth.startsWith("Bearer ")) return auth.slice(7).trim();
  const xapi = req.headers.get("x-api-key");
  if (xapi) return xapi.trim();
  const goog = req.headers.get("x-goog-api-key");
  if (goog) return goog.trim();
  const keyParam = new URL(req.url).searchParams.get("key");
  if (keyParam) return keyParam.trim();
  return "";
}

function serviceInfo(): Response {
  return json({
    service: "vertex-master-cf",
    version: VERSION,
    based_on: "vertex-master（全功能移植：anon 匿名端点 + official 单 Key 直连）",
    panel: "/admin",
    features: [
      "上游双通道：anon 匿名 batchGraphql 端点（免 Key，reCAPTCHA 现抓）/ official 官方单 Key 直连",
      "openai/anthropic/gemini 三协议转换 + OpenAI Responses/TTS/Images 端点",
      "假流式：fake-/假流式- 前缀全端点生效 + aggregate_stream（原项目 fakestream 移植）",
      "Anthropic 流式协议级 event: ping 保活（15s，Claude Code SDK 依赖）",
      "socks4/4a/socks5/http 出站代理（cloudflare:sockets 隧道 + 纯 JS TLS 1.3）",
      "对冲竞速（原项目 race engine 移植）+ 429 单 IP 窗口轮转 / 每日配额硬失败分类",
      "节点健康度 + 粘性优选 + 节点内重试 + 直连模式退避重试 + 全池熔断兜底",
      "Cron 定时健康巡检 / 订阅差异更新 / keepalive 保活",
      "OpenAI n 多候选（max_n，原项目 CompleteChatN 移植）",
      "Images n 张有界并发生成（滑动窗口 concurrency=3，结果保序）",
      "official 模式上游镜像/中转基地址（gemini_base_url）",
      "请求指标（JSON / Prometheus）+ Claude 提示词诊断",
      "订阅拉取 + 不支持协议自动剔除（KV 缓存，上限 1000 节点）",
      "官方 ListModels 拉取模型表（内置 + 面板一键重新拉取）",
      "KV 配置热更新 + max_concurrent_requests/max_request_mb 闸门（仅出站端点占名额）",
      "count_tokens 缓存（single-flight + LRU + TTL；anon 模式走匿名 CountTokens 操作）",
      "Gemini 原生响应规范化（占位符清理/空流错误帧/补 STOP 帧）",
      "用量/请求日志持久化",
    ],
    endpoints: {
      openai: [
        "GET /v1/models",
        "GET /v1/models/{model}",
        "POST /v1/chat/completions",
        "POST /v1/responses",
        "POST /v1/audio/speech",
        "POST /v1/images/generations",
        "POST /v1/images/edits",
        "POST /v1/images/variations",
      ],
      anthropic: ["POST /v1/messages", "POST /v1/messages/count_tokens"],
      gemini: [
        "GET /v1beta/models",
        "GET /v1beta/models/{model}",
        "POST /v1beta/models/{model}:{generateContent|streamGenerateContent|countTokens|predict|predictLongRunning}",
      ],
      admin: [
        "GET /admin (Web 面板)",
        "GET|POST /admin/config",
        "GET /admin/models",
        "POST /admin/models/refresh (从官方重新拉取模型表)",
        "POST /admin/models/reset (恢复内置表)",
        "GET /admin/usage",
        "POST /admin/usage/reset",
        "GET /admin/logs",
        "POST /admin/logs/clear",
        "POST /admin/proxies/refresh",
        "POST /admin/cron/run (手动执行一轮定时任务，light 模式)",
        "POST /admin/proxy/test",
        "POST /admin/proxies/test-all",
        "GET /admin/health",
        "POST /admin/health/sweep (手动巡检)",
        "POST /admin/health/reset",
        "GET /admin/metrics[?format=prometheus]",
        "GET /admin/prompt-diagnostics",
        "POST /admin/prompt-diagnostics/clear",
      ],
    },
    outbound_proxies: ["socks4://", "socks4a://", "socks5://", "http://"],
    probes: ["GET /health", "GET /healthz", "GET /readyz", "GET|HEAD /keepalive", "GET|HEAD /api/hello"],
    proxy_notes: "https:// 代理在 Workers 上不可用（无法 TLS-in-TLS），配置与订阅中的此类链接会被自动剔除",
  });
}

export default {
  async fetch(req: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    if (req.method === "OPTIONS") {
      return new Response(null, { status: 204, headers: corsHeaders() });
    }
    const started = Date.now();
    const url = new URL(req.url);
    metricsBegin();
    try {
      const resp = await route(req, env, ctx);
      // 刷盘（用量/指标）均为 25s 批量合并 + 内部吞错；这里再加防御性 catch，
      // 保证 waitUntil 拒绝不会以未处理异常形式出现在平台日志里
      ctx.waitUntil(flushIfDue(env).catch(() => {}));
      // v1.4.1：anon 路径学习的节点健康也随请求尾部刷盘（此前仅竞速路径刷，
      // anon 的学习结果只能靠 15 分钟一次的 cron 落盘）
      ctx.waitUntil(flushHealthIfDue(env).catch(() => {}));
      ctx.waitUntil(flushMetricsIfDue(env).catch(() => {}));
      metricsFinish(resp.status, Date.now() - started, protocolOf(url.pathname));
      pushLog({
        at: new Date().toISOString(),
        protocol: protocolOf(url.pathname),
        method: req.method,
        path: url.pathname,
        model: "-",
        status: resp.status,
        ms: Date.now() - started,
        via: (resp as Response & { __via?: string }).__via ?? "-",
        key: maskClientKey(extractClientKey(req)),
        ok: resp.status < 400,
      });
      return withCors(resp);
    } catch (e) {
      const message = e instanceof Error ? e.message : String(e);
      metricsFinish(500, Date.now() - started, protocolOf(url.pathname));
      pushLog({
        at: new Date().toISOString(),
        protocol: protocolOf(url.pathname),
        method: req.method,
        path: url.pathname,
        model: "-",
        status: 500,
        ms: Date.now() - started,
        via: "-",
        key: maskClientKey(extractClientKey(req)),
        ok: false,
      });
      // v1.8.0：内部错误按协议家族返回 —— Anthropic/Gemini 客户端拿到各自原生的错误结构
      return withCors(protocolErrorResponse(protocolOf(url.pathname), 500, "internal error: " + message));
    }
  },

  // Cron Triggers：定时健康巡检 + 订阅差异更新 + keepalive 保活（见 src/cron.ts）
  async scheduled(_controller: unknown, env: Env, ctx: ExecutionContext): Promise<void> {
    ctx.waitUntil(
      runScheduledTasks(env).catch((e) => {
        console.log("[cron] scheduled tasks failed:", e instanceof Error ? e.message : String(e));
      }),
    );
  },
} satisfies ExportedHandler<Env>;

async function route(req: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
  const url = new URL(req.url);
  const path = url.pathname.replace(/\/+$/, "") || "/";

  if (req.method === "GET" && (path === "/" || path === "")) return serviceInfo();
  if (req.method === "GET" && (path === "/healthz" || path === "/readyz" || path === "/health"))
    return json({ ok: true, version: VERSION });

  // ===== 鉴权豁免探测端点（对齐 Go 版 withAPIKey excluded 表）=====
  if (path === "/keepalive") {
    if (req.method !== "GET" && req.method !== "HEAD") return new Response(null, { status: 405, headers: { Allow: "GET, HEAD" } });
    return new Response(null, { status: 200 });
  }
  if (path === "/api/hello") {
    // Claude Code 启动时对自定义 LLM gateway 的轻量探测（原项目 handleAnthropicHello）
    if (req.method === "HEAD") return new Response(null, { status: 200 });
    if (req.method !== "GET") return new Response(null, { status: 405, headers: { Allow: "GET, HEAD" } });
    return json({ status: "ok" });
  }
  if (path === "/favicon.ico") return new Response(null, { status: 204 });

  // ===== 管理端点（面板 HTML 无需 token，API 走 ADMIN_TOKEN） =====
  if (path.startsWith("/admin")) {
    return handleAdmin(req, path, env, (p) => ctx.waitUntil(p));
  }

  // ===== 业务端点：加载配置 + 鉴权 =====
  const cfg = await loadConfig(env);
  const proto = protocolOf(path);
  if (cfg.api_keys.length === 0) {
    return protocolErrorResponse(
      proto,
      403,
      "服务未配置任何客户端 API Key：请打开 Web 面板 /admin 配置，或通过 wrangler secret（API_KEYS）提供",
    );
  }
  const clientKey = extractClientKey(req);
  // v1.8.0：常量时间比对（与 ADMIN_TOKEN 的 tokensEqual 硬化对齐，缓解时序侧信道）
  if (!clientKey || !cfg.api_keys.some((k) => tokensEqual(k, clientKey))) {
    return protocolErrorResponse(proto, 401, "Invalid API key");
  }
  // v1.3.0：拒绝示例密钥（对齐 Go 版 withAPIKey —— 防止用户照抄示例配置后裸奍上线）
  if (clientKey === "sk-your-key-here") {
    return protocolErrorResponse(proto, 401, "示例密钥禁止调用，请新建密钥。");
  }

  // ===== 路由匹配（纯路由表见 src/router.ts）=====
  const routeInfo: ApiRoute = matchApiRoute(req.method, path);
  if (routeInfo.kind === "unknown") {
    return protocolErrorResponse(proto, 404, "未知的路径：" + path + "，可用端点见 GET /");
  }

  // ===== 闸门 1（预检）：请求体上限（max_request_mb）=====
  // 先用 content-length 头预检（超大请求直接 413，不占并发名额、不读 body）；
  // 实际字节数复核移到并发名额获取之后（见下）——读 body 前先占名额，
  // 在飞大体积缓冲数量受 max_concurrent_requests 约束（isolate 内存 128MB 上限保护）。
  const maxBytes = cfg.max_request_mb * 1024 * 1024;
  if (req.method === "POST" || req.method === "PUT") {
    const pre = bodyLimitViolation(req.headers.get("content-length"), 0, maxBytes);
    if (pre) return protocolErrorResponse(proto, 413, pre);
  }

  // ===== 闸门 2：全局并发门（max_concurrent_requests，超出 503 + Retry-After）=====
  // v1.3.0：对齐 Go 版 isUpstreamWorkloadPath —— 仅出站（需上游/代理池）的端点占并发名额；
  // 模型列表/详情（GET /v1/models、/v1beta/models、单模型查询）不读 body、不出站，
  // 不应消耗名额：否则满载流式请求时客户端连模型列表都拿不到（SDK 启动即报错）。
  const needsGate = NEEDS_UPSTREAM.has(routeInfo.kind);
  const slot = needsGate ? acquireSlot(cfg.max_concurrent_requests) : { acquired: true, release: () => {} };
  if (!slot.acquired) {
    // v1.8.0：繁忙响应按协议家族返回（旧实现固定 OpenAI 形态，Anthropic/Gemini 客户端解析不到对应字段）
    return protocolErrorResponse(
      proto,
      503,
      "服务繁忙：在飞请求已达上限 " + cfg.max_concurrent_requests + "（max_concurrent_requests），请稍后重试",
      { code: "server_busy", retryAfter: "1" },
    );
  }

  try {
    // ===== 闸门 1（复核）：读出 body 复核实际字节数（覆盖无头/分块场景），重包装交给处理器 =====
    if (req.method === "POST" || req.method === "PUT") {
      let body: ArrayBuffer;
      try {
        body = await req.arrayBuffer();
      } catch {
        return protocolErrorResponse(proto, 400, "Invalid request body");
      }
      const post = bodyLimitViolation(null, body.byteLength, maxBytes);
      if (post) return protocolErrorResponse(proto, 413, post);
      const headers = new Headers(req.headers);
      headers.delete("content-length");
      headers.delete("transfer-encoding");
      req = new Request(req.url, { method: req.method, headers, body: body.byteLength > 0 ? body : undefined });
    }

    // 模型表 + 代理池并行加载（各自带 isolate 缓存；冷缓存时省一次串行 KV 往返）
    let pool: ProxyPool | null = null;
    await Promise.all([
      // 模型表：KV 动态表（官方拉取）优先，否则内置表（60s 内存缓存，KV 读失败回退内置）
      loadActiveModels(env),
      // 代理池：仅出站类端点需要（列表/详情类端点跳过，省 KV 读）
      NEEDS_UPSTREAM.has(routeInfo.kind)
        ? resolveProxyPool(env, cfg, (p) => ctx.waitUntil(p)).then((p) => {
            pool = p;
          })
        : Promise.resolve(),
    ]);
    const hctx: HandlerCtx = {
      env,
      cfg,
      pool,
      waitUntil: (p) => ctx.waitUntil(p),
      clientKey,
    };

    const dispatchToHandler = async (): Promise<Response> => {
      switch (routeInfo.kind) {
      // ---- OpenAI ----
      case "chat_completions":
        return await handleChatCompletions(req, hctx);
      case "responses":
        return await handleResponses(req, hctx);
      case "audio_speech":
        return await handleAudioSpeech(req, hctx);
      case "images_generations":
        return await handleImagesGenerations(req, hctx);
      case "images_edits":
        return await handleImagesEdits(req, hctx, false);
      case "images_variations":
        return await handleImagesEdits(req, hctx, true);
      case "openai_models":
        return handleOpenAIModels();
      case "openai_model_detail": {
        const id = routeInfo.model ?? "";
        if (!withFakeVariants(listChatModels()).includes(id)) {
          return protocolErrorResponse(proto, 404, "模型 '" + id + "' 不存在（可用模型见 GET /v1/models）");
        }
        return json({ id, object: "model", created: Math.floor(Date.now() / 1000), owned_by: "google" });
      }

      // ---- Anthropic ----
      case "anthropic_messages":
        return await handleAnthropicMessages(req, hctx);
      case "anthropic_count_tokens":
        return await handleAnthropicCountTokens(req, hctx);

      // ---- Gemini 原生 ----
      case "gemini_list_models":
        return handleGeminiListModels();
      case "gemini_model_detail": {
        // 原项目 handleModelInfo：未知名 404 Gemini 错误结构；已知名返回详情
        const rawId = routeInfo.model ?? "";
        const bare = rawId.replace(/^models\//, "");
        const known = isKnownModelActive(bare) || withFakeVariants(listChatModels()).includes(bare);
        if (!known) {
          return json({ error: { code: 404, message: "Model '" + rawId + "' not found.", status: "NOT_FOUND" } }, 404);
        }
        return json(geminiModelInfo(bare));
      }
      case "gemini_native":
        return await handleGeminiNative(routeInfo.model ?? "", routeInfo.action ?? "", req, hctx);
      }
      return protocolErrorResponse(proto, 404, "未知的路径：" + path + "，可用端点见 GET /");
    };
    const resp = await dispatchToHandler();
    // v2.2：处理器重建 Response 会丢失 callGemini 设置的 __via —— 统一从 ctx.lastVia 回填，
    // 使 /admin/logs 的 via 字段对全部协议稳定可见（含 [warm] 连接复用标记）
    const tagged = resp as Response & { __via?: string };
    if (!tagged.__via && hctx.lastVia) tagged.__via = hctx.lastVia;
    return resp;
  } finally {
    slot.release();
  }
}
