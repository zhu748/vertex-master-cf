// payload.ts —— 匿名 batchGraphql 请求体构建（移植自 vertex-master internal/vertex/payload.go）。
//
// 匿名端点：POST https://cloudconsole-pa.clients6.google.com/v3/entityServices/
//           AiplatformEntityService/schemas/AIPLATFORM_GRAPHQL:batchGraphql?key=<anonKey>
// 请求体 = 固定包壳（operationName/querySignature/requestContext）+ variables。
// variables 由 Gemini 载荷归一化而来（BuildVertexVariables 移植），并强制注入
// region=global 与 recaptchaToken。
//
// 纯逻辑模块：可在 Node 单测。

// batchGraphql 请求的固定包壳常量（逐字节保持既定常量，对齐 Go 版）。
export const ANON_BASE_URL = "https://cloudconsole-pa.clients6.google.com";
export const BATCH_GRAPHQL_PATH = "/v3/entityServices/AiplatformEntityService/schemas/AIPLATFORM_GRAPHQL:batchGraphql";
export const ANON_API_KEY = "AIzaSyCI-zsRP85UVOi0DjtiCwWBwQ1djDy741g";
export const CHAT_QUERY_SIGNATURE = "2/l8eCsMMY49imcDQ/lwwXyL8cYtTjxZBF2dNqy69LodY=";
export const CHAT_OPERATION_NAME = "StreamGenerateContentAnonymous";
export const CHAT_CLIENT_VERSION = "boq_cloud-boq-clientweb-vertexaistudio_20260630.00_p0";
export const CHAT_PAGE_PATH = "/agent-platform/studio/multimodal";

// CountTokens 操作（移植自 counttokens.go buildCountTokensPayload）。
export const COUNT_QUERY_SIGNATURE = "2/mENOSldfC+HZM+tGhVuJLrl8M6gEyK3HRjUKuA5AM58=";
export const COUNT_OPERATION_NAME = "CountTokens";
export const COUNT_CLIENT_VERSION = "boq_cloud-boq-clientweb-vertexaistudio_20260402.09_p0";
export const COUNT_PAGE_PATH = "/vertex-ai/studio/multimodal";

// 默认安全设置（Go 版 defaultSafetySettings）：finishReason=SAFETY 重试时注入。
export const DEFAULT_SAFETY_SETTINGS = [
  { category: "HARM_CATEGORY_HARASSMENT", threshold: "BLOCK_NONE" },
  { category: "HARM_CATEGORY_HATE_SPEECH", threshold: "BLOCK_NONE" },
  { category: "HARM_CATEGORY_SEXUALLY_EXPLICIT", threshold: "BLOCK_NONE" },
  { category: "HARM_CATEGORY_DANGEROUS_CONTENT", threshold: "BLOCK_NONE" },
  { category: "HARM_CATEGORY_CIVIC_INTEGRITY", threshold: "BLOCK_NONE" },
];

// safetyCategories（Go 版 transform/request.go）：buildSafetySettingsFromMap 的固定类别顺序
const SAFETY_CATEGORIES = [
  "HARM_CATEGORY_HARASSMENT",
  "HARM_CATEGORY_HATE_SPEECH",
  "HARM_CATEGORY_SEXUALLY_EXPLICIT",
  "HARM_CATEGORY_DANGEROUS_CONTENT",
  "HARM_CATEGORY_CIVIC_INTEGRITY",
];

/**
 * normalizeSafetySettings（移植自 Go 版 transform/request_contents.go，BuildVertexVariables
 * 在每个出站 chat 请求上无条件执行）—— 让原生 Gemini 请求与转换请求行为一致：
 *   - 客户端提供了非空 safetySettings：逐项校验，阈值空/含 UNSPECIFIED →
 *     BLOCK_NONE，配置了 safety_settings[category] 则用配置值；显式阈值原样保留；
 *   - 未提供/为空/类型不对：按固定类别顺序用配置 map 填充（缺省 BLOCK_NONE）。
 * 保存为单一事实源：converted OpenAI/Anthropic 请求不带 safetySettings，
 * 归一化后同样恒携带 5 类阈值（与 Go 版出站 JSON 形态一致）。
 */
export function normalizeSafetySettings(raw: unknown, configured: Record<string, string>): Array<{ category: string; threshold: string }> {
  if (Array.isArray(raw) && raw.length > 0) {
    const out: Array<{ category: string; threshold: string }> = [];
    for (const item of raw) {
      if (!item || typeof item !== "object" || Array.isArray(item)) continue;
      const category = String((item as Record<string, unknown>)["category"] ?? "").trim();
      if (!category) continue;
      let threshold = String((item as Record<string, unknown>)["threshold"] ?? "").trim().toUpperCase();
      if (!threshold || threshold.includes("UNSPECIFIED")) {
        threshold = "BLOCK_NONE";
        const c = configured[category];
        if (c) threshold = c;
      }
      out.push({ category, threshold });
    }
    return out;
  }
  // 缺失/为空：按固定类别顺序构建（配置 map 优先，否则 BLOCK_NONE）
  return SAFETY_CATEGORIES.map((category) => ({
    category,
    threshold: configured[category] || "BLOCK_NONE",
  }));
}

/** 从 geminiPayload 透传进 variables 的字段（camel/snake 别名，对齐 Go 版 supportedVarFields） */
const SUPPORTED_VAR_FIELDS: Array<[string, string]> = [
  ["contents", "contents"],
  ["tools", "tools"],
  ["toolConfig", "tool_config"],
  ["systemInstruction", "system_instruction"],
  ["safetySettings", "safety_settings"],
  ["generationConfig", "generation_config"],
];

/** 模型名解析（移植 parseModelName）：剥 "models/" 前缀与 ":generateContent" 之类后缀 */
export function parseModelName(model: string): string {
  let m = (model || "").trim();
  if (m.startsWith("models/")) m = m.slice("models/".length);
  const colon = m.indexOf(":");
  if (colon > 0) m = m.slice(0, colon);
  return m;
}

/**
 * BuildVertexVariables（移植版）：Gemini 载荷 → batchGraphql variables。
 * Workers 版保留了字段透传 / systemInstruction 归壳 / 空内容过滤 / region 注入；
 * Go 版针对上游 protobuf 畸形响应的 contents 深度规范化在响应侧（streamparse.ts）处理。
 */
export function buildVertexVariables(
  model: string,
  geminiPayload: Record<string, unknown>,
  opts?: { dropMaxTokens?: boolean; safetySettingsMap?: Record<string, string> },
): Record<string, unknown> {
  const vars: Record<string, unknown> = {};
  vars["model"] = parseModelName(model);
  for (const [camel, snake] of SUPPORTED_VAR_FIELDS) {
    const value = camel in geminiPayload ? geminiPayload[camel] : snake in geminiPayload ? geminiPayload[snake] : undefined;
    if (value !== undefined) vars[camel] = value;
  }
  // safetySettings 归一化（Go 版 BuildVertexVariables 无条件执行）：未提供时按
  // 配置 map/BLOCK_NONE 默认填充 —— 每个出站请求恒携带 5 类阈值
  vars["safetySettings"] = normalizeSafetySettings(vars["safetySettings"], opts?.safetySettingsMap ?? {});
  // systemInstruction：字符串/部分对象 → 标准 {parts:[...]} 形态
  const sys = vars["systemInstruction"];
  if (sys !== undefined) vars["systemInstruction"] = normalizeSystemInstruction(sys);
  // contents：过滤完全空的条目
  if (Array.isArray(vars["contents"])) {
    vars["contents"] = (vars["contents"] as unknown[]).filter((c) => c !== null && typeof c === "object");
  }
  // generationConfig：dropMaxTokens 语义与 official 模式一致（思考 token 不挤占正文）
  if (opts?.dropMaxTokens && vars["generationConfig"] && typeof vars["generationConfig"] === "object") {
    const gc = { ...(vars["generationConfig"] as Record<string, unknown>) };
    delete gc["maxOutputTokens"];
    vars["generationConfig"] = gc;
  }
  vars["region"] = "global";
  return vars;
}

function normalizeSystemInstruction(raw: unknown): unknown {
  if (typeof raw === "string") return { parts: [{ text: raw }], role: "user" };
  if (raw && typeof raw === "object" && !Array.isArray(raw)) {
    const r = raw as Record<string, unknown>;
    if (Array.isArray(r["parts"])) return r;
    if (typeof r["text"] === "string") return { parts: [{ text: r["text"] }], role: "user" };
  }
  return raw;
}

// ---------- 随机标识（移植 payload.go 的随机数语义） ----------

/** 随机数字串（首位 'd' + 16 位数字，对齐 fillRandomTrackingID） */
export function randomTrackingID(): string {
  let out = "d";
  while (out.length < 17) {
    const buf = crypto.getRandomValues(new Uint8Array(32));
    for (const v of buf) {
      if (v >= 250) continue;
      out += String(v % 10);
      if (out.length === 17) break;
    }
  }
  return out;
}

/** 随机 pageViewId：1e15 ~ 1e16-1 */
export function randomPageViewID(): number {
  const minValue = 1_000_000_000_000_000;
  const span = 9_000_000_000_000_000;
  const raw = crypto.getRandomValues(new BigUint64Array(1))[0];
  return minValue + Number(raw % BigInt(span));
}

const HEX = "0123456789ABCDEF";

/** RFC4122 v4 UUID（大写十六进制，对齐 fillRandomUUID） */
export function randomUUID(): string {
  const b = crypto.getRandomValues(new Uint8Array(16));
  b[6] = (b[6] & 0x0f) | 0x40;
  b[8] = (b[8] & 0x3f) | 0x80;
  let out = "";
  for (let i = 0; i < 16; i++) {
    if (i === 4 || i === 6 || i === 8 || i === 10) out += "-";
    out += HEX[b[i] >> 4] + HEX[b[i] & 0x0f];
  }
  return out;
}

function randomString(n: number): string {
  const charset = "abcdefghijklmnopqrstuvwxyz0123456789";
  const buf = crypto.getRandomValues(new Uint8Array(n));
  let out = "";
  for (const v of buf) out += charset[v % charset.length];
  return out;
}

// ---------- 请求壳 ----------

export interface AnonOverrides {
  /** batchGraphql ?key= 覆盖（默认内置匿名 Key） */
  anonApiKey?: string;
  /** 匿名端点基地址覆盖（调试/镜像） */
  anonBaseUrl?: string;
  /** CountTokens querySignature 覆盖 */
  countTokensQuerySignature?: string;
}

export function batchGraphqlURL(overrides?: AnonOverrides): string {
  const base = (overrides?.anonBaseUrl || ANON_BASE_URL).replace(/\/+$/, "");
  const key = overrides?.anonApiKey || ANON_API_KEY;
  return base + BATCH_GRAPHQL_PATH + "?key=" + encodeURIComponent(key) + "&prettyPrint=false";
}

export function anonHeaders(): Record<string, string> {
  return xhrHeaders(
    "application/json",
    "*/*",
    "https://console.cloud.google.com",
    "https://console.cloud.google.com/",
    "cross-site",
  );
}

/**
 * XHR/fetch 风格完整 Chrome 请求头（移植 transport.XHRHeaders，逐字节对齐）。
 * 经代理隧道出站时按对象键序原样写出（等价 Go 版 HeaderOrderKey）。
 * x-browser-validation 在首次调用时异步计算并缓存（SHA-1 可用 WebCrypto）。
 */
export function xhrHeaders(
  contentType: string,
  accept: string,
  origin: string,
  referer: string,
  site: string,
): Record<string, string> {
  const headers: Record<string, string> = {
    "sec-ch-ua": CH_UA,
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
    "sec-ch-ua-arch": '"x86"',
    "sec-ch-ua-bitness": '"64"',
    "sec-ch-ua-full-version": '"150.0.7871.13"',
    "sec-ch-ua-full-version-list": '"Not;A=Brand";v="8.0.0.0", "Chromium";v="150.0.7871.13", "Google Chrome";v="150.0.7871.13"',
    "sec-ch-ua-platform-version": '"19.0.0"',
    "sec-ch-ua-model": '""',
    "sec-ch-ua-wow64": "?0",
    "sec-ch-ua-form-factors": '"Desktop"',
    "user-agent": USER_AGENT,
    "content-type": contentType,
    accept,
    "x-goog-authuser": "0",
    "x-browser-channel": "stable",
    "x-browser-copyright": "Copyright 2026 Google LLC. All Rights Reserved.",
    "x-browser-validation": cachedValidation,
    "x-browser-year": "2026",
    "x-goog-ext-353267353-jspb": "[null,null,null,194274]",
    origin,
    "sec-fetch-site": site,
    "sec-fetch-mode": "cors",
    "sec-fetch-dest": "empty",
    referer,
    // v1.5.3 CF 适配：Go 版 tls-client 自动透明解压 gzip/deflate/br，而
    // Workers DecompressionStream 仅支持 gzip/deflate —— 只声明可解码的编码，
    // 避免 br 压缩体无法解压（egress 层已配套 applyContentDecoding）。
    "accept-encoding": "gzip",
    "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
    priority: "u=1, i",
  };
  if (!contentType) delete headers["content-type"];
  return headers;
}

export const USER_AGENT =
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/150.0.0.0 Safari/537.36";
export const CH_UA = '"Not;A=Brand";v="8", "Chromium";v="150", "Google Chrome";v="150"';

/** recaptcha anchor iframe 导航请求头（移植 transport.AnchorHeaders） */
export function anchorHeaders(): Record<string, string> {
  return {
    "sec-ch-ua": CH_UA,
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
    "sec-ch-ua-arch": '"x86"',
    "sec-ch-ua-bitness": '"64"',
    "sec-ch-ua-full-version": '"150.0.7871.13"',
    "sec-ch-ua-full-version-list": '"Not;A=Brand";v="8.0.0.0", "Chromium";v="150.0.7871.13", "Google Chrome";v="150.0.7871.13"',
    "sec-ch-ua-platform-version": '"19.0.0"',
    "sec-ch-ua-model": '""',
    "sec-ch-ua-wow64": "?0",
    "sec-ch-ua-form-factors": '"Desktop"',
    "upgrade-insecure-requests": "1",
    "user-agent": USER_AGENT,
    accept: "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "sec-fetch-site": "cross-site",
    "sec-fetch-mode": "navigate",
    "sec-fetch-dest": "iframe",
    "accept-encoding": "gzip",
    "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
  };
}

// x-browser-validation = base64(SHA1(apiKey + UA))；UA 固定为 Windows → 用 Windows Key
const XBROWSER_WINDOWS_KEY = "AIzaSyA2KlwBX3mkFo30om9LUFYQhpqLoa_BNhE";
let cachedValidation = "";

export async function ensureBrowserValidation(): Promise<string> {
  if (cachedValidation) return cachedValidation;
  const digest = await crypto.subtle.digest("SHA-1", new TextEncoder().encode(XBROWSER_WINDOWS_KEY + USER_AGENT));
  cachedValidation = btoa(String.fromCharCode(...new Uint8Array(digest)));
  return cachedValidation;
}

/** 对话壳（StreamGenerateContentAnonymous 操作） */
export function buildChatPayload(
  model: string,
  geminiPayload: Record<string, unknown>,
  recaptchaToken: string,
  overrides?: AnonOverrides,
  opts?: { dropMaxTokens?: boolean; extraSafetySettings?: boolean; safetySettingsMap?: Record<string, string> },
): Record<string, unknown> {
  const vars = buildVertexVariables(model, geminiPayload, opts);
  if (opts?.extraSafetySettings && !vars["safetySettings"]) vars["safetySettings"] = DEFAULT_SAFETY_SETTINGS;
  return envelope(
    {
      clientVersion: CHAT_CLIENT_VERSION,
      pagePath: CHAT_PAGE_PATH,
    },
    CHAT_QUERY_SIGNATURE,
    CHAT_OPERATION_NAME,
    {
      contents: vars["contents"],
      generationConfig: vars["generationConfig"],
      model: vars["model"],
      recaptchaToken,
      region: vars["region"],
      safetySettings: vars["safetySettings"],
      systemInstruction: vars["systemInstruction"],
      toolConfig: vars["toolConfig"],
      tools: vars["tools"],
    },
  );
}

/** CountTokens 壳（CountTokens 操作） */
export function buildCountTokensPayload(
  model: string,
  contents: unknown,
  recaptchaToken: string,
  overrides?: AnonOverrides,
): Record<string, unknown> {
  return envelope(
    {
      clientVersion: COUNT_CLIENT_VERSION,
      pagePath: COUNT_PAGE_PATH,
    },
    overrides?.countTokensQuerySignature || COUNT_QUERY_SIGNATURE,
    COUNT_OPERATION_NAME,
    {
      contents: contents ?? [],
      endpoint: "",
      model: parseModelName(model),
      region: "global",
      recaptchaToken,
    },
  );
}

function envelope(ctx: { clientVersion: string; pagePath: string }, querySignature: string, operationName: string, variables: Record<string, unknown>): Record<string, unknown> {
  return {
    operationName,
    querySignature,
    requestContext: {
      backendOverrides: {},
      clientSessionId: randomUUID(),
      clientVersion: ctx.clientVersion,
      jurisdiction: "global",
      localizationData: { locale: "zh_CN", timezone: "Asia/Hong_Kong" },
      pagePath: ctx.pagePath,
      pageViewId: randomPageViewID(),
      selectedPurview: {},
      trackingId: randomTrackingID(),
    },
    variables,
  };
}

export { randomString };
