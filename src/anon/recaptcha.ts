// recaptcha.ts —— Google reCAPTCHA Enterprise 匿名 token 现抓现用
// （移植自 vertex-master internal/recaptcha/recaptcha.go）。
//
// 流程：anchor iframe GET 抠出 base token → reload POST 拿到最终 token（rresp）。
// token 用于 batchGraphql variables.recaptchaToken 字段。
//
// Workers 适配说明：
//   - 传输层复用 egress：代理池可用时经隧道出站（请求头逐字节可控、出口 IP 可轮换）；
//     无代理时回退普通 fetch（TLS 指纹为 Cloudflare 栈，匿名端点可能拒绝 —— 部署
//     文档需明示该风险）；
//   - anchor/reload 均为小型控制响应，限制读取体积 4MiB；
//   - 每次最多 3 次尝试（对齐 Go 版语义）；失败返回 null（调用方按「空则重试/换路」处理）。
import {
  anchorHeaders,
  ensureBrowserValidation,
  randomString,
  xhrHeaders,
} from "./payload.ts";
import type { ProxyEntry } from "../egress/pool.ts";
import { viaProxy } from "../egress/racefetch.ts";

// recaptcha 相关硬编码常量（逐字节保持既定常量，对齐 Go 版）。
const RECAPTCHA_BASE = "https://www.google.com";
const SITE_KEY = "6LdCjtspAAAAAMcV4TGdWLJqRTEk1TfpdLqEnKdj";
const RECAPTCHA_CO = "aHR0cHM6Ly9jb25zb2xlLmNsb3VkLmdvb2dsZS5jb206NDQz";
const RECAPTCHA_HL = "zh-CN";
const RECAPTCHA_V = "jdMmXeCQEkPbnFDy9T04NbgJ";
const RECAPTCHA_VH = "6581054572";
const RESPONSE_MAX_BYTES = 4 << 20;

const TOKEN_RE = /id="recaptcha-token"[^>]*value="([^"]+)"/;
const RRESP_RE = /rresp","(.*?)"/;

export interface RecaptchaTransport {
  /** 代理条目列表（可能为空 = 无代理直连） */
  proxies: ProxyEntry[];
}

/** anchor/reload 小响应整读（限 4MiB） */
async function readLimited(resp: Response): Promise<string> {
  const cl = Number(resp.headers.get("content-length") ?? "0");
  if (Number.isFinite(cl) && cl > RESPONSE_MAX_BYTES) throw new Error("recaptcha: response too large");
  const buf = await resp.arrayBuffer();
  if (buf.byteLength > RESPONSE_MAX_BYTES) throw new Error("recaptcha: response too large");
  return new TextDecoder().decode(buf);
}

async function getViaTransport(
  url: string,
  headers: Record<string, string>,
  transport: RecaptchaTransport,
  signal?: AbortSignal,
): Promise<{ status: number; body: string }> {
  if (transport.proxies.length > 0) {
    const p = transport.proxies[0];
    const resp = await viaProxy(p, url, { method: "GET", headers }, signal);
    return { status: resp.status, body: await readLimited(resp) };
  }
  const resp = await fetch(url, { method: "GET", headers, signal });
  return { status: resp.status, body: await readLimited(resp) };
}

async function postViaTransport(
  url: string,
  headers: Record<string, string>,
  body: string,
  transport: RecaptchaTransport,
  signal?: AbortSignal,
): Promise<{ status: number; body: string }> {
  if (transport.proxies.length > 0) {
    const p = transport.proxies[0];
    const resp = await viaProxy(p, url, { method: "POST", headers, body }, signal);
    return { status: resp.status, body: await readLimited(resp) };
  }
  const resp = await fetch(url, { method: "POST", headers, body, signal });
  return { status: resp.status, body: await readLimited(resp) };
}

/** 单次 anchor + reload 交换 */
async function fetchOnce(transport: RecaptchaTransport, signal?: AbortSignal): Promise<string> {
  await ensureBrowserValidation();
  const cb = randomString(10);
  const anchorURL =
    RECAPTCHA_BASE +
    "/recaptcha/enterprise/anchor?ar=1&k=" + SITE_KEY +
    "&co=" + RECAPTCHA_CO + "&hl=" + RECAPTCHA_HL + "&v=" + RECAPTCHA_V +
    "&size=invisible&anchor-ms=20000&execute-ms=15000&cb=" + cb;

  const anchor = await getViaTransport(anchorURL, anchorHeaders(), transport, signal);
  if (anchor.status !== 200) throw new Error("recaptcha: anchor HTTP " + anchor.status);
  const m = TOKEN_RE.exec(anchor.body);
  if (!m) {
    const prefix = anchor.body.slice(0, 500);
    throw new Error("recaptcha: anchor token not found, body prefix: " + prefix);
  }
  const baseToken = m[1];

  const form = new URLSearchParams({
    v: RECAPTCHA_V,
    reason: "q",
    k: SITE_KEY,
    c: baseToken,
    co: RECAPTCHA_CO,
    hl: RECAPTCHA_HL,
    size: "invisible",
    vh: RECAPTCHA_VH,
    chr: "",
    bg: "",
  });
  const reloadURL = RECAPTCHA_BASE + "/recaptcha/enterprise/reload?k=" + SITE_KEY;
  const reloadHeaders = xhrHeaders(
    "application/x-www-form-urlencoded;charset=UTF-8",
    "*/*",
    RECAPTCHA_BASE,
    anchorURL,
    "same-origin",
  );
  const reload = await postViaTransport(reloadURL, reloadHeaders, form.toString(), transport, signal);
  if (reload.status !== 200) {
    console.log("[recaptcha] reload HTTP " + reload.status + ": " + reload.body.slice(0, 200));
  }
  const rm = RRESP_RE.exec(reload.body);
  if (!rm) throw new Error("recaptcha: rresp not found in reload response");
  return rm[1];
}

/**
 * 抓取一个 reCAPTCHA token：最多 3 次尝试（对齐 Go 版 FetchRecaptchaTokenContext）。
 * 全部失败返回 null。
 */
export async function fetchRecaptchaToken(transport: RecaptchaTransport, signal?: AbortSignal): Promise<string | null> {
  for (let retry = 0; retry < 3; retry++) {
    if (signal?.aborted) return null;
    try {
      const token = await fetchOnce(transport, signal);
      if (token) return token;
    } catch (err) {
      console.log("[recaptcha] attempt " + (retry + 1) + "/3 failed: " + (err instanceof Error ? err.message : String(err)));
    }
  }
  return null;
}
