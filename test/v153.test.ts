// v1.5.3 回归：隧道 HTTP 客户端 Content-Encoding 解压。
// 背景：Go 版隐式依赖 bogdanfinn/tls-client 的自动透明解压，CF 移植缺失 ——
// 请求头声明 accept-encoding 后 Google 返回 gzip 体，token 抠取/JSON 解析
// 拿到二进制垃圾（线上实测 anchor 响应以 gzip magic 1f8b 开头）。
import { test } from "node:test";
import assert from "node:assert/strict";
import * as zlib from "node:zlib";
import { applyContentDecoding, decodeFullBodyBytes } from "../src/egress/httpio.ts";
import { anchorHeaders, xhrHeaders } from "../src/anon/payload.ts";

function gzipBytes(text: string): Uint8Array {
  return new Uint8Array(zlib.gzipSync(Buffer.from(text, "utf8")));
}

function streamOf(bytes: Uint8Array): ReadableStream<Uint8Array> {
  return new ReadableStream<Uint8Array>({
    start(controller) {
      controller.enqueue(bytes);
      controller.close();
    },
  });
}

async function readText(stream: ReadableStream<Uint8Array>): Promise<string> {
  return new TextDecoder().decode(await new Response(stream as unknown as BodyInit).arrayBuffer());
}

test("v1.5.3 applyContentDecoding: gzip 响应体解压 + content-encoding 头移除", async () => {
  const headers: Record<string, string> = {
    "content-type": "text/html",
    "content-encoding": "gzip",
    connection: "close",
  };
  const body = applyContentDecoding(headers, streamOf(gzipBytes('<input id="recaptcha-token" value="tok123">')));
  const text = await readText(body);
  assert.equal(text, '<input id="recaptcha-token" value="tok123">');
  assert.equal(headers["content-encoding"], undefined);
  assert.equal(headers["content-type"], "text/html"); // 其余头不受影响
});

test("v1.5.3 applyContentDecoding: identity / 无头 → 原样透传", async () => {
  const h1: Record<string, string> = { "content-encoding": "identity" };
  assert.equal(await readText(applyContentDecoding(h1, streamOf(new TextEncoder().encode("plain")))), "plain");
  assert.equal(h1["content-encoding"], "identity"); // identity 不算解压，头保留
  const h2: Record<string, string> = {};
  assert.equal(await readText(applyContentDecoding(h2, streamOf(new TextEncoder().encode("plain")))), "plain");
});

test("v1.5.3 applyContentDecoding: br（Workers DecompressionStream 不支持）→ 原样透传且保留头", async () => {
  const headers: Record<string, string> = { "content-encoding": "br" };
  const raw = new TextEncoder().encode("raw-bytes");
  const out = applyContentDecoding(headers, streamOf(raw));
  assert.equal(await readText(out), "raw-bytes");
  assert.equal(headers["content-encoding"], "br"); // 未解压 → 头保留（语义一致）
});

test("v1.5.3 decodeFullBodyBytes: gzip 整读解压；不支持编码原样返回", async () => {
  const gz = await decodeFullBodyBytes(gzipBytes("hello gzip"), "gzip");
  assert.equal(new TextDecoder().decode(gz), "hello gzip");
  const raw = await decodeFullBodyBytes(new TextEncoder().encode("raw"), "br");
  assert.equal(new TextDecoder().decode(raw), "raw");
});

test("v1.5.3 请求头只声明可解码的 gzip（不再声明 deflate/br）", () => {
  assert.equal(xhrHeaders("application/json", "*/*", "https://x", "https://x/", "cross-site")["accept-encoding"], "gzip");
  assert.equal(anchorHeaders()["accept-encoding"], "gzip");
});
