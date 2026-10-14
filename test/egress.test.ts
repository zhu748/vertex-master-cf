// egress 层纯逻辑单测：代理条目解析/清洗 + HTTP/1.1 编解码 + chunked 解码 + 响应体流 + 连接池
import { test } from "node:test";
import assert from "node:assert/strict";
import { ByteBufReader } from "../src/egress/byteio.ts";
import {
  ChunkedDecoder,
  determineBodyShape,
  isBodylessStatus,
  makeBodyStream,
  parseResponseHeadBlock,
  readFullBody,
  responseAllowsReuse,
} from "../src/egress/httpio.ts";
import { classifyProxyLine, cleanProxyList, decodeSubscription, parseProxyUrl, ProxyPool } from "../src/egress/pool.ts";
import { clearForTest, acquire, release, discard, isWarm, stats } from "../src/egress/connpool.ts";
import { sanitizeApiMode } from "../src/config.ts";
import { defaultConfig } from "../src/config.ts";

// ===== 代理条目解析 =====

test("pool: parseProxyUrl 支持协议与默认端口", () => {
  const s5 = parseProxyUrl("socks5://1.2.3.4:1080")!;
  assert.equal(s5.kind, "socks5");
  assert.equal(s5.port, 1080);
  const bare = parseProxyUrl("1.2.3.4:9999")!;
  assert.equal(bare.kind, "socks5"); // 裸 host:port → socks5
  const s4 = parseProxyUrl("socks4a://h:1")!;
  assert.equal(s4.kind, "socks4");
  const http = parseProxyUrl("http://u:p@h")!;
  assert.equal(http.kind, "http");
  assert.equal(http.port, 80);
  assert.equal(http.username, "u");
  assert.equal(http.password, "p");
});

test("pool: https 代理入口剔除（Workers 无法 TLS-in-TLS）", () => {
  const v = classifyProxyLine("https://1.2.3.4:443");
  assert.equal(v.status, "unsupported");
  const v2 = classifyProxyLine("vmess://asdf");
  assert.equal(v2.status, "unsupported");
  const v3 = classifyProxyLine("socks5://1.2.3.4:1080");
  assert.equal(v3.status, "ok");
});

test("pool: cleanProxyList 去重与剔除明细", () => {
  const r = cleanProxyList(["socks5://1.1.1.1:1080", "socks5://1.1.1.1:1080", "https://2.2.2.2", ""]);
  assert.deepEqual(r.kept, ["socks5://1.1.1.1:1080"]);
  assert.equal(r.removed.length, 1);
  assert.equal(r.removed[0].reason.includes("TLS-in-TLS"), true);
});

test("pool: ProxyPool.fromStrings 去重", () => {
  const pool = ProxyPool.fromStrings(["socks5://a:1", "socks5://a:1", "http://b:2"]);
  assert.equal(pool.size, 2);
});

test("pool: 订阅解析（纯文本 / 整段 Base64 / 注释行）", () => {
  const lines = decodeSubscription("socks5://a:1\n#comment\n\nhttp://b:2");
  assert.deepEqual(lines, ["socks5://a:1", "http://b:2"]);
  const b64 = Buffer.from("socks5://c:3\nsocks4://d:4").toString("base64");
  assert.deepEqual(decodeSubscription(b64), ["socks5://c:3", "socks4://d:4"]);
});

// ===== HTTP/1.1 =====

test("httpio: parseResponseHeadBlock", () => {
  const head = parseResponseHeadBlock(new TextEncoder().encode("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nX-A: 1\r\nX-A: 2\r\n\r\n"));
  assert.equal(head.status, 200);
  assert.equal(head.reason, "OK");
  assert.equal(head.headers["content-type"], "application/json");
  assert.equal(head.headers["x-a"], "1, 2");
});

test("httpio: determineBodyShape / isBodylessStatus / responseAllowsReuse", () => {
  assert.deepEqual(determineBodyShape({ "transfer-encoding": "chunked" }), { chunked: true, contentLength: null });
  assert.deepEqual(determineBodyShape({ "content-length": " 42 " }), { chunked: false, contentLength: 42 });
  assert.deepEqual(determineBodyShape({}), { chunked: false, contentLength: null });
  assert.equal(isBodylessStatus("HEAD", 200), true);
  assert.equal(isBodylessStatus("GET", 204), true);
  assert.equal(responseAllowsReuse({ status: 200, reason: "", headers: { connection: "keep-alive" } }), true);
  assert.equal(responseAllowsReuse({ status: 200, reason: "", headers: { connection: "close" } }), false);
});

test("httpio: ChunkedDecoder 基本解码与跨 chunk", () => {
  const d = new ChunkedDecoder();
  const dec = (s: string) => new TextDecoder().decode(d.push(new TextEncoder().encode(s)));
  // 已到达的字节立即产出（不等完整 chunk）
  assert.equal(dec("5\r\nhel"), "hel");
  assert.equal(dec("lo\r\n"), "lo");
  assert.equal(dec("3\r\nabc\r\n0\r\n\r\n"), "abc");
  assert.equal(d.finished, true);
});

test("httpio: ChunkedDecoder chunk size 跨 chunk 与 trailer", () => {
  const d = new ChunkedDecoder();
  const parts: string[] = [];
  const push = (s: string) => {
    const out = d.push(new TextEncoder().encode(s));
    if (out.length) parts.push(new TextDecoder().decode(out));
  };
  push("4;ext=1\r\nab"); // size 行带扩展
  push("cd\r\n0\r\nX-Trailer: 1\r\n\r\n");
  // 每个 push 立即产出已到达的字节（流式语义）
  assert.deepEqual(parts, ["ab", "cd"]);
  assert.equal(d.finished, true);
});

test("httpio: content-length 响应体流（完整消费）", async () => {
  // 构造一个已含 body 的 reader
  const body = new TextEncoder().encode("hello world");
  const src = new ReadableStream<Uint8Array>({
    start(c) {
      c.enqueue(body);
      c.close();
    },
  });
  const reader = new ByteBufReader(src.getReader());
  const stream = makeBodyStream(reader, { chunked: false, contentLength: 11 }, () => {});
  const out = new TextDecoder().decode(await readFullBody(new ByteBufReader(stream.getReader()), { chunked: false, contentLength: null }));
  assert.equal(out, "hello world");
});

// ===== 连接池 =====

function fakeConn(proxy: string, target: string, closed: { v: boolean }) {
  return {
    proxyRaw: proxy,
    target,
    close: () => {
      closed.v = true;
    },
    created: Date.now(),
    lastUsed: Date.now(),
    uses: 0,
  };
}

test("connpool: acquire/release/isWarm/discard 生命周期", () => {
  clearForTest();
  const closed = { v: false };
  const conn = fakeConn("socks5://a:1", "h:443", closed);
  assert.equal(isWarm("socks5://a:1"), false);
  release(conn, true);
  assert.equal(stats().idleConns, 1);
  assert.equal(stats().warmProxies, 1);
  assert.equal(isWarm("socks5://a:1"), true);
  const got = acquire("socks5://a:1", "h:443");
  assert.ok(got);
  assert.equal(got!.uses, 1);
  assert.equal(acquire("socks5://a:1", "h:443"), null);
  discard(got!);
  assert.equal(closed.v, true);
  assert.equal(stats().idleConns, 0);
});

test("connpool: release reusable=false 关闭连接", () => {
  clearForTest();
  const closed = { v: false };
  const conn = fakeConn("socks5://b:2", "h:443", closed);
  release(conn, false);
  assert.equal(closed.v, true);
  assert.equal(stats().idleConns, 0);
});

// ===== 配置 =====

test("config: sanitizeApiMode 别名归一", () => {
  assert.equal(sanitizeApiMode("anon"), "anon");
  assert.equal(sanitizeApiMode("ANONYMOUS"), "anon");
  assert.equal(sanitizeApiMode("official"), "official");
  assert.equal(sanitizeApiMode("api"), "official");
  assert.equal(sanitizeApiMode("garbage"), null);
  assert.equal(sanitizeApiMode(undefined), null);
});

test("config: defaultConfig 环境变量兜底（API_MODE/GEMINI_API_KEY）", () => {
  const cfg = defaultConfig({ API_MODE: "official", GEMINI_API_KEY: "AIzaTest", API_KEYS: "k1,k2" } as never);
  assert.equal(cfg.apimode, "official");
  assert.equal(cfg.gemini_key, "AIzaTest");
  assert.deepEqual(cfg.api_keys, ["k1", "k2"]);
  const cfg2 = defaultConfig({} as never);
  assert.equal(cfg2.apimode, "anon"); // 默认匿名端点（对齐原项目）
});
