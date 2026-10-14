// v1.6.4 回归：修复「流式空回且无报错」—— 线上实测（2026-10-14 13:08 面板日志
// POST /v1beta/models/gemini-3.8-flash:streamGenerateContent 200 0ms）：
// 旧链路 anon 全池 429 时返回 200 + 懒流，流内发带内错误帧后，geminihygiene
// 因「未见 finishReason」追加合成 STOP 帧 —— Gemini/OpenAI/Anthropic 客户端
// 一律解读为「正常完成、内容为空」，429 被完全掩盖。
// 修复两处：
//   F1 callAnonStream 竞速预冲：200 只在拿到首个内容帧后发出；无内容终局
//      以真实 HTTP 429/401/403/502 返回（本文件测 streambridge 纯逻辑）；
//   F2 normalizeNativeStreamFrames：错误帧即终局，其后不再追加合成 STOP 帧。
import { test } from "node:test";
import assert from "node:assert/strict";
import { AnonUpstreamError } from "../src/anon/streamparse.ts";
import {
  bridgeToSseResponse,
  createRaceBridge,
  noContentErrorResponse,
  terminalErrorBody,
  type ChatRaceResult,
} from "../src/anon/streambridge.ts";
import { normalizeNativeStreamFrames } from "../src/geminihygiene.ts";
import type { GResponse } from "../src/types.ts";

async function collectSse(resp: Response): Promise<string> {
  const reader = resp.body!.getReader();
  const decoder = new TextDecoder();
  let out = "";
  for (;;) {
    const { value, done } = await reader.read();
    if (done) break;
    out += decoder.decode(value, { stream: true });
  }
  return out;
}

function parseFrames(sse: string): Array<Record<string, unknown>> {
  return sse
    .split("\n\n")
    .filter((b) => b.trim().length > 0)
    .map((b) => JSON.parse(b.replace(/^data: /, "")));
}

// ===== F1a：流事件桥（竞速后台 ↔ 消费侧） =====

test("v1.6.4 createRaceBridge: push 先于 take → 缓冲按序取出", async () => {
  const b = createRaceBridge();
  b.push({ chunk: { a: 1 } });
  b.push({ chunk: { b: 2 } });
  b.push({ done: { ok: true, lastTerminal: null, last429: null } });
  assert.deepEqual((await b.take()).chunk, { a: 1 });
  assert.deepEqual((await b.take()).chunk, { b: 2 });
  assert.equal((await b.take()).done?.ok, true);
});

test("v1.6.4 createRaceBridge: take 先于 push → 挂起等待，事件到达即唤醒", async () => {
  const b = createRaceBridge();
  const p = b.take();
  b.push({ chunk: { late: true } });
  assert.deepEqual((await p).chunk, { late: true });
});

// ===== F1b：无内容终局 → 真实 HTTP 错误（预冲判定） =====

test("v1.6.4 noContentErrorResponse: 全池 429（last429）→ HTTP 429 + RESOURCE_EXHAUSTED", async () => {
  const r: ChatRaceResult = { ok: false, lastTerminal: null, last429: new AnonUpstreamError(429, "quota body", "in-band 429") };
  const resp = noContentErrorResponse(r);
  assert.equal(resp.status, 429);
  const body = (await resp.json()) as { error: { code: number; status: string; message: string } };
  assert.equal(body.error.code, 429);
  assert.equal(body.error.status, "RESOURCE_EXHAUSTED");
  assert.ok(body.error.message.includes("429"));
});

test("v1.6.4 noContentErrorResponse: 真实上游终局（lastTerminal 401）→ HTTP 401 + UNAUTHENTICATED", async () => {
  const r: ChatRaceResult = { ok: false, lastTerminal: new AnonUpstreamError(401, "", "token rejected"), last429: null };
  const resp = noContentErrorResponse(r);
  assert.equal(resp.status, 401);
  const body = (await resp.json()) as { error: { status: string } };
  assert.equal(body.error.status, "UNAUTHENTICATED");
});

test("v1.6.4 noContentErrorResponse: 无任何终局信息 → 502 兜底（不再 200 空流）", async () => {
  const resp = noContentErrorResponse({ ok: false, lastTerminal: null, last429: null });
  assert.equal(resp.status, 502);
});

// ===== F1c：已有内容 → 200 SSE（空帧跳过 / 终止帧 / 收流） =====

test("v1.6.4 bridgeToSseResponse: 内容帧透传 + 空帧跳过 + 正常收流", async () => {
  const b = createRaceBridge();
  b.push({ chunk: { candidates: [{ content: { role: "model", parts: [{ text: "he" }] } }] } });
  b.push({ chunk: {} }); // 空帧：不产出 SSE 行
  b.push({ chunk: { candidates: [{ content: { role: "model", parts: [{ text: "llo" }] } }] } });
  b.push({ done: { ok: true, lastTerminal: null, last429: null } });
  const resp = bridgeToSseResponse(b, { abort: () => {} });
  assert.equal(resp.status, 200);
  assert.equal(resp.headers.get("content-type"), "text/event-stream");
  const frames = parseFrames(await collectSse(resp));
  assert.equal(frames.length, 2);
  const f0 = frames[0] as { candidates: Array<{ content: { parts: Array<{ text: string }> } }> };
  const f1 = frames[1] as { candidates: Array<{ content: { parts: Array<{ text: string }> } }> };
  assert.equal(f0.candidates[0].content.parts[0].text, "he");
  assert.equal(f1.candidates[0].content.parts[0].text, "llo");
});

test("v1.6.4 bridgeToSseResponse: 内容后错误（lastTerminal）→ 终止错误帧后收流", async () => {
  const b = createRaceBridge();
  b.push({ chunk: { candidates: [{ content: { role: "model", parts: [{ text: "部分内容" }] } }] } });
  b.push({ done: { ok: true, lastTerminal: new AnonUpstreamError(500, "", "mid-stream cut"), last429: null } });
  const resp = bridgeToSseResponse(b, { abort: () => {} });
  assert.equal(resp.status, 200);
  const frames = parseFrames(await collectSse(resp));
  assert.equal(frames.length, 2);
  const err = frames[1].error as { code: number; message: string };
  assert.equal(err.code, 500);
  assert.ok(err.message.includes("mid-stream cut"));
});

// ===== F2：geminihygiene 错误帧后不补合成 STOP（核心回归） =====

async function* gen<T>(items: T[]): AsyncGenerator<T> {
  for (const it of items) yield it;
}

test("v1.6.4 normalizeNativeStreamFrames: 带内错误帧 → 原样透传且不再追加合成 STOP（旧 bug：error 帧后跟 STOP 帧，客户端显示空回无错）", async () => {
  const errFrame = { error: { code: 429, message: "anon rate limited: all candidates exhausted (429)", status: "RESOURCE_EXHAUSTED" } };
  const out: GResponse[] = [];
  for await (const f of normalizeNativeStreamFrames(gen([errFrame as unknown as GResponse]))) out.push(f);
  assert.equal(out.length, 1); // 只有错误帧 —— 修复前是 2 帧（error + 伪 STOP）
  assert.deepEqual((out[0] as unknown as { error: { code: number } }).error.code, 429);
});

test("v1.6.4 normalizeNativeStreamFrames: 正常帧流无 finishReason → 仍补合成 STOP（既有行为回归）", async () => {
  const chunk = { candidates: [{ content: { role: "model", parts: [{ text: "hi" }] }, index: 0 }] };
  const out: GResponse[] = [];
  for await (const f of normalizeNativeStreamFrames(gen([chunk as GResponse]))) out.push(f);
  assert.equal(out.length, 2);
  assert.equal(out[1].candidates?.[0]?.finishReason, "STOP");
});

test("v1.6.4 normalizeNativeStreamFrames: 内容帧 + 终止错误帧 → STOP 不追加（错误即终局）", async () => {
  const chunk = { candidates: [{ content: { role: "model", parts: [{ text: "hi" }] }, index: 0 }] };
  const errFrame = { error: { code: 500, message: "cut", status: "INTERNAL" } };
  const out: GResponse[] = [];
  for await (const f of normalizeNativeStreamFrames(gen([chunk as GResponse, errFrame as unknown as GResponse]))) out.push(f);
  assert.equal(out.length, 2);
  assert.equal(out[1].candidates, undefined);
  assert.ok((out[1] as unknown as { error?: unknown }).error);
});

// ===== 端到端链路形态：预冲 + 渲染组合（旧 bug 场景全链模拟） =====

test("v1.6.4 全链模拟: 全池 429 → 客户端拿到 HTTP 429 JSON（而非 200 空流）", async () => {
  const b = createRaceBridge();
  b.push({ done: { ok: false, lastTerminal: null, last429: new AnonUpstreamError(429, "exhausted", "anon rate limited") } });
  // 预冲取首个事件 → done → noContentErrorResponse（callAnonStream 同款判定）
  const first = await b.take();
  assert.ok(first.done);
  const resp = noContentErrorResponse(first.done);
  assert.notEqual(resp.status, 200);
  assert.equal(resp.status, 429);
});

test("v1.6.4 terminalErrorBody: 状态 → Gemini status 字符串映射（429/401/500/400）", () => {
  assert.equal(terminalErrorBody(new AnonUpstreamError(429, "", "m")).status, "RESOURCE_EXHAUSTED");
  assert.equal(terminalErrorBody(new AnonUpstreamError(401, "", "m")).status, "UNAUTHENTICATED");
  assert.equal(terminalErrorBody(new AnonUpstreamError(403, "", "m")).status, "UNAUTHENTICATED");
  assert.equal(terminalErrorBody(new AnonUpstreamError(500, "", "m")).status, "UNAVAILABLE");
  assert.equal(terminalErrorBody(new AnonUpstreamError(400, "", "m")).status, "INVALID_ARGUMENT");
  assert.equal(terminalErrorBody(new AnonUpstreamError(99, "", "m")).code, 502); // 越界钳位
});
