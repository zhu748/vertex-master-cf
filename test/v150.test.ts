// v1.5.0 回归：anon 无头链路「verify-fail 首帧预期 + 同 token 重试 + in-band 429」
// 背景实测（2026-10-14，本机直连完整复现）：
//   #1 新 token 首次调用  → HTTP 200 + results[].errors "Failed to verify action"（预期首帧）
//   #2 同 token 第二次调用 → 验证通过（warm-up 帧 streamGenerateContentAnonymous:[]），
//                            随后紧跟 in-band 配额错误：
//                            gRPC code 8 "Resource has been exhausted (e.g. check quota)"
//   —— 即「无头版没问题，失败只是 429 限流，重试即可」（用户判断）。
// 旧版两处缺陷（v1.5.0 修复）：
//   F1 in-band verify-fail 直接丢弃 token 换新 token → 每个新 token 又从首帧
//      verify-fail 开始撞，永远无法通过验证（runChatCandidate 已修，此处回归解析层）；
//   F2 in-band code 8 配额错误落入「无结构化错误」分支 → 节点不冷却、不换点、
//      客户端拿到 502 而非 429（streamparse.parseInBandErrors + mapInBandStatus 修复）。
import { test } from "node:test";
import assert from "node:assert/strict";
import {
  AnonAuthError,
  AnonUpstreamError,
  mapInBandStatus,
  parseInBandErrors,
  processStreamingObject,
  scanJsonObjects,
} from "../src/anon/streamparse.ts";

// ===== mapInBandStatus（gRPC/HTTP → 对外状态码，对齐 Go raiseForStatus） =====

test("v1.5.0 mapInBandStatus: gRPC code 8 / RESOURCE_EXHAUSTED → 429（in-band 限流核心映射）", () => {
  assert.equal(mapInBandStatus(8, ""), 429);
  assert.equal(mapInBandStatus(0, "RESOURCE_EXHAUSTED"), 429);
  assert.equal(mapInBandStatus(429, ""), 429);
});

test("v1.5.0 mapInBandStatus: 其余 gRPC/HTTP 状态映射", () => {
  assert.equal(mapInBandStatus(16, ""), 401);
  assert.equal(mapInBandStatus(0, "UNAUTHENTICATED"), 401);
  assert.equal(mapInBandStatus(7, ""), 403);
  assert.equal(mapInBandStatus(3, ""), 400);
  assert.equal(mapInBandStatus(5, ""), 404);
  assert.equal(mapInBandStatus(14, ""), 503);
  assert.equal(mapInBandStatus(404, ""), 404);
  assert.equal(mapInBandStatus(0, ""), 500); // 未知 → 500
  assert.equal(mapInBandStatus(503, ""), 503);
});

// ===== parseInBandErrors（GraphQL 风格 errors 数组，对齐 Go parseErrorResponse case 2） =====

// 线上实测样本：extensions.status 携带 gRPC code 8
const LIVE_QUOTA_ERR = [
  {
    message: "Resource has been exhausted (e.g. check quota).",
    errorType: "DATA_FETCHING_EXCEPTION",
    path: ["ui", "streamGenerateContentAnonymous"],
    extensions: {
      status: { code: 8, message: "Resource has been exhausted (e.g. check quota)." },
    },
  },
];

test("v1.5.0 parseInBandErrors: 线上实测 code 8 → status 429 + 原始 message", () => {
  const parsed = parseInBandErrors(LIVE_QUOTA_ERR);
  assert.ok(parsed);
  assert.equal(parsed.status, 429);
  assert.equal(parsed.code, 8);
  assert.equal(parsed.message, "Resource has been exhausted (e.g. check quota).");
});

test("v1.5.0 parseInBandErrors: 扁平字段回退（无 extensions）", () => {
  const parsed = parseInBandErrors([{ code: 500, message: "boom" }]);
  assert.ok(parsed);
  assert.equal(parsed.status, 500);
  assert.equal(parsed.message, "boom");
});

test("v1.5.0 parseInBandErrors: 字符串首元素/空数组 → null（对齐 Go：非 map 直接落空）", () => {
  assert.equal(parseInBandErrors(["bare string"]), null);
  assert.equal(parseInBandErrors([]), null);
});

test("v1.5.0 parseInBandErrors: 空对象 → Go 语义默认 500 错误（toInt 默认值）", () => {
  // Go parseErrorResponse：errs[0] 是 map 即进入解析，code 缺省 500、message "Unknown error"
  const parsed = parseInBandErrors([{}]);
  assert.ok(parsed);
  assert.equal(parsed.status, 500);
  assert.equal(parsed.message, "Unknown error");
});

// ===== processStreamingObject：in-band 错误分类 =====

test("v1.5.0 processStreamingObject: in-band code 8 → 抛 AnonUpstreamError(429)（旧版静默跳过）", () => {
  const obj = { results: [{ data: null, errors: LIVE_QUOTA_ERR, path: ["ui"] }] };
  assert.throws(
    () => processStreamingObject(obj as Record<string, unknown>, () => true),
    (err: unknown) => {
      assert.ok(err instanceof AnonUpstreamError);
      assert.equal(err.status, 429);
      assert.ok(err.body.includes("Resource has been exhausted"));
      return true;
    },
  );
});

test("v1.5.0 processStreamingObject: verify-fail 仍抛 AnonAuthError（回归）", () => {
  assert.throws(
    () =>
      processStreamingObject(
        { results: [{ errors: [{ message: "Failed to verify action: xyz" }] }] } as Record<string, unknown>,
        () => true,
      ),
    AnonAuthError,
  );
});

test("v1.5.0 processStreamingObject: 无法解析的结构化错误 → 不抛（继续后续 result，对齐 Go）", () => {
  // errors 首元素是字符串且不匹配 verify-fail → parseInBandErrors null → continue
  const chunks: Array<Record<string, unknown>> = [];
  const stop = processStreamingObject(
    { results: [{ errors: ["unparseable"] }, { data: { ui: { streamGenerateContentAnonymous: { candidates: [{ content: { parts: [{ text: "ok" }] } }] } } } }] } as Record<string, unknown>,
    (c) => {
      chunks.push(c);
      return true;
    },
  );
  assert.equal(stop, true);
  assert.equal(chunks.length, 1);
  const parts = (chunks[0] as { candidates?: Array<{ content?: { parts?: Array<{ text?: string }> } }> }).candidates?.[0]?.content?.parts;
  assert.equal(parts?.[0]?.text, "ok");
});

// ===== 端到端序列：warm-up 帧 + 内容帧 + in-band 429（线上实测完整三连帧） =====

function s2stream(s: string): ReadableStream<Uint8Array> {
  const encoder = new TextEncoder();
  let sent = false;
  return new ReadableStream<Uint8Array>({
    pull(controller) {
      if (sent) {
        controller.close();
        return;
      }
      sent = true;
      controller.enqueue(encoder.encode(s));
    },
  });
}

test("v1.5.0 端到端: 线上实测序列（warm-up 空帧 + 内容 + code 8）→ 内容已 emit，429 错误可捕获", async () => {
  // 顶层数组流：[warm-up, content, quota-error]（真实响应以 ",\r\n" 分隔，此处等价拼接）
  const frames = [
    JSON.stringify({ results: [{ data: { ui: { streamGenerateContentAnonymous: [] } }, path: [] }], responseContext: { eti: "x" } }),
    ",\r\n",
    JSON.stringify({ results: [{ data: { ui: { streamGenerateContentAnonymous: { candidates: [{ content: { parts: [{ text: "成功" }] } }] } } } }] }),
    ",\r\n",
    JSON.stringify({ results: [{ data: null, errors: LIVE_QUOTA_ERR, path: ["ui", "streamGenerateContentAnonymous"] }] }),
  ].join("");
  const chunks: Array<Record<string, unknown>> = [];
  const scanErrors: Error[] = [];
  await scanJsonObjects(s2stream(frames), (raw) => {
    try {
      const obj = JSON.parse(new TextDecoder().decode(raw));
      processStreamingObject(obj, (c) => {
        chunks.push(c);
        return true;
      });
    } catch (e) {
      scanErrors.push(e instanceof Error ? e : new Error(String(e)));
      return true; // 对齐 runChatCandidate：错误即停扫
    }
    return false;
  });
  // 内容帧已产出
  assert.equal(chunks.length, 1);
  const parts = (chunks[0] as { candidates?: Array<{ content?: { parts?: Array<{ text?: string }> } }> }).candidates?.[0]?.content?.parts;
  assert.equal(parts?.[0]?.text, "成功");
  // in-band 429 被识别为 AnonUpstreamError(429) → 上层将 relay 换点而非 502
  assert.equal(scanErrors.length, 1);
  assert.ok(scanErrors[0] instanceof AnonUpstreamError);
  assert.equal((scanErrors[0] as AnonUpstreamError).status, 429);
});

test("v1.5.0 端到端: 首帧 verify-fail 序列 → AnonAuthError（供同 token 首验重试分支识别）", async () => {
  const frames = JSON.stringify({
    results: [{ data: null, errors: [{ message: "Failed to verify action", code: 3 }] }],
  });
  const scanErrors: Error[] = [];
  await scanJsonObjects(s2stream(frames), (raw) => {
    try {
      processStreamingObject(JSON.parse(new TextDecoder().decode(raw)), () => true);
    } catch (e) {
      scanErrors.push(e instanceof Error ? e : new Error(String(e)));
      return true;
    }
    return false;
  });
  assert.equal(scanErrors.length, 1);
  assert.ok(scanErrors[0] instanceof AnonAuthError);
});
