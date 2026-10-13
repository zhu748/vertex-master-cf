// anon 层单元测试：batchGraphql payload 构建 + 流解析 + chunk 聚合 + countTokens 解析
// （移植自 vertex-master payload_test / stream_test / counttokens_test 的核心断言）
import { test } from "node:test";
import assert from "node:assert/strict";
import {
  ANON_API_KEY,
  batchGraphqlURL,
  buildChatPayload,
  buildCountTokensPayload,
  buildVertexVariables,
  parseModelName,
  randomTrackingID,
  randomUUID,
} from "../src/anon/payload.ts";
import {
  AnonAuthError,
  ChunkCollector,
  extractChunk,
  parseCountTokensResult,
  processStreamingObject,
  scanJsonObjects,
  StreamTooLargeError,
} from "../src/anon/streamparse.ts";

// ===== payload =====

test("payload: parseModelName 剥前缀与动作后缀", () => {
  assert.equal(parseModelName("gemini-3.6-flash"), "gemini-3.6-flash");
  assert.equal(parseModelName("models/gemini-3.6-flash"), "gemini-3.6-flash");
  assert.equal(parseModelName("models/gemini-3.6-flash:generateContent"), "gemini-3.6-flash");
});

test("payload: buildVertexVariables 字段透传 + region 注入 + snake 别名", () => {
  const vars = buildVertexVariables("models/gemini-3.6-flash:generateContent", {
    contents: [{ role: "user", parts: [{ text: "hi" }] }],
    system_instruction: { parts: [{ text: "sys" }] },
    generation_config: { temperature: 0.7, maxOutputTokens: 100 },
    tools: [],
  } as Record<string, unknown>);
  assert.equal(vars["model"], "gemini-3.6-flash");
  assert.equal(vars["region"], "global");
  assert.deepEqual(vars["systemInstruction"], { parts: [{ text: "sys" }] });
  assert.deepEqual(vars["generationConfig"], { temperature: 0.7, maxOutputTokens: 100 });
});

test("payload: dropMaxTokens 删除输出上限（对齐原项目语义）", () => {
  const vars = buildVertexVariables("gemini-3.6-flash", {
    contents: [],
    generationConfig: { temperature: 1, maxOutputTokens: 42 },
  }, { dropMaxTokens: true });
  assert.deepEqual(vars["generationConfig"], { temperature: 1 });
});

test("payload: chat 壳固定包壳 + recaptchaToken + 匿名 Key", () => {
  const payload = buildChatPayload("gemini-3.6-flash", { contents: [{ role: "user", parts: [{ text: "hi" }] }] }, "tok123");
  assert.equal(payload["operationName"], "StreamGenerateContentAnonymous");
  assert.equal(payload["querySignature"], "2/l8eCsMMY49imcDQ/lwwXyL8cYtTjxZBF2dNqy69LodY=");
  const vars = payload["variables"] as Record<string, unknown>;
  assert.equal(vars["recaptchaToken"], "tok123");
  assert.equal(vars["model"], "gemini-3.6-flash");
  assert.equal(vars["region"], "global");
  const rc = payload["requestContext"] as Record<string, unknown>;
  assert.equal(rc["clientVersion"], "boq_cloud-boq-clientweb-vertexaistudio_20260630.00_p0");
  assert.equal(rc["pagePath"], "/agent-platform/studio/multimodal");
});

test("payload: countTokens 壳独立 querySignature + endpoint 字段", () => {
  const payload = buildCountTokensPayload("gemini-3.6-flash", [{ role: "user", parts: [{ text: "hello" }] }], "tok");
  assert.equal(payload["operationName"], "CountTokens");
  assert.equal(payload["querySignature"], "2/mENOSldfC+HZM+tGhVuJLrl8M6gEyK3HRjUKuA5AM58=");
  const vars = payload["variables"] as Record<string, unknown>;
  assert.equal(vars["endpoint"], "");
  assert.equal(vars["recaptchaToken"], "tok");
});

test("payload: batchGraphqlURL 默认匿名 Key / 覆盖生效", () => {
  const url = batchGraphqlURL();
  assert.ok(url.includes(ANON_API_KEY));
  assert.ok(url.startsWith("https://cloudconsole-pa.clients6.google.com/"));
  const custom = batchGraphqlURL({ anonApiKey: "k2", anonBaseUrl: "https://mirror.example.com/" });
  assert.ok(custom.startsWith("https://mirror.example.com/"));
  assert.ok(custom.includes("key=k2"));
});

test("payload: 随机标识格式", () => {
  const t = randomTrackingID();
  assert.equal(t.length, 17);
  assert.equal(t[0], "d");
  assert.ok(/^\d+$/.test(t.slice(1)));
  const u = randomUUID();
  assert.ok(/^[0-9A-F]{8}-[0-9A-F]{4}-4[0-9A-F]{3}-[89AB][0-9A-F]{3}-[0-9A-F]{12}$/.test(u));
});

// ===== 流扫描 =====

function streamOf(chunks: string[]): ReadableStream<Uint8Array> {
  const encoder = new TextEncoder();
  return new ReadableStream<Uint8Array>({
    start(controller) {
      for (const c of chunks) controller.enqueue(encoder.encode(c));
      controller.close();
    },
  });
}

test("scanJsonObjects: 多对象跨 chunk 边界", async () => {
  const objects: string[] = [];
  await scanJsonObjects(streamOf(['{"a":1}{"b":', '2}{"c":{"d":"}"}}']), async (raw) => {
    objects.push(new TextDecoder().decode(raw));
    return false;
  });
  assert.deepEqual(objects, ['{"a":1}', '{"b":2}', '{"c":{"d":"}"}}']);
});

test("scanJsonObjects: 字符串内的花括号/转义不干扰配对", async () => {
  const objects: string[] = [];
  await scanJsonObjects(streamOf(['{"s":"a{b}c\\\\d\\"e"}{"t":[]}']), async (raw) => {
    objects.push(new TextDecoder().decode(raw));
    return false;
  });
  assert.deepEqual(objects, ['{"s":"a{b}c\\\\d\\"e"}', '{"t":[]}']);
});

test("scanJsonObjects: 截断 JSON 报错", async () => {
  await assert.rejects(
    () => scanJsonObjects(streamOf(['{"a":1}{"b":']), async () => false),
    /truncated/,
  );
});

test("scanJsonObjects: 超大对象拒绝（防御异常上游）", async () => {
  const big = JSON.stringify({ data: "x".repeat(100) });
  await assert.rejects(
    () => scanJsonObjects(streamOf([big]), async () => false, 50),
    StreamTooLargeError,
  );
});

// ===== 对象处理 =====

test("processStreamingObject: 解包 ui.streamGenerateContentAnonymous（map 形态）", () => {
  const chunks: Record<string, unknown>[] = [];
  const keep = processStreamingObject(
    {
      results: [
        {
          data: {
            ui: {
              streamGenerateContentAnonymous: {
                candidates: [{ content: { parts: [{ text: "hello" }], role: "model" } }],
              },
            },
          },
        },
      ],
    },
    (c) => {
      chunks.push(c);
      return true;
    },
  );
  assert.equal(keep, true);
  assert.equal(chunks.length, 1);
  const cands = chunks[0]["candidates"] as unknown[];
  assert.equal((cands[0] as Record<string, unknown>)["finishReason"], undefined);
});

test("processStreamingObject: list 形态逐项提取 + 外层元数据下放", () => {
  const chunks: Record<string, unknown>[] = [];
  processStreamingObject(
    {
      results: [
        {
          data: {
            usageMetadata: { totalTokenCount: 9 },
            ui: {
              streamGenerateContentAnonymous: [
                { candidates: [{ content: { parts: [{ text: "a" }], role: "model" } }] },
                { candidates: [{ content: { parts: [{ text: "b" }], role: "model" } }] },
              ],
            },
          },
        },
      ],
    },
    (c) => {
      chunks.push(c);
      return true;
    },
  );
  assert.equal(chunks.length, 2);
  assert.deepEqual(chunks[0]["usageMetadata"], { totalTokenCount: 9 });
  assert.deepEqual(chunks[1]["usageMetadata"], { totalTokenCount: 9 });
});

test("processStreamingObject: verify-fail 错误 → AnonAuthError", () => {
  assert.throws(
    () =>
      processStreamingObject(
        { results: [{ errors: [{ message: "Failed to verify action: xyz" }] }] },
        () => true,
      ),
    AnonAuthError,
  );
});

test("processStreamingObject: 客户端断开 → 返回 false", () => {
  const keep = processStreamingObject(
    {
      results: [
        { data: { ui: { streamGenerateContentAnonymous: { candidates: [{ content: { parts: [{ text: "x" }], role: "model" } }] } } } },
        { data: { ui: { streamGenerateContentAnonymous: { candidates: [{ content: { parts: [{ text: "y" }], role: "model" } }] } } } },
      ],
    },
    () => false,
  );
  assert.equal(keep, false);
});

// ===== extractChunk 清洗 =====

test("extractChunk: protobuf 空默认字段剔除（对齐 Go 版 cleanPart 流式路径：保留显式 thought 标记）", () => {
  const chunk = extractChunk({
    candidates: [
      {
        content: {
          parts: [{ data: "text", text: "hi", thought: false, thoughtSignature: "", fileData: {}, functionCall: {}, functionResponse: {}, inlineData: {} }],
          role: "model",
        },
      },
    ],
    modelStatus: "MODEL_STATUS_UNSPECIFIED",
  } as Record<string, unknown>);
  assert.ok(chunk);
  const cands = chunk!["candidates"] as unknown[];
  const part = ((cands[0] as Record<string, unknown>)["content"] as Record<string, unknown>)["parts"] as Record<string, unknown>[];
  // 流式路径与 Go cleanPart 一致：删 data/空 fileData/空 functionCall/空 functionResponse/空 inlineData；
  // 显式 thought/thoughtSignature 标记保留（与规范快路径 materialize 一致）
  assert.deepEqual(part, [{ text: "hi", thought: false, thoughtSignature: "" }]);
  assert.equal(chunk!["modelStatus"], undefined); // 非白名单键删除
});

test("extractChunk: functionCall args 字符串转对象", () => {
  const chunk = extractChunk({
    candidates: [{ content: { parts: [{ data: "functionCall", functionCall: { name: "f", args: "{\"a\":1}" } }], role: "model" } }],
  } as Record<string, unknown>);
  const part = (((chunk!["candidates"] as unknown[])[0] as Record<string, unknown>)["content"] as Record<string, unknown>)["parts"] as unknown[];
  assert.deepEqual(part, [{ functionCall: { name: "f", args: { a: 1 } } }]);
});

test("extractChunk: 纯 thought 空帧丢弃（part 级删除，候选保留空 parts —— 对齐 Go 版语义）", () => {
  const chunk = extractChunk({
    candidates: [{ content: { parts: [{ thought: false, thoughtSignature: "" }], role: "model" } }],
  } as Record<string, unknown>);
  // Go 版 extractChunk：candidates 键存在即保留（即使空列表）；thought-only part 被删除
  assert.ok(chunk);
  const cands = chunk!["candidates"] as unknown[];
  const parts = ((cands[0] as Record<string, unknown>)["content"] as Record<string, unknown>)["parts"] as unknown[];
  assert.deepEqual(parts, []);
});

// ===== 非流式聚合 =====

test("ChunkCollector: 相邻同类 text 合并 / thought 独立成段", () => {
  const collector = new ChunkCollector();
  collector.add({ candidates: [{ content: { parts: [{ text: "Hello" }], role: "model" } }] });
  collector.add({ candidates: [{ content: { parts: [{ text: " world" }], role: "model" } }] });
  collector.add({ candidates: [{ content: { parts: [{ text: "thinking...", thought: true, thoughtSignature: "sig1" }], role: "model" } }] });
  collector.add({ candidates: [{ content: { parts: [{ text: "!" }], role: "model" } }], });
  collector.add({ candidates: [{ finishReason: "STOP", content: { parts: [], role: "model" } }], });
  collector.add({ usageMetadata: { promptTokenCount: 3, candidatesTokenCount: 5, totalTokenCount: 8 } });
  const resp = collector.buildResponse();
  assert.ok(resp);
  const cand = (resp!["candidates"] as unknown[])[0] as Record<string, unknown>;
  assert.equal(cand["finishReason"], "STOP");
  const parts = (cand["content"] as Record<string, unknown>)["parts"] as Record<string, unknown>[];
  assert.deepEqual(parts, [
    { text: "Hello world" },
    { text: "thinking...", thought: true, thoughtSignature: "sig1" },
    { text: "!" },
  ]);
  assert.deepEqual(resp!["usageMetadata"], { promptTokenCount: 3, candidatesTokenCount: 5, totalTokenCount: 8 });
});

test("ChunkCollector: 空流返回 null", () => {
  assert.equal(new ChunkCollector().buildResponse(), null);
});

// ===== countTokens 解析 =====

test("parseCountTokensResult: ui.countTokensV2 路径", () => {
  const n = parseCountTokensResult(
    JSON.stringify({ results: [{ data: { ui: { countTokensV2: { totalTokens: 42 } } } }] }),
  );
  assert.equal(n, 42);
});

test("parseCountTokensResult: data.countTokensV2 / data.countTokens 路径", () => {
  assert.equal(parseCountTokensResult(JSON.stringify({ results: [{ data: { countTokensV2: { totalTokens: 7 } } }] })), 7);
  assert.equal(parseCountTokensResult(JSON.stringify({ results: [{ data: { countTokens: { totalTokens: 9 } } }] })), 9);
});

test("parseCountTokensResult: 非法输入返回 0", () => {
  assert.equal(parseCountTokensResult("not json"), 0);
  assert.equal(parseCountTokensResult("{}"), 0);
});
