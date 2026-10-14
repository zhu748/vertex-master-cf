// v1.3.0 改动回归测试：
//   1. Anthropic 协议级 ping 保活（fakestream.anthropicPingFrame + sse.ts 协议感知 pingFrame）
//   2. thinking levelToBudget HIGH → 24576（对齐 Go budgetForThinkingLevel default 分支）
//   3. runBounded 有界并发原语（保序 / 限并发 / fail-fast）
//   4. normalizeThinkingConfig 对 3.7/3.8-flash 的三档语义（NONE/MINIMAL → LOW）
import { test } from "node:test";
import assert from "node:assert/strict";
import { anthropicPingFrame, ssePingFrame } from "../src/fakestream.ts";
import { sseResponseFromGenerator } from "../src/handlers/sse.ts";
import { runBounded } from "../src/bounded.ts";
import {
  budgetToLevel,
  levelToBudget,
  normalizeThinkingConfig,
  reasoningEffortToLevel,
  thinkingKindForModel,
} from "../src/thinking.ts";

// ===== G1：Anthropic 协议级 ping =====

test("v1.3.0 anthropicPingFrame 输出 Anthropic 协议 ping 事件（对齐 Go emitPing）", () => {
  assert.equal(anthropicPingFrame(), "event: ping\ndata: {}\n\n");
});

test("v1.3.0 ssePingFrame 保持 SSE 注释帧（默认非 Anthropic 协议）", () => {
  assert.equal(ssePingFrame(), ": ping\n\n");
});

test("v1.3.0 sseResponseFromGenerator 自定义 pingFrame：慢流期间注入协议级 ping 帧", async () => {
  async function* slowGen(): AsyncGenerator<string> {
    yield "event: message_start\ndata: {}\n\n";
    // 模拟长思考：上游 80ms 不发内容（测试用小间隔 ping）
    await new Promise((r) => setTimeout(r, 80));
    yield "event: message_stop\ndata: {}\n\n";
  }
  const resp = sseResponseFromGenerator(slowGen(), null, undefined, {
    pingFrame: anthropicPingFrame,
    pingIntervalMs: 20,
  });
  assert.equal(resp.status, 200);
  const text = await new Response(resp.body).text();
  // 数据帧完整保留
  assert.ok(text.includes("event: message_start"));
  assert.ok(text.includes("event: message_stop"));
  // 至少注入了一个协议级 ping（80ms / 20ms ≥ 3 个窗口，留足余量断言 ≥1）
  assert.ok(text.includes("event: ping\ndata: {}\n\n"), "应包含 Anthropic ping 事件帧");
});

test("v1.3.0 sseResponseFromGenerator 默认仍为注释帧 ping，且快流不打扰数据帧", async () => {
  async function* fastGen(): AsyncGenerator<string> {
    yield "data: a\n\n";
    yield "data: b\n\n";
  }
  const resp = sseResponseFromGenerator(fastGen(), null, undefined, { pingIntervalMs: 20 });
  const text = await new Response(resp.body).text();
  assert.equal(text, "data: a\n\ndata: b\n\n");
});

// ===== G2：levelToBudget HIGH → 24576（Go 语义对齐）=====

test("v1.3.0 levelToBudget HIGH 在全部 2.5 系返回 24576（对齐 Go budgetForThinkingLevel）", () => {
  assert.equal(levelToBudget("HIGH", "budget-pro"), 24576);
  assert.equal(levelToBudget("HIGH", "budget-flash"), 24576);
  assert.equal(levelToBudget("HIGH", "budget-flash-lite"), 24576);
});

test("levelToBudget 其余档位维持原映射（回归）", () => {
  assert.equal(levelToBudget("NONE", "budget-pro"), 128); // pro 无法关闭
  assert.equal(levelToBudget("NONE", "budget-flash"), 0);
  assert.equal(levelToBudget("MINIMAL", "budget-flash"), 1024);
  assert.equal(levelToBudget("LOW", "budget-pro"), 1024);
  assert.equal(levelToBudget("MEDIUM", "budget-flash"), 8192);
});

test("levelToBudget 与 budgetToLevel 往返一致（24576 → HIGH）", () => {
  for (const kind of ["budget-pro", "budget-flash", "budget-flash-lite"] as const) {
    assert.equal(budgetToLevel(levelToBudget("HIGH", kind)), "HIGH");
  }
});

// ===== 3.7/3.8-flash 三档思考语义（对齐 Go thinkingKindForModel）=====

test("3.7/3.8-flash 归类为 levels-no-minimal（NONE/MINIMAL 折叠为 LOW）", () => {
  assert.equal(thinkingKindForModel("gemini-3.7-flash"), "levels-no-minimal");
  assert.equal(thinkingKindForModel("gemini-3.8-flash"), "levels-no-minimal");
  assert.equal(thinkingKindForModel("fake-gemini-3.8-flash"), "levels-no-minimal"); // 假流式前缀防御性剥离
  const out = normalizeThinkingConfig("gemini-3.8-flash", { thinkingLevel: "MINIMAL" });
  assert.deepEqual(out, { thinkingLevel: "LOW" });
  const none = normalizeThinkingConfig("gemini-3.7-flash", { thinkingLevel: "NONE" });
  assert.deepEqual(none, { thinkingLevel: "LOW" });
});

test("3.6-flash 保持四档（MINIMAL 原样保留）", () => {
  assert.equal(thinkingKindForModel("gemini-3.6-flash"), "levels-all");
  const out = normalizeThinkingConfig("gemini-3.6-flash", { thinkingLevel: "MINIMAL" });
  assert.deepEqual(out, { thinkingLevel: "MINIMAL" });
});

test("reasoning_effort auto/default 不设置 thinkingConfig（模型用官方默认行为）", () => {
  assert.equal(reasoningEffortToLevel("auto"), null);
  assert.equal(reasoningEffortToLevel("default"), null);
  assert.equal(reasoningEffortToLevel(""), null);
  assert.equal(reasoningEffortToLevel("xhigh"), "HIGH");
  assert.equal(reasoningEffortToLevel("max"), "HIGH");
});

// ===== O2：runBounded 有界并发原语 =====

test("runBounded 结果按下标保序（并发完成顺序无关）", async () => {
  const out = await runBounded(8, 3, async (i) => {
    // 偶数下标人为更慢，制造完成顺序与下标顺序不一致
    await new Promise((r) => setTimeout(r, i % 2 === 0 ? 20 : 2));
    return i * 10;
  });
  assert.deepEqual(out, [0, 10, 20, 30, 40, 50, 60, 70]);
});

test("runBounded 严格限制同时在飞数量", async () => {
  let inFlight = 0;
  let peak = 0;
  await runBounded(10, 3, async () => {
    inFlight++;
    peak = Math.max(peak, inFlight);
    await new Promise((r) => setTimeout(r, 5));
    inFlight--;
    return null;
  });
  assert.ok(peak <= 3, "峰值并发不得超过 limit，实际 " + peak);
  assert.ok(peak >= 2, "应观察到真实并发，实际 " + peak);
});

test("runBounded fail-fast：首个错误抛出，后续任务不再领取", async () => {
  let started = 0;
  await assert.rejects(
    runBounded(20, 2, async (i) => {
      started++;
      await new Promise((r) => setTimeout(r, 5));
      if (i === 0) throw new Error("boom");
      return i;
    }),
    /boom/,
  );
  // 2 个 worker：worker-0 在 i=0 抛错；worker-1 已领取 i=1 完成后停止领取
  assert.ok(started <= 4, "错误后不应继续领取大量任务，实际启动 " + started);
});

test("runBounded count=0 返回空数组、limit 钳位", async () => {
  assert.deepEqual(await runBounded(0, 3, async () => 1), []);
  assert.deepEqual(await runBounded(2, 99, async (i) => i), [0, 1]);
});
