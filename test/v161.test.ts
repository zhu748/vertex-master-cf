// v1.6.1 回归：巡检模式感知 —— anon 模式探测目标改为双主机（anchor@www.google.com
// + batchGraphql@cloudconsole-pa），批量钳位 20 保子请求限额。
// 背景（线上 tail 实测）：旧巡检探测 generativelanguage.googleapis.com（official
// 主机）对 anon 失真 —— 节点过 official 探测却在 recaptcha 首跳就死（socks5
// CONNECT rep=5 / 隧道 TLS handshake_failure），12 发竞速弹药浪费在死节点上。
// 纯逻辑：sweepBatchCap（批量钳位）；探测本体 testProxyAnon 依赖
// cloudflare:sockets，仅 Workers 运行时可跑（线上验证）。
import { test } from "node:test";
import assert from "node:assert/strict";
import { sweepBatchCap } from "../src/racing.ts";

test("v1.6.1 sweepBatchCap: anon 模式 → 20（双主机探测 ≤2 子请求/节点，20×2+KV ≈ 44 < 50）", () => {
  assert.equal(sweepBatchCap("anon"), 20);
});

test("v1.6.1 sweepBatchCap: official 模式 → 40（单探测维持原上限）", () => {
  assert.equal(sweepBatchCap("official"), 40);
});

test("v1.6.1 sweepBatchCap: 未知/空模式按 official 语义兜底 40", () => {
  assert.equal(sweepBatchCap(""), 40);
  assert.equal(sweepBatchCap("something-else"), 40);
});

// anchor 探测 URL 构造（recaptcha.ts 为纯常量拼接，无 Workers 依赖，可直接导入）
test("v1.6.1 anchorProbeUrl: 与 fetchOnce 逐字节同参（site key / co / v / cb 后缀）", async () => {
  const { anchorProbeUrl } = await import("../src/anon/payload.ts");
  const url = anchorProbeUrl("abc123XYZ0");
  assert.ok(url.startsWith("https://www.google.com/recaptcha/enterprise/anchor?ar=1&k=6LdCjtspAAAAAMcV4TGdWLJqRTEk1TfpdLqEnKdj"));
  assert.ok(url.includes("co=aHR0cHM6Ly9jb25zb2xlLmNsb3VkLmdvb2dsZS5jb206NDQz"));
  assert.ok(url.includes("v=jdMmXeCQEkPbnFDy9T04NbgJ"));
  assert.ok(url.endsWith("&size=invisible&anchor-ms=20000&execute-ms=15000&cb=abc123XYZ0"));
});
