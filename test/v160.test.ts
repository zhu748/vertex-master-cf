// v1.6.0 回归：anon PVP 竞速调优 ——「429 是 per-IP 窗口抢占而非每日配额，
// 窗口秒级释放、可以抢到」（用户线上实测确认）。
// 基于该判断的参数调整（弹药 6 → 12 发）：
//   - ANON_CANDIDATE_BUDGET 6 → 12：4 轮 × 3 并发（子请求预算 24~36 < 50）；
//   - anonRoundCount 钳位 1~3 → 1~4 轮（复用 racing.rate_limit_retries，
//     KV 配套 rate_limit_retries=3 → 满配 4 轮）；
//   - ANON_TOTAL_RACE_DEADLINE_MS = 60s：整场墙钟硬限，防 4 轮 × 22s 单轮截止
//     叠加（最坏 ~88s）逼近 CF 代理 ~100s 首字节超时；单轮截止钳入剩余预算。
// 纯逻辑落在 racing.ts（anon/upstream.ts 依赖 cloudflare:sockets，Node 不可导入）。
import { test } from "node:test";
import assert from "node:assert/strict";
import { anonRoundCount, anonRoundDeadlineMs } from "../src/racing.ts";

// ===== anonRoundCount（整轮 429 重试轮数，钳 1~4） =====

test("v1.6.0 anonRoundCount: 未配置 → 默认 1 次重试 = 2 轮（v1.5.0 默认行为不变）", () => {
  assert.equal(anonRoundCount(undefined), 2);
  assert.equal(anonRoundCount(Number.NaN), 2);
});

test("v1.6.0 anonRoundCount: rate_limit_retries=3（PVP 推荐配置）→ 4 轮（v1.5.5 上限是 3，本次放宽）", () => {
  assert.equal(anonRoundCount(3), 4);
});

test("v1.6.0 anonRoundCount: 钳位 —— 0 → 1 轮；负数 → 1 轮；超上限 4+ → 4 轮；小数截断", () => {
  assert.equal(anonRoundCount(0), 1);
  assert.equal(anonRoundCount(-5), 1);
  assert.equal(anonRoundCount(4), 4);
  assert.equal(anonRoundCount(99), 4);
  assert.equal(anonRoundCount(2.9), 3);
});

test("v1.6.0 anonRoundCount: 旧默认 rate_limit_retries=1 → 2 轮；=2（v1.5.5 KV 值）→ 3 轮", () => {
  assert.equal(anonRoundCount(1), 2);
  assert.equal(anonRoundCount(2), 3);
});

// ===== anonRoundDeadlineMs（单轮内容前截止 = min(22s 上限, 整场剩余)，≥3s 保底） =====

test("v1.6.0 anonRoundDeadlineMs: 剩余预算充足 → 单轮上限 22s 生效（首轮常态）", () => {
  assert.equal(anonRoundDeadlineMs(60_000, 22_000), 22_000);
  assert.equal(anonRoundDeadlineMs(120_000, 22_000), 22_000);
});

test("v1.6.0 anonRoundDeadlineMs: 剩余预算不足 → 钳入剩余（后续轮次，总墙钟 60s 硬限）", () => {
  assert.equal(anonRoundDeadlineMs(10_000, 22_000), 10_000);
  assert.equal(anonRoundDeadlineMs(5_000, 22_000), 5_000);
});

test("v1.6.0 anonRoundDeadlineMs: 剩余极少 → 3s 保底（在飞轮至少给 3s，而非立即全灭）", () => {
  assert.equal(anonRoundDeadlineMs(500, 22_000), 3_000);
  assert.equal(anonRoundDeadlineMs(0, 22_000), 3_000);
  assert.equal(anonRoundDeadlineMs(-5_000, 22_000), 3_000);
});

// ===== PVP 语义回归：in-band 429 判定为 per-ip（换 IP 轮转可绕，而非每日配额硬失败） =====

test("v1.6.0 PVP 语义: gRPC code 8 'Resource has been exhausted' 文案 → per-ip 而非 project（抢得到）", async () => {
  const { classify429Body } = await import("../src/racing.ts");
  const body = JSON.stringify({
    errors: [
      {
        code: 8,
        message: "Resource has been exhausted (e.g. check quota)., source: anonymous",
      },
    ],
  });
  const cls = classify429Body(body);
  assert.equal(cls.level, "per-ip");
});
