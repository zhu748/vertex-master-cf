// v1.6.2 回归：anon PVP 候选排序 —— 「429 = 活着但在竞争」。
// 背景（线上实测）：通用 healthScore 的 429 计次降权把反复撞 429 的活节点压到
// 未测试节点（80 分）之下，竞速宁可去撞 95% 死亡率的未测试节点（recaptcha
// 阶段即死 → 非 429 relay → 整轮 break → 客户端拿 502），也不复用已知活节点。
import { test } from "node:test";
import assert from "node:assert/strict";
import { anonPvpCandidateOrder, emptyHealth, type ProxyHealth } from "../src/racing.ts";

interface Node {
  raw: string;
}

const NOW = 1_800_000_000_000; // 固定当前时刻（epoch ms）

function h(partial: Partial<ProxyHealth>): ProxyHealth {
  return { ...emptyHealth(), ...partial };
}

function live(fresh = true): ProxyHealth {
  return h({ success: 5, last_success_at: NOW / 1000 - 60, avg_ms: 800, last_ms: 800 });
}

function rateLimited429(count: number, cooldownRemainSec: number): ProxyHealth {
  // 撞 429 的活节点：有成功史 + rate_limit_count 高 + 冷却中
  return h({
    success: 5,
    last_success_at: NOW / 1000 - 120,
    avg_ms: 900,
    rate_limit_count: count,
    last_fail_at: NOW / 1000 - 5,
    cooldown_until: NOW / 1000 + cooldownRemainSec,
    last_error: "429 Rate Limit",
  });
}

function deadCooling(): ProxyHealth {
  return h({
    fail: 6,
    consec_fail: 6,
    last_fail_at: NOW / 1000 - 10,
    cooldown_until: NOW / 1000 + 1700,
    last_error: "connect timeout",
  });
}

test("v1.6.2 活节点（429 计次再高）排在未测试节点之前 —— 修掉 502 no-content 回退", () => {
  const entries: Node[] = [
    { raw: "untested-1" },
    { raw: "untested-2" },
    { raw: "hot429" }, // rate_limit_count=6 → 通用评分 ~65 < 80（旧版被未测试压住）
  ];
  const map = new Map<string, ProxyHealth>([["hot429", rateLimited429(6, 25)]]);
  const out = anonPvpCandidateOrder(entries, map, NOW, 3);
  // 未冷却的活节点第一，但这里 hot429 在冷却 → 层 2；未测试层 3 —— 仍在其后
  assert.deepEqual(
    out.map((e) => e.raw),
    ["hot429", "untested-1", "untested-2"],
  );
});

test("v1.6.2 层 1（未冷却活节点）→ 层 2（429 冷却活节点）→ 层 3（未测试）", () => {
  const entries: Node[] = [
    { raw: "untested" },
    { raw: "cool-429" },
    { raw: "alive" },
  ];
  const map = new Map<string, ProxyHealth>([
    ["alive", live()],
    ["cool-429", rateLimited429(3, 20)],
  ]);
  const out = anonPvpCandidateOrder(entries, map, NOW, 3);
  assert.deepEqual(
    out.map((e) => e.raw),
    ["alive", "cool-429", "untested"],
  );
});

test("v1.6.2 层 2 按最早恢复排序（跨请求天然轮转 28 个活节点）", () => {
  const entries: Node[] = [{ raw: "late" }, { raw: "early" }, { raw: "mid" }];
  const map = new Map<string, ProxyHealth>([
    ["early", rateLimited429(1, 5)],
    ["mid", rateLimited429(2, 15)],
    ["late", rateLimited429(3, 25)],
  ]);
  const out = anonPvpCandidateOrder(entries, map, NOW, 3);
  assert.deepEqual(
    out.map((e) => e.raw),
    ["early", "mid", "late"],
  );
});

test("v1.6.2 连败死节点（无近期成功史）冷却中垫底 —— 未测试优先于死节点探索", () => {
  const entries: Node[] = [{ raw: "dead" }, { raw: "fresh-untested" }];
  const map = new Map<string, ProxyHealth>([["dead", deadCooling()]]);
  const out = anonPvpCandidateOrder(entries, map, NOW, 2);
  assert.deepEqual(
    out.map((e) => e.raw),
    ["fresh-untested", "dead"],
  );
});

test("v1.6.2 陈旧成功（>6h）冷却节点不进层 2（与 healthScore 新鲜度衰减语义对齐）", () => {
  const entries: Node[] = [{ raw: "stale-live" }, { raw: "untested" }];
  const map = new Map<string, ProxyHealth>([
    ["stale-live", h({ success: 2, last_success_at: NOW / 1000 - 7 * 3600, cooldown_until: NOW / 1000 + 10 })],
  ]);
  const out = anonPvpCandidateOrder(entries, map, NOW, 2);
  assert.deepEqual(
    out.map((e) => e.raw),
    ["untested", "stale-live"],
  );
});

test("v1.6.2 limit 截断 + 去重", () => {
  const entries: Node[] = [
    { raw: "a" },
    { raw: "a" },
    { raw: "b" },
    { raw: "c" },
  ];
  const map = new Map<string, ProxyHealth>([
    ["a", live()],
    ["b", live()],
    ["c", live()],
  ]);
  const out = anonPvpCandidateOrder(entries, map, NOW, 2);
  assert.equal(out.length, 2);
  assert.equal(new Set(out.map((e) => e.raw)).size, 2);
});
