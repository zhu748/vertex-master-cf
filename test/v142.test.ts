// v1.4.2 回归测试：healthScore 成功记录新鲜度衰减
// 背景：线上事故 —— KV 健康快照里残留 16 天前的 success=2 记录（快照 at 字段被
// cron 反复刷新），陈旧成功把早已死掉的节点顶到候选最前，anon 接力在死节点上
// 耗尽（CONNECT 挂起 + 无超时的 body 读）。修复：成功侧加分 6h 全额 → 24h 线性
// 衰减到 0；基础分 60 与失败/限流惩罚不衰减。
import { test } from "node:test";
import assert from "node:assert/strict";
import { healthScore, emptyHealth, selectCandidates, DEFAULT_RACING, type ProxyHealth } from "../src/racing.ts";

const NOW = 1_700_000_000_000; // epoch ms
const SEC = Math.floor(NOW / 1000);

function mkHealth(p: Partial<ProxyHealth>): ProxyHealth {
  return { ...emptyHealth(), ...p };
}

function rc(p: Partial<typeof DEFAULT_RACING> = {}) {
  return { ...DEFAULT_RACING, ...p };
}

test("v1.4.2 healthScore：新鲜成功全额计分（6h 宽限期内 ageFactor=1）", () => {
  const fresh = mkHealth({ success: 10, fail: 0, avg_ms: 200, last_success_at: SEC });
  // 60 + 20*1 + clamp(20-4)=16 = 96
  assert.equal(healthScore(fresh, NOW), 96);

  const graceEdge = mkHealth({ success: 10, fail: 0, avg_ms: 200, last_success_at: SEC - 6 * 3600 });
  assert.equal(healthScore(graceEdge, NOW), 96, "恰好 6h：仍在宽限期，全额");
});

test("v1.4.2 healthScore：陈旧成功衰减到未测试档（80）之下", () => {
  // 16 天前的 success=2（线上事故的实际形态）
  const stale = mkHealth({ success: 2, fail: 0, avg_ms: 766, last_success_at: SEC - 16 * 86400 });
  const s = healthScore(stale, NOW);
  assert.equal(s, 60, "成功侧加分归零，只剩基础分 60");
  assert.ok(s < 80, "低于未测试档 80 → 接力顺序让位给未测试节点");

  // 24h 边界（与 HEALTH_TTL_SEC 对齐）
  const dayOld = mkHealth({ success: 5, fail: 0, avg_ms: 100, sticky: true, last_success_at: SEC - 24 * 3600 });
  assert.equal(healthScore(dayOld, NOW), 60, "恰好 24h：ageFactor=0，粘性/延迟加分一并清零");
});

test("v1.4.2 healthScore：6h~24h 线性衰减（15h 中点 ageFactor=0.5）", () => {
  const mid = mkHealth({ success: 4, fail: 0, avg_ms: 200, last_success_at: SEC - 15 * 3600 });
  // 成功侧加分 = 20*1 + 16 = 36 → 36*0.5 = 18 → 60+18 = 78
  assert.ok(Math.abs(healthScore(mid, NOW) - 78) < 1e-9);
});

test("v1.4.2 healthScore：失败/限流惩罚不随成功记录老化而衰减", () => {
  const staleBad = mkHealth({ success: 2, fail: 8, consec_fail: 3, last_success_at: SEC - 16 * 86400 });
  // 60 - 8*3 = 36（连败惩罚全额保留）
  assert.equal(healthScore(staleBad, NOW), 36);

  const staleRL = mkHealth({ success: 2, fail: 0, rate_limit_count: 4, last_success_at: SEC - 16 * 86400 });
  // 60 - 5*4 = 40
  assert.equal(healthScore(staleRL, NOW), 40);

  // 从未成功过（last_success_at=0）→ ageFactor=0：fail-only 节点 = 60 - 惩罚
  const failOnly = mkHealth({ fail: 10, consec_fail: 2, last_fail_at: SEC });
  assert.equal(healthScore(failOnly, NOW), 60 - 16);
});

test("v1.4.2 端到端排序：新鲜好节点 > 未测试 > 陈旧干净 > 陈旧连败", () => {
  const health = new Map<string, ProxyHealth>();
  const freshGood = mkHealth({ success: 6, fail: 0, avg_ms: 300, last_success_at: SEC });
  const staleClean = mkHealth({ success: 2, fail: 0, avg_ms: 700, last_success_at: SEC - 16 * 86400 });
  const staleBad = mkHealth({ success: 2, fail: 5, consec_fail: 5, last_success_at: SEC - 16 * 86400 });
  health.set("socks5://fresh:1080", freshGood);
  health.set("socks5://stale-clean:1080", staleClean);
  health.set("socks5://stale-bad:1080", staleBad);
  // 未测试节点无健康记录

  const entries = [
    { raw: "socks5://stale-bad:1080" },
    { raw: "socks5://untested-a:1080" },
    { raw: "socks5://stale-clean:1080" },
    { raw: "socks5://fresh:1080" },
    { raw: "socks5://untested-b:1080" },
  ];
  const picked = selectCandidates(entries as never, health, rc({ top_k: 3, max_attempts: 8 }), NOW);

  const pos = (raw: string): number => picked.findIndex((e: { raw: string }) => e.raw === raw);
  assert.equal(picked[0].raw, "socks5://fresh:1080", "新鲜好节点第一");
  // 未测试节点（探索名额）排在陈旧节点之前
  assert.ok(picked.slice(1).some((e: { raw: string }) => e.raw.startsWith("socks5://untested")));
  // 前三名（anon 接力 orderedCandidates 取 top-K 的对应语义）不含陈旧连败节点
  assert.ok(!picked.slice(0, 3).some((e: { raw: string }) => e.raw === "socks5://stale-bad:1080"), "陈旧连败节点不进前三");
  assert.ok(pos("socks5://untested-a:1080") < pos("socks5://stale-bad:1080"), "未测试先于陈旧连败");
  assert.ok(pos("socks5://stale-clean:1080") < pos("socks5://stale-bad:1080"), "陈旧干净先于陈旧连败");
});
