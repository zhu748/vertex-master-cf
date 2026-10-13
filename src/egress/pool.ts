// pool.ts —— 代理条目解析 / 清洗 / 订阅拉取。
//
// 行为（对齐 cf-vproxy proxyfetch.ts + frames.ts，重新实现）：
//   - 支持协议：socks5://（含 socks5h/socks 别名）、socks4://（含 socks4a）、http://；
//   - https:// 代理一律剔除（Workers 无法 TLS-in-TLS）；vmess/vless/ss/trojan 等
//     同样在入口剔除并计数；
//   - 订阅链接：纯文本或整段 Base64 的 `type://host:port` 行式列表；结果缓存 KV，
//     按 subscription_refresh_minutes 惰性刷新（过期先返回旧数据、后台刷新）；
//   - MAX_PROXIES 1000：KV 值上限 25MiB，1000 节点仅 ~50KB，不构成压力。
import type { Env } from "../config.ts";
import type { VProxyConfig } from "../types.ts";
import type { ProxyEndpoint } from "./handshake.ts";

export type ProxyKind = "socks5" | "socks4" | "http";
export type ProxyEntry = ProxyEndpoint & { kind: ProxyKind };

export const SUB_CACHE_KEY = "proxy_cache";
export const MAX_PROXIES = 1000;
export const SUB_FETCH_TIMEOUT_MS = 45_000;

const SUPPORTED_SCHEMES = new Set(["socks4", "socks4a", "socks5", "socks5h", "socks", "http"]);

/** 解析一行代理 URL；裸 host:port 视为 socks5。解析失败返回 null。 */
export function parseProxyUrl(input: string): ProxyEntry | null {
  let s = input.trim();
  if (!s) return null;
  if (!/^[a-z][a-z0-9+.-]*:\/\//i.test(s)) s = "socks5://" + s;
  let u: URL;
  try {
    u = new URL(s);
  } catch {
    return null;
  }
  const scheme = u.protocol.replace(":", "").toLowerCase();
  let kind: ProxyKind | null = null;
  if (scheme === "socks5" || scheme === "socks5h" || scheme === "socks") kind = "socks5";
  else if (scheme === "socks4" || scheme === "socks4a") kind = "socks4";
  else if (scheme === "http") kind = "http";
  if (!kind) return null;
  const host = u.hostname;
  const port = u.port ? Number(u.port) : kind === "http" ? 80 : 1080;
  if (!host || !Number.isInteger(port) || port <= 0 || port > 65535) return null;
  return {
    kind,
    host,
    port,
    username: u.username ? decodeURIComponent(u.username) : undefined,
    password: u.password ? decodeURIComponent(u.password) : undefined,
    raw: s,
  };
}

export type ProxyLineVerdict = { status: "ok"; entry: ProxyEntry } | { status: "unsupported"; reason: string };

/** 入口分类：可用 / 不支持（含原因）。入口即拦截 Workers 做不了的协议。 */
export function classifyProxyLine(input: string): ProxyLineVerdict {
  const s = input.trim();
  if (!s) return { status: "unsupported", reason: "空行" };
  const m = /^([a-z][a-z0-9+.-]*):\/\//i.exec(s);
  const scheme = (m ? m[1] : "socks5").toLowerCase();
  if (scheme === "https" || scheme === "tls" || scheme === "ssl") {
    return { status: "unsupported", reason: "https 代理在 Workers 上不可用（无法 TLS-in-TLS），已自动剔除" };
  }
  if (!SUPPORTED_SCHEMES.has(scheme)) {
    return { status: "unsupported", reason: "不支持的协议 " + scheme + "://" };
  }
  const entry = parseProxyUrl(s);
  if (!entry) return { status: "unsupported", reason: "无法解析（缺少 host/port 或端口非法）" };
  return { status: "ok", entry };
}

/** 批量清洗代理列表：剔除不支持条目 + 去重 */
export function cleanProxyList(list: string[]): {
  kept: string[];
  removed: Array<{ raw: string; reason: string }>;
} {
  const kept: string[] = [];
  const seen = new Set<string>();
  const removed: Array<{ raw: string; reason: string }> = [];
  for (const line of list) {
    const s = line.trim();
    if (!s) continue;
    const v = classifyProxyLine(s);
    if (v.status !== "ok") {
      removed.push({ raw: s, reason: v.reason });
      continue;
    }
    const dedup = v.entry.kind + "|" + v.entry.host + "|" + v.entry.port + "|" + (v.entry.username ?? "");
    if (seen.has(dedup)) continue;
    seen.add(dedup);
    kept.push(v.entry.raw);
  }
  return { kept, removed };
}

// ---------- 代理池 ----------

export class ProxyPool {
  readonly entries: ProxyEntry[];
  readonly removed: Array<{ raw: string; reason: string }>;

  constructor(entries: ProxyEntry[], removed: Array<{ raw: string; reason: string }> = []) {
    this.entries = entries;
    this.removed = removed;
  }

  get size(): number {
    return this.entries.length;
  }

  static fromStrings(strs: string[]): ProxyPool {
    const seen = new Set<string>();
    const entries: ProxyEntry[] = [];
    const removed: Array<{ raw: string; reason: string }> = [];
    for (const s of strs) {
      const v = classifyProxyLine(s);
      if (v.status !== "ok") {
        removed.push({ raw: s.trim(), reason: v.reason });
        continue;
      }
      const p = v.entry;
      const dedup = p.kind + "|" + p.host + "|" + p.port + "|" + (p.username ?? "");
      if (seen.has(dedup)) continue;
      seen.add(dedup);
      entries.push(p);
    }
    return new ProxyPool(entries, removed);
  }
}

// ---------- 订阅 ----------

interface SubCache {
  at: number; // epoch ms
  proxies: string[];
  skipped: number;
}

/** 解析订阅文本（纯文本 / 整段 Base64）→ 代理行列表 */
export function decodeSubscription(text: string): string[] {
  let content = text.trim();
  if (!content.includes("://")) {
    try {
      const bin = atob(content.replace(/\s+/g, ""));
      if (bin.includes("://")) content = bin;
    } catch {
      // 不是 Base64，按纯文本处理
    }
  }
  return content
    .split(/[\r\n]+/)
    .map((l) => l.trim())
    .filter((l) => l.length > 0 && !l.startsWith("#"));
}

/** 拉取订阅并写 KV 缓存；失败返回 null（保留旧缓存） */
export async function refreshSubscription(env: Env, url: string): Promise<SubCache | null> {
  try {
    const resp = await fetch(url, {
      headers: { "User-Agent": "vertex-master-cf/1.0" },
      signal: AbortSignal.timeout(SUB_FETCH_TIMEOUT_MS),
    });
    if (!resp.ok) throw new Error("HTTP " + resp.status);
    const lines = decodeSubscription(await resp.text());
    const proxies: string[] = [];
    let skipped = 0;
    for (const line of lines) {
      const v = classifyProxyLine(line);
      if (v.status === "ok") {
        proxies.push(v.entry.raw);
        if (proxies.length >= MAX_PROXIES) break;
      } else if (line.includes("://")) {
        skipped += 1;
      }
    }
    const cache: SubCache = { at: Date.now(), proxies, skipped };
    await env.VPROXY_KV.put(SUB_CACHE_KEY, JSON.stringify(cache));
    subMem = { at: Date.now(), cache };
    return cache;
  } catch {
    return null;
  }
}

// isolate 内存缓存（30s TTL；KV 读失败回退旧值）与池解析缓存
const SUB_MEM_TTL_MS = 30_000;
let subMem: { at: number; cache: SubCache } | null = null;
let poolCache: { key: string; pool: ProxyPool } | null = null;

/** KV 订阅缓存时间戳（epoch ms；无缓存/读失败返回 0）—— cron 判新鲜度用 */
export async function subscriptionCachedAt(env: Env): Promise<number> {
  try {
    const cache = (await env.VPROXY_KV.get(SUB_CACHE_KEY, "json")) as SubCache | null;
    return cache && typeof cache.at === "number" ? cache.at : 0;
  } catch {
    return 0;
  }
}

/** 静态列表 + 订阅列表 → ProxyPool（带双层缓存：订阅内存缓存 + 池解析缓存） */
export async function resolveProxyPool(
  env: Env,
  cfg: VProxyConfig,
  waitUntil: (p: Promise<unknown>) => void,
): Promise<ProxyPool> {
  const staticList = [...cfg.proxies];
  if (cfg.subscription) {
    const ttlMs = Math.max(5, cfg.subscription_refresh_minutes) * 60_000;
    let cache: SubCache | null = null;
    const memFresh = subMem !== null && Date.now() - subMem.at < Math.min(SUB_MEM_TTL_MS, ttlMs);
    if (memFresh) {
      cache = subMem!.cache;
    } else {
      let kvReadOk = true;
      try {
        cache = (await env.VPROXY_KV.get(SUB_CACHE_KEY, "json")) as SubCache | null;
      } catch {
        cache = null;
        kvReadOk = false;
      }
      if (cache) {
        subMem = { at: Date.now(), cache };
      } else if (!kvReadOk && subMem) {
        cache = subMem.cache; // KV 读失败：回退内存旧值（可用性优先）
      }
    }
    const fresh = cache && Date.now() - cache.at < ttlMs;
    if (!cache) {
      const got = await refreshSubscription(env, cfg.subscription);
      if (got) staticList.push(...got.proxies);
    } else {
      staticList.push(...cache.proxies);
      if (!fresh) {
        waitUntil(refreshSubscription(env, cfg.subscription)); // 过期：后台刷新，本次用旧数据
      }
    }
  }
  const poolKey = staticList.join("\n");
  if (poolCache && poolCache.key === poolKey) return poolCache.pool;
  const pool = ProxyPool.fromStrings(staticList);
  poolCache = { key: poolKey, pool };
  if (pool.removed.length > 0) {
    console.log("[egress] auto-removed unsupported proxy links:", JSON.stringify(pool.removed));
  }
  return pool;
}

/** 手动触发订阅刷新（管理端点用） */
export async function forceRefreshSubscription(env: Env, cfg: VProxyConfig): Promise<SubCache | null> {
  if (!cfg.subscription) return null;
  const got = await refreshSubscription(env, cfg.subscription);
  poolCache = null; // 池解析缓存失效，下次请求重建
  return got;
}
