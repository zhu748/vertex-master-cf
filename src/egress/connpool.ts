// connpool.ts —— HTTP/1.1 keep-alive 连接复用池。
//
// 同一（代理, 上游）组合的 TLS 隧道建立成本高（TCP + 代理握手 + TLS 1.3 握手 ≈
// 数百毫秒到数秒），池化后命中即跳过全部握手。语义（借鉴 cf-vproxy v2.2，重新实现）：
//   - acquire(proxyRaw, target)：取回空闲暖连接（FIFO），池按 proxy|target 分桶；
//   - release(conn, reusable)：响应体完整消费且可复用 → 回池；否则关闭；
//   - discard(conn)：请求失败 / 中途出错 → 关闭并从池中移除；
//   - 池容量上限 + 空闲 TTL，超限/过期连接在每次取用时惰性清理；
//   - isWarm(proxyRaw)：健康候选排序用 —— 有暖连接的节点优先参与竞速。
//
// 纯内存（isolate 生命周期），无 KV、无平台 API，可在 Node 单测。

export interface PooledConn {
  /** 所属代理原始串（健康表键） */
  proxyRaw: string;
  /** 上游 host:port */
  target: string;
  /** 关闭底层 TLS/socket（丢弃连接时调用） */
  close(): void;
  /** 连接元数据（诊断） */
  created: number;
  lastUsed: number;
  uses: number;
}

export const MAX_POOL = 128;
export const IDLE_TTL_MS = 120_000;

const buckets = new Map<string, PooledConn[]>();

function bucketKey(proxyRaw: string, target: string): string {
  return proxyRaw + "|" + target;
}

function closeConn(conn: PooledConn): void {
  try {
    conn.close();
  } catch {
    // ignore
  }
}

function evictExpired(now: number): void {
  for (const [key, conns] of buckets) {
    const kept = conns.filter((c) => {
      if (now - c.lastUsed > IDLE_TTL_MS) {
        closeConn(c);
        return false;
      }
      return true;
    });
    if (kept.length === 0) buckets.delete(key);
    else buckets.set(key, kept);
  }
}

function totalConns(): number {
  let n = 0;
  for (const conns of buckets.values()) n += conns.length;
  return n;
}

/** 取一条空闲暖连接（无则 null）；取出的连接从池中移除（由请求全权处置） */
export function acquire(proxyRaw: string, target: string): PooledConn | null {
  const key = bucketKey(proxyRaw, target);
  const conns = buckets.get(key);
  if (!conns || conns.length === 0) return null;
  const conn = conns.shift()!;
  if (conns.length === 0) buckets.delete(key);
  conn.uses += 1;
  return conn;
}

/** 取用前顺手清理过期连接（每个请求至多触发一次全池扫描，池很小、开销可忽略） */
export function acquireWithSweep(proxyRaw: string, target: string): PooledConn | null {
  evictExpired(Date.now());
  return acquire(proxyRaw, target);
}

/** 归还连接（reusable=false 时关闭） */
export function release(conn: PooledConn, reusable: boolean): void {
  if (!reusable) {
    discard(conn);
    return;
  }
  conn.lastUsed = Date.now();
  if (Date.now() - conn.created > IDLE_TTL_MS || totalConns() >= MAX_POOL) {
    discard(conn);
    return;
  }
  const key = bucketKey(conn.proxyRaw, conn.target);
  const conns = buckets.get(key);
  if (conns) conns.push(conn);
  else buckets.set(key, [conn]);
}

/** 丢弃连接（关闭底层资源并出池） */
export function discard(conn: PooledConn): void {
  closeConn(conn);
  const key = bucketKey(conn.proxyRaw, conn.target);
  const conns = buckets.get(key);
  if (!conns) return;
  const idx = conns.indexOf(conn);
  if (idx >= 0) conns.splice(idx, 1);
  if (conns.length === 0) buckets.delete(key);
}

/** 该代理当前是否有暖连接（竞速候选排序用） */
export function isWarm(proxyRaw: string): boolean {
  for (const [key, conns] of buckets) {
    if (key.startsWith(proxyRaw + "|") && conns.length > 0) return true;
  }
  return false;
}

export interface ConnPoolStats {
  idleConns: number;
  warmProxies: number;
  conns: Array<{ proxy: string; target: string; uses: number; idleMs: number }>;
}

/** 池状态（面板/诊断用；对齐 cf-vproxy connPool.stats 语义） */
export function stats(): ConnPoolStats {
  const now = Date.now();
  const conns: ConnPoolStats["conns"] = [];
  const warmProxies = new Set<string>();
  for (const [key, list] of buckets) {
    const proxy = key.slice(0, key.lastIndexOf("|"));
    for (const c of list) {
      conns.push({ proxy: c.proxyRaw, target: c.target, uses: c.uses, idleMs: now - c.lastUsed });
      warmProxies.add(c.proxyRaw);
    }
  }
  return { idleConns: conns.length, warmProxies: warmProxies.size, conns };
}

/** 池状态（面板/诊断用） */
export function poolStats(): { buckets: number; conns: number } {
  return { buckets: buckets.size, conns: totalConns() };
}

export { stats as connPoolStats };

/** 清空（测试用） */
export function clearForTest(): void {
  for (const conns of buckets.values()) for (const c of conns) closeConn(c);
  buckets.clear();
}
