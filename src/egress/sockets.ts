// sockets.ts —— cloudflare:sockets TCP 连接封装。
//
// 仅此文件 import "cloudflare:sockets"（以及依赖它的握手/请求模块只能在 Workers
// 运行时加载；Node 单测只覆盖 egress/ 下的纯逻辑模块）。
import { connect } from "cloudflare:sockets";

export interface TcpConn {
  readonly readable: ReadableStream<Uint8Array>;
  readonly writable: WritableStream<Uint8Array>;
  readonly opened: Promise<void>;
  close(): Promise<void>;
}

/** 建立到 host:port 的 TCP 连接（Workers outbound TCP） */
export function tcpConnect(hostname: string, port: number): TcpConn {
  const socket = connect({ hostname, port });
  const opened = socket.opened.then(
    () => undefined,
    (err) => {
      throw err;
    },
  );
  return {
    readable: socket.readable,
    writable: socket.writable,
    opened,
    close: async () => {
      try {
        await socket.close();
      } catch {
        // ignore
      }
    },
  };
}

export function closeQuietly(c: { close(): Promise<void> } | null | undefined): void {
  try {
    if (c) void c.close();
  } catch {
    // ignore
  }
}

/** withTimeout：给 promise 加超时；超时后底层 promise 的 rejection 会被 no-op 吸收
 *  （避免竞速败者的悬挂 promise 变成 unhandled rejection 记入 workerd 异常日志）。 */
export function withTimeout<T>(p: Promise<T>, ms: number, message: string): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  const timeout = new Promise<never>((_, reject) => {
    timer = setTimeout(() => reject(new Error(message)), ms);
  });
  p.catch(() => {});
  return Promise.race([p, timeout]).finally(() => {
    if (timer) clearTimeout(timer);
  }) as Promise<T>;
}

export const sleep = (ms: number): Promise<void> => new Promise<void>((r) => setTimeout(r, ms));
