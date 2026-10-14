// handshake.ts —— 代理隧道握手：SOCKS5（含用户名/密码认证）、SOCKS4/4a、HTTP CONNECT。
//
// 设计（借鉴 cf-vproxy src/proxy/tunnel.ts 的分层，重新实现）：
//   - 全部握手在「TCP 字节流」上进行：入参为 ByteBufReader + writer 的结构性接口，
//     不感知底层是裸 TCP 还是 TLS；握手残留字节保留在 reader 缓冲中交给上层
//     （TLS ClientHello 直接从同一 reader 读取，不丢字节）；
//   - 每类握手都是纯 async 函数，超时由调用方用 withTimeout 包裹；
//   - SOCKS5 域名走 ATYP=domain（远端代理解析 DNS，socks5h 语义）；
//     SOCKS4 目标为域名时自动升级 4a（由代理解析 DNS）。
//
// 纯协议逻辑：不 import "cloudflare:sockets"，可在 Node 单测（配对 socket 注入）。
import { ByteBufReader, concatBytes } from "./byteio.ts";

export interface ProxyEndpoint {
  kind: "socks5" | "socks4" | "http";
  host: string;
  port: number;
  username?: string;
  password?: string;
  /** 原始配置串（日志/健康表键） */
  raw: string;
}

export interface TunnelWriter {
  write(chunk: Uint8Array): Promise<void>;
}

export interface Tunnel {
  reader: ByteBufReader;
  writer: TunnelWriter;
}

// ---------- SOCKS5 ----------

export async function socks5Handshake(io: Tunnel, p: ProxyEndpoint, targetHost: string, targetPort: number): Promise<Tunnel> {
  const hasAuth = p.username !== undefined && p.username !== "";
  await io.writer.write(new Uint8Array(hasAuth ? [5, 2, 0, 2] : [5, 1, 0]));
  const method = await io.reader.readExact(2);
  if (method[0] !== 5) throw new Error("socks5: bad version " + method[0]);
  if (method[1] === 0xff) throw new Error("socks5: no acceptable auth method");
  if (method[1] === 2) {
    if (!hasAuth) throw new Error("socks5: proxy requires auth but none configured");
    const user = new TextEncoder().encode(p.username!);
    const pass = new TextEncoder().encode(p.password ?? "");
    await io.writer.write(
      concatBytes([new Uint8Array([1, user.length]), user, new Uint8Array([pass.length]), pass]),
    );
    const auth = await io.reader.readExact(2);
    if (auth[1] !== 0) throw new Error("socks5: auth failed (status " + auth[1] + ")");
  } else if (method[1] !== 0) {
    throw new Error("socks5: unsupported auth method " + method[1]);
  }

  const host = new TextEncoder().encode(targetHost);
  if (host.length > 255) throw new Error("socks5: hostname too long");
  await io.writer.write(
    concatBytes([new Uint8Array([5, 1, 0, 3, host.length]), host, new Uint8Array([(targetPort >> 8) & 0xff, targetPort & 0xff])]),
  );
  const reply = await io.reader.readExact(4);
  if (reply[1] !== 0) throw new Error("socks5: CONNECT rep=" + reply[1]);
  const atyp = reply[3];
  if (atyp === 1) await io.reader.readExact(6); // IPv4 + port
  else if (atyp === 3) {
    const len = await io.reader.readExact(1);
    await io.reader.readExact(len[0] + 2);
  } else if (atyp === 4) await io.reader.readExact(18); // IPv6 + port
  else throw new Error("socks5: unknown ATYP " + atyp);
  return io;
}

// ---------- SOCKS4 / 4a ----------

const IPV4_RE = /^(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3})$/;

export async function socks4Handshake(io: Tunnel, p: ProxyEndpoint, targetHost: string, targetPort: number): Promise<Tunnel> {
  const host = new TextEncoder().encode(targetHost);
  const isIp = IPV4_RE.exec(targetHost);
  const userid = new TextEncoder().encode(p.username ?? "");
  let request: Uint8Array;
  if (isIp) {
    request = concatBytes([
      new Uint8Array([4, 1, (targetPort >> 8) & 0xff, targetPort & 0xff, +isIp[1], +isIp[2], +isIp[3], +isIp[4]]),
      userid,
      new Uint8Array([0]),
    ]);
  } else {
    // SOCKS4a：目标为域名时由代理解析（userid 之后跟域名 + NUL）
    request = concatBytes([
      new Uint8Array([4, 1, (targetPort >> 8) & 0xff, targetPort & 0xff, 0, 0, 0, 1]),
      userid,
      new Uint8Array([0]),
      host,
      new Uint8Array([0]),
    ]);
  }
  await io.writer.write(request);
  const reply = await io.reader.readExact(8);
  if (reply[1] !== 0x5a) throw new Error("socks4: CONNECT rep=0x" + reply[1].toString(16));
  return io;
}

// ---------- HTTP CONNECT ----------

export async function httpConnectHandshake(io: Tunnel, p: ProxyEndpoint, targetHost: string, targetPort: number): Promise<Tunnel> {
  const lines = [
    "CONNECT " + targetHost + ":" + targetPort + " HTTP/1.1",
    "Host: " + targetHost + ":" + targetPort,
  ];
  if (p.username !== undefined && p.username !== "") {
    const cred = btoa(p.username + ":" + (p.password ?? ""));
    lines.push("Proxy-Authorization: Basic " + cred);
  }
  lines.push("", "");
  await io.writer.write(new TextEncoder().encode(lines.join("\r\n")));
  const head = new TextDecoder().decode(await io.reader.readHeaderBlock());
  const statusLine = head.split(/\r?\n/)[0] ?? "";
  if (!/^HTTP\/\d(?:\.\d)?\s+2\d\d/.test(statusLine.trim())) {
    throw new Error("http-proxy CONNECT failed: " + statusLine.trim().slice(0, 120));
  }
  return io;
}

/** 按代理类型分派握手 */
export function proxyHandshake(io: Tunnel, p: ProxyEndpoint, targetHost: string, targetPort: number): Promise<Tunnel> {
  if (p.kind === "socks5") return socks5Handshake(io, p, targetHost, targetPort);
  if (p.kind === "socks4") return socks4Handshake(io, p, targetHost, targetPort);
  return httpConnectHandshake(io, p, targetHost, targetPort);
}
