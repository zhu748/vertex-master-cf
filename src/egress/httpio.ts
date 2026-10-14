// httpio.ts —— 隧道之上的 HTTP/1.1 客户端。
//
// 在「代理握手后的 TLS 隧道」字节流上发送请求并解析响应：
//   - 请求：Content-Length 定长 + Connection: keep-alive（连接复用池的请求侧前提）；
//   - 响应：chunked / content-length / EOF 三种体形态，流式体以 ReadableStream 透出
//     （SSE 转换层逐帧消费），完整消费后按可复用判定归还连接池；
//   - 解析函数全部为纯函数，可在 Node 单测。
import { ByteBufReader } from "./byteio.ts";

export interface TunnelRequest {
  method: string;
  /** 完整 URL（拆 path/host 用） */
  url: string;
  headers: Record<string, string>;
  body: Uint8Array;
}

export interface ResponseHead {
  status: number;
  reason: string;
  headers: Record<string, string>;
}

/** 写出 HTTP/1.1 请求（头+体合并单次 write：少一次底层调用，TCP 打包更友好） */
export async function writeTunnelRequest(
  writer: { write(chunk: Uint8Array): Promise<void> },
  req: TunnelRequest,
): Promise<void> {
  const u = new URL(req.url);
  const path = u.pathname + u.search;
  const enc = new TextEncoder();
  const lines: string[] = [req.method + " " + path + " HTTP/1.1", "Host: " + u.host];
  const seen = new Set<string>();
  for (const [k, v] of Object.entries(req.headers)) {
    const lk = k.toLowerCase();
    if (["host", "content-length", "transfer-encoding", "connection"].includes(lk)) continue;
    if (seen.has(lk)) continue;
    seen.add(lk);
    lines.push(k + ": " + v);
  }
  lines.push("Content-Length: " + req.body.length, "Connection: keep-alive");
  const head = enc.encode(lines.join("\r\n") + "\r\n\r\n");
  if (req.body.length === 0) {
    await writer.write(head);
    return;
  }
  const out = new Uint8Array(head.length + req.body.length);
  out.set(head, 0);
  out.set(req.body, head.length);
  await writer.write(out);
}

/** 解析完整响应头块 → 状态/原因/头部（键小写；重复键合并逗号） */
export function parseResponseHeadBlock(block: Uint8Array): ResponseHead {
  const text = new TextDecoder().decode(block);
  const sep = text.indexOf("\r\n\r\n");
  const headText = sep >= 0 ? text.slice(0, sep) : text;
  const lines = headText.split(/\r?\n/);
  const statusLine = (lines.shift() ?? "").trim();
  const m = /^HTTP\/\d(?:\.\d)?\s+(\d{3})(?:\s+(.*))?$/.exec(statusLine);
  if (!m) throw new Error("httpio: malformed status line: " + statusLine.slice(0, 120));
  const headers: Record<string, string> = {};
  for (const line of lines) {
    const idx = line.indexOf(":");
    if (idx <= 0) continue;
    const k = line.slice(0, idx).trim().toLowerCase();
    const v = line.slice(idx + 1).trim();
    headers[k] = headers[k] ? headers[k] + ", " + v : v;
  }
  return { status: Number(m[1]), reason: (m[2] ?? "").trim(), headers };
}

export interface BodyShape {
  chunked: boolean;
  contentLength: number | null; // null = 读到 EOF
}

export function determineBodyShape(headers: Record<string, string>): BodyShape {
  const te = (headers["transfer-encoding"] ?? "").toLowerCase();
  if (te.includes("chunked")) return { chunked: true, contentLength: null };
  const cl = headers["content-length"];
  if (cl !== undefined && /^\d+$/.test(cl.trim())) return { chunked: false, contentLength: Number(cl.trim()) };
  return { chunked: false, contentLength: null };
}

/** 无体语义：HEAD 响应与 204/304（RFC 9110：无论是否存在 Content-Length 都无 body） */
export function isBodylessStatus(method: string, status: number): boolean {
  return method.toUpperCase() === "HEAD" || status === 204 || status === 304;
}

/** 响应是否允许连接复用（Connection: close / HTTP/1.0 均不复用） */
export function responseAllowsReuse(head: ResponseHead): boolean {
  const conn = (head.headers["connection"] ?? "").toLowerCase();
  if (conn.includes("close")) return false;
  if (head.status < 200) return false;
  return true;
}

// ---------- chunked 解码 ----------

const CRLF = [13, 10];

function findCrlf(buf: Uint8Array, from: number): number {
  for (let i = from; i + 1 < buf.length; i++) {
    if (buf[i] === 13 && buf[i + 1] === 10) return i;
  }
  return -1;
}

/** chunked 流解码器：增量解码，跨 chunk 维护状态 */
export class ChunkedDecoder {
  private buf: Uint8Array = new Uint8Array(0);
  private done = false;
  private remaining = 0; // 当前 chunk 剩余字节；0 = 等待新 chunk 头
  private atHead = true;

  /** 喂入新数据，返回可透传的已解码字节（可能为空） */
  push(chunk: Uint8Array): Uint8Array {
    if (this.done || chunk.length === 0) return new Uint8Array(0);
    this.buf = concat2(this.buf, chunk);
    const outs: Uint8Array[] = [];
    for (;;) {
      if (this.atHead) {
        const idx = findCrlf(this.buf, 0);
        if (idx < 0) {
          if (this.buf.length > 16 * 1024) throw new Error("httpio: chunk header too large");
          break;
        }
        const line = new TextDecoder().decode(this.buf.subarray(0, idx)).trim();
        this.buf = this.buf.subarray(idx + 2);
        const semi = line.indexOf(";");
        const sizeHex = (semi >= 0 ? line.slice(0, semi) : line).trim();
        const size = parseInt(sizeHex, 16);
        if (!Number.isFinite(size) || size < 0) throw new Error("httpio: bad chunk size: " + sizeHex.slice(0, 32));
        if (size === 0) {
          this.done = true; // 尾随 CRLF 与 trailer 一并丢弃
          break;
        }
        this.remaining = size;
        this.atHead = false;
      } else {
        if (this.buf.length === 0) break;
        const take = Math.min(this.remaining, this.buf.length);
        if (take > 0) {
          outs.push(this.buf.subarray(0, take));
          this.buf = this.buf.subarray(take);
          this.remaining -= take;
        }
        if (this.remaining === 0) {
          // chunk 体结束：吃掉紧随的 CRLF
          if (this.buf.length >= 2 && this.buf[0] === 13 && this.buf[1] === 10) {
            this.buf = this.buf.subarray(2);
            this.atHead = true;
          } else if (this.buf.length === 1 && this.buf[0] === 13) {
            this.buf = this.buf.subarray(1);
            break; // 等下一个 chunk 的 LF
          } else if (this.buf.length === 0) {
            break; // 等下一个 chunk 的 CRLF
          } else {
            throw new Error("httpio: missing chunk terminator");
          }
        }
      }
    }
    return outs.length === 0 ? new Uint8Array(0) : concat2(outs[0], ...outs.slice(1));
  }

  get finished(): boolean {
    return this.done;
  }
}

function concat2(a: Uint8Array, ...rest: Uint8Array[]): Uint8Array {
  if (rest.length === 0) return a;
  const total = a.length + rest.reduce((n, p) => n + p.length, 0);
  const out = new Uint8Array(total);
  out.set(a, 0);
  let off = a.length;
  for (const p of rest) {
    out.set(p, off);
    off += p.length;
  }
  return out;
}

// ---------- 响应体流 ----------

/** 把 reader（TLS 记录层之上）转换为响应体 ReadableStream。
 *  cleanup(reusable)：流被完整消费/取消时回调 —— 连接池据此决定回池或废弃。 */
export function makeBodyStream(
  reader: ByteBufReader,
  shape: BodyShape,
  cleanup: (reusable: boolean) => void,
): ReadableStream<Uint8Array> {
  let settled = false;
  const finish = (reusable: boolean) => {
    if (settled) return;
    settled = true;
    cleanup(reusable);
  };
  if (shape.chunked) {
    const decoder = new ChunkedDecoder();
    return new ReadableStream<Uint8Array>({
      async pull(controller) {
        try {
          for (;;) {
            if (decoder.finished) {
              finish(true);
              controller.close();
              return;
            }
            const raw = await reader.readAvailable();
            if (raw === null) {
              // EOF：chunked 未声明结束视为截断（不复用）
              finish(false);
              controller.close();
              return;
            }
            const decoded = decoder.push(raw);
            if (decoded.length > 0) {
              controller.enqueue(decoded);
              return; // 背压：交还消费节奏
            }
          }
        } catch (err) {
          finish(false);
          controller.error(err);
        }
      },
      cancel() {
        finish(false);
      },
    });
  }
  if (shape.contentLength !== null) {
    let remaining = shape.contentLength;
    return new ReadableStream<Uint8Array>({
      async pull(controller) {
        try {
          if (remaining <= 0) {
            finish(true);
            controller.close();
            return;
          }
          const chunk = await reader.readAvailable(Math.min(remaining, 256 * 1024));
          if (chunk === null) {
            finish(false); // 截断
            controller.close();
            return;
          }
          remaining -= chunk.length;
          controller.enqueue(chunk);
        } catch (err) {
          finish(false);
          controller.error(err);
        }
      },
      cancel() {
        finish(false);
      },
    });
  }
  // EOF 形态：读到连接关闭（不复用 —— 无法判定边界）
  return new ReadableStream<Uint8Array>({
    async pull(controller) {
      try {
        const chunk = await reader.readAvailable();
        if (chunk === null) {
          finish(false);
          controller.close();
          return;
        }
        controller.enqueue(chunk);
      } catch (err) {
        finish(false);
        controller.error(err);
      }
    },
    cancel() {
      finish(false);
    },
  });
}

/** 整读响应体（响应头已解析；管理端点 / 非流式小响应用） */
export async function readFullBody(reader: ByteBufReader, shape: BodyShape, limit = 32 * 1024 * 1024): Promise<Uint8Array> {
  if (shape.chunked) {
    const decoder = new ChunkedDecoder();
    const parts: Uint8Array[] = [];
    let total = 0;
    while (!decoder.finished) {
      const raw = await reader.readAvailable();
      if (raw === null) break;
      const decoded = decoder.push(raw);
      if (decoded.length > 0) {
        total += decoded.length;
        if (total > limit) throw new Error("httpio: body too large");
        parts.push(decoded);
      }
    }
    const out = new Uint8Array(total);
    let off = 0;
    for (const p of parts) {
      out.set(p, off);
      off += p.length;
    }
    return out;
  }
  const parts: Uint8Array[] = [];
  let total = 0;
  for (;;) {
    const chunk = await reader.readAvailable(256 * 1024);
    if (chunk === null) break;
    total += chunk.length;
    if (shape.contentLength !== null && total > shape.contentLength) break;
    if (total > limit) throw new Error("httpio: body too large");
    parts.push(chunk);
    if (shape.contentLength !== null && total >= shape.contentLength) break;
  }
  const out = new Uint8Array(total);
  let off = 0;
  for (const p of parts) {
    out.set(p, off);
    off += p.length;
  }
  return out;
}

export { CRLF };

// ---------- v1.5.3：Content-Encoding 解压（隧道 HTTP 客户端配套） ----------

/**
 * 响应体 Content-Encoding 解压 —— Go 版隐式依赖 bogdanfinn/tls-client 的
 * 「自动透明解压 gzip/deflate/br」能力，CF 移植的自定义隧道 HTTP 客户端
 * 此前完全缺失这层：请求头声明 accept-encoding 后，Google 返回 gzip 压缩体，
 * token 抠取 / JSON 解析拿到的全是二进制垃圾（线上实测 anchor 响应体
 * 以 gzip magic 1f 8b 开头、token regex 必然失配）。
 *
 * Workers/Node 的 DecompressionStream 仅支持 gzip / deflate（无 br）——
 * 因此隧道请求头同步改为只声明 gzip（见 payload.ts）；不支持的编码
 * 原样透传且保留响应头（语义一致：调用方看到的是未解压字节）。
 *
 * 解压成功时从 headers 记录中移除 content-encoding（调用方拷贝响应头时
 * 不会带上「已解压却仍声明 gzip」的矛盾头）。
 */
export function applyContentDecoding(
  headers: Record<string, string>,
  body: ReadableStream<Uint8Array>,
): ReadableStream<Uint8Array> {
  const enc = (headers["content-encoding"] || "").trim().toLowerCase();
  if (enc !== "gzip" && enc !== "deflate") return body;
  delete headers["content-encoding"];
  return body.pipeThrough(new DecompressionStream(enc));
}

/** 整读字节的解压版（非流式路径配套；编码不支持时原样返回） */
export async function decodeFullBodyBytes(body: Uint8Array, encoding: string): Promise<Uint8Array> {
  const enc = (encoding || "").trim().toLowerCase();
  if (enc !== "gzip" && enc !== "deflate") return body;
  const source = new ReadableStream<Uint8Array>({
    start(controller) {
      controller.enqueue(body);
      controller.close();
    },
  });
  const stream = source.pipeThrough(new DecompressionStream(enc));
  return new Uint8Array(await new Response(stream).arrayBuffer());
}
