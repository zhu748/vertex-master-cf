// streambridge.ts —— anon 流式竞速桥 + 终局渲染（纯逻辑，Node 单测可直接导入）。
//
// v1.6.4 根因修复「流式空回且无报错」：
//   旧 callAnonStream 立即返回 200 + 懒流，竞速在流内异步进行；全池 429 耗尽
//   时只能在流里发一个带内错误帧 —— 而 geminihygiene 见「无 finishReason」又
//   追加了合成 STOP 帧，Gemini/OpenAI/Anthropic 客户端一律解读为「正常完成、
//   内容为空」，错误被完全掩盖（线上实测：error 帧后紧跟 STOP 帧）。
//
//   新架构「竞速预冲」：200 状态码只在拿到首个内容帧后才发出；无内容终局
//   （429/401/403/502）以真实 HTTP 错误返回 —— 三个协议处理器的 !upstream.ok
//   分支接住后给客户端原生错误结构（可显示、可重试）。
//
// 依赖均为纯模块（streamparse / convert/common），无 cloudflare:sockets。
import { errGemini } from "../convert/common.ts";
import { AnonUpstreamError } from "./streamparse.ts";

/** chat 竞速终局：ok = 已有内容经 emit 流出（lastTerminal 为内容后错误） */
export interface ChatRaceResult {
  ok: boolean;
  lastTerminal: AnonUpstreamError | null;
  last429: AnonUpstreamError | null;
}

/** 流事件：内容帧（chunk）或竞速终局（done），二者互斥 */
export interface RaceStreamEvent {
  chunk?: Record<string, unknown>;
  done?: ChatRaceResult;
}

/** 流事件桥 —— 后台竞速推事件，消费侧按需拉取。
 *  push 先于 take：入缓冲数组；take 先于 push：挂起等待者，事件到达即唤醒。
 *  单生产者（竞速 emit / 终局）单消费者（预冲 / ReadableStream pull），无需锁。 */
export function createRaceBridge(): {
  push(e: RaceStreamEvent): void;
  take(): Promise<RaceStreamEvent>;
} {
  const buffered: RaceStreamEvent[] = [];
  let waiter: ((e: RaceStreamEvent) => void) | null = null;
  return {
    push(e: RaceStreamEvent): void {
      if (waiter) {
        const w = waiter;
        waiter = null;
        w(e);
      } else {
        buffered.push(e);
      }
    },
    take(): Promise<RaceStreamEvent> {
      const e = buffered.shift();
      if (e) return Promise.resolve(e);
      return new Promise((resolve) => {
        waiter = resolve;
      });
    },
  };
}

/** AnonUpstreamError → Gemini 错误体（带内帧 / HTTP 错误共用） */
export function terminalErrorBody(err: AnonUpstreamError): Record<string, unknown> {
  const status = err.status >= 400 && err.status <= 599 ? err.status : 502;
  const geminiStatus =
    status === 401 || status === 403
      ? "UNAUTHENTICATED"
      : status === 429
        ? "RESOURCE_EXHAUSTED"
        : status >= 500
          ? "UNAVAILABLE"
          : "INVALID_ARGUMENT";
  return { code: status, message: err.message || "anon upstream error", status: geminiStatus };
}

/** 无内容终局 → 真实 HTTP 错误响应（429 语义保留，处理器 !ok 分支接住） */
export function terminalResponse(err: AnonUpstreamError): Response {
  const status = err.status >= 400 && err.status <= 599 ? err.status : 502;
  const geminiStatus =
    status === 401 || status === 403
      ? "UNAUTHENTICATED"
      : status === 429
        ? "RESOURCE_EXHAUSTED"
        : status >= 500
          ? "UNAVAILABLE"
          : "INVALID_ARGUMENT";
  return errGemini(status, err.message || "anon upstream error", geminiStatus);
}

/** 预冲终局判定：done 事件的竞速结果 → HTTP 错误响应。
 *  优先级：lastTerminal（真实上游终局）→ last429（全池限流）→ 502 兜底。 */
export function noContentErrorResponse(r: ChatRaceResult): Response {
  if (r.lastTerminal) return terminalResponse(r.lastTerminal);
  if (r.last429) return terminalResponse(new AnonUpstreamError(429, r.last429.body, "anon rate limited: all candidates exhausted (429)"));
  return errGemini(502, "anon upstream failed: all candidates exhausted (no content)", "UNAVAILABLE");
}

/** 已有内容（预冲通过）：剩余事件渲染为 Gemini 原生 SSE 流。
 *  - 空帧（{}）跳过不吐；非空帧原样透传；
 *  - done 事件：内容后错误（lastTerminal）→ 带内错误帧后收流，否则直接收流；
 *  - cancel：调用方注入的 abort（中止后台竞速）。
 *  首个内容帧已由预冲消费，不经过本流（调用方负责丢弃或自行回放）。
 *  ⚠️ pull 必须以 enqueue/close/error 之一收尾（内部 for(;;) 循环跳过空帧）：
 *  按 Streams 规范，pull 兑现但未入队时不会自动重拉，直接 return 会停摆。 */
export function bridgeToSseResponse(
  bridge: { take(): Promise<RaceStreamEvent> },
  abort: { abort(): void },
): Response {
  const encoder = new TextEncoder();
  const stream = new ReadableStream<Uint8Array>({
    async pull(controller) {
      try {
        for (;;) {
          const e = await bridge.take();
          if (e.done) {
            if (e.done.lastTerminal) {
              controller.enqueue(encoder.encode("data: " + JSON.stringify({ error: terminalErrorBody(e.done.lastTerminal) }) + "\n\n"));
            }
            controller.close();
            return;
          }
          const chunk = e.chunk!;
          if (Object.keys(chunk).length === 0) continue; // 空帧：跳过，继续拉
          controller.enqueue(encoder.encode("data: " + JSON.stringify(chunk) + "\n\n"));
          return;
        }
      } catch (err) {
        controller.error(err);
      }
    },
    cancel() {
      abort.abort();
    },
  });
  return new Response(stream, {
    status: 200,
    headers: { "content-type": "text/event-stream", "cache-control": "no-cache" },
  });
}
