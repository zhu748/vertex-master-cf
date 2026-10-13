// streamparse.ts —— 匿名 batchGraphql 流解析（移植自 vertex-master internal/vertex/stream.go）。
//
// 上游响应是「连续拼接的 JSON 对象流」（无分隔符、非 SSE）。解析分四层：
//   1. scanJsonObjects：跨网络 chunk 增量扫描花括号配对（字符串感知），逐个完整
//      JSON 对象回调 —— O(n)，不重扫前缀；
//   2. processStreamingObject：识别 results[].errors（verify-fail → auth 错误），
//      解包 data.ui.streamGenerateContentAnonymous（map / list 两态）；
//   3. extractChunk：清洗 Gemini 帧 —— candidates 保留、usageMetadata 等元数据
//      真值保留、其余键删除；part 级移除 protobuf 空默认字段（data/fileData{}/
//      functionCall{}/functionResponse{}/inlineData{} 等）；
//   4. ChunkCollector：非流式路径把全部 chunk 聚合为单个 Gemini 响应
//      （ContentBlockMerger 语义：相邻同类 text part 合并，thought 块独立成段）。
//
// 纯逻辑模块：可在 Node 单测。

export interface StreamChunk {
  data: Record<string, unknown> | null;
}

export class AnonAuthError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "AnonAuthError";
  }
}

export class AnonUpstreamError extends Error {
  status: number;
  body: string;
  constructor(status: number, body: string, message?: string) {
    super(message ?? "anon upstream error HTTP " + status);
    this.name = "AnonUpstreamError";
    this.status = status;
    this.body = body;
  }
}

// ---------- 1. JSON 对象流扫描 ----------

/**
 * 增量扫描拼接 JSON 对象流，逐个完整对象回调 onObject。
 * onObject 返回 false = 停止扫描（客户端断开/上游要求）。
 * 抛出 StreamTooLargeError 表示单对象超过 maxObjectBytes（防御异常上游）。
 */
export class StreamTooLargeError extends Error {
  constructor(maxBytes: number) {
    super("upstream stream object too large: exceeds " + maxBytes + " byte limit");
    this.name = "StreamTooLargeError";
  }
}

export async function scanJsonObjects(
  body: ReadableStream<Uint8Array>,
  onObject: (raw: Uint8Array) => Promise<boolean> | boolean,
  maxObjectBytes = 64 << 20,
): Promise<void> {
  const decoder = body.getReader();
  let buffer: Uint8Array<ArrayBufferLike> = new Uint8Array(0);
  let scanPos = 0;
  let startIdx = -1;
  let braceCount = 0;
  let inString = false;
  let escape = false;

  const handleObject = async (jsonObject: Uint8Array, nextObject: number): Promise<boolean> => {
    if (maxObjectBytes > 0 && jsonObject.length > maxObjectBytes) throw new StreamTooLargeError(maxObjectBytes);
    const stop = await onObject(jsonObject);
    if (nextObject >= buffer.length) {
      buffer = new Uint8Array(0);
      scanPos = 0;
    } else {
      scanPos = nextObject;
    }
    return stop;
  };

  for (;;) {
    const { value, done } = await decoder.read();
    if (value && value.length > 0) {
      buffer = concat(buffer, value);
      for (;;) {
        if (startIdx < 0) {
          const relative = buffer.indexOf(0x7b, scanPos); // '{'
          if (relative === -1) {
            buffer = new Uint8Array(0);
            scanPos = 0;
            break;
          }
          startIdx = relative;
          scanPos = relative;
          braceCount = 0;
          inString = false;
          escape = false;
        }
        let endIdx = -1;
        for (let i = scanPos; i < buffer.length; i++) {
          const ch = buffer[i];
          if (escape) {
            escape = false;
            continue;
          }
          if (ch === 0x5c) {
            escape = true;
            continue;
          }
          if (ch === 0x22) {
            inString = !inString;
            continue;
          }
          if (!inString) {
            if (ch === 0x7b) braceCount++;
            else if (ch === 0x7d) {
              braceCount--;
              if (braceCount === 0) {
                endIdx = i;
                break;
              }
            }
          }
        }
        if (endIdx !== -1) {
          const jsonObject = buffer.subarray(startIdx, endIdx + 1);
          const remaining = buffer.length - (endIdx + 1);
          startIdx = -1;
          braceCount = 0;
          inString = false;
          escape = false;
          const stop = await handleObject(jsonObject, endIdx + 1);
          if (stop) return;
          if (remaining === 0) {
            buffer = new Uint8Array(0);
            scanPos = 0;
            break;
          }
        } else {
          scanPos = buffer.length;
          if (maxObjectBytes > 0 && braceCount > 0 && buffer.length - startIdx > maxObjectBytes) {
            throw new StreamTooLargeError(maxObjectBytes);
          }
          if (startIdx > 0) {
            // 压缩已处理前缀，避免大流长期占用内存
            buffer = buffer.slice(startIdx);
            scanPos -= startIdx;
            startIdx = 0;
          }
          break;
        }
      }
    }
    if (done) {
      if (startIdx >= 0 && braceCount > 0) {
        throw new Error("upstream stream ended with truncated JSON");
      }
      return;
    }
  }
}

function concat(a: Uint8Array, b: Uint8Array): Uint8Array {
  if (a.length === 0) return b;
  const out = new Uint8Array(a.length + b.length);
  out.set(a, 0);
  out.set(b, a.length);
  return out;
}

// ---------- 2/3. 对象处理与清洗 ----------

function isTruthy(v: unknown): boolean {
  if (v === null || v === undefined) return false;
  if (typeof v === "string") return v.length > 0;
  if (typeof v === "number") return v !== 0;
  if (typeof v === "boolean") return v;
  if (Array.isArray(v)) return v.length > 0;
  if (typeof v === "object") return Object.keys(v as object).length > 0;
  return !!v;
}

function toStr(v: unknown): string {
  return typeof v === "string" ? v : "";
}

const META_KEYS = new Set(["usageMetadata", "modelVersion", "responseId", "promptFeedback", "createTime"]);

/** 处理单个上游 JSON 对象，产出（可能多个）Gemini chunk。返回 false = 停止（客户端断开）。 */
export function processStreamingObject(
  obj: Record<string, unknown>,
  emit: (chunk: Record<string, unknown>) => boolean,
): boolean | never {
  const results = Array.isArray(obj["results"]) ? (obj["results"] as unknown[]) : [];
  for (const rRaw of results) {
    if (!rRaw || typeof rRaw !== "object" || Array.isArray(rRaw)) continue;
    const result = rRaw as Record<string, unknown>;

    // results 内错误处理：verify-fail / permission → auth 错误触发上层重试；
    // 其余错误不中断（对齐 Go 版：parseErrorResponse 未命中时继续处理后续 result）
    const errs = result["errors"];
    if (Array.isArray(errs) && errs.length > 0) {
      const first = errs[0];
      const errMsg = first && typeof first === "object" ? toStr((first as Record<string, unknown>)["message"]) : toStr(first);
      if (errMsg.includes("Failed to verify action") || errMsg.includes("The caller does not have permission")) {
        throw new AnonAuthError(errMsg);
      }
      continue;
    }

    const dataRaw = result["data"];
    if (!dataRaw || typeof dataRaw !== "object" || Array.isArray(dataRaw)) continue;
    let data = dataRaw as Record<string, unknown>;

    // 解包 data.ui.streamGenerateContentAnonymous
    const ui = data["ui"];
    if (ui && typeof ui === "object" && !Array.isArray(ui)) {
      const uiObj = ui as Record<string, unknown>;
      if ("streamGenerateContentAnonymous" in uiObj) {
        const inner = uiObj["streamGenerateContentAnonymous"];
        if (inner && typeof inner === "object" && !Array.isArray(inner)) {
          data = inner as Record<string, unknown>;
        } else if (Array.isArray(inner)) {
          // 极少数情况 inner 是 list：逐项提取 + 附带外层元数据
          const outerMeta: Record<string, unknown> = {};
          for (const key of META_KEYS) {
            const v = data[key];
            if (v !== undefined && isTruthy(v)) outerMeta[key] = v;
          }
          for (const itemRaw of inner) {
            if (itemRaw && typeof itemRaw === "object" && !Array.isArray(itemRaw)) {
              const item = itemRaw as Record<string, unknown>;
              for (const [k, v] of Object.entries(outerMeta)) {
                if (!(k in item)) item[k] = v;
              }
              const chunk = extractChunk(item);
              if (chunk && !emit(chunk)) return false;
            }
          }
          continue;
        } else {
          continue;
        }
      }
    }

    const chunk = extractChunk(data);
    if (chunk && !emit(chunk)) return false;
  }
  return true;
}

/**
 * extractChunk：清洗单个 Gemini 数据帧。
 * candidates 键存在且非 null 时保留（即使空列表）；元数据键真值保留；其余删除。
 * part 级清理移除 protobuf 空默认字段。完全无字段返回 null。
 */
export function extractChunk(data: Record<string, unknown>): Record<string, unknown> | null {
  const rawCandidates = data["candidates"];
  if (rawCandidates !== undefined && rawCandidates !== null) {
    if (Array.isArray(rawCandidates)) {
      const cleaned = cleanStreamCandidates(rawCandidates);
      data["candidates"] = cleaned;
    } else {
      delete data["candidates"];
    }
  } else {
    delete data["candidates"];
  }

  for (const key of Object.keys(data)) {
    if (key === "candidates") continue;
    if (META_KEYS.has(key)) {
      if (!isTruthy(data[key])) delete data[key];
    } else {
      delete data[key];
    }
  }

  return Object.keys(data).length === 0 ? null : data;
}

function cleanStreamCandidates(candidates: unknown[]): unknown[] {
  const valid: unknown[] = [];
  for (const cRaw of candidates) {
    if (!cRaw || typeof cRaw !== "object" || Array.isArray(cRaw)) continue;
    const candidate = cRaw as Record<string, unknown>;
    const content = candidate["content"];
    if (content && typeof content === "object" && !Array.isArray(content)) {
      const c = content as Record<string, unknown>;
      if (Array.isArray(c["parts"])) {
        const cleanedParts = cleanStreamParts(c["parts"] as unknown[]);
        c["parts"] = cleanedParts;
        let role = typeof c["role"] === "string" ? (c["role"] as string) : "";
        if (!role) role = "model";
        c["role"] = role;
        for (const key of Object.keys(c)) {
          if (key !== "role" && key !== "parts") delete c[key];
        }
      }
    }
    valid.push(candidate);
  }
  return valid;
}

function cleanStreamParts(parts: unknown[]): unknown[] {
  const out: unknown[] = [];
  for (const pRaw of parts) {
    if (!pRaw || typeof pRaw !== "object" || Array.isArray(pRaw)) continue;
    const part = pRaw as Record<string, unknown>;
    const textVal = part["text"];
    if (textVal !== undefined && typeof textVal !== "string") {
      // 畸形嵌套：text 值是 list/dict 而非字符串，递归提取真正文本
      const extracted = extractTextRecursive(textVal, 0);
      if (extracted) {
        const newPart = cleanPart(part);
        if (newPart) {
          newPart["text"] = extracted;
          out.push(newPart);
        }
      }
      continue;
    }
    const cp = cleanPart(part);
    if (cp) out.push(cp);
  }
  return out;
}

/** 清洗单个 part：移除 protobuf oneof 指示器与空默认字段，仅保留真实内容字段 */
function cleanPart(part: Record<string, unknown>): Record<string, unknown> | null {
  delete part["data"]; // 内部 oneof 指示器（恒为 "text"/"inlineData"/...）

  const fd = part["fileData"];
  if (fd && typeof fd === "object" && !Array.isArray(fd)) {
    const f = fd as Record<string, unknown>;
    if (!toStr(f["fileUri"]) && !toStr(f["mimeType"]) && !toStr(f["mime_type"])) delete part["fileData"];
  }

  const fc = part["functionCall"];
  if (fc && typeof fc === "object" && !Array.isArray(fc)) {
    const f = fc as Record<string, unknown>;
    const hasName = !!toStr(f["name"]);
    const hasArgs = f["args"] !== undefined && f["args"] !== null && typeof f["args"] === "object" && Object.keys(f["args"] as object).length > 0;
    if (!hasName && !hasArgs) delete part["functionCall"];
    else if (hasName && typeof f["args"] === "string" && (f["args"] as string) !== "") {
      try {
        f["args"] = JSON.parse(f["args"] as string);
      } catch {
        // 保留原样
      }
    }
  }

  const fr = part["functionResponse"];
  if (fr && typeof fr === "object" && !Array.isArray(fr)) {
    const f = fr as Record<string, unknown>;
    const hasName = !!toStr(f["name"]);
    const hasResp = f["response"] !== undefined && f["response"] !== null && typeof f["response"] === "object" && Object.keys(f["response"] as object).length > 0;
    if (!hasName && !hasResp) delete part["functionResponse"];
    else if (typeof f["response"] === "string" && (f["response"] as string) !== "") {
      f["response"] = { result: f["response"] };
    }
  }

  const inline = part["inlineData"];
  if (inline && typeof inline === "object" && !Array.isArray(inline)) {
    const i = inline as Record<string, unknown>;
    if (!toStr(i["data"])) delete part["inlineData"];
  }

  // 代码块/代码执行结果透传
  for (const key of ["executableCode", "codeExecutionResult"]) {
    const v = part[key];
    if (v !== undefined && isTruthy(v)) return part;
  }

  // 只剩 thought/thoughtSignature 等非内容标记 → 丢弃
  for (const k of Object.keys(part)) {
    if (k !== "thought" && k !== "thoughtSignature") return part;
  }
  return null;
}

const MAX_RECURSIVE_DEPTH = 20;
const MAX_RECURSIVE_LEAF_BYTES = 500;

function extractTextRecursive(val: unknown, depth: number): string {
  if (depth > MAX_RECURSIVE_DEPTH) {
    const s = toStr(val);
    return s.length > MAX_RECURSIVE_LEAF_BYTES ? s.slice(0, MAX_RECURSIVE_LEAF_BYTES) : s;
  }
  if (typeof val === "string") return val;
  if (val && typeof val === "object" && !Array.isArray(val)) {
    const t = (val as Record<string, unknown>)["text"];
    return t !== undefined ? extractTextRecursive(t, depth + 1) : "";
  }
  if (Array.isArray(val)) return val.map((item) => extractTextRecursive(item, depth + 1)).join("");
  return "";
}

// ---------- 4. 非流式聚合 ----------

interface ParseResult {
  parts: Record<string, unknown>[];
  finishReason: string;
  finishMessage: unknown;
  safetyRatings: unknown;
  citationMetadata: unknown;
  groundingMetadata: unknown;
  tokenCount: unknown;
  avgLogprobs: unknown;
  logprobsResult: unknown;
  candidateIndex: number;
  promptFeedback: Record<string, unknown>;
  usageMetadata: Record<string, unknown>;
  createTime: unknown;
  modelVersion: unknown;
  responseId: unknown;
}

/** 相邻同类 text part 合并器（移植 ContentBlockMerger 语义） */
class ContentBlockMerger {
  private merged: Record<string, unknown>[] = [];
  private currentText = "";
  private currentCount = 0;
  private currentThought = false;
  private currentSignature: unknown;
  private hasSignature = false;

  add(part: Record<string, unknown>): void {
    const text = part["text"];
    if (typeof text !== "string" || text === "") {
      const cleaned = cleanPart(part);
      if (!cleaned) return;
      this.flushText();
      this.merged.push(cleaned);
      return;
    }
    // 对齐 Go 版 normalizeOwnedTextPart：文本 part 只保留 text（thought 字段仅 thought=true 时保留）
    const isThought = part["thought"] === true;
    for (const key of Object.keys(part)) {
      if (key === "text" || (isThought && (key === "thought" || key === "thoughtSignature"))) continue;
      delete part[key];
    }
    if (isThought) part["thought"] = true;
    if (this.currentCount > 0 && this.currentThought === isThought) {
      this.currentText += text;
      this.currentCount++;
      if (isThought && !this.hasSignature && "thoughtSignature" in part) {
        this.currentSignature = part["thoughtSignature"];
        this.hasSignature = true;
      }
      return;
    }
    this.flushText();
    this.currentText = text;
    this.currentCount = 1;
    this.currentThought = isThought;
    if (isThought && "thoughtSignature" in part) {
      this.currentSignature = part["thoughtSignature"];
      this.hasSignature = true;
    }
  }

  addPlainText(text: string): void {
    if (!text) return;
    if (this.currentCount > 0 && !this.currentThought) {
      this.currentText += text;
      this.currentCount++;
      return;
    }
    this.flushText();
    this.currentText = text;
    this.currentCount = 1;
    this.currentThought = false;
  }

  result(): Record<string, unknown>[] {
    this.flushText();
    return this.merged;
  }

  private flushText(): void {
    if (this.currentCount === 0) return;
    const current: Record<string, unknown> = { text: this.currentText };
    if (this.currentThought) {
      current["thought"] = true;
      if (this.hasSignature) current["thoughtSignature"] = this.currentSignature;
    }
    this.merged.push(current);
    this.currentCount = 0;
    this.currentText = "";
    this.currentThought = false;
    this.hasSignature = false;
    this.currentSignature = undefined;
  }
}

/** 流式 chunk 聚合器（移植 chunkCollector） */
export class ChunkCollector {
  private result: ParseResult;
  private merger = new ContentBlockMerger();
  private count = 0;

  constructor() {
    this.result = {
      parts: [],
      finishReason: "",
      finishMessage: undefined,
      safetyRatings: undefined,
      citationMetadata: undefined,
      groundingMetadata: undefined,
      tokenCount: undefined,
      avgLogprobs: undefined,
      logprobsResult: undefined,
      candidateIndex: 0,
      promptFeedback: {},
      usageMetadata: {},
      createTime: undefined,
      modelVersion: undefined,
      responseId: undefined,
    };
  }

  add(chunk: Record<string, unknown>): void {
    this.count++;
    const r = this.result;
    const candidates = chunk["candidates"];
    if (Array.isArray(candidates) && candidates.length > 0) {
      const candidate = candidates[0];
      if (candidate && typeof candidate === "object" && !Array.isArray(candidate)) {
        const c = candidate as Record<string, unknown>;
        if (isTruthy(c["finishReason"])) r.finishReason = toStr(c["finishReason"]);
        if ("finishMessage" in c) r.finishMessage = c["finishMessage"];
        if (isTruthy(c["safetyRatings"])) r.safetyRatings = c["safetyRatings"];
        if (isTruthy(c["citationMetadata"])) r.citationMetadata = c["citationMetadata"];
        if (isTruthy(c["groundingMetadata"])) r.groundingMetadata = c["groundingMetadata"];
        if ("tokenCount" in c) r.tokenCount = c["tokenCount"];
        if ("avgLogprobs" in c) r.avgLogprobs = c["avgLogprobs"];
        if ("logprobsResult" in c) r.logprobsResult = c["logprobsResult"];
        if (c["index"] !== undefined && c["index"] !== null) {
          const n = Number(c["index"]);
          if (Number.isFinite(n)) r.candidateIndex = Math.floor(n);
        }
        const content = c["content"];
        if (content && typeof content === "object" && !Array.isArray(content)) {
          const parts = (content as Record<string, unknown>)["parts"];
          if (Array.isArray(parts)) {
            for (const p of parts) {
              if (p && typeof p === "object" && !Array.isArray(p)) this.merger.add(p as Record<string, unknown>);
            }
          }
        }
      }
    }
    const feedback = chunk["promptFeedback"];
    if (feedback && typeof feedback === "object" && !Array.isArray(feedback) && Object.keys(feedback).length > 0) {
      if (Object.keys(r.promptFeedback).length === 0) r.promptFeedback = feedback as Record<string, unknown>;
    }
    const usage = chunk["usageMetadata"];
    if (usage && typeof usage === "object" && !Array.isArray(usage) && Object.keys(usage).length > 0) {
      r.usageMetadata = usage as Record<string, unknown>;
    }
    if ("createTime" in chunk) r.createTime = chunk["createTime"];
    if (isTruthy(chunk["modelVersion"])) r.modelVersion = chunk["modelVersion"];
    if (isTruthy(chunk["responseId"])) r.responseId = chunk["responseId"];
  }

  addCanonicalText(text: string, finishReason: string, hasFinishReason: boolean): void {
    this.count++;
    if (hasFinishReason && finishReason) this.result.finishReason = finishReason;
    if (text) this.merger.addPlainText(text);
  }

  len(): number {
    return this.count;
  }

  /** 聚合结果 → 完整 Gemini 非流式响应（移植 buildCompleteResponse） */
  buildResponse(): Record<string, unknown> | null {
    const r = this.result;
    r.parts = this.merger.result();
    if (r.parts.length === 0 && !r.finishReason && Object.keys(r.promptFeedback).length === 0) {
      return null; // 空响应
    }
    const allParts = r.parts.length > 0 ? r.parts : [{ text: " " }];
    const candidate: Record<string, unknown> = {
      index: r.candidateIndex,
      content: { parts: allParts, role: "model" },
    };
    if (r.finishReason) candidate["finishReason"] = r.finishReason.toUpperCase();
    if (r.finishMessage !== undefined) candidate["finishMessage"] = r.finishMessage;
    if (isTruthy(r.safetyRatings)) candidate["safetyRatings"] = r.safetyRatings;
    if (isTruthy(r.citationMetadata)) candidate["citationMetadata"] = r.citationMetadata;
    if (isTruthy(r.groundingMetadata)) candidate["groundingMetadata"] = r.groundingMetadata;
    if (r.tokenCount !== undefined) candidate["tokenCount"] = r.tokenCount;
    if (r.avgLogprobs !== undefined) candidate["avgLogprobs"] = r.avgLogprobs;
    if (r.logprobsResult !== undefined) candidate["logprobsResult"] = r.logprobsResult;

    const resp: Record<string, unknown> = { candidates: [candidate] };
    if (r.createTime !== undefined) resp["createTime"] = r.createTime;
    if (isTruthy(r.modelVersion)) resp["modelVersion"] = r.modelVersion;
    if (Object.keys(r.promptFeedback).length > 0) resp["promptFeedback"] = r.promptFeedback;
    if (isTruthy(r.responseId)) resp["responseId"] = r.responseId;
    if (Object.keys(r.usageMetadata).length > 0) resp["usageMetadata"] = r.usageMetadata;
    return resp;
  }
}

// ---------- CountTokens 响应解析（移植 parseCountTokensResult） ----------

/** 从 CountTokens 响应抠 totalTokens：results → data.ui.countTokensV2 / data.countTokensV2 / data.countTokens */
export function parseCountTokensResult(rawText: string): number {
  let obj: unknown;
  try {
    obj = JSON.parse(rawText);
  } catch {
    return 0;
  }
  const results = (obj as Record<string, unknown>)?.["results"];
  const arr = Array.isArray(results) ? results : [obj];
  for (const r of arr) {
    if (!r || typeof r !== "object") continue;
    const data = (r as Record<string, unknown>)["data"];
    if (!data || typeof data !== "object" || Array.isArray(data)) continue;
    const d = data as Record<string, unknown>;
    const ui = d["ui"];
    const holders: unknown[] = [];
    if (ui && typeof ui === "object" && !Array.isArray(ui)) {
      const u = ui as Record<string, unknown>;
      holders.push(u["countTokensV2"], u["countTokens"]);
    }
    holders.push(d["countTokensV2"], d["countTokens"]);
    for (const h of holders) {
      if (h && typeof h === "object" && !Array.isArray(h)) {
        const total = (h as Record<string, unknown>)["totalTokens"];
        const n = Number(total);
        if (Number.isFinite(n) && n > 0) return Math.floor(n);
      }
    }
  }
  return 0;
}
