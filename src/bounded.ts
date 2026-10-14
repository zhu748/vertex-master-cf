// 有界并发执行器 —— v1.3.0 新增（Workers 优化：n 张图并发生成的核心原语）。
//
// 语义：
//   - 最多 limit 个 worker 同时在飞（滑动窗口：谁完成谁取下一个下标）；
//   - 结果按下标保序返回（并发完成顺序无关）；
//   - fail-fast：任一任务抛错 → 记录首个错误，worker 不再领取新任务（已在飞的
//     任务自然完成、结果丢弃），全部 worker 退出后抛出首个错误（对齐 Go 版
//     串行循环「遇错即返回」的外部语义，同时避免后台未处理 rejection）。
//
// ⚠️ 纯逻辑文件：不 import 任何运行时 API，可在 Node 单测中直接运行。

export async function runBounded<R>(
  count: number,
  limit: number,
  fn: (index: number) => Promise<R>,
): Promise<R[]> {
  const total = Math.max(0, Math.floor(count));
  if (total === 0) return [];
  const workers = Math.max(1, Math.min(Math.floor(limit) || 1, total));
  const results = new Array<R>(total);
  // TS 无法跟踪闭包内赋值（worker 并发写），用对象持有避免控制流收窄成 never
  const state: { next: number; hasError: boolean; firstError: unknown } = { next: 0, hasError: false, firstError: undefined };
  const worker = async (): Promise<void> => {
    for (;;) {
      const i = state.next++;
      if (i >= total || state.hasError) return;
      try {
        results[i] = await fn(i);
      } catch (e) {
        if (!state.hasError) {
          state.hasError = true;
          state.firstError = e;
        }
        return;
      }
    }
  };
  await Promise.all(Array.from({ length: workers }, worker));
  if (state.hasError) throw state.firstError;
  return results;
}
