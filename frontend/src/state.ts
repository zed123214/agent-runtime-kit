import type { Patch, View } from "./types";

export class SequenceGap extends Error {}

export function applyPatch(view: View, patch: Patch): View {
  if (
    view.instance_id !== patch.instance_id ||
    view.session_id !== patch.session_id
  )
    return view;
  if (patch.seq <= view.seq) return view;
  if (patch.seq !== view.seq + 1)
    throw new SequenceGap("事件序号不连续，需要重新获取快照。");
  let next = { ...view, seq: patch.seq };
  for (const operation of patch.operations) {
    if (operation.op === "meta") next = { ...next, ...operation.value };
    else {
      const collection = { ...next[operation.collection] } as Record<
        string,
        unknown
      >;
      if (operation.op === "put") collection[operation.key] = operation.value;
      else delete collection[operation.key];
      next = { ...next, [operation.collection]: collection };
    }
  }
  return next;
}

export const labels: Record<string, string> = {
  ready: "可继续对话",
  running: "执行中",
  processing: "处理中",
  approval: "等待审批",
  syncing: "同步结果中",
  sync_error: "结果同步失败",
  closed: "会话已结束",
  closing: "结束中",
  lost: "连接已丢失",
  success: "已完成",
  failed: "失败",
  denied: "已拒绝",
  pending: "待审批",
  submitting: "提交中",
  resolved: "已处理",
  expired: "已失效",
  unknown: "结果未确认",
};
