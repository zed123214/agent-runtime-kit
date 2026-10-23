"""Bounded presentation state. Core events remain the authority."""

from __future__ import annotations

import json
from copy import deepcopy
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4


def now() -> str:
    return datetime.now(UTC).isoformat()


def bounded(value: object, limit: int = 65536) -> tuple[str, bool]:
    text = value if isinstance(value, str) else json.dumps(value, ensure_ascii=False)
    data = text.encode("utf-8")
    return data[:limit].decode("utf-8", errors="ignore"), len(data) > limit


class Projection:
    def __init__(self, sid: str, instance: str, core: dict[str, Any]) -> None:
        self.data: dict[str, Any] = {
            "schema_version": 1,
            "instance_id": instance,
            "session_id": sid,
            "seq": 0,
            "title": "新会话",
            "state": "ready",
            "error": None,
            "core": core,
            "permissions": {"mode": "ask", "allowed_tools": [], "revision": 0},
            "current_run_id": None,
            "updated_at": now(),
            "messages": {},
            "tools": {},
            "approvals": {},
            "runs": {},
            "reports": {},
        }
        self.operations: list[dict[str, Any]] = []
        self.cursors: dict[str, int] = {}
        self.summaries: dict[str, dict[str, Any]] = {}

    def meta(self, **values: Any) -> None:
        self.data.update(values)
        self.operations.append({"op": "meta", "value": values})

    def put(self, collection: str, key: str, value: dict[str, Any]) -> None:
        self.data[collection][key] = value
        self.operations.append({"op": "put", "collection": collection, "key": key, "value": value})

    def remove(self, collection: str, key: str) -> None:
        self.data[collection].pop(key, None)
        self.operations.append({"op": "remove", "collection": collection, "key": key})

    def drain(self) -> dict[str, Any] | None:
        if not self.operations:
            return None
        self.data["seq"] += 1
        self.data["updated_at"] = now()
        compact: dict[str, dict[str, Any]] = {}
        metadata: dict[str, Any] = {}
        for operation in self.operations:
            if operation["op"] == "meta":
                metadata.update(operation["value"])
            else:
                compact[f"{operation['collection']}:{operation['key']}"] = operation
        operations = list(compact.values())
        if metadata:
            operations.append({"op": "meta", "value": metadata})
        result = {
            "type": "patch",
            "instance_id": self.data["instance_id"],
            "session_id": self.data["session_id"],
            "seq": self.data["seq"],
            "operations": operations,
        }
        self.operations = []
        return deepcopy(result)

    def snapshot(self) -> dict[str, Any]:
        return deepcopy(self.data)

    def event(self, event: dict[str, Any]) -> None:
        kind = str(event.get("type", ""))
        rid = str(event.get("run_id", ""))
        seq = event.get("event_seq")
        if rid and isinstance(seq, int):
            if seq <= self.cursors.get(rid, 0):
                return
            self.cursors[rid] = seq
        if kind in {"run.started", "subagent.started"}:
            if rid not in self.data["runs"] and len(self.data["runs"]) >= 100:
                raise OverflowError("运行记录容量已满")
            existing_run = self.data["runs"].get(rid, {})
            parent = event.get("parent_run_id") or existing_run.get("parent_run_id")
            root = event.get("correlation_id") or rid
            self.put(
                "runs",
                rid,
                {
                    **self.data["runs"].get(rid, {}),
                    "id": rid,
                    "parent_run_id": parent,
                    "root_id": root,
                    "title": event.get("description") or event.get("goal", "任务"),
                    "status": "running",
                    "started_at": event.get("ts"),
                },
            )
            if not parent and root == rid:
                self.meta(current_run_id=rid)
        elif kind in {"run.finished", "subagent.finished"}:
            run = dict(self.data["runs"].get(rid, {"id": rid}))
            run.update(
                status=event.get("status"),
                reason=event.get("reason"),
                finished_at=event.get("ts"),
                steps=event.get("steps"),
            )
            self.put("runs", rid, run)
            if rid == self.data["current_run_id"] and self.data["state"] != "closing":
                self.meta(state="syncing")
        elif kind == "llm.token":
            if rid != self.data["current_run_id"]:
                run = dict(self.data["runs"].get(rid, {"id": rid}))
                output, clipped = bounded(str(run.get("output", "")) + str(event.get("token", "")))
                run.update(
                    output=output, output_truncated=run.get("output_truncated", False) or clipped
                )
                self.put("runs", rid, run)
                self._trim()
                return
            step = self.data["runs"].get(rid, {}).get("step", 0)
            mid = f"draft:{rid}:{step}"
            message = dict(
                self.data["messages"].get(
                    mid,
                    {
                        "id": mid,
                        "role": "assistant",
                        "text": "",
                        "run_id": rid,
                        "draft": True,
                    },
                )
            )
            message["text"], clipped = bounded(message["text"] + str(event.get("token", "")))
            message["truncated"] = message.get("truncated", False) or clipped
            self.put("messages", mid, message)
        elif kind == "step.started":
            run = dict(self.data["runs"].get(rid, {"id": rid}))
            run["step"] = event.get("step")
            self.put("runs", rid, run)
        elif kind == "llm.model_selected":
            self.meta(core={**self.data["core"], "model": event.get("model")})
        elif kind.startswith("tool.") or kind.startswith("permission."):
            self._tool_event(event)
        elif kind == "skill.invoked":
            run = dict(self.data["runs"].get(rid, {"id": rid}))
            run["skill"] = bounded(event.get("skill_name", ""), 200)[0]
            self.put("runs", rid, run)
        self._trim()

    def _tool_event(self, event: dict[str, Any]) -> None:
        kind = str(event["type"])
        rid, tid = str(event.get("run_id", "")), str(event.get("tool_use_id", ""))
        key = f"{rid}:{tid}"
        if key not in self.summaries and len(self.summaries) >= 1000:
            raise OverflowError("调用记录容量已满")
        tool = dict(
            self.data["tools"].get(
                key,
                self.summaries.get(
                    key,
                    {
                        "id": key,
                        "run_id": rid,
                        "tool_use_id": tid,
                        "root_id": event.get("correlation_id") or rid,
                        "name": event.get("tool_name", "tool"),
                        "status": "processing",
                        "attempts": [],
                        "output": "",
                        "truncated": False,
                    },
                ),
            )
        )
        if kind == "tool.call_started":
            tool.update(
                params=bounded(event.get("params", {}), 16384)[0], started_at=event.get("ts")
            )
        elif kind == "permission.requested":
            if len([a for a in self.data["approvals"].values() if a["status"] == "pending"]) >= 64:
                raise OverflowError("待审批调用过多")
            params, clipped = bounded(event.get("params", {}), 16384)
            approval: dict[str, Any] = {
                "id": uuid4().hex,
                "run_id": rid,
                "tool_use_id": tid,
                "tool_id": key,
                "name": event.get("tool_name"),
                "params": params,
                "can_allow": not clipped,
                "status": "pending",
                "expires_at": event.get("expires_at"),
            }
            self.put("approvals", approval["id"], approval)
            tool.update(status="approval", approval_id=approval["id"])
            self.meta(state="approval")
        elif kind in {"permission.granted", "permission.denied"}:
            tool["decision"] = event.get("decision")
            tool["status"] = "processing" if kind.endswith("granted") else "denied"
            for aid, approval in list(self.data["approvals"].items()):
                if approval["tool_id"] == key:
                    self.put(
                        "approvals",
                        aid,
                        {**approval, "status": "resolved", "decision": event.get("decision")},
                    )
            if self.data["state"] not in {"closing", "closed", "lost"}:
                pending = any(
                    a["status"] in {"pending", "submitting"}
                    for a in self.data["approvals"].values()
                )
                self.meta(state="approval" if pending else "running")
        elif kind == "tool.call_finished":
            output, clipped = bounded(event.get("output", ""))
            tool.update(
                status="success",
                output=output,
                truncated=clipped,
                elapsed_ms=event.get("elapsed_ms"),
                finished_at=event.get("ts"),
            )
        elif kind == "tool.call_failed":
            attempt = {
                "attempt": event.get("attempt", 1),
                "error": bounded(event.get("error_message", ""), 4096)[0],
            }
            tool.update(
                status="failed",
                attempts=[*tool["attempts"][-9:], attempt],
                elapsed_ms=event.get("elapsed_ms"),
                error_class=event.get("error_class"),
            )
        self.put("tools", key, tool)
        self.summaries[key] = {k: v for k, v in tool.items() if k not in {"output", "params"}}
        if len(json.dumps(self.summaries, ensure_ascii=False).encode()) > 1024 * 1024:
            raise OverflowError("调用控制摘要容量已满，执行记录不完整")

    def _trim(self) -> None:
        for collection, limit in (
            ("messages", 100),
            ("tools", 200),
            ("approvals", 200),
            ("reports", 10),
        ):
            for key, item in list(self.data[collection].items()):
                if len(self.data[collection]) <= limit:
                    break
                if item.get("status") in {"pending", "submitting", "processing", "approval"}:
                    continue
                self.remove(collection, key)
        # Body data can be discarded while compact control summaries remain available.
        if len(json.dumps(self.data, ensure_ascii=False).encode()) > 4 * 1024 * 1024:
            for key, item in list(self.data["tools"].items()):
                if item.get("output"):
                    self.put(
                        "tools", key, {**item, "output": item["output"][:512], "truncated": True}
                    )
            if len(json.dumps(self.data, ensure_ascii=False).encode()) > 4 * 1024 * 1024:
                raise OverflowError("展示容量已满")

    def synchronize(self, history: list[dict[str, Any]], result: dict[str, Any]) -> None:
        previous = self.snapshot()
        previous_operations = self.operations.copy()
        try:
            self._commit_report(history, result)
        except OverflowError:
            self.data = previous
            self.operations = previous_operations
            raise

    def _commit_report(self, history: list[dict[str, Any]], result: dict[str, Any]) -> None:
        rid = str(result["run_id"])
        for key in list(self.data["messages"]):
            self.remove("messages", key)
        for index, message in enumerate(history[-100:]):
            content = message.get("content", "")
            if isinstance(content, list):
                content = "\n".join(
                    str(b.get("text", ""))
                    for b in content
                    if isinstance(b, dict) and b.get("type") == "text"
                )
            if not content:
                continue
            text, clipped = bounded(content)
            mid = f"history:{index}"
            self.put(
                "messages",
                mid,
                {
                    "id": mid,
                    "role": message["role"],
                    "text": text,
                    "draft": False,
                    "truncated": clipped,
                },
            )
        tools = [deepcopy(tool) for tool in self.summaries.values() if tool.get("root_id") == rid]
        output_budget = 65536
        for tool in tools:
            retained = self.data["tools"].get(tool["id"])
            output = retained.get("output", "") if retained else ""
            text, clipped = bounded(output, min(4096, output_budget))
            output_budget -= len(text.encode("utf-8"))
            tool.update(
                output=text,
                truncated=clipped or not retained or bool(retained.get("truncated")),
                params=retained.get("params", "") if retained else "",
            )
        report = {
            "schema_version": 1,
            "run_id": rid,
            "title": self.data["runs"].get(rid, {}).get("request_content", "未记录本轮任务"),
            "request_id": self.data["runs"].get(rid, {}).get("request_id"),
            "generated_at": now(),
            "core": deepcopy(self.data["core"]),
            "result": result,
            "tools": tools,
            "note": (
                "执行事实来自 Core；模型结论不代替检查证据。"
                "每项输出最多保留 4 KiB，报告输出合计最多 64 KiB；省略内容会明确标注。"
            ),
        }
        self.put("reports", rid, report)
        self.meta(state="ready", error=None)
        self._trim()


def report_markdown(report: dict[str, Any]) -> str:
    result = report["result"]
    lines = [
        "# KitAgent 运行报告",
        "",
        f"任务：{report['title']}",
        f"运行：{report['run_id']}",
        f"结果：{result['status']}",
        f"模式：{report['core']['engine']} / {report['core']['sandbox_backend']}",
        f"生成时间：{report['generated_at']}",
        f"提交引用：{result['transcript_ref']}",
        "",
        "## 本轮模型结论",
        "",
        result.get("final_answer") or "本轮没有已提交的最终答复。",
        "",
        "## 执行事实",
        "",
    ]
    for tool in report["tools"]:
        lines.append(f"- {tool['name']} · {tool['status']} · {tool['id']}")
        if tool.get("decision"):
            lines.append(f"  审批：{tool['decision']}")
        if tool.get("params"):
            lines += ["", "  调用参数：", ""]
            lines.extend(f"    {line}" for line in tool["params"].splitlines())
        if tool.get("output"):
            lines += ["", "  工具输出：", ""]
            lines.extend(f"    {line}" for line in tool["output"].splitlines())
        if tool.get("truncated"):
            lines.append("  （工具正文已截断或不再保留）")
        for attempt in tool.get("attempts", []):
            lines.append(f"  尝试 {attempt['attempt']}：{attempt['error']}")
    lines += ["", report["note"], "", f"结论截断：{'是' if result.get('truncated') else '否'}"]
    return "\n".join(lines)
