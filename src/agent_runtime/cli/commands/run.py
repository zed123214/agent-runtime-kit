from __future__ import annotations

import asyncio
import json
import sys
import time
from typing import Any

from agent_runtime.core.config import RuntimeConfig
from agent_runtime.core.transport.socket_client import IpcError, SocketClient

_SAFE_CHANGED_FIELDS = frozenset(
    {
        "errors",
        "final_answer",
        "messages",
        "pending_tool_action",
        "pending_tool_calls",
        "reason",
        "status",
        "step",
        "tool_call_count",
        "tool_results",
        "trace_events",
    }
)


def _safe_code(value: object, limit: int = 48) -> str:
    if not isinstance(value, str):
        return ""
    cleaned = " ".join(value.split())
    return cleaned[:limit]


def _first_int(diff: dict[str, Any], *keys: str) -> int | None:
    for key in keys:
        value = diff.get(key)
        if isinstance(value, int) and not isinstance(value, bool):
            return value
    return None


def _state_diff_summary(value: object) -> str:
    """Format only bounded, non-content Graph state metadata."""

    if not isinstance(value, dict):
        return "updated"

    diff: dict[str, Any] = value
    parts: list[str] = []
    changed_value = diff.get("changed_fields")
    if isinstance(changed_value, list):
        changed = [
            item for item in changed_value if isinstance(item, str) and item in _SAFE_CHANGED_FIELDS
        ][:8]
        if changed:
            parts.append("changed=" + ",".join(changed))

    before = _first_int(diff, "message_count_before", "messages_before")
    after = _first_int(diff, "message_count_after", "messages_after")
    if before is not None and after is not None:
        parts.append(f"messages={before}->{after}")
    else:
        delta = _first_int(diff, "message_count_delta")
        count = _first_int(diff, "message_count")
        if delta is not None:
            parts.append(f"messages+={delta}")
        elif count is not None:
            parts.append(f"messages={count}")

    for key, label in (
        ("step", "step"),
        ("tool_call_count", "tools"),
        ("pending_tool_call_count", "pending"),
        ("tool_result_count", "results"),
        ("error_count", "errors"),
        ("errors_count", "errors"),
        ("trace_event_count", "trace"),
    ):
        count = _first_int(diff, key)
        if count is not None and not any(part.startswith(f"{label}=") for part in parts):
            parts.append(f"{label}={count}")

    for key in ("status", "reason"):
        code = _safe_code(diff.get(key))
        if code:
            parts.append(f"{key}={code}")

    return " ".join(parts) if parts else "updated"


class StdoutPrinter:
    # 接收 dict 格式的事件并将运行进度格式化打印到终端
    def __init__(self) -> None:
        self._inline = False  # True while LLM tokens are mid-line
        self._run_start: float = 0.0

    # 若当前行有未换行的 token，补一个换行符
    def _ensure_newline(self) -> None:
        if self._inline:
            print()
            self._inline = False

    # 根据事件 type 字段分发并格式化打印到 stdout/stderr
    async def handle(self, event: dict[str, Any]) -> None:
        t = event.get("type", "")

        if t == "run.started":
            self._run_start = time.monotonic()
            print(f"[run] {event.get('run_id', '')}")

        elif t == "node.started":
            self._ensure_newline()
            node_id = _safe_code(event.get("node_id")) or "unknown"
            print(f"[node {node_id}] started")

        elif t == "node.finished":
            self._ensure_newline()
            node_id = _safe_code(event.get("node_id")) or "unknown"
            status = _safe_code(event.get("status")) or "unknown"
            print(f"[node {node_id}] {status}")

        elif t == "state.diff":
            self._ensure_newline()
            node_id = _safe_code(event.get("node_id")) or "unknown"
            print(f"[state {node_id}] {_state_diff_summary(event.get('diff'))}")

        elif t == "step.started":
            self._ensure_newline()
            print(f"[step {event.get('step')}] planning...")

        elif t == "llm.token":
            print(event.get("token", ""), end="", flush=True)
            self._inline = True

        elif isinstance(t, str) and t.startswith("sandbox."):
            self._ensure_newline()
            state = _safe_code(t.removeprefix("sandbox.")) or "updated"
            reason = _safe_code(event.get("terminal_reason"))
            print(f"[sandbox] {state}" + (f" ({reason})" if reason else ""))

        elif t == "tool.call_started":
            self._ensure_newline()
            params_str = json.dumps(event.get("params", {}), ensure_ascii=False)
            print(f"[tool] {event.get('tool_name', '')} {params_str}")

        elif t == "tool.call_finished":
            print(f"[tool] {event.get('tool_name', '')} ✓  {event.get('elapsed_ms')}ms")

        elif t == "tool.call_failed":
            print(
                f"[tool] {event.get('tool_name', '')} ✗  {event.get('error_message', '')}",
                file=sys.stderr,
            )

        elif t == "step.finished":
            self._ensure_newline()
            print(f"[step {event.get('step')}] done")

        elif t == "run.finished":
            self._ensure_newline()
            elapsed = time.monotonic() - self._run_start
            reason = str(event.get("reason") or "")
            reason_text = f"  reason={reason}" if reason else ""
            print(
                f"[run] {event.get('status', '')}  {event.get('steps')} steps  "
                f"{elapsed:.1f}s{reason_text}"
            )
            error = event.get("error")
            if isinstance(error, dict) and error.get("message"):
                print(
                    f"[run] {error.get('code', 'error')}: {error['message']}",
                    file=sys.stderr,
                )


# 异步核心：连接 daemon，订阅事件，触发 run，等待 run.finished
async def _run_async(goal: str, config: RuntimeConfig) -> int:
    client = SocketClient(config.host, config.port)
    try:
        await client.connect()
    except (ConnectionRefusedError, OSError):
        print(f"error: core not running ({config.host}:{config.port})", file=sys.stderr)
        return 1

    printer = StdoutPrinter()
    finished = asyncio.Event()
    exit_code = 0

    async def on_event(event: dict[str, Any]) -> None:
        nonlocal exit_code
        await printer.handle(event)
        if event.get("type") == "run.finished":
            if event.get("status") != "success":
                exit_code = 1
            finished.set()

    client.on_event(on_event)
    loop_task = asyncio.create_task(client.run_event_loop())

    try:
        await client.send_command(
            "event.subscribe",
            {
                "topics": [
                    "run.*",
                    "node.*",
                    "state.diff",
                    "step.*",
                    "tool.*",
                    "sandbox.*",
                    "llm.token",
                    "llm.usage",
                ],
                "scope": "global",
            },
        )
        await client.send_command("agent.run", {"goal": goal})
    except IpcError as e:
        print(f"error: {e}", file=sys.stderr)
        loop_task.cancel()
        await client.close()
        return 1

    await finished.wait()

    loop_task.cancel()
    try:
        await loop_task
    except asyncio.CancelledError:
        pass

    await client.close()
    return exit_code


# 执行 agentrt run --goal "..." 命令
def cmd_run(goal: str, config: RuntimeConfig) -> None:
    try:
        exit_code = asyncio.run(_run_async(goal, config))
    except KeyboardInterrupt:
        sys.exit(130)
    sys.exit(exit_code)
