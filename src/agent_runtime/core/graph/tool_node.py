from __future__ import annotations

from datetime import UTC, datetime
from functools import partial
from typing import TYPE_CHECKING, Any, Literal

from agent_runtime.core.bus.events import StepFinishedEvent
from agent_runtime.core.events.bus import EventBus
from agent_runtime.core.graph.state import RuntimeState, StateUpdate
from agent_runtime.core.llm.types import ToolCallBlock
from agent_runtime.core.tools.invocation import invoke_tool
from agent_runtime.core.tools.registry import ToolRegistry

if TYPE_CHECKING:
    from agent_runtime.core.graph.recovery import RecoveryStore
    from agent_runtime.core.permissions.manager import PermissionManager

_MAX_TOKENS_RESULT = (
    "Error: output token limit reached before this tool call could be completed. "
    "Please break the task into smaller steps and try again."
)


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _deserialize_call(raw: dict[str, Any]) -> ToolCallBlock:
    call_id = raw.get("id")
    name = raw.get("name")
    params = raw.get("input")
    if not isinstance(call_id, str) or not call_id:
        raise ValueError("pending tool call id must be a non-empty string")
    if not isinstance(name, str) or not name:
        raise ValueError("pending tool call name must be a non-empty string")
    if not isinstance(params, dict):
        raise ValueError("pending tool call input must be an object")
    return ToolCallBlock(id=call_id, name=name, input=dict(params))


def _result_block(
    call: ToolCallBlock,
    content: str,
    *,
    is_error: bool,
) -> dict[str, Any]:
    block: dict[str, Any] = {
        "type": "tool_result",
        "tool_use_id": call.id,
        "content": content,
    }
    if is_error:
        block["is_error"] = True
    return block


class KitToolNode:
    """Sequentially execute one model batch through KitAgent's governance path."""

    def __init__(
        self,
        registry: ToolRegistry,
        *,
        permission_manager: PermissionManager | None = None,
        recovery_store: RecoveryStore | None = None,
        durable: bool = False,
        session_id: str = "",
        forced_permission_tool_use_id: str | None = None,
        tool_call_budget: int = 64,
        max_steps: int,
    ) -> None:
        if tool_call_budget <= 0:
            raise ValueError("tool_call_budget must be positive")
        if max_steps <= 0:
            raise ValueError("max_steps must be positive")
        self._registry = registry
        self._permission_manager = permission_manager
        self._recovery_store = recovery_store
        self._durable = durable
        self._session_id = session_id
        self._forced_permission_tool_use_id = forced_permission_tool_use_id
        self._tool_call_budget = tool_call_budget
        self._max_steps = max_steps

    async def __call__(self, state: RuntimeState, node_bus: EventBus) -> StateUpdate:
        if state["status"] != "running":
            raise RuntimeError("KitToolNode requires running state")

        # Validate the whole checkpointed batch before any tool can have a side effect.
        calls = [_deserialize_call(raw) for raw in state["pending_tool_calls"]]
        if not calls:
            raise RuntimeError("KitToolNode requires at least one pending tool call")

        action = state.get("pending_tool_action") or "execute"
        if action not in ("execute", "synthetic_error"):
            raise ValueError(f"unsupported pending tool action: {action!r}")

        tool_count = state["tool_call_count"]
        blocks: list[dict[str, Any]] = []
        errors = list(state["errors"])
        status = "running"
        reason: str | None = None

        if action == "synthetic_error":
            for call in calls:
                blocks.append(_result_block(call, _MAX_TOKENS_RESULT, is_error=True))
                errors.append("max_tokens_incomplete_tool_call")
        elif tool_count + len(calls) > self._tool_call_budget:
            message = (
                "Error: tool call budget would be exceeded by this batch; "
                "no tools in the batch were executed."
            )
            for call in calls:
                blocks.append(_result_block(call, message, is_error=True))
                errors.append("tool_call_budget_rejected")
            status = "failed"
            reason = "exceeded_tool_call_budget"
        else:
            for call in calls:
                # Count a root call exactly once when it is submitted. Retries remain
                # an implementation detail inside invoke_tool.
                tool_count += 1
                durable_permission = None
                if self._durable:
                    from agent_runtime.core.graph.interrupts import check_durable_permission

                    durable_permission = partial(
                        check_durable_permission,
                        self._permission_manager,
                        call,
                        session_id=self._session_id,
                        run_id=state["run_id"],
                        force_interrupt=call.id == self._forced_permission_tool_use_id,
                    )
                result = await invoke_tool(
                    self._registry,
                    call,
                    node_bus,
                    state["run_id"],
                    permission_manager=(None if self._durable else self._permission_manager),
                    session_id=self._session_id,
                    recovery_store=self._recovery_store,
                    durable_permission=durable_permission,
                )
                blocks.append(_result_block(call, result.content, is_error=result.is_error))
                if result.is_error:
                    errors.append(f"tool_error:{result.error_type or 'runtime_error'}")

        if status == "running" and state["step"] >= self._max_steps:
            status = "failed"
            reason = "exceeded_max_steps"

        await node_bus.publish(
            StepFinishedEvent(run_id=state["run_id"], step=state["step"], ts=_now())
        )

        all_results = [*state["tool_results"], *blocks]
        return {
            "messages": [{"role": "user", "content": blocks}],
            "pending_tool_calls": [],
            "pending_tool_action": None,
            "tool_results": all_results,
            "errors": errors,
            "tool_call_count": tool_count,
            "status": status,
            "reason": reason,
        }


def route_after_tools(state: RuntimeState) -> Literal["model", "end"]:
    return "model" if state["status"] == "running" else "end"
