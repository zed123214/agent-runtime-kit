from __future__ import annotations

from copy import deepcopy
from typing import Annotated, Any, Literal, TypedDict

from agent_runtime.core.context import ExecutionContext, ExecutionStatus

type StateUpdate = dict[str, object]
type PendingToolAction = Literal["execute", "synthetic_error"]


def append_messages(
    current: list[dict[str, Any]] | None,
    incoming: list[dict[str, Any]] | None,
) -> list[dict[str, Any]]:
    """Append a checkpoint update without deduplicating real repeated messages."""

    return [*(current or []), *(incoming or [])]


class RuntimeState(TypedDict):
    """JSON-compatible state persisted by the optional in-memory checkpointer."""

    run_id: str
    goal: str
    messages: Annotated[list[dict[str, Any]], append_messages]
    pending_tool_calls: list[dict[str, Any]]
    pending_tool_action: PendingToolAction | None
    tool_results: list[dict[str, Any]]
    errors: list[str]
    step: int
    tool_call_count: int
    status: ExecutionStatus
    reason: str | None
    final_answer: str
    trace_events: list[dict[str, object]]


def new_run_input(
    context: ExecutionContext,
    messages: list[dict[str, Any]],
) -> RuntimeState:
    """Create a fresh run-scoped state seeded from the authoritative transcript."""

    return RuntimeState(
        run_id=context.run_id,
        goal=context.goal,
        messages=deepcopy(messages),
        pending_tool_calls=[],
        pending_tool_action=None,
        tool_results=[],
        errors=[],
        step=0,
        tool_call_count=0,
        status="running",
        reason=None,
        final_answer="",
        trace_events=[],
    )
