from __future__ import annotations

from typing import Any

import pytest
from pydantic import BaseModel

import agent_runtime.core.tools.invocation as invocation_module
from agent_runtime.core.context import ExecutionContext
from agent_runtime.core.events.bus import EventBus
from agent_runtime.core.graph.state import RuntimeState, new_run_input
from agent_runtime.core.graph.tool_node import KitToolNode, route_after_tools
from agent_runtime.core.tools.base import BaseTool, ToolResult
from agent_runtime.core.tools.registry import ToolRegistry

pytestmark = pytest.mark.graph


class _RecordingTool(BaseTool):
    description = "record invocation order"
    input_schema: dict[str, object] = {"type": "object", "properties": {}}

    def __init__(self, name: str, order: list[str]) -> None:
        self.name = name
        self._order = order

    async def invoke(self, params: dict[str, object]) -> ToolResult:
        self._order.append(self.name)
        return ToolResult(content=f"result-{self.name}")


class _RetryOnceTool(BaseTool):
    name = "retry"
    description = "fail once"
    input_schema: dict[str, object] = {"type": "object", "properties": {}}

    def __init__(self) -> None:
        self.calls = 0

    async def invoke(self, params: dict[str, object]) -> ToolResult:
        self.calls += 1
        if self.calls == 1:
            return ToolResult(content="retry", is_error=True, error_type="runtime_error")
        return ToolResult(content="ok")


def _state(
    calls: list[dict[str, Any]],
    *,
    action: str = "execute",
    step: int = 1,
) -> RuntimeState:
    context = ExecutionContext(run_id="run-1", goal="goal", max_steps=3)
    state = new_run_input(context, context.messages)
    state["pending_tool_calls"] = calls
    state["pending_tool_action"] = action  # type: ignore[assignment]
    state["step"] = step
    return state


async def _events(bus: EventBus) -> list[BaseModel]:
    events: list[BaseModel] = []

    async def collect(event: BaseModel) -> None:
        events.append(event)

    bus.subscribe(collect)
    return events


async def test_multi_tool_batch_is_sequential_and_merged_into_one_message() -> None:
    order: list[str] = []
    registry = ToolRegistry()
    registry.register(_RecordingTool("one", order))
    registry.register(_RecordingTool("two", order))
    calls = [
        {"id": "a", "name": "one", "input": {}},
        {"id": "b", "name": "two", "input": {}},
    ]
    bus = EventBus()
    events = await _events(bus)

    update = await KitToolNode(registry, max_steps=3)(_state(calls), bus)

    assert order == ["one", "two"]
    assert update["tool_call_count"] == 2
    assert len(update["messages"]) == 1  # type: ignore[arg-type]
    content = update["messages"][0]["content"]  # type: ignore[index]
    assert [block["tool_use_id"] for block in content] == ["a", "b"]
    assert [event.type for event in events] == [  # type: ignore[attr-defined]
        "tool.call_started",
        "tool.call_finished",
        "tool.call_started",
        "tool.call_finished",
        "step.finished",
    ]


async def test_budget_preflight_rejects_whole_batch_without_side_effects() -> None:
    order: list[str] = []
    registry = ToolRegistry()
    registry.register(_RecordingTool("one", order))
    registry.register(_RecordingTool("two", order))
    calls = [
        {"id": "a", "name": "one", "input": {}},
        {"id": "b", "name": "two", "input": {}},
    ]
    state = _state(calls)
    state["tool_call_count"] = 1
    bus = EventBus()
    events = await _events(bus)

    update = await KitToolNode(registry, tool_call_budget=2, max_steps=3)(state, bus)

    assert order == []
    assert update["tool_call_count"] == 1
    assert update["status"] == "failed"
    assert update["reason"] == "exceeded_tool_call_budget"
    assert all(block["is_error"] for block in update["tool_results"])  # type: ignore[index]
    assert [event.type for event in events] == ["step.finished"]  # type: ignore[attr-defined]


async def test_max_tokens_batch_pairs_results_without_invoking_or_counting() -> None:
    order: list[str] = []
    registry = ToolRegistry()
    registry.register(_RecordingTool("one", order))
    state = _state(
        [{"id": "partial", "name": "one", "input": {}}],
        action="synthetic_error",
    )

    update = await KitToolNode(registry, tool_call_budget=1, max_steps=3)(state, EventBus())

    assert order == []
    assert update["tool_call_count"] == 0
    assert update["status"] == "running"
    block = update["tool_results"][0]  # type: ignore[index]
    assert block["tool_use_id"] == "partial"
    assert block["is_error"] is True
    assert "output token limit" in block["content"]


async def test_unknown_tool_is_observation_not_node_failure() -> None:
    update = await KitToolNode(ToolRegistry(), max_steps=3)(
        _state([{"id": "missing", "name": "missing", "input": {}}]),
        EventBus(),
    )

    assert update["status"] == "running"
    assert update["reason"] is None
    assert update["tool_results"][0]["is_error"] is True  # type: ignore[index]
    assert route_after_tools({**_state([]), **update}) == "model"  # type: ignore[arg-type]


async def test_last_step_executes_complete_batch_before_max_steps_failure() -> None:
    order: list[str] = []
    registry = ToolRegistry()
    registry.register(_RecordingTool("one", order))

    update = await KitToolNode(registry, max_steps=1)(
        _state([{"id": "a", "name": "one", "input": {}}], step=1),
        EventBus(),
    )

    assert order == ["one"]
    assert update["status"] == "failed"
    assert update["reason"] == "exceeded_max_steps"
    assert route_after_tools({**_state([]), **update}) == "end"  # type: ignore[arg-type]


async def test_retry_attempts_count_as_one_budgeted_root_call(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(invocation_module, "_RETRY_BASE_S", 0.0)
    tool = _RetryOnceTool()
    registry = ToolRegistry()
    registry.register(tool)

    update = await KitToolNode(registry, tool_call_budget=1, max_steps=3)(
        _state([{"id": "retry-1", "name": "retry", "input": {}}]),
        EventBus(),
    )

    assert tool.calls == 2
    assert update["tool_call_count"] == 1
    assert update["status"] == "running"


@pytest.mark.parametrize("budget,max_steps", [(0, 1), (1, 0)])
def test_node_budget_configuration_must_be_positive(budget: int, max_steps: int) -> None:
    with pytest.raises(ValueError):
        KitToolNode(ToolRegistry(), tool_call_budget=budget, max_steps=max_steps)
