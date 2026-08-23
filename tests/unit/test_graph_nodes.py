from __future__ import annotations

import pytest
from pydantic import BaseModel

from agent_runtime.core.context import ExecutionContext
from agent_runtime.core.events.bus import EventBus
from agent_runtime.core.graph.fake_provider import ScriptedProvider
from agent_runtime.core.graph.nodes import ModelNode, route_after_model
from agent_runtime.core.graph.state import RuntimeState, new_run_input
from agent_runtime.core.llm.types import LlmResponse, ToolCallBlock

pytestmark = pytest.mark.graph


def _state(*, max_steps: int = 3) -> RuntimeState:
    context = ExecutionContext(run_id="run-1", goal="goal", max_steps=max_steps)
    return new_run_input(context, context.messages)


async def _events(bus: EventBus) -> list[BaseModel]:
    events: list[BaseModel] = []

    async def collect(event: BaseModel) -> None:
        events.append(event)

    bus.subscribe(collect)
    return events


async def test_model_node_preserves_canonical_assistant_block_order() -> None:
    thinking = [
        {"type": "thinking", "thinking": "first", "signature": "sig-1"},
        {"type": "thinking", "thinking": "second", "signature": "sig-2"},
    ]
    calls = [
        ToolCallBlock(id="a", name="one", input={"x": 1}),
        ToolCallBlock(id="b", name="two", input={"y": 2}),
    ]
    provider = ScriptedProvider(
        [
            LlmResponse(
                stop_reason="tool_use",
                thinking_blocks=thinking,
                text="working",
                tool_calls=calls,
            )
        ]
    )
    bus = EventBus()
    events = await _events(bus)
    observed_steps: list[int] = []
    node = ModelNode(provider, [], "system", 3, observed_steps.append)

    update = await node(_state(), bus)

    assistant = update["messages"][0]  # type: ignore[index]
    assert assistant["content"] == [  # type: ignore[index]
        *thinking,
        {"type": "text", "text": "working"},
        {"type": "tool_use", "id": "a", "name": "one", "input": {"x": 1}},
        {"type": "tool_use", "id": "b", "name": "two", "input": {"y": 2}},
    ]
    assert update["pending_tool_action"] == "execute"
    assert update["step"] == 1
    assert observed_steps == [1]
    assert [event.type for event in events] == [  # type: ignore[attr-defined]
        "step.started",
        "llm.model_selected",
        "llm.token",
    ]


async def test_end_turn_wins_on_last_allowed_step() -> None:
    bus = EventBus()
    events = await _events(bus)
    node = ModelNode(ScriptedProvider.direct("done"), [], "system", 1)

    update = await node(_state(max_steps=1), bus)

    assert update["status"] == "success"
    assert update["final_answer"] == "done"
    assert update["reason"] is None
    assert events[-1].type == "step.finished"  # type: ignore[attr-defined]


async def test_tool_use_on_last_step_defers_max_steps_until_tools_finish() -> None:
    response = LlmResponse(
        stop_reason="tool_use",
        tool_calls=[ToolCallBlock(id="t1", name="echo", input={})],
    )
    bus = EventBus()
    events = await _events(bus)
    node = ModelNode(ScriptedProvider([response]), [], "system", 1)

    update = await node(_state(max_steps=1), bus)

    assert update["status"] == "running"
    assert update["pending_tool_calls"]
    assert route_after_model({**_state(max_steps=1), **update}) == "kit_tools"  # type: ignore[arg-type]
    assert "step.finished" not in [event.type for event in events]  # type: ignore[attr-defined]


async def test_max_tokens_tool_calls_are_deferred_as_synthetic_batch() -> None:
    response = LlmResponse(
        stop_reason="max_tokens",
        tool_calls=[ToolCallBlock(id="partial", name="echo", input={"x": 1})],
    )
    bus = EventBus()
    events = await _events(bus)
    node = ModelNode(ScriptedProvider([response]), [], "system", 3)

    update = await node(_state(), bus)

    assert update["pending_tool_action"] == "synthetic_error"
    assert update["status"] == "running"
    assert "step.finished" not in [event.type for event in events]  # type: ignore[attr-defined]


async def test_provider_failure_maps_to_llm_error_without_step_finished() -> None:
    bus = EventBus()
    events = await _events(bus)
    node = ModelNode(ScriptedProvider.failure(RuntimeError("offline failure")), [], "system", 3)

    update = await node(_state(), bus)

    assert update["status"] == "failed"
    assert update["reason"] == "llm_error"
    assert update["step"] == 1
    assert [event.type for event in events] == [  # type: ignore[attr-defined]
        "step.started",
        "llm.model_selected",
    ]


@pytest.mark.parametrize("stop_reason", ["max_tokens", "pause_turn", "refusal"])
async def test_nonterminal_response_without_calls_stops_at_max_steps(stop_reason: str) -> None:
    node = ModelNode(
        ScriptedProvider([LlmResponse(stop_reason=stop_reason)]),
        [],
        "system",
        1,
    )

    update = await node(_state(max_steps=1), EventBus())

    assert update["status"] == "failed"
    assert update["reason"] == "exceeded_max_steps"


def test_route_after_model_distinguishes_loop_tools_and_end() -> None:
    state = _state()
    assert route_after_model(state) == "model"
    state["pending_tool_calls"] = [{"id": "t", "name": "echo", "input": {}}]
    assert route_after_model(state) == "kit_tools"
    state["status"] = "failed"
    assert route_after_model(state) == "end"
