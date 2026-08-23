from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest
from pydantic import BaseModel

from agent_runtime.core.config import RuntimeConfig
from agent_runtime.core.context import ExecutionContext
from agent_runtime.core.engine.base import EngineRunConfig, RunOutcome
from agent_runtime.core.events.bus import EventBus
from agent_runtime.core.llm.types import LlmResponse, ToolCallBlock
from agent_runtime.core.permissions.manager import PermissionManager
from agent_runtime.core.permissions.policy import PermissionDecision, ToolPolicy
from agent_runtime.core.runner import AgentRunner
from agent_runtime.core.tools.base import BaseTool, ToolResult
from agent_runtime.core.tools.registry import ToolRegistry

pytest.importorskip("langgraph")

from agent_runtime.core.graph.engine import GraphExecutionEngine  # noqa: E402
from agent_runtime.core.graph.fake_provider import ScriptedProvider  # noqa: E402
from agent_runtime.core.graph.runtime import GraphRuntime, thread_config  # noqa: E402

pytestmark = pytest.mark.graph

_WAIT_S = 2.0


class _RecordingTool(BaseTool):
    description = "record deterministic side effects"
    input_schema: dict[str, object] = {"type": "object", "properties": {}}

    def __init__(self, name: str, order: list[str]) -> None:
        self.name = name
        self._order = order

    async def invoke(self, params: dict[str, object]) -> ToolResult:
        self._order.append(self.name)
        return ToolResult(content=f"result-{self.name}")


class _ErrorTool(BaseTool):
    name = "fail"
    description = "return a deterministic non-retryable error"
    input_schema: dict[str, object] = {"type": "object", "properties": {}}

    async def invoke(self, params: dict[str, object]) -> ToolResult:
        return ToolResult(content="expected failure", is_error=True, error_type="schema_error")


class _BlockingTool(BaseTool):
    name = "block"
    description = "block until cancelled"
    input_schema: dict[str, object] = {"type": "object", "properties": {}}

    def __init__(self) -> None:
        self.entered = asyncio.Event()
        self.cancelled = asyncio.Event()

    async def invoke(self, params: dict[str, object]) -> ToolResult:
        self.entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            self.cancelled.set()
        raise AssertionError("unreachable")


class _ExitGateRuntime(GraphRuntime):
    """Pause one task immediately after its per-thread lock is released."""

    def __init__(self) -> None:
        super().__init__()
        self.gated_task: asyncio.Task[object] | None = None
        self.scope_exited = asyncio.Event()
        self.release_exit = asyncio.Event()

    @asynccontextmanager
    async def thread_scope(self, thread_id: str) -> AsyncIterator[None]:
        try:
            async with super().thread_scope(thread_id):
                yield
        finally:
            if asyncio.current_task() is self.gated_task:
                self.scope_exited.set()
                await self.release_exit.wait()


class _SlowDeleteRuntime(GraphRuntime):
    """Expose the one-shot deletion window to deterministic cancellation."""

    def __init__(self) -> None:
        super().__init__()
        self.delete_entered = asyncio.Event()
        self.release_delete = asyncio.Event()

    async def _delete_thread_unlocked(self, thread_id: str) -> None:
        self.delete_entered.set()
        await self.release_delete.wait()
        await super()._delete_thread_unlocked(thread_id)


def _context(
    *,
    run_id: str = "run-1",
    goal: str = "goal",
    max_steps: int = 4,
    messages: list[dict[str, Any]] | None = None,
) -> ExecutionContext:
    return ExecutionContext(
        run_id=run_id,
        goal=goal,
        max_steps=max_steps,
        prefill_messages=messages or [],
    )


def _config(
    *,
    thread_id: str = "thread-1",
    retain_thread: bool = False,
    **changes: object,
) -> EngineRunConfig:
    base = EngineRunConfig(
        session_id="session-1",
        thread_id=thread_id,
        retain_thread=retain_thread,
        wall_time_s=1.0,
        trace_event_limit=8,
    )
    return replace(base, **changes)


async def _run(
    engine: GraphExecutionEngine,
    context: ExecutionContext,
    *,
    tools: ToolRegistry | None = None,
    events: EventBus | None = None,
    config: EngineRunConfig | None = None,
) -> RunOutcome:
    return await asyncio.wait_for(
        engine.run(
            context,
            tools=tools or ToolRegistry(),
            events=events or EventBus(),
            config=config or _config(),
        ),
        timeout=_WAIT_S,
    )


async def _events(bus: EventBus) -> list[BaseModel]:
    collected: list[BaseModel] = []

    async def collect(event: BaseModel) -> None:
        collected.append(event)

    bus.subscribe(collect)
    return collected


def _tool_result_blocks(context: ExecutionContext) -> list[dict[str, Any]]:
    blocks: list[dict[str, Any]] = []
    for message in context.messages:
        content = message.get("content")
        if message.get("role") == "user" and isinstance(content, list):
            blocks.extend(
                block
                for block in content
                if isinstance(block, dict) and block.get("type") == "tool_result"
            )
    return blocks


async def test_direct_answer_syncs_context_and_redacted_node_metadata() -> None:
    runtime = GraphRuntime()
    provider = ScriptedProvider.direct("top-secret-answer")
    engine = GraphExecutionEngine(provider, runtime=runtime)
    context = _context(max_steps=1)
    bus = EventBus(correlation_id="corr-1", session_id="session-1")
    events = await _events(bus)

    outcome = await _run(engine, context, events=bus)

    assert outcome == RunOutcome(
        status="success",
        result="top-secret-answer",
        reason=None,
        steps=1,
    )
    assert [event.type for event in events] == [  # type: ignore[attr-defined]
        "node.started",
        "step.started",
        "llm.model_selected",
        "llm.token",
        "step.finished",
        "node.finished",
        "state.diff",
    ]
    node_events = [event for event in events if event.type.startswith("node.")]  # type: ignore[attr-defined]
    assert all(event.correlation_id == "corr-1" for event in node_events)  # type: ignore[attr-defined]
    assert all(event.session_id == "session-1" for event in node_events)  # type: ignore[attr-defined]
    diff_json = json.dumps(events[-1].diff)  # type: ignore[attr-defined]
    assert "top-secret-answer" not in diff_json


async def test_single_tool_end_to_end_pairs_messages() -> None:
    runtime = GraphRuntime()
    order: list[str] = []
    tools = ToolRegistry()
    tools.register(_RecordingTool("echo", order))
    provider = ScriptedProvider.tool("echo", {"value": 1}, final_text="finished")
    context = _context()

    outcome = await _run(GraphExecutionEngine(provider, runtime=runtime), context, tools=tools)

    assert outcome.status == "success"
    assert outcome.result == "finished"
    assert outcome.steps == 2
    assert order == ["echo"]
    blocks = _tool_result_blocks(context)
    assert [block["tool_use_id"] for block in blocks] == ["tool-1"]


async def test_multi_tool_end_to_end_preserves_order_and_one_result_message() -> None:
    runtime = GraphRuntime()
    order: list[str] = []
    tools = ToolRegistry()
    tools.register(_RecordingTool("one", order))
    tools.register(_RecordingTool("two", order))
    provider = ScriptedProvider.multi(
        [
            ToolCallBlock(id="a", name="one", input={}),
            ToolCallBlock(id="b", name="two", input={}),
        ]
    )
    context = _context()

    outcome = await _run(GraphExecutionEngine(provider, runtime=runtime), context, tools=tools)

    assert outcome.status == "success"
    assert order == ["one", "two"]
    result_messages = [
        message
        for message in context.messages
        if message.get("role") == "user"
        and isinstance(message.get("content"), list)
        and message["content"]
        and message["content"][0].get("type") == "tool_result"
    ]
    assert len(result_messages) == 1
    assert [block["tool_use_id"] for block in result_messages[0]["content"]] == ["a", "b"]


async def test_tool_failure_is_observed_then_model_can_succeed() -> None:
    runtime = GraphRuntime()
    tools = ToolRegistry()
    tools.register(_ErrorTool())
    provider = ScriptedProvider.tool("fail", final_text="recovered")
    context = _context()

    outcome = await _run(GraphExecutionEngine(provider, runtime=runtime), context, tools=tools)

    assert outcome.status == "success"
    assert outcome.result == "recovered"
    blocks = _tool_result_blocks(context)
    assert len(blocks) == 1
    assert blocks[0]["is_error"] is True
    assert blocks[0]["content"] == "expected failure"


async def test_two_engine_instances_share_thread_and_submit_only_message_delta() -> None:
    runtime = GraphRuntime()
    first_provider = ScriptedProvider.direct("assistant-1")
    first_context = _context(
        run_id="run-1",
        messages=[{"role": "user", "content": "user-1"}],
    )
    retained = _config(thread_id="chat-thread", retain_thread=True)
    await _run(
        GraphExecutionEngine(first_provider, runtime=runtime),
        first_context,
        config=retained,
    )

    second_history = [
        *first_context.messages,
        {"role": "user", "content": "user-2"},
    ]
    second_provider = ScriptedProvider.direct("assistant-2")
    second_context = _context(run_id="run-2", messages=second_history)
    outcome = await _run(
        GraphExecutionEngine(second_provider, runtime=runtime),
        second_context,
        config=retained,
    )

    assert outcome.status == "success"
    seen = second_provider.calls[0]["messages"]
    assert [message.get("content") for message in seen].count("user-1") == 1
    assert (
        sum(
            1
            for message in seen
            if message.get("role") == "assistant"
            and any(
                isinstance(block, dict) and block.get("text") == "assistant-1"
                for block in message.get("content", [])
            )
        )
        == 1
    )
    assert [message.get("content") for message in seen].count("user-2") == 1
    assert second_context.messages[: len(second_history)] == second_history
    await asyncio.wait_for(runtime.close(), timeout=_WAIT_S)


async def test_prefix_mismatch_resets_checkpoint_before_reseed() -> None:
    runtime = GraphRuntime()
    retained = _config(thread_id="compact-thread", retain_thread=True)
    old_context = _context(messages=[{"role": "user", "content": "old-user"}])
    await _run(
        GraphExecutionEngine(ScriptedProvider.direct("old-answer"), runtime=runtime),
        old_context,
        config=retained,
    )

    compacted = [{"role": "user", "content": "compacted-summary"}]
    provider = ScriptedProvider.direct("new-answer")
    new_context = _context(run_id="run-new", messages=compacted)
    outcome = await _run(
        GraphExecutionEngine(provider, runtime=runtime),
        new_context,
        config=retained,
    )

    assert outcome.status == "success"
    assert provider.calls[0]["messages"] == compacted
    assert all("old" not in json.dumps(message) for message in new_context.messages)
    await asyncio.wait_for(runtime.close(), timeout=_WAIT_S)


async def test_one_shot_thread_is_deleted_after_terminal_state() -> None:
    runtime = GraphRuntime()
    config = _config(thread_id="one-shot", retain_thread=False)

    outcome = await _run(
        GraphExecutionEngine(ScriptedProvider.direct(), runtime=runtime),
        _context(),
        config=config,
    )

    assert outcome.status == "success"
    checkpoint = await asyncio.wait_for(
        runtime.saver.aget_tuple(thread_config("one-shot")),  # type: ignore[arg-type]
        timeout=_WAIT_S,
    )
    assert checkpoint is None
    assert "one-shot" not in runtime.known_threads


async def test_end_turn_on_last_allowed_model_step_wins() -> None:
    outcome = await _run(
        GraphExecutionEngine(ScriptedProvider.direct("done"), runtime=GraphRuntime()),
        _context(max_steps=1),
    )

    assert outcome.status == "success"
    assert outcome.steps == 1
    assert outcome.reason is None


async def test_infinite_tool_loop_executes_last_batch_then_exceeds_max_steps() -> None:
    runtime = GraphRuntime()
    order: list[str] = []
    tools = ToolRegistry()
    tools.register(_RecordingTool("echo", order))
    provider = ScriptedProvider.infinite("echo")
    context = _context(max_steps=2)

    outcome = await _run(GraphExecutionEngine(provider, runtime=runtime), context, tools=tools)

    assert outcome.status == "failed"
    assert outcome.reason == "exceeded_max_steps"
    assert outcome.steps == 2
    assert provider.call_count == 2
    assert order == ["echo", "echo"]
    assert len(_tool_result_blocks(context)) == 2


async def test_tool_budget_exact_boundary_executes_all_calls() -> None:
    runtime = GraphRuntime()
    order: list[str] = []
    tools = ToolRegistry()
    tools.register(_RecordingTool("one", order))
    tools.register(_RecordingTool("two", order))
    provider = ScriptedProvider.multi(
        [
            ToolCallBlock(id="a", name="one", input={}),
            ToolCallBlock(id="b", name="two", input={}),
        ]
    )

    outcome = await _run(
        GraphExecutionEngine(provider, runtime=runtime),
        _context(),
        tools=tools,
        config=_config(tool_call_budget=2),
    )

    assert outcome.status == "success"
    assert order == ["one", "two"]


async def test_tool_budget_rejects_oversized_batch_with_zero_side_effects() -> None:
    runtime = GraphRuntime()
    order: list[str] = []
    tools = ToolRegistry()
    tools.register(_RecordingTool("one", order))
    tools.register(_RecordingTool("two", order))
    provider = ScriptedProvider.multi(
        [
            ToolCallBlock(id="a", name="one", input={}),
            ToolCallBlock(id="b", name="two", input={}),
        ]
    )
    context = _context()
    bus = EventBus()
    events = await _events(bus)

    outcome = await _run(
        GraphExecutionEngine(provider, runtime=runtime),
        context,
        tools=tools,
        events=bus,
        config=_config(tool_call_budget=1),
    )

    assert outcome.status == "failed"
    assert outcome.reason == "exceeded_tool_call_budget"
    assert order == []
    assert len(_tool_result_blocks(context)) == 2
    assert "tool.call_started" not in [event.type for event in events]  # type: ignore[attr-defined]


async def test_wall_time_cancels_blocked_provider() -> None:
    provider = ScriptedProvider.block()
    context = _context()
    bus = EventBus()
    events = await _events(bus)
    engine = GraphExecutionEngine(provider, runtime=GraphRuntime())
    task = asyncio.create_task(
        engine.run(
            context,
            tools=ToolRegistry(),
            events=bus,
            config=_config(wall_time_s=0.5),
        )
    )
    await asyncio.wait_for(provider.entered.wait(), timeout=_WAIT_S)

    outcome = await asyncio.wait_for(task, timeout=_WAIT_S)

    assert outcome.status == "failed"
    assert outcome.reason == "exceeded_wall_time"
    finished = [event for event in events if event.type == "node.finished"]  # type: ignore[attr-defined]
    assert len(finished) == 1
    assert finished[0].status == "cancelled"  # type: ignore[attr-defined]


async def test_wall_time_cancels_blocked_tool() -> None:
    tool = _BlockingTool()
    tools = ToolRegistry()
    tools.register(tool)
    context = _context()
    engine = GraphExecutionEngine(ScriptedProvider.tool("block"), runtime=GraphRuntime())
    task = asyncio.create_task(
        engine.run(
            context,
            tools=tools,
            events=EventBus(),
            config=_config(wall_time_s=0.5),
        )
    )
    await asyncio.wait_for(tool.entered.wait(), timeout=_WAIT_S)

    outcome = await asyncio.wait_for(task, timeout=_WAIT_S)

    assert outcome.status == "failed"
    assert outcome.reason == "exceeded_wall_time"
    await asyncio.wait_for(tool.cancelled.wait(), timeout=_WAIT_S)


async def test_wall_time_cleans_permission_ask_pending() -> None:
    manager = PermissionManager(
        policies={"ask": ToolPolicy(default=PermissionDecision.ASK)},
        timeout_s=0,
    )
    order: list[str] = []
    tools = ToolRegistry()
    tools.register(_RecordingTool("ask", order))
    bus = EventBus()
    events = await _events(bus)
    requested = asyncio.Event()

    async def observe_request(event: BaseModel) -> None:
        if getattr(event, "type", None) == "permission.requested":
            requested.set()

    bus.subscribe(observe_request)
    engine = GraphExecutionEngine(
        ScriptedProvider.tool("ask"),
        runtime=GraphRuntime(),
        permission_manager=manager,
    )
    task = asyncio.create_task(
        engine.run(
            _context(),
            tools=tools,
            events=bus,
            config=_config(wall_time_s=0.5),
        )
    )
    await asyncio.wait_for(requested.wait(), timeout=_WAIT_S)

    outcome = await asyncio.wait_for(task, timeout=_WAIT_S)

    assert outcome.status == "failed"
    assert outcome.reason == "exceeded_wall_time"
    assert order == []
    assert manager._pending == {}
    assert "permission.requested" in [event.type for event in events]  # type: ignore[attr-defined]


async def test_recursion_limit_has_precise_reason_separate_from_max_steps() -> None:
    runtime = GraphRuntime()
    order: list[str] = []
    tools = ToolRegistry()
    tools.register(_RecordingTool("echo", order))
    context = _context(max_steps=10)

    outcome = await _run(
        GraphExecutionEngine(ScriptedProvider.infinite("echo"), runtime=runtime),
        context,
        tools=tools,
        config=_config(recursion_limit=2),
    )

    assert outcome.status == "failed"
    assert outcome.reason == "exceeded_recursion_limit"
    assert outcome.reason != "exceeded_max_steps"


async def test_engine_cancel_pairs_node_events_and_propagates_cancellation() -> None:
    provider = ScriptedProvider.block()
    runtime = GraphRuntime()
    engine = GraphExecutionEngine(provider, runtime=runtime)
    context = _context()
    bus = EventBus()
    events = await _events(bus)
    task = asyncio.create_task(
        engine.run(
            context,
            tools=ToolRegistry(),
            events=bus,
            config=_config(),
        )
    )
    await asyncio.wait_for(provider.entered.wait(), timeout=_WAIT_S)

    await asyncio.wait_for(engine.cancel(), timeout=_WAIT_S)

    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, timeout=_WAIT_S)
    assert context.status == "failed"
    assert context.reason == "cancelled"
    started = [event for event in events if event.type == "node.started"]  # type: ignore[attr-defined]
    finished = [event for event in events if event.type == "node.finished"]  # type: ignore[attr-defined]
    assert len(started) == len(finished) == 1
    assert finished[0].status == "cancelled"  # type: ignore[attr-defined]
    assert "state.diff" not in [event.type for event in events]  # type: ignore[attr-defined]


async def test_cancel_after_model_completion_flushes_success_and_diff_before_propagating() -> None:
    runtime = GraphRuntime()
    engine = GraphExecutionEngine(ScriptedProvider.direct("answer"), runtime=runtime)
    context = _context()
    bus = EventBus()
    events = await _events(bus)
    finish_entered = asyncio.Event()
    release_finish = asyncio.Event()

    async def slow_after_observing_finished(event: BaseModel) -> None:
        if getattr(event, "type", None) == "node.finished":
            finish_entered.set()
            await release_finish.wait()

    bus.subscribe(slow_after_observing_finished)
    task = asyncio.create_task(
        engine.run(
            context,
            tools=ToolRegistry(),
            events=bus,
            config=_config(),
        )
    )
    await asyncio.wait_for(finish_entered.wait(), timeout=_WAIT_S)

    await asyncio.wait_for(engine.cancel(), timeout=_WAIT_S)
    release_finish.set()

    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, timeout=_WAIT_S)
    lifecycle = [
        event
        for event in events
        if event.type in {"node.started", "node.finished", "state.diff"}  # type: ignore[attr-defined]
    ]
    assert [event.type for event in lifecycle] == [  # type: ignore[attr-defined]
        "node.started",
        "node.finished",
        "state.diff",
    ]
    assert lifecycle[1].status == "success"  # type: ignore[attr-defined]
    assert context.status == "failed"
    assert context.reason == "cancelled"


async def test_cancelled_run_cannot_sync_a_same_thread_successor_checkpoint() -> None:
    runtime = _ExitGateRuntime()
    blocked = ScriptedProvider.block()
    first_engine = GraphExecutionEngine(blocked, runtime=runtime)
    first_context = _context(run_id="run-old", goal="user-old")
    retained = _config(thread_id="shared-thread", retain_thread=True, wall_time_s=1.0)
    first_task = asyncio.create_task(
        first_engine.run(
            first_context,
            tools=ToolRegistry(),
            events=EventBus(),
            config=retained,
        )
    )
    runtime.gated_task = first_task
    await asyncio.wait_for(blocked.entered.wait(), timeout=_WAIT_S)

    successor_provider = ScriptedProvider.direct("successor-answer")
    successor_context = _context(run_id="run-new", goal="user-new")
    successor_task = asyncio.create_task(
        GraphExecutionEngine(successor_provider, runtime=runtime).run(
            successor_context,
            tools=ToolRegistry(),
            events=EventBus(),
            config=retained,
        )
    )
    await asyncio.sleep(0)
    await asyncio.wait_for(first_engine.cancel(), timeout=_WAIT_S)
    await asyncio.wait_for(runtime.scope_exited.wait(), timeout=_WAIT_S)

    try:
        successor = await asyncio.wait_for(successor_task, timeout=_WAIT_S)
    finally:
        runtime.release_exit.set()

    assert successor.result == "successor-answer"
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(first_task, timeout=_WAIT_S)
    assert first_context.status == "failed"
    assert first_context.reason == "cancelled"
    assert first_context.result == ""
    assert first_context.messages == [{"role": "user", "content": "user-old"}]


async def test_timed_out_run_cannot_sync_a_same_thread_successor_checkpoint() -> None:
    runtime = _ExitGateRuntime()
    blocked = ScriptedProvider.block()
    first_context = _context(run_id="run-old", goal="user-old")
    first_task = asyncio.create_task(
        GraphExecutionEngine(blocked, runtime=runtime).run(
            first_context,
            tools=ToolRegistry(),
            events=EventBus(),
            config=_config(
                thread_id="shared-thread",
                retain_thread=True,
                wall_time_s=0.1,
            ),
        )
    )
    runtime.gated_task = first_task
    await asyncio.wait_for(blocked.entered.wait(), timeout=_WAIT_S)

    successor_provider = ScriptedProvider.direct("successor-answer")
    successor_context = _context(run_id="run-new", goal="user-new")
    successor_task = asyncio.create_task(
        GraphExecutionEngine(successor_provider, runtime=runtime).run(
            successor_context,
            tools=ToolRegistry(),
            events=EventBus(),
            config=_config(
                thread_id="shared-thread",
                retain_thread=True,
                wall_time_s=1.0,
            ),
        )
    )
    await asyncio.wait_for(runtime.scope_exited.wait(), timeout=_WAIT_S)

    try:
        successor = await asyncio.wait_for(successor_task, timeout=_WAIT_S)
    finally:
        runtime.release_exit.set()

    timed_out = await asyncio.wait_for(first_task, timeout=_WAIT_S)
    assert successor.result == "successor-answer"
    assert timed_out.status == "failed"
    assert timed_out.reason == "exceeded_wall_time"
    assert first_context.result == ""
    assert first_context.messages == [{"role": "user", "content": "user-old"}]


async def test_cancel_during_one_shot_delete_cleans_checkpoint_and_engine_guard() -> None:
    runtime = _SlowDeleteRuntime()
    provider = ScriptedProvider(
        [
            LlmResponse(stop_reason="end_turn", text="first-answer"),
            LlmResponse(stop_reason="end_turn", text="second-answer"),
        ]
    )
    engine = GraphExecutionEngine(provider, runtime=runtime)
    context = _context(run_id="run-first", goal="first", max_steps=1)
    task = asyncio.create_task(
        engine.run(
            context,
            tools=ToolRegistry(),
            events=EventBus(),
            config=_config(thread_id="one-shot-first", retain_thread=False),
        )
    )
    await asyncio.wait_for(runtime.delete_entered.wait(), timeout=_WAIT_S)

    await asyncio.wait_for(engine.cancel(), timeout=_WAIT_S)
    await asyncio.wait_for(engine.cancel(), timeout=_WAIT_S)
    runtime.release_delete.set()

    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, timeout=_WAIT_S)
    assert engine._active_task is None
    assert context.status == "failed"
    assert context.reason == "cancelled"
    assert await runtime.saver.aget_tuple(thread_config("one-shot-first")) is None  # type: ignore[arg-type]

    second = await _run(
        engine,
        _context(run_id="run-second", goal="second", max_steps=1),
        config=_config(thread_id="one-shot-second", retain_thread=False),
    )
    assert second.status == "success"
    assert second.result == "second-answer"


async def test_non_retained_cleanup_precedes_same_thread_successor() -> None:
    runtime = GraphRuntime()
    first_provider = ScriptedProvider.block()
    first_task = asyncio.create_task(
        GraphExecutionEngine(first_provider, runtime=runtime).run(
            _context(run_id="run-first", goal="first", max_steps=1),
            tools=ToolRegistry(),
            events=EventBus(),
            config=_config(thread_id="shared-cleanup", retain_thread=False),
        )
    )
    await asyncio.wait_for(first_provider.entered.wait(), timeout=_WAIT_S)

    successor_provider = ScriptedProvider.direct("successor-answer")
    successor_task = asyncio.create_task(
        GraphExecutionEngine(successor_provider, runtime=runtime).run(
            _context(run_id="run-second", goal="second", max_steps=1),
            tools=ToolRegistry(),
            events=EventBus(),
            config=_config(thread_id="shared-cleanup", retain_thread=True),
        )
    )
    await asyncio.sleep(0)
    first_provider.release()

    first, successor = await asyncio.wait_for(
        asyncio.gather(first_task, successor_task),
        timeout=_WAIT_S,
    )
    assert first.status == "success"
    assert successor.result == "successor-answer"
    assert successor_provider.calls[0]["messages"] == [{"role": "user", "content": "second"}]
    assert await runtime.saver.aget_tuple(thread_config("shared-cleanup")) is not None  # type: ignore[arg-type]
    assert runtime.known_threads == frozenset({"shared-cleanup"})


@pytest.mark.parametrize(
    ("termination", "expected_reason"),
    [("cancel", "cancelled"), ("timeout", "exceeded_wall_time")],
)
async def test_interrupted_retained_tool_batch_is_paired_and_checkpoint_reset(
    termination: str,
    expected_reason: str,
) -> None:
    runtime = GraphRuntime()
    tool = _BlockingTool()
    tools = ToolRegistry()
    tools.register(tool)
    engine = GraphExecutionEngine(ScriptedProvider.tool("block"), runtime=runtime)
    context = _context()
    task = asyncio.create_task(
        engine.run(
            context,
            tools=tools,
            events=EventBus(),
            config=_config(
                thread_id="retained-interrupted-tool",
                retain_thread=True,
                wall_time_s=0.5 if termination == "timeout" else 1.0,
            ),
        )
    )
    await asyncio.wait_for(tool.entered.wait(), timeout=_WAIT_S)
    if termination == "cancel":
        await asyncio.wait_for(engine.cancel(), timeout=_WAIT_S)

    if termination == "cancel":
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, timeout=_WAIT_S)
    else:
        outcome = await asyncio.wait_for(task, timeout=_WAIT_S)
        assert outcome.status == "failed"
        assert outcome.reason == expected_reason

    assert context.status == "failed"
    assert context.reason == expected_reason
    blocks = _tool_result_blocks(context)
    assert len(blocks) == 1
    assert blocks[0]["tool_use_id"] == "tool-1"
    assert blocks[0]["is_error"] is True
    assert "execution outcome is unknown" in blocks[0]["content"]
    assert (
        await runtime.saver.aget_tuple(  # type: ignore[arg-type]
            thread_config("retained-interrupted-tool")
        )
        is None
    )


async def test_recursion_before_tool_node_pairs_history_and_resets_checkpoint() -> None:
    runtime = GraphRuntime()
    order: list[str] = []
    tools = ToolRegistry()
    tools.register(_RecordingTool("echo", order))
    context = _context(max_steps=5)

    outcome = await _run(
        GraphExecutionEngine(ScriptedProvider.infinite("echo"), runtime=runtime),
        context,
        tools=tools,
        config=_config(
            thread_id="retained-recursion",
            retain_thread=True,
            recursion_limit=1,
        ),
    )

    assert outcome.status == "failed"
    assert outcome.reason == "exceeded_recursion_limit"
    assert order == []
    blocks = _tool_result_blocks(context)
    assert len(blocks) == 1
    assert blocks[0]["tool_use_id"] == "loop-tool"
    assert blocks[0]["is_error"] is True
    assert "execution outcome is unknown" in blocks[0]["content"]
    assert (
        await runtime.saver.aget_tuple(  # type: ignore[arg-type]
            thread_config("retained-recursion")
        )
        is None
    )


async def test_interrupted_call_is_paired_even_when_an_older_turn_reused_its_id() -> None:
    runtime = GraphRuntime()
    tool = _BlockingTool()
    tools = ToolRegistry()
    tools.register(tool)
    context = ExecutionContext(
        run_id="run-reused-id",
        goal="current-user",
        max_steps=3,
        prefill_messages=[
            {"role": "user", "content": "older-user"},
            {
                "role": "assistant",
                "content": [{"type": "tool_use", "id": "tool-1", "name": "block", "input": {}}],
            },
            {
                "role": "user",
                "content": [
                    {
                        "type": "tool_result",
                        "tool_use_id": "tool-1",
                        "content": "older-result",
                    }
                ],
            },
            {"role": "user", "content": "current-user"},
        ],
    )
    engine = GraphExecutionEngine(ScriptedProvider.tool("block"), runtime=runtime)
    task = asyncio.create_task(
        engine.run(
            context,
            tools=tools,
            events=EventBus(),
            config=_config(thread_id="reused-id", retain_thread=True),
        )
    )
    await asyncio.wait_for(tool.entered.wait(), timeout=_WAIT_S)
    await asyncio.wait_for(engine.cancel(), timeout=_WAIT_S)

    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, timeout=_WAIT_S)
    results = _tool_result_blocks(context)
    assert [block["tool_use_id"] for block in results] == ["tool-1", "tool-1"]
    assert results[-1]["is_error"] is True
    assert "execution outcome is unknown" in results[-1]["content"]


async def test_unexpected_tool_node_exception_repairs_retained_pending_calls() -> None:
    runtime = GraphRuntime()
    order: list[str] = []
    tools = ToolRegistry()
    tools.register(_RecordingTool("echo", order))
    events = EventBus()

    async def fail_after_tool_side_effect(event: BaseModel) -> None:
        if getattr(event, "type", None) == "step.finished":
            raise RuntimeError("terminal subscriber failed")

    events.subscribe(fail_after_tool_side_effect)
    context = _context()
    engine = GraphExecutionEngine(ScriptedProvider.tool("echo"), runtime=runtime)

    with pytest.raises(RuntimeError, match="terminal subscriber failed"):
        await asyncio.wait_for(
            engine.run(
                context,
                tools=tools,
                events=events,
                config=_config(thread_id="retained-tool-exception", retain_thread=True),
            ),
            timeout=_WAIT_S,
        )

    assert order == ["echo"]
    blocks = _tool_result_blocks(context)
    assert len(blocks) == 1
    assert blocks[0]["tool_use_id"] == "tool-1"
    assert blocks[0]["is_error"] is True
    assert "engine_execution_error" in blocks[0]["content"]
    assert (
        await runtime.saver.aget_tuple(  # type: ignore[arg-type]
            thread_config("retained-tool-exception")
        )
        is None
    )


async def test_runner_persists_node_finished_before_forwarding_to_slow_sink(
    tmp_path: Path,
) -> None:
    config = RuntimeConfig()
    config.agent.engine = "graph"
    config.compaction.auto_threshold = 0
    forwarded = EventBus()
    finish_entered = asyncio.Event()
    release_finish = asyncio.Event()

    async def slow_external_sink(event: BaseModel) -> None:
        if getattr(event, "type", None) == "node.finished":
            finish_entered.set()
            await release_finish.wait()

    forwarded.subscribe(slow_external_sink)
    runner = AgentRunner(
        config,
        bus=forwarded,
        provider=ScriptedProvider.direct("writer-first"),
        runs_dir=tmp_path,
    )
    run_id = "run-writer-first"
    task = asyncio.create_task(runner.run_and_capture("goal", run_id=run_id))
    await asyncio.wait_for(finish_entered.wait(), timeout=_WAIT_S)

    rows = [
        json.loads(line)
        for line in (tmp_path / run_id / "events.jsonl").read_text(encoding="utf-8").splitlines()
    ]
    assert [row["type"] for row in rows][-1] == "node.finished"

    release_finish.set()
    outcome = await asyncio.wait_for(task, timeout=_WAIT_S)
    assert outcome.status == "success"
