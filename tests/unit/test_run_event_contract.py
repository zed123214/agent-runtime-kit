from __future__ import annotations

import asyncio
import json
from pathlib import Path

from pydantic import BaseModel, TypeAdapter

from agent_runtime.core.bus.events import (
    Event,
    LlmReasoningEvent,
    NodeFinishedEvent,
    NodeStartedEvent,
    StateDiffEvent,
    StepStartedEvent,
)
from agent_runtime.core.config import RuntimeConfig
from agent_runtime.core.events.bus import EventBus
from agent_runtime.core.llm.types import LlmResponse, ToolCallBlock
from agent_runtime.core.runner import AgentRunner
from agent_runtime.core.session.model import Session


class _EndTurnProvider:
    async def chat(
        self,
        messages: list[dict[str, object]],
        tool_schemas: list[dict[str, object]],
        bus: EventBus,
        run_id: str,
        *,
        step: int = 0,
        system: str | None = None,
    ) -> LlmResponse:
        return LlmResponse(stop_reason="end_turn", text="done")


class _ListThenFinishProvider:
    def __init__(self) -> None:
        self.calls = 0

    async def chat(
        self,
        messages: list[dict[str, object]],
        tool_schemas: list[dict[str, object]],
        bus: EventBus,
        run_id: str,
        *,
        step: int = 0,
        system: str | None = None,
    ) -> LlmResponse:
        self.calls += 1
        if self.calls == 1:
            return LlmResponse(
                stop_reason="tool_use",
                tool_calls=[
                    ToolCallBlock(
                        id="list-1",
                        name="list_dir",
                        input={"path": ".", "max_depth": 1},
                    )
                ],
            )
        return LlmResponse(stop_reason="end_turn", text="listed")


class _RunBarrier:
    def __init__(self) -> None:
        self.arrivals = 0
        self.release = asyncio.Event()

    async def wait(self) -> None:
        self.arrivals += 1
        if self.arrivals == 2:
            self.release.set()
        await self.release.wait()


class _BarrierProvider:
    def __init__(self, barrier: _RunBarrier) -> None:
        self._barrier = barrier

    async def chat(
        self,
        messages: list[dict[str, object]],
        tool_schemas: list[dict[str, object]],
        bus: EventBus,
        run_id: str,
        *,
        step: int = 0,
        system: str | None = None,
    ) -> LlmResponse:
        await self._barrier.wait()
        return LlmResponse(stop_reason="end_turn", text=run_id)


def _config() -> RuntimeConfig:
    config = RuntimeConfig()
    config.agent.max_steps = 5
    return config


async def test_event_bus_fills_context_without_mutating_or_overriding_events() -> None:
    bus = EventBus(
        correlation_id="corr-1",
        session_id="session-1",
        node_id="default-node",
    )
    collected: list[BaseModel] = []

    async def collect(event: BaseModel) -> None:
        collected.append(event)

    bus.subscribe(collect)
    step = StepStartedEvent(run_id="run-1", step=1, ts="t")
    explicit_node = NodeStartedEvent(run_id="run-1", node_id="model", ts="t")

    await bus.publish(step)
    await bus.publish(explicit_node)

    published_step = collected[0]
    assert published_step.correlation_id == "corr-1"  # type: ignore[attr-defined]
    assert published_step.session_id == "session-1"  # type: ignore[attr-defined]
    assert published_step.node_id == "default-node"  # type: ignore[attr-defined]
    assert step.correlation_id is None
    assert step.session_id is None
    assert step.node_id is None
    assert collected[1].node_id == "model"  # type: ignore[attr-defined]


def test_node_and_state_diff_events_round_trip_through_event_union() -> None:
    adapter = TypeAdapter(Event)
    events = [
        LlmReasoningEvent(
            run_id="run-1",
            correlation_id="corr-1",
            session_id="session-1",
            reasoning="checking the next action",
            ts="t0",
        ),
        NodeStartedEvent(
            run_id="run-1",
            correlation_id="corr-1",
            session_id="session-1",
            node_id="model",
            ts="t1",
        ),
        NodeFinishedEvent(
            run_id="run-1",
            correlation_id="corr-1",
            session_id="session-1",
            node_id="model",
            status="success",
            ts="t2",
        ),
        StateDiffEvent(
            run_id="run-1",
            correlation_id="corr-1",
            session_id="session-1",
            node_id="model",
            diff={"answer": {"before": None, "after": "42"}},
            ts="t3",
        ),
    ]

    round_tripped = [adapter.validate_json(event.model_dump_json()) for event in events]

    assert round_tripped == events
    assert [event.type for event in round_tripped] == [
        "llm.reasoning",
        "node.started",
        "node.finished",
        "state.diff",
    ]


def _legacy_event_payloads() -> list[dict[str, object]]:
    payloads = [
        {"type": "core.started", "listen_addr": "127.0.0.1:7437", "version": "1"},
        {"type": "run.started", "run_id": "r", "goal": "g", "ts": "t"},
        {"type": "run.finished", "run_id": "r", "status": "success", "steps": 1, "ts": "t"},
        {"type": "step.started", "run_id": "r", "step": 1, "ts": "t"},
        {"type": "step.finished", "run_id": "r", "step": 1, "ts": "t"},
        {
            "type": "tool.call_started",
            "run_id": "r",
            "tool_use_id": "u",
            "tool_name": "read_file",
            "params": {},
            "ts": "t",
        },
        {
            "type": "tool.call_finished",
            "run_id": "r",
            "tool_use_id": "u",
            "tool_name": "read_file",
            "elapsed_ms": 1,
            "ts": "t",
        },
        {
            "type": "tool.call_failed",
            "run_id": "r",
            "tool_use_id": "u",
            "tool_name": "read_file",
            "error_class": "runtime_error",
            "error_message": "failed",
            "elapsed_ms": 1,
            "ts": "t",
        },
        {"type": "llm.token", "run_id": "r", "token": "x", "ts": "t"},
        {
            "type": "llm.usage",
            "run_id": "r",
            "input_tokens": 1,
            "output_tokens": 1,
            "cache_read_input_tokens": 0,
            "cache_creation_input_tokens": 0,
            "ts": "t",
        },
        {
            "type": "llm.model_selected",
            "run_id": "r",
            "model": "m",
            "strategy": "static",
            "ts": "t",
        },
        {
            "type": "log.line",
            "run_id": "r",
            "level": "INFO",
            "source": "test",
            "message": "ok",
            "ts": "t",
        },
        {"type": "session.created", "session_id": "s", "mode": "chat", "ts": "t"},
        {
            "type": "session.message_received",
            "session_id": "s",
            "content": "hello",
            "ts": "t",
        },
        {
            "type": "session.waiting_for_input",
            "session_id": "s",
            "last_run_id": "r",
            "ts": "t",
        },
        {"type": "session.resumed", "session_id": "s", "ts": "t"},
        {"type": "session.closed", "session_id": "s", "ts": "t"},
        {
            "type": "context.compacted",
            "run_id": "r",
            "session_id": "s",
            "original_tokens": 10,
            "summary_tokens": 4,
            "ts": "t",
        },
        {
            "type": "permission.requested",
            "run_id": "r",
            "tool_use_id": "u",
            "tool_name": "bash",
            "params": {},
            "param_preview": "{}",
            "session_id": "s",
            "ts": "t",
        },
        {
            "type": "permission.granted",
            "run_id": "r",
            "tool_use_id": "u",
            "decision": "allow_once",
            "ts": "t",
        },
        {
            "type": "permission.denied",
            "run_id": "r",
            "tool_use_id": "u",
            "decision": "deny_once",
            "ts": "t",
        },
        {
            "type": "subagent.started",
            "run_id": "child",
            "parent_run_id": "r",
            "description": "task",
            "ts": "t",
        },
        {
            "type": "subagent.finished",
            "run_id": "child",
            "parent_run_id": "r",
            "status": "success",
            "ts": "t",
        },
        {
            "type": "skill.invoked",
            "run_id": "r",
            "skill_name": "skill",
            "arguments": "",
            "ts": "t",
        },
    ]

    return payloads


def test_all_legacy_event_payloads_remain_valid_without_new_metadata() -> None:
    payloads = _legacy_event_payloads()

    parsed = [TypeAdapter(Event).validate_python(payload) for payload in payloads]

    assert len(parsed) == 24
    assert [event.type for event in parsed] == [payload["type"] for payload in payloads]
    for event in parsed:
        if hasattr(event, "correlation_id"):
            assert event.correlation_id is None
            assert event.node_id is None


def test_legacy_event_schema_snapshot_preserves_fields_and_requiredness() -> None:
    expected: dict[str, tuple[set[str], set[str]]] = {
        "core.started": (
            {"type", "listen_addr", "version"},
            {"listen_addr", "version"},
        ),
        "run.started": (
            {"type", "run_id", "goal", "ts"},
            {"run_id", "goal", "ts"},
        ),
        "run.finished": (
            {"type", "run_id", "status", "reason", "steps", "ts"},
            {"run_id", "status", "steps", "ts"},
        ),
        "step.started": (
            {"type", "run_id", "step", "ts"},
            {"run_id", "step", "ts"},
        ),
        "step.finished": (
            {"type", "run_id", "step", "ts"},
            {"run_id", "step", "ts"},
        ),
        "tool.call_started": (
            {"type", "run_id", "tool_use_id", "tool_name", "params", "ts"},
            {"run_id", "tool_use_id", "tool_name", "params", "ts"},
        ),
        "tool.call_finished": (
            {
                "type",
                "run_id",
                "tool_use_id",
                "tool_name",
                "elapsed_ms",
                "output",
                "ts",
            },
            {"run_id", "tool_use_id", "tool_name", "elapsed_ms", "ts"},
        ),
        "tool.call_failed": (
            {
                "type",
                "run_id",
                "tool_use_id",
                "tool_name",
                "error_class",
                "error_message",
                "elapsed_ms",
                "attempt",
                "ts",
            },
            {
                "run_id",
                "tool_use_id",
                "tool_name",
                "error_class",
                "error_message",
                "elapsed_ms",
                "ts",
            },
        ),
        "llm.token": (
            {"type", "run_id", "token", "ts"},
            {"run_id", "token", "ts"},
        ),
        "llm.usage": (
            {
                "type",
                "run_id",
                "input_tokens",
                "output_tokens",
                "cache_read_input_tokens",
                "cache_creation_input_tokens",
                "context_pct",
                "ts",
            },
            {
                "run_id",
                "input_tokens",
                "output_tokens",
                "cache_read_input_tokens",
                "cache_creation_input_tokens",
                "ts",
            },
        ),
        "llm.model_selected": (
            {"type", "run_id", "model", "strategy", "ts"},
            {"run_id", "model", "strategy", "ts"},
        ),
        "log.line": (
            {"type", "run_id", "level", "source", "message", "ts"},
            {"run_id", "level", "source", "message", "ts"},
        ),
        "session.created": (
            {"type", "session_id", "mode", "ts"},
            {"session_id", "mode", "ts"},
        ),
        "session.message_received": (
            {"type", "session_id", "content", "ts"},
            {"session_id", "content", "ts"},
        ),
        "session.waiting_for_input": (
            {"type", "session_id", "last_run_id", "ts"},
            {"session_id", "last_run_id", "ts"},
        ),
        "session.resumed": (
            {"type", "session_id", "ts"},
            {"session_id", "ts"},
        ),
        "session.closed": (
            {"type", "session_id", "ts"},
            {"session_id", "ts"},
        ),
        "context.compacted": (
            {
                "type",
                "session_id",
                "run_id",
                "original_tokens",
                "summary_tokens",
                "ts",
            },
            {"session_id", "run_id", "original_tokens", "summary_tokens", "ts"},
        ),
        "permission.requested": (
            {
                "type",
                "tool_use_id",
                "tool_name",
                "params",
                "param_preview",
                "session_id",
                "run_id",
                "ts",
            },
            {
                "tool_use_id",
                "tool_name",
                "params",
                "param_preview",
                "session_id",
                "run_id",
                "ts",
            },
        ),
        "permission.granted": (
            {"type", "tool_use_id", "decision", "run_id", "ts"},
            {"tool_use_id", "decision", "run_id", "ts"},
        ),
        "permission.denied": (
            {"type", "tool_use_id", "decision", "run_id", "ts"},
            {"tool_use_id", "decision", "run_id", "ts"},
        ),
        "subagent.started": (
            {"type", "run_id", "parent_run_id", "description", "ts"},
            {"run_id", "parent_run_id", "description", "ts"},
        ),
        "subagent.finished": (
            {"type", "run_id", "parent_run_id", "status", "ts"},
            {"run_id", "parent_run_id", "status", "ts"},
        ),
        "skill.invoked": (
            {"type", "skill_name", "arguments", "run_id", "ts"},
            {"skill_name", "arguments", "run_id", "ts"},
        ),
    }
    parsed = [TypeAdapter(Event).validate_python(payload) for payload in _legacy_event_payloads()]

    assert {event.type for event in parsed} == set(expected)
    for event in parsed:
        legacy_fields, legacy_required = expected[event.type]
        schema = type(event).model_json_schema()
        assert legacy_fields <= set(schema["properties"]), event.type
        assert set(schema.get("required", [])) == legacy_required, event.type


async def test_loop_run_has_stable_terminal_order_and_no_graph_events(
    tmp_path: Path,
) -> None:
    collected: list[BaseModel] = []

    async def collect(event: BaseModel) -> None:
        collected.append(event)

    runner = AgentRunner(
        _config(),
        provider=_EndTurnProvider(),  # type: ignore[arg-type]
        extra_handlers=[collect],
        runs_dir=tmp_path,
    )

    outcome = await runner.run_and_capture("goal", run_id="run-no-tool")

    event_types = [event.type for event in collected]  # type: ignore[attr-defined]
    assert outcome.status == "success"
    assert outcome.steps == 1
    assert event_types == [
        "run.started",
        "step.started",
        "step.finished",
        "run.finished",
    ]
    assert not {
        "llm.reasoning",
        "node.started",
        "node.finished",
        "state.diff",
    }.intersection(event_types)


async def test_loop_tool_run_preserves_full_key_event_order(tmp_path: Path) -> None:
    collected: list[BaseModel] = []

    async def collect(event: BaseModel) -> None:
        collected.append(event)

    provider = _ListThenFinishProvider()
    runner = AgentRunner(
        _config(),
        provider=provider,  # type: ignore[arg-type]
        extra_handlers=[collect],
        runs_dir=tmp_path,
    )

    outcome = await runner.run_and_capture("list files", run_id="run-tool")

    assert outcome.status == "success"
    assert outcome.result == "listed"
    assert outcome.steps == 2
    assert provider.calls == 2
    assert [event.type for event in collected] == [  # type: ignore[attr-defined]
        "run.started",
        "step.started",
        "tool.call_started",
        "tool.call_finished",
        "step.finished",
        "step.started",
        "step.finished",
        "run.finished",
    ]


async def test_run_correlation_is_stable_and_isolated_between_runs(
    tmp_path: Path,
) -> None:
    collected: list[BaseModel] = []

    async def collect(event: BaseModel) -> None:
        collected.append(event)

    runner = AgentRunner(
        _config(),
        provider=_EndTurnProvider(),  # type: ignore[arg-type]
        extra_handlers=[collect],
        runs_dir=tmp_path,
    )
    session = Session(
        id="session-a",
        mode="chat",
        status="active",
        title="",
        created_at="t",
        updated_at="t",
    )

    await runner.run_and_capture("first", run_id="run-a", session=session)
    await runner.run_and_capture("second", run_id="run-b")

    first = [event for event in collected if getattr(event, "run_id", None) == "run-a"]
    second = [event for event in collected if getattr(event, "run_id", None) == "run-b"]
    first_correlations = {getattr(event, "correlation_id", None) for event in first}
    second_correlations = {getattr(event, "correlation_id", None) for event in second}
    assert first_correlations == {"run-a"}
    assert second_correlations == {"run-b"}
    assert all(getattr(event, "session_id", None) == "session-a" for event in first)
    assert all(getattr(event, "session_id", None) is None for event in second)
    assert all(getattr(event, "node_id", None) is None for event in first + second)


async def test_concurrent_runs_keep_event_files_and_metadata_isolated(
    tmp_path: Path,
) -> None:
    barrier = _RunBarrier()
    global_bus = EventBus()
    runner_a = AgentRunner(
        _config(),
        bus=global_bus,
        provider=_BarrierProvider(barrier),  # type: ignore[arg-type]
        runs_dir=tmp_path,
    )
    runner_b = AgentRunner(
        _config(),
        bus=global_bus,
        provider=_BarrierProvider(barrier),  # type: ignore[arg-type]
        runs_dir=tmp_path,
    )

    outcomes = await asyncio.wait_for(
        asyncio.gather(
            runner_a.run_and_capture("first", run_id="run-a"),
            runner_b.run_and_capture("second", run_id="run-b"),
        ),
        timeout=5.0,
    )

    assert [outcome.status for outcome in outcomes] == ["success", "success"]
    for run_id in ("run-a", "run-b"):
        event_path = tmp_path / run_id / "events.jsonl"
        events = [json.loads(line) for line in event_path.read_text().splitlines()]
        assert [event["type"] for event in events] == [
            "run.started",
            "step.started",
            "step.finished",
            "run.finished",
        ]
        assert {event["run_id"] for event in events} == {run_id}
        assert {event["correlation_id"] for event in events} == {run_id}
