from __future__ import annotations

import asyncio
from dataclasses import replace
from pathlib import Path

import pytest
from pydantic import BaseModel

from agent_runtime.core.bus.events import PermissionRequestedEvent, ToolCallFinishedEvent
from agent_runtime.core.context import ExecutionContext
from agent_runtime.core.engine.base import EngineRunConfig, RunOutcome, RunSuspension
from agent_runtime.core.events.bus import EventBus
from agent_runtime.core.graph.recovery import RecoveryStore, hash_tool_input
from agent_runtime.core.llm.types import LlmResponse, ToolCallBlock
from agent_runtime.core.permissions.manager import PermissionManager
from agent_runtime.core.permissions.policy import PermissionDecision, ToolPolicy
from agent_runtime.core.tools.base import BaseTool, ToolResult
from agent_runtime.core.tools.invocation import ToolOutcomeUnknownError, invoke_tool
from agent_runtime.core.tools.registry import ToolRegistry

pytest.importorskip("langgraph")

from agent_runtime.core.graph.engine import GraphExecutionEngine  # noqa: E402
from agent_runtime.core.graph.fake_provider import (  # noqa: E402
    ScriptedProvider,
    scripted_tool_call,
)
from agent_runtime.core.graph.runtime import (  # noqa: E402
    GraphRuntime,
    GraphStateConflictError,
)

pytestmark = [pytest.mark.graph, pytest.mark.recovery]


class _RecordingTool(BaseTool):
    description = "record one deterministic side effect"
    input_schema: dict[str, object] = {"type": "object", "properties": {}}

    def __init__(self, name: str, calls: list[str]) -> None:
        self.name = name
        self._calls = calls

    async def invoke(self, params: dict[str, object]) -> ToolResult:
        self._calls.append(self.name)
        return ToolResult(content=f"result-{self.name}")


class _SuspendThenBlockProvider:
    def __init__(self) -> None:
        self.call_count = 0
        self.second_call_started = asyncio.Event()

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
        del messages, tool_schemas, bus, run_id, step, system
        self.call_count += 1
        if self.call_count == 1:
            return LlmResponse(
                stop_reason="tool_use",
                tool_calls=[scripted_tool_call("danger")],
            )
        self.second_call_started.set()
        await asyncio.Event().wait()
        raise AssertionError("unreachable")


def _context(run_id: str) -> ExecutionContext:
    return ExecutionContext(run_id=run_id, goal="goal", max_steps=6)


def _config(session_id: str, thread_id: str) -> EngineRunConfig:
    return EngineRunConfig(
        session_id=session_id,
        thread_id=thread_id,
        retain_thread=True,
        wall_time_s=5.0,
        trace_event_limit=16,
        durable_recovery=True,
    )


async def _create_running_manifest(
    store: RecoveryStore,
    *,
    session_id: str,
    run_id: str,
    thread_id: str,
) -> None:
    await store.create_run(
        session_id=session_id,
        run_id=run_id,
        thread_id=thread_id,
        engine="graph",
        status="running",
    )


async def _collect(bus: EventBus) -> list[BaseModel]:
    events: list[BaseModel] = []

    async def append(event: BaseModel) -> None:
        events.append(event)

    bus.subscribe(append)
    return events


def _tool_result_ids(context: ExecutionContext) -> list[str]:
    result: list[str] = []
    for message in context.messages:
        content = message.get("content")
        if message.get("role") != "user" or not isinstance(content, list):
            continue
        for block in content:
            if isinstance(block, dict) and block.get("type") == "tool_result":
                tool_use_id = block.get("tool_use_id")
                if isinstance(tool_use_id, str):
                    result.append(tool_use_id)
    return result


async def test_journal_completes_before_finished_event_and_reuse_skips_permission(
    tmp_path: Path,
) -> None:
    store = RecoveryStore(tmp_path / "recovery.sqlite")
    session_id = "session-journal"
    run_id = "run-journal"
    await _create_running_manifest(
        store,
        session_id=session_id,
        run_id=run_id,
        thread_id="thread-journal",
    )
    calls: list[str] = []
    registry = ToolRegistry()
    registry.register(_RecordingTool("effect", calls))
    tool_call = ToolCallBlock(id="tool-1", name="effect", input={"value": 1})
    bus = EventBus()
    observed_status: list[str] = []

    async def inspect_finished(event: BaseModel) -> None:
        if isinstance(event, ToolCallFinishedEvent):
            record = await store.get_tool(session_id, run_id, tool_call.id)
            assert record is not None
            observed_status.append(record.status)

    bus.subscribe(inspect_finished)
    first = await invoke_tool(
        registry,
        tool_call,
        bus,
        run_id,
        session_id=session_id,
        recovery_store=store,
    )

    permission_checks = 0

    def unexpected_permission() -> tuple[bool, str]:
        nonlocal permission_checks
        permission_checks += 1
        return False, "deny_once"

    reuse_bus = EventBus()
    reused_events = await _collect(reuse_bus)
    reused = await invoke_tool(
        registry,
        tool_call,
        reuse_bus,
        run_id,
        session_id=session_id,
        recovery_store=store,
        durable_permission=unexpected_permission,
    )

    assert first == reused == ToolResult(content="result-effect")
    assert calls == ["effect"]
    assert observed_status == ["completed"]
    assert permission_checks == 0
    assert reused_events == []


@pytest.mark.parametrize("failure_point", ["complete", "finished_fanout"])
async def test_post_invoke_failures_never_retry_side_effect(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure_point: str,
) -> None:
    store = RecoveryStore(tmp_path / "recovery.sqlite")
    session_id = "session-post-invoke"
    run_id = f"run-{failure_point}"
    await _create_running_manifest(
        store,
        session_id=session_id,
        run_id=run_id,
        thread_id=f"thread-{failure_point}",
    )
    calls: list[str] = []
    registry = ToolRegistry()
    registry.register(_RecordingTool("effect", calls))
    tool_call = ToolCallBlock(id="tool-1", name="effect", input={})
    bus = EventBus()

    if failure_point == "complete":

        async def fail_complete(**_kwargs: object) -> None:
            raise RuntimeError("journal unavailable")

        monkeypatch.setattr(store, "complete_tool", fail_complete)
    else:

        async def fail_finished(event: BaseModel) -> None:
            if isinstance(event, ToolCallFinishedEvent):
                raise RuntimeError("finished fanout unavailable")

        bus.subscribe(fail_finished)

    with pytest.raises(RuntimeError):
        await invoke_tool(
            registry,
            tool_call,
            bus,
            run_id,
            session_id=session_id,
            recovery_store=store,
        )

    assert calls == ["effect"]
    record = await store.get_tool(session_id, run_id, tool_call.id)
    assert record is not None
    assert record.status == ("started" if failure_point == "complete" else "completed")


@pytest.mark.parametrize("failure_mode", ["runtime_error", "timeout"])
async def test_durable_dispatch_failure_is_outcome_unknown_and_never_replayed(
    tmp_path: Path,
    failure_mode: str,
) -> None:
    store = RecoveryStore(tmp_path / "recovery.sqlite")
    session_id = "session-dispatch-unknown"
    run_id = f"run-{failure_mode}"
    await _create_running_manifest(
        store,
        session_id=session_id,
        run_id=run_id,
        thread_id=f"thread-{failure_mode}",
    )
    calls: list[str] = []

    class _SideEffectThenFailure(BaseTool):
        name = "effect_then_failure"
        description = "records a side effect before losing the outcome"
        input_schema: dict[str, object] = {"type": "object", "properties": {}}

        async def invoke(self, params: dict[str, object]) -> ToolResult:
            calls.append(failure_mode)
            if failure_mode == "runtime_error":
                raise RuntimeError("result channel failed after side effect")
            await asyncio.Event().wait()
            return ToolResult(content="unreachable")

    registry = ToolRegistry()
    registry.register(_SideEffectThenFailure())
    tool_call = ToolCallBlock(id="tool-1", name="effect_then_failure", input={})
    bus = EventBus()
    events = await _collect(bus)

    with pytest.raises(ToolOutcomeUnknownError):
        await invoke_tool(
            registry,
            tool_call,
            bus,
            run_id,
            timeout=0.01,
            session_id=session_id,
            recovery_store=store,
        )

    record = await store.get_tool(session_id, run_id, tool_call.id)
    assert record is not None
    assert record.status == "outcome_unknown"
    assert record.serialized_result is None
    assert calls == [failure_mode]
    assert [getattr(event, "type", None) for event in events].count("tool.call_failed") == 1

    with pytest.raises(ToolOutcomeUnknownError):
        await invoke_tool(
            registry,
            tool_call,
            EventBus(),
            run_id,
            timeout=0.01,
            session_id=session_id,
            recovery_store=store,
        )
    assert calls == [failure_mode]


async def test_sqlite_one_shot_without_manifest_uses_process_local_permission(
    tmp_path: Path,
) -> None:
    session_id = "session-one-shot"
    run_id = "run-one-shot"
    thread_id = "thread-one-shot"
    store = RecoveryStore(tmp_path / "recovery.sqlite")
    runtime = GraphRuntime(
        checkpoint_backend="sqlite",
        sqlite_path=tmp_path / "checkpoints.sqlite",
        recovery_store=store,
    )
    calls: list[str] = []
    tools = ToolRegistry()
    tools.register(_RecordingTool("danger", calls))
    manager = PermissionManager(
        {"danger": ToolPolicy(default=PermissionDecision.ASK)},
        timeout_s=5.0,
    )
    bus = EventBus(correlation_id=run_id, session_id=session_id)
    requested: list[str] = []

    async def approve_in_process(event: BaseModel) -> None:
        if isinstance(event, PermissionRequestedEvent):
            requested.append(event.tool_use_id)
            assert event.interrupt_id is None
            assert manager.respond(
                event.tool_use_id,
                "allow_once",
                authorized_session_ids={session_id},
                session_id=session_id,
                run_id=run_id,
            )

    bus.subscribe(approve_in_process)
    config = replace(
        _config(session_id, thread_id),
        retain_thread=False,
        durable_recovery=False,
    )

    try:
        outcome = await GraphExecutionEngine(
            ScriptedProvider.tool("danger"),
            runtime=runtime,
            permission_manager=manager,
        ).run(
            _context(run_id),
            tools=tools,
            events=bus,
            config=config,
        )

        assert isinstance(outcome, RunOutcome)
        assert outcome.status == "success"
        assert calls == ["danger"]
        assert requested == ["tool-1"]
        assert await store.get_run(session_id, run_id) is None
        assert await store.get_tool(session_id, run_id, "tool-1") is None
        assert await runtime.latest_checkpoint(thread_id) is None
    finally:
        await runtime.close()


async def test_expired_permission_consumes_interrupt_and_terminal_resume_is_noop(
    tmp_path: Path,
) -> None:
    session_id = "session-expiry"
    run_id = "run-expiry"
    thread_id = "thread-expiry"
    store = RecoveryStore(tmp_path / "recovery.sqlite")
    await _create_running_manifest(
        store,
        session_id=session_id,
        run_id=run_id,
        thread_id=thread_id,
    )
    runtime = GraphRuntime(
        checkpoint_backend="sqlite",
        sqlite_path=tmp_path / "checkpoints.sqlite",
        recovery_store=store,
    )
    calls: list[str] = []
    tools = ToolRegistry()
    tools.register(_RecordingTool("danger", calls))
    manager = PermissionManager(
        {"danger": ToolPolicy(default=PermissionDecision.ASK)},
        timeout_s=0.001,
    )
    provider = ScriptedProvider.tool("danger", tool_use_id="approval-1")
    config = _config(session_id, thread_id)
    events_bus = EventBus(correlation_id=run_id, session_id=session_id)
    events = await _collect(events_bus)

    try:
        suspended = await GraphExecutionEngine(
            provider,
            runtime=runtime,
            permission_manager=manager,
        ).run(
            _context(run_id),
            tools=tools,
            events=events_bus,
            config=config,
        )
        assert isinstance(suspended, RunSuspension)
        assert suspended.reason == "permission"
        assert suspended.interrupt_id
        assert calls == []

        summary = await runtime.latest_state(
            thread_id,
            session_id=session_id,
            run_id=run_id,
        )
        assert summary is not None
        assert summary.pending_expires_at is not None
        assert summary.checkpoint_revision == suspended.checkpoint_revision
        await asyncio.sleep(0.02)

        terminal_context = _context(run_id)
        terminal = await GraphExecutionEngine(
            provider,
            runtime=runtime,
            permission_manager=manager,
        ).resume(
            terminal_context,
            tools=tools,
            events=events_bus,
            config=replace(
                config,
                expected_checkpoint_revision=suspended.checkpoint_revision,
                resume_value="allow_once",
            ),
        )
        assert isinstance(terminal, RunOutcome)
        assert terminal.status == "success"
        assert terminal.checkpoint_revision
        assert calls == []
        checkpoint = await runtime.latest_checkpoint(thread_id)
        assert checkpoint is not None
        assert all(
            channel != "__interrupt__"
            for _task_id, channel, _value in checkpoint.pending_writes or ()
        )
        assert [event.type for event in events].count("permission.requested") == 1  # type: ignore[attr-defined]
        assert [event.type for event in events].count("permission.denied") == 1  # type: ignore[attr-defined]
        assert [
            event.decision
            for event in events
            if getattr(event, "type", None) == "permission.denied"
        ] == ["timeout"]

        await store.mark_run_suspended(
            session_id,
            run_id,
            reason="process_recovery",
            checkpoint_revision=terminal.checkpoint_revision,
            event_seq=events_bus.last_event_seq,
            expected_resume_epoch=0,
        )
        terminal_pending_coordination = await runtime.latest_state(
            thread_id,
            session_id=session_id,
            run_id=run_id,
        )
        assert terminal_pending_coordination is not None
        assert terminal_pending_coordination.status == "suspended"
        assert terminal_pending_coordination.resumable is True

        no_call_provider = ScriptedProvider.failure(AssertionError("provider must not run"))
        replay_context = _context(run_id)
        replay = await GraphExecutionEngine(
            no_call_provider,
            runtime=runtime,
            permission_manager=manager,
        ).resume(
            replay_context,
            tools=tools,
            events=EventBus(),
            config=replace(
                config,
                expected_checkpoint_revision=terminal.checkpoint_revision,
            ),
        )
        assert isinstance(replay, RunOutcome)
        assert replay == terminal
        assert no_call_provider.call_count == 0
        assert replay_context.status == "success"
    finally:
        await runtime.close()


async def test_terminal_manifest_with_pending_interrupt_is_checkpoint_conflict(
    tmp_path: Path,
) -> None:
    session_id = "session-conflict"
    run_id = "run-conflict"
    thread_id = "thread-conflict"
    store = RecoveryStore(tmp_path / "recovery.sqlite")
    await _create_running_manifest(
        store,
        session_id=session_id,
        run_id=run_id,
        thread_id=thread_id,
    )
    runtime = GraphRuntime(
        checkpoint_backend="sqlite",
        sqlite_path=tmp_path / "checkpoints.sqlite",
        recovery_store=store,
    )
    tools = ToolRegistry()
    tools.register(_RecordingTool("danger", []))
    manager = PermissionManager(
        {"danger": ToolPolicy(default=PermissionDecision.ASK)},
        timeout_s=60.0,
    )

    try:
        suspended = await GraphExecutionEngine(
            ScriptedProvider.tool("danger"),
            runtime=runtime,
            permission_manager=manager,
        ).run(
            _context(run_id),
            tools=tools,
            events=EventBus(),
            config=_config(session_id, thread_id),
        )
        assert isinstance(suspended, RunSuspension)
        await store.mark_run_terminal(
            session_id,
            run_id,
            status="success",
            checkpoint_revision=suspended.checkpoint_revision,
            event_seq=0,
            transcript_commit_count=0,
            transcript_commit_hash=None,
            expected_resume_epoch=0,
        )

        with pytest.raises(GraphStateConflictError) as exc_info:
            await runtime.latest_state(
                thread_id,
                session_id=session_id,
                run_id=run_id,
            )
        assert exc_info.value.code == "checkpoint_conflict"
    finally:
        await runtime.close()


async def test_multi_tool_resume_reuses_completed_prefix_once(tmp_path: Path) -> None:
    session_id = "session-multi"
    run_id = "run-multi"
    thread_id = "thread-multi"
    store = RecoveryStore(tmp_path / "recovery.sqlite")
    await _create_running_manifest(
        store,
        session_id=session_id,
        run_id=run_id,
        thread_id=thread_id,
    )
    runtime = GraphRuntime(
        checkpoint_backend="sqlite",
        sqlite_path=tmp_path / "checkpoints.sqlite",
        recovery_store=store,
    )
    calls: list[str] = []
    tools = ToolRegistry()
    tools.register(_RecordingTool("first", calls))
    tools.register(_RecordingTool("second", calls))
    manager = PermissionManager(
        {
            "first": ToolPolicy(default=PermissionDecision.ALLOW),
            "second": ToolPolicy(default=PermissionDecision.ASK),
        },
        timeout_s=60.0,
    )
    provider = ScriptedProvider.multi(
        [
            scripted_tool_call("first", tool_use_id="tool-first"),
            scripted_tool_call("second", tool_use_id="tool-second"),
        ]
    )
    config = _config(session_id, thread_id)
    events_bus = EventBus(correlation_id=run_id, session_id=session_id)
    events = await _collect(events_bus)

    try:
        first_context = _context(run_id)
        suspended = await GraphExecutionEngine(
            provider,
            runtime=runtime,
            permission_manager=manager,
        ).run(
            first_context,
            tools=tools,
            events=events_bus,
            config=config,
        )
        assert isinstance(suspended, RunSuspension)
        assert calls == ["first"]

        resumed_context = _context(run_id)
        terminal = await GraphExecutionEngine(
            provider,
            runtime=runtime,
            permission_manager=manager,
        ).resume(
            resumed_context,
            tools=tools,
            events=events_bus,
            config=replace(
                config,
                expected_checkpoint_revision=suspended.checkpoint_revision,
                resume_value="allow_once",
            ),
        )

        assert isinstance(terminal, RunOutcome)
        assert terminal.status == "success"
        assert calls == ["first", "second"]
        assert _tool_result_ids(resumed_context) == ["tool-first", "tool-second"]
        assert provider.call_count == 2
        assert [event.type for event in events].count("permission.requested") == 1  # type: ignore[attr-defined]
        assert [
            event.tool_use_id for event in events if isinstance(event, ToolCallFinishedEvent)
        ] == ["tool-first", "tool-second"]
    finally:
        await runtime.close()


async def test_pending_native_deny_is_not_bypassed_by_later_persistent_allow(
    tmp_path: Path,
) -> None:
    store = RecoveryStore(tmp_path / "recovery.sqlite")
    runtime = GraphRuntime(
        checkpoint_backend="sqlite",
        sqlite_path=tmp_path / "checkpoints.sqlite",
        recovery_store=store,
    )
    manager = PermissionManager(
        {"danger": ToolPolicy(default=PermissionDecision.ASK)},
        timeout_s=60.0,
    )
    calls: list[str] = []
    tools = ToolRegistry()
    tools.register(_RecordingTool("danger", calls))
    suspended: dict[str, RunSuspension] = {}
    providers: dict[str, ScriptedProvider] = {}

    try:
        for suffix in ("a", "b"):
            session_id = f"session-{suffix}"
            run_id = f"run-{suffix}"
            thread_id = f"thread-{suffix}"
            await _create_running_manifest(
                store,
                session_id=session_id,
                run_id=run_id,
                thread_id=thread_id,
            )
            provider = ScriptedProvider.tool("danger", tool_use_id=f"tool-{suffix}")
            providers[suffix] = provider
            result = await GraphExecutionEngine(
                provider,
                runtime=runtime,
                permission_manager=manager,
            ).run(
                _context(run_id),
                tools=tools,
                events=EventBus(),
                config=_config(session_id, thread_id),
            )
            assert isinstance(result, RunSuspension)
            suspended[suffix] = result

        result_b = await GraphExecutionEngine(
            providers["b"],
            runtime=runtime,
            permission_manager=manager,
        ).resume(
            _context("run-b"),
            tools=tools,
            events=EventBus(),
            config=replace(
                _config("session-b", "thread-b"),
                expected_checkpoint_revision=suspended["b"].checkpoint_revision,
                resume_value="always_allow",
            ),
        )
        assert isinstance(result_b, RunOutcome)
        assert calls == ["danger"]

        context_a = _context("run-a")
        result_a = await GraphExecutionEngine(
            providers["a"],
            runtime=runtime,
            permission_manager=manager,
        ).resume(
            context_a,
            tools=tools,
            events=EventBus(),
            config=replace(
                _config("session-a", "thread-a"),
                expected_checkpoint_revision=suspended["a"].checkpoint_revision,
                resume_value="deny_once",
            ),
        )

        assert isinstance(result_a, RunOutcome)
        assert calls == ["danger"]
        assert _tool_result_ids(context_a) == ["tool-a"]
    finally:
        await runtime.close()


@pytest.mark.parametrize("change", ["replace", "append"])
async def test_resume_rejects_transcript_that_is_not_the_checkpoint_prefix(
    tmp_path: Path,
    change: str,
) -> None:
    session_id = f"session-transcript-{change}"
    run_id = f"run-transcript-{change}"
    thread_id = f"thread-transcript-{change}"
    store = RecoveryStore(tmp_path / "recovery.sqlite")
    await _create_running_manifest(
        store,
        session_id=session_id,
        run_id=run_id,
        thread_id=thread_id,
    )
    runtime = GraphRuntime(
        checkpoint_backend="sqlite",
        sqlite_path=tmp_path / "checkpoints.sqlite",
        recovery_store=store,
    )
    manager = PermissionManager(
        {"danger": ToolPolicy(default=PermissionDecision.ASK)},
        timeout_s=60.0,
    )
    calls: list[str] = []
    tools = ToolRegistry()
    tools.register(_RecordingTool("danger", calls))
    provider = ScriptedProvider.tool("danger")
    config = _config(session_id, thread_id)

    try:
        suspended = await GraphExecutionEngine(
            provider,
            runtime=runtime,
            permission_manager=manager,
        ).run(
            _context(run_id),
            tools=tools,
            events=EventBus(),
            config=config,
        )
        assert isinstance(suspended, RunSuspension)
        provider_calls = provider.call_count
        history = (
            [{"role": "user", "content": "tampered"}]
            if change == "replace"
            else [
                {"role": "user", "content": "goal"},
                {"role": "user", "content": "unexpected extra message"},
            ]
        )
        resumed_context = ExecutionContext(
            run_id=run_id,
            goal="goal",
            max_steps=6,
            prefill_messages=history,
        )

        with pytest.raises(GraphStateConflictError, match="transcript"):
            await GraphExecutionEngine(
                provider,
                runtime=runtime,
                permission_manager=manager,
            ).resume(
                resumed_context,
                tools=tools,
                events=EventBus(),
                config=replace(
                    config,
                    expected_checkpoint_revision=suspended.checkpoint_revision,
                    resume_value="allow_once",
                ),
            )

        assert provider.call_count == provider_calls
        assert calls == []
    finally:
        await runtime.close()


async def test_step_and_wall_time_budgets_keep_resume_semantics(tmp_path: Path) -> None:
    session_id = "session-step-budget"
    run_id = "run-step-budget"
    thread_id = "thread-step-budget"
    store = RecoveryStore(tmp_path / "recovery.sqlite")
    await _create_running_manifest(
        store,
        session_id=session_id,
        run_id=run_id,
        thread_id=thread_id,
    )
    runtime = GraphRuntime(
        checkpoint_backend="sqlite",
        sqlite_path=tmp_path / "checkpoints.sqlite",
        recovery_store=store,
    )
    calls: list[str] = []
    tools = ToolRegistry()
    tools.register(_RecordingTool("danger", calls))
    manager = PermissionManager(
        {"danger": ToolPolicy(default=PermissionDecision.ASK)},
        timeout_s=0,
    )
    provider = ScriptedProvider.tool("danger")
    config = replace(
        _config(session_id, thread_id),
        wall_time_s=1.0,
    )
    context = ExecutionContext(run_id=run_id, goal="goal", max_steps=1)

    try:
        suspended = await GraphExecutionEngine(
            provider,
            runtime=runtime,
            permission_manager=manager,
        ).run(context, tools=tools, events=EventBus(), config=config)
        assert isinstance(suspended, RunSuspension)
        assert provider.call_count == 1

        # Human wait and daemon downtime are outside each active attempt budget.
        await asyncio.sleep(1.1)
        resumed_context = ExecutionContext(run_id=run_id, goal="goal", max_steps=1)
        terminal = await GraphExecutionEngine(
            provider,
            runtime=runtime,
            permission_manager=manager,
        ).resume(
            resumed_context,
            tools=tools,
            events=EventBus(),
            config=replace(
                config,
                expected_checkpoint_revision=suspended.checkpoint_revision,
                resume_value="allow_once",
            ),
        )

        assert isinstance(terminal, RunOutcome)
        assert terminal.status == "failed"
        assert terminal.reason == "exceeded_max_steps"
        assert provider.call_count == 1
        assert calls == ["danger"]
    finally:
        await runtime.close()


async def test_resumed_attempt_gets_fresh_wall_time_and_enforces_active_timeout(
    tmp_path: Path,
) -> None:
    session_id = "session-resume-wall-time"
    run_id = "run-resume-wall-time"
    thread_id = "thread-resume-wall-time"
    store = RecoveryStore(tmp_path / "recovery.sqlite")
    await _create_running_manifest(
        store,
        session_id=session_id,
        run_id=run_id,
        thread_id=thread_id,
    )
    runtime = GraphRuntime(
        checkpoint_backend="sqlite",
        sqlite_path=tmp_path / "checkpoints.sqlite",
        recovery_store=store,
    )
    calls: list[str] = []
    tools = ToolRegistry()
    tools.register(_RecordingTool("danger", calls))
    manager = PermissionManager(
        {"danger": ToolPolicy(default=PermissionDecision.ASK)},
        timeout_s=0,
    )
    provider = _SuspendThenBlockProvider()
    config = replace(_config(session_id, thread_id), wall_time_s=0.5)

    try:
        suspended = await GraphExecutionEngine(
            provider,
            runtime=runtime,
            permission_manager=manager,
        ).run(_context(run_id), tools=tools, events=EventBus(), config=config)
        assert isinstance(suspended, RunSuspension)

        # Waiting for a human is outside the active-attempt wall-time budget.
        await asyncio.sleep(0.6)
        terminal = await GraphExecutionEngine(
            provider,
            runtime=runtime,
            permission_manager=manager,
        ).resume(
            _context(run_id),
            tools=tools,
            events=EventBus(),
            config=replace(
                config,
                expected_checkpoint_revision=suspended.checkpoint_revision,
                resume_value="allow_once",
            ),
        )

        assert provider.second_call_started.is_set()
        assert isinstance(terminal, RunOutcome)
        assert terminal.status == "failed"
        assert terminal.reason == "exceeded_wall_time"
        assert provider.call_count == 2
        assert calls == ["danger"]
    finally:
        await runtime.close()


async def test_recursion_limit_is_enforced_on_the_resume_invocation(tmp_path: Path) -> None:
    session_id = "session-resume-recursion"
    run_id = "run-resume-recursion"
    thread_id = "thread-resume-recursion"
    store = RecoveryStore(tmp_path / "recovery.sqlite")
    await _create_running_manifest(
        store,
        session_id=session_id,
        run_id=run_id,
        thread_id=thread_id,
    )
    runtime = GraphRuntime(
        checkpoint_backend="sqlite",
        sqlite_path=tmp_path / "checkpoints.sqlite",
        recovery_store=store,
    )
    calls: list[str] = []
    tools = ToolRegistry()
    tools.register(_RecordingTool("danger", calls))
    manager = PermissionManager(
        {"danger": ToolPolicy(default=PermissionDecision.ASK)},
        timeout_s=0,
    )
    provider = ScriptedProvider.infinite("danger")
    resume_manager = PermissionManager(
        {"danger": ToolPolicy(default=PermissionDecision.ALLOW)},
        timeout_s=0,
    )

    try:
        suspended = await GraphExecutionEngine(
            provider,
            runtime=runtime,
            permission_manager=manager,
        ).run(
            _context(run_id),
            tools=tools,
            events=EventBus(),
            config=_config(session_id, thread_id),
        )
        assert isinstance(suspended, RunSuspension)

        terminal = await GraphExecutionEngine(
            provider,
            runtime=runtime,
            permission_manager=resume_manager,
        ).resume(
            _context(run_id),
            tools=tools,
            events=EventBus(),
            config=replace(
                _config(session_id, thread_id),
                recursion_limit=2,
                expected_checkpoint_revision=suspended.checkpoint_revision,
                resume_value="allow_once",
            ),
        )

        assert isinstance(terminal, RunOutcome)
        assert terminal.status == "failed"
        assert terminal.reason == "exceeded_recursion_limit"
    finally:
        await runtime.close()


async def test_tool_call_budget_accumulates_across_permission_resume(tmp_path: Path) -> None:
    session_id = "session-tool-budget"
    run_id = "run-tool-budget"
    thread_id = "thread-tool-budget"
    store = RecoveryStore(tmp_path / "recovery.sqlite")
    await _create_running_manifest(
        store,
        session_id=session_id,
        run_id=run_id,
        thread_id=thread_id,
    )
    runtime = GraphRuntime(
        checkpoint_backend="sqlite",
        sqlite_path=tmp_path / "checkpoints.sqlite",
        recovery_store=store,
    )
    calls: list[str] = []
    tools = ToolRegistry()
    tools.register(_RecordingTool("first", calls))
    tools.register(_RecordingTool("second", calls))
    manager = PermissionManager(
        {
            "first": ToolPolicy(default=PermissionDecision.ALLOW),
            "second": ToolPolicy(default=PermissionDecision.ASK),
        },
        timeout_s=0,
    )
    provider = ScriptedProvider(
        [
            LlmResponse(
                stop_reason="tool_use",
                tool_calls=[scripted_tool_call("first", tool_use_id="tool-first")],
            ),
            LlmResponse(
                stop_reason="tool_use",
                tool_calls=[scripted_tool_call("second", tool_use_id="tool-second")],
            ),
            LlmResponse(stop_reason="end_turn", text="done"),
        ]
    )
    config = replace(_config(session_id, thread_id), tool_call_budget=2)

    try:
        suspended = await GraphExecutionEngine(
            provider,
            runtime=runtime,
            permission_manager=manager,
        ).run(
            _context(run_id),
            tools=tools,
            events=EventBus(),
            config=config,
        )
        assert isinstance(suspended, RunSuspension)
        assert calls == ["first"]
        before = await runtime.latest_checkpoint(thread_id)
        assert before is not None
        assert before.checkpoint["channel_values"]["tool_call_count"] == 1

        terminal = await GraphExecutionEngine(
            provider,
            runtime=runtime,
            permission_manager=manager,
        ).resume(
            _context(run_id),
            tools=tools,
            events=EventBus(),
            config=replace(
                config,
                expected_checkpoint_revision=suspended.checkpoint_revision,
                resume_value="allow_once",
            ),
        )
        assert isinstance(terminal, RunOutcome)
        assert terminal.status == "success"
        assert calls == ["first", "second"]
        after = await runtime.latest_checkpoint(thread_id)
        assert after is not None
        assert after.checkpoint["channel_values"]["tool_call_count"] == 2
    finally:
        await runtime.close()


async def test_outcome_unknown_checkpoint_is_not_resumable(tmp_path: Path) -> None:
    session_id = "session-unknown"
    run_id = "run-unknown"
    thread_id = "thread-unknown"
    store = RecoveryStore(tmp_path / "recovery.sqlite")
    await _create_running_manifest(
        store,
        session_id=session_id,
        run_id=run_id,
        thread_id=thread_id,
    )
    tool_call = ToolCallBlock(id="tool-unknown", name="effect", input={})
    input_hash = hash_tool_input(tool_call.input)
    await store.claim_tool(
        session_id=session_id,
        run_id=run_id,
        tool_use_id=tool_call.id,
        tool_name=tool_call.name,
        input_hash=input_hash,
    )
    await store.mark_tool_outcome_unknown(
        session_id=session_id,
        run_id=run_id,
        tool_use_id=tool_call.id,
        tool_name=tool_call.name,
        input_hash=input_hash,
    )
    runtime = GraphRuntime(
        checkpoint_backend="sqlite",
        sqlite_path=tmp_path / "checkpoints.sqlite",
        recovery_store=store,
    )
    calls: list[str] = []
    tools = ToolRegistry()
    tools.register(_RecordingTool("effect", calls))
    provider = ScriptedProvider.tool("effect", tool_use_id=tool_call.id)
    config = _config(session_id, thread_id)

    try:
        suspended = await GraphExecutionEngine(provider, runtime=runtime).run(
            _context(run_id),
            tools=tools,
            events=EventBus(),
            config=config,
        )
        assert isinstance(suspended, RunSuspension)
        assert suspended.reason == "outcome_unknown"
        assert calls == []
        assert provider.call_count == 1

        summary = await runtime.latest_state(
            thread_id,
            session_id=session_id,
            run_id=run_id,
        )
        assert summary is not None
        assert summary.suspension_reason == "outcome_unknown"
        assert summary.pending_tool_use_id == tool_call.id
        assert summary.pending_tool_name == tool_call.name
        assert summary.resumable is False
    finally:
        await runtime.close()
