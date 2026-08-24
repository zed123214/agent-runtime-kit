from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime
from pathlib import Path

import pytest
from pydantic import BaseModel

from agent_runtime.core.bus.events import RunFinishedEvent
from agent_runtime.core.config import RuntimeConfig
from agent_runtime.core.context import ExecutionContext
from agent_runtime.core.engine.base import EngineRunConfig, RunOutcome, RunSuspension
from agent_runtime.core.events.bus import EventBus
from agent_runtime.core.graph.recovery import RecoveryStore
from agent_runtime.core.llm.types import ToolCallBlock
from agent_runtime.core.runner import AgentRunner
from agent_runtime.core.session.model import Session
from agent_runtime.core.session.store import SessionStore
from agent_runtime.core.tools.base import BaseTool, ToolResult
from agent_runtime.core.tools.invocation import ToolOutcomeUnknownError, invoke_tool
from agent_runtime.core.tools.registry import ToolRegistry

pytestmark = pytest.mark.recovery


def _now() -> str:
    return datetime.now(UTC).isoformat()


class _UnusedProvider:
    async def chat(self, *args: object, **kwargs: object) -> object:
        raise AssertionError("the injected recovery engine must not call the provider")


class _SuspendThenFinishEngine:
    name = "graph"

    async def run(
        self,
        context: ExecutionContext,
        *,
        tools: ToolRegistry,
        events: EventBus,
        config: EngineRunConfig,
    ) -> RunSuspension:
        del tools, events, config
        return RunSuspension(
            run_id=context.run_id,
            session_id="sess-recovery",
            reason="permission",
            checkpoint_revision="revision-1",
            interrupt_id="interrupt-1",
            event_seq=0,
        )

    async def resume(
        self,
        context: ExecutionContext,
        *,
        tools: ToolRegistry,
        events: EventBus,
        config: EngineRunConfig,
    ) -> RunOutcome:
        del tools, events
        assert config.expected_checkpoint_revision == "revision-1"
        assert config.resume_value == "allow_once"
        context.messages.append(
            {"role": "assistant", "content": [{"type": "text", "text": "recovered"}]}
        )
        context.result = "recovered"
        context.step = 2
        context.mark_success()
        return RunOutcome.from_context(context)

    async def cancel(self) -> None:
        return None


class _AlreadyTerminalEngine(_SuspendThenFinishEngine):
    async def resume(
        self,
        context: ExecutionContext,
        *,
        tools: ToolRegistry,
        events: EventBus,
        config: EngineRunConfig,
    ) -> RunOutcome:
        del tools, events
        assert config.expected_checkpoint_revision == "revision-terminal"
        context.result = "already committed"
        context.step = 1
        context.mark_success()
        return RunOutcome.from_context(context, checkpoint_revision="revision-terminal")


class _TerminalCheckpointEngine(_AlreadyTerminalEngine):
    async def resume(
        self,
        context: ExecutionContext,
        *,
        tools: ToolRegistry,
        events: EventBus,
        config: EngineRunConfig,
    ) -> RunOutcome:
        context.messages.append({"role": "assistant", "content": "checkpoint answer"})
        return await super().resume(context, tools=tools, events=events, config=config)


class _ImmediateEngine(_SuspendThenFinishEngine):
    async def run(
        self,
        context: ExecutionContext,
        *,
        tools: ToolRegistry,
        events: EventBus,
        config: EngineRunConfig,
    ) -> RunOutcome:
        del tools, events
        assert config.durable_recovery is False
        context.messages.append({"role": "assistant", "content": "one shot"})
        context.result = "one shot"
        context.step = 1
        context.mark_success()
        return RunOutcome.from_context(context, checkpoint_revision="sqlite-revision")


class _BlockingSideEffectTool(BaseTool):
    name = "blocking_effect"
    description = "record one side effect and block"
    input_schema: dict[str, object] = {"type": "object", "properties": {}}

    def __init__(self, effects: list[str], applied: asyncio.Event) -> None:
        self._effects = effects
        self._applied = applied

    async def invoke(self, params: dict[str, object]) -> ToolResult:
        del params
        self._effects.append("applied")
        self._applied.set()
        await asyncio.Event().wait()
        raise AssertionError("the blocking tool must be cancelled")


class _BlockingJournalEngine(_SuspendThenFinishEngine):
    def __init__(self, recovery: RecoveryStore) -> None:
        self._recovery = recovery

    async def run(
        self,
        context: ExecutionContext,
        *,
        tools: ToolRegistry,
        events: EventBus,
        config: EngineRunConfig,
    ) -> RunOutcome:
        await invoke_tool(
            tools,
            ToolCallBlock(id="tool-blocking", name="blocking_effect", input={}),
            events,
            context.run_id,
            session_id=config.session_id,
            recovery_store=self._recovery,
        )
        raise AssertionError("the blocking invocation must not return")


def _session() -> Session:
    return Session(
        id="sess-recovery",
        mode="chat",
        status="active",
        title="recovery",
        created_at=_now(),
        updated_at=_now(),
        run_ids=["run-recovery"],
        durable=True,
    )


@pytest.mark.asyncio
async def test_cancellation_marks_started_tool_unknown_before_single_terminal_event(
    tmp_path: Path,
) -> None:
    session = _session()
    store = SessionStore(tmp_path / "sessions")
    store.write_meta(session)
    store.append_message(session.id, "user", "cancel after side effect")
    recovery = RecoveryStore(tmp_path / "recovery.sqlite3")
    await recovery.create_run(
        session_id=session.id,
        run_id="run-recovery",
        thread_id=session.id,
        engine="graph",
        status="running",
    )
    effects: list[str] = []
    applied = asyncio.Event()
    engine = _BlockingJournalEngine(recovery)
    terminal_journal_statuses: list[str] = []

    async def inspect_terminal(event: BaseModel) -> None:
        if isinstance(event, RunFinishedEvent):
            record = await recovery.get_tool(session.id, "run-recovery", "tool-blocking")
            assert record is not None
            terminal_journal_statuses.append(record.status)

    def resolver(_name: str):
        def build(*args: object, **kwargs: object) -> _BlockingJournalEngine:
            del args, kwargs
            return engine

        return build

    config = RuntimeConfig()
    config.agent.engine = "graph"
    runner = AgentRunner(
        config,
        provider=_UnusedProvider(),  # type: ignore[arg-type]
        engine_resolver=resolver,
        recovery_store=recovery,
        extra_tools=[_BlockingSideEffectTool(effects, applied)],
        extra_handlers=[inspect_terminal],
    )
    task = asyncio.create_task(
        runner.run_and_capture(
            "cancel after side effect",
            run_id="run-recovery",
            session=session,
            store=store,
        )
    )
    await asyncio.wait_for(applied.wait(), timeout=2.0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert effects == ["applied"]
    journal = await recovery.get_tool(session.id, "run-recovery", "tool-blocking")
    assert journal is not None
    assert journal.status == "outcome_unknown"
    assert terminal_journal_statuses == ["outcome_unknown"]
    replay_registry = ToolRegistry()
    replay_registry.register(_BlockingSideEffectTool(effects, asyncio.Event()))
    with pytest.raises(ToolOutcomeUnknownError):
        await invoke_tool(
            replay_registry,
            ToolCallBlock(id="tool-blocking", name="blocking_effect", input={}),
            EventBus(),
            "run-recovery",
            session_id=session.id,
            recovery_store=recovery,
        )
    assert effects == ["applied"]
    manifest = await recovery.get_run(session.id, "run-recovery")
    assert manifest is not None
    assert manifest.status == "failed"
    event_path = store.runs_dir(session.id) / "run-recovery" / "events.jsonl"
    events = [json.loads(line) for line in event_path.read_text(encoding="utf-8").splitlines()]
    terminal = [event for event in events if event["type"] == "run.finished"]
    assert len(terminal) == 1
    assert terminal[0]["status"] == "failed"
    assert terminal[0]["reason"] == "cancelled"


@pytest.mark.asyncio
async def test_runner_preserves_one_logical_lifecycle_across_suspension(tmp_path: Path) -> None:
    session = _session()
    store = SessionStore(tmp_path / "sessions")
    store.write_meta(session)
    store.append_message(session.id, "user", "recover me")
    recovery = RecoveryStore(tmp_path / "recovery.sqlite3")
    await recovery.create_run(
        session_id=session.id,
        run_id="run-recovery",
        thread_id=session.id,
        engine="graph",
        status="running",
    )

    config = RuntimeConfig()
    config.agent.engine = "graph"
    engine = _SuspendThenFinishEngine()

    def resolver(_name: str):
        def build(*args: object, **kwargs: object) -> _SuspendThenFinishEngine:
            del args, kwargs
            return engine

        return build

    runner = AgentRunner(
        config,
        provider=_UnusedProvider(),  # type: ignore[arg-type]
        engine_resolver=resolver,
        recovery_store=recovery,
    )

    suspended = await runner.run_and_capture(
        "recover me",
        run_id="run-recovery",
        session=session,
        store=store,
    )
    assert isinstance(suspended, RunSuspension)
    assert suspended.event_seq == 2

    event_path = store.runs_dir(session.id) / "run-recovery" / "events.jsonl"
    first_events = [
        json.loads(line) for line in event_path.read_text(encoding="utf-8").splitlines()
    ]
    assert [event["type"] for event in first_events] == ["run.started", "run.suspended"]
    assert [event["event_seq"] for event in first_events] == [1, 2]
    assert store.read_messages(session.id) == [{"role": "user", "content": "recover me"}]

    lease = await recovery.acquire_resume_lease(
        session.id,
        "run-recovery",
        expected_checkpoint_revision="revision-1",
    )
    outcome = await runner.run_and_capture(
        "recover me",
        run_id="run-recovery",
        session=session,
        store=store,
        resume=True,
        resume_value="allow_once",
        expected_checkpoint_revision="revision-1",
        resume_epoch=lease.resume_epoch,
    )
    assert isinstance(outcome, RunOutcome)
    assert outcome.status == "success"

    events = [json.loads(line) for line in event_path.read_text(encoding="utf-8").splitlines()]
    assert [event["type"] for event in events] == [
        "run.started",
        "run.suspended",
        "run.resumed",
        "run.finished",
    ]
    assert [event["event_seq"] for event in events] == [1, 2, 3, 4]
    assert events[-1]["type"] == "run.finished"
    assert sum(event["type"] == "run.started" for event in events) == 1
    assert sum(event["type"] == "run.finished" for event in events) == 1

    messages = store.read_messages(session.id)
    assert messages == [
        {"role": "user", "content": "recover me"},
        {"role": "assistant", "content": [{"type": "text", "text": "recovered"}]},
    ]
    manifest = await recovery.get_run(session.id, "run-recovery")
    assert manifest is not None
    assert manifest.status == "success"
    assert manifest.event_seq == 4
    assert manifest.transcript_commit_count == 1


@pytest.mark.asyncio
async def test_explicit_close_terminalizes_suspended_run_once(tmp_path: Path) -> None:
    session = _session()
    store = SessionStore(tmp_path / "sessions")
    store.write_meta(session)
    store.append_message(session.id, "user", "close me")
    recovery = RecoveryStore(tmp_path / "recovery.sqlite3")
    await recovery.create_run(
        session_id=session.id,
        run_id="run-recovery",
        thread_id=session.id,
        engine="graph",
        status="running",
    )
    config = RuntimeConfig()
    config.agent.engine = "graph"
    engine = _SuspendThenFinishEngine()

    def resolver(_name: str):
        def build(*args: object, **kwargs: object) -> _SuspendThenFinishEngine:
            del args, kwargs
            return engine

        return build

    runner = AgentRunner(
        config,
        provider=_UnusedProvider(),  # type: ignore[arg-type]
        engine_resolver=resolver,
        recovery_store=recovery,
    )
    suspended = await runner.run_and_capture(
        "close me",
        run_id="run-recovery",
        session=session,
        store=store,
    )
    assert isinstance(suspended, RunSuspension)

    first = await runner.close_suspended(
        session=session,
        store=store,
        run_id="run-recovery",
        checkpoint_revision="revision-1",
        resume_epoch=0,
    )
    second = await runner.close_suspended(
        session=session,
        store=store,
        run_id="run-recovery",
        checkpoint_revision="revision-1",
        resume_epoch=0,
    )
    assert first.status == second.status == "failed"
    assert first.reason == second.reason == "session_closed"

    event_path = store.runs_dir(session.id) / "run-recovery" / "events.jsonl"
    events = [json.loads(line) for line in event_path.read_text(encoding="utf-8").splitlines()]
    assert [event["type"] for event in events] == [
        "run.started",
        "run.suspended",
        "run.finished",
    ]
    assert [event["event_seq"] for event in events] == [1, 2, 3]
    assert events[-1]["reason"] == "session_closed"
    manifest = await recovery.get_run(session.id, "run-recovery")
    assert manifest is not None
    assert manifest.status == "failed"
    assert manifest.event_seq == 3
    assert manifest.transcript_commit_count == 0


@pytest.mark.asyncio
async def test_terminal_resume_reuses_already_committed_run_increment(tmp_path: Path) -> None:
    session = _session()
    store = SessionStore(tmp_path / "sessions")
    store.write_meta(session)
    store.append_message(session.id, "user", "recover committed terminal")
    committed = [{"role": "assistant", "content": "already committed"}]
    first_commit = store.append_messages(session.id, committed, run_id="run-recovery")
    recovery = RecoveryStore(tmp_path / "recovery.sqlite3")
    await recovery.create_run(
        session_id=session.id,
        run_id="run-recovery",
        thread_id=session.id,
        engine="graph",
        status="suspended",
        suspension_reason="process_recovery",
        checkpoint_revision="revision-terminal",
    )
    event_path = store.runs_dir(session.id) / "run-recovery" / "events.jsonl"
    event_path.parent.mkdir(parents=True, exist_ok=True)
    event_path.write_text(
        "\n".join(
            json.dumps(event)
            for event in (
                {
                    "type": "run.started",
                    "run_id": "run-recovery",
                    "goal": "recover committed terminal",
                    "event_seq": 1,
                    "ts": _now(),
                },
                {
                    "type": "run.finished",
                    "run_id": "run-recovery",
                    "status": "success",
                    "reason": None,
                    "steps": 1,
                    "event_seq": 2,
                    "ts": _now(),
                },
            )
        )
        + "\n",
        encoding="utf-8",
    )

    config = RuntimeConfig()
    config.agent.engine = "graph"
    engine = _AlreadyTerminalEngine()

    def resolver(_name: str):
        def build(*args: object, **kwargs: object) -> _AlreadyTerminalEngine:
            del args, kwargs
            return engine

        return build

    runner = AgentRunner(
        config,
        provider=_UnusedProvider(),  # type: ignore[arg-type]
        engine_resolver=resolver,
        recovery_store=recovery,
    )
    lease = await recovery.acquire_resume_lease(
        session.id,
        "run-recovery",
        expected_checkpoint_revision="revision-terminal",
    )
    outcome = await runner.run_and_capture(
        "recover committed terminal",
        run_id="run-recovery",
        session=session,
        store=store,
        resume=True,
        expected_checkpoint_revision="revision-terminal",
        resume_epoch=lease.resume_epoch,
    )

    assert isinstance(outcome, RunOutcome)
    assert outcome.status == "success"
    assert store.read_run_messages_strict(session.id, "run-recovery") == committed
    final_commit = store.transcript_commit(session.id, "run-recovery")
    assert final_commit.transcript_commit_count == first_commit.transcript_commit_count
    assert final_commit.transcript_commit_hash == first_commit.transcript_commit_hash
    events = [json.loads(line) for line in event_path.read_text(encoding="utf-8").splitlines()]
    assert [event["type"] for event in events] == ["run.started", "run.finished"]


@pytest.mark.asyncio
@pytest.mark.parametrize("finished_persisted", [False, True])
async def test_terminal_checkpoint_reconciles_missing_event_or_transcript_once(
    tmp_path: Path,
    finished_persisted: bool,
) -> None:
    session = _session()
    store = SessionStore(tmp_path / "sessions")
    store.write_meta(session)
    store.append_message(session.id, "user", "recover terminal checkpoint")
    recovery = RecoveryStore(tmp_path / "recovery.sqlite3")
    await recovery.create_run(
        session_id=session.id,
        run_id="run-recovery",
        thread_id=session.id,
        engine="graph",
        status="suspended",
        suspension_reason="process_recovery",
        checkpoint_revision="revision-terminal",
    )
    event_path = store.runs_dir(session.id) / "run-recovery" / "events.jsonl"
    event_path.parent.mkdir(parents=True, exist_ok=True)
    events: list[dict[str, object]] = [
        {
            "type": "run.started",
            "run_id": "run-recovery",
            "goal": "recover terminal checkpoint",
            "event_seq": 1,
            "ts": _now(),
        }
    ]
    if finished_persisted:
        events.append(
            {
                "type": "run.finished",
                "run_id": "run-recovery",
                "status": "success",
                "reason": None,
                "steps": 1,
                "event_seq": 2,
                "ts": _now(),
            }
        )
    event_path.write_text(
        "\n".join(json.dumps(event) for event in events) + "\n",
        encoding="utf-8",
    )

    config = RuntimeConfig()
    config.agent.engine = "graph"
    engine = _TerminalCheckpointEngine()

    def resolver(_name: str):
        def build(*args: object, **kwargs: object) -> _TerminalCheckpointEngine:
            del args, kwargs
            return engine

        return build

    runner = AgentRunner(
        config,
        provider=_UnusedProvider(),  # type: ignore[arg-type]
        engine_resolver=resolver,
        recovery_store=recovery,
    )
    lease = await recovery.acquire_resume_lease(
        session.id,
        "run-recovery",
        expected_checkpoint_revision="revision-terminal",
    )
    outcome = await runner.run_and_capture(
        "recover terminal checkpoint",
        run_id="run-recovery",
        session=session,
        store=store,
        resume=True,
        expected_checkpoint_revision="revision-terminal",
        resume_epoch=lease.resume_epoch,
    )

    assert isinstance(outcome, RunOutcome)
    persisted_events = [
        json.loads(line) for line in event_path.read_text(encoding="utf-8").splitlines()
    ]
    expected_types = (
        ["run.started", "run.finished"]
        if finished_persisted
        else ["run.started", "run.resumed", "run.finished"]
    )
    assert [event["type"] for event in persisted_events] == expected_types
    assert sum(event["type"] == "run.finished" for event in persisted_events) == 1
    assert persisted_events[-1]["type"] == "run.finished"
    assert store.read_run_messages_strict(session.id, "run-recovery") == [
        {"role": "assistant", "content": "checkpoint answer"}
    ]
    manifest = await recovery.get_run(session.id, "run-recovery")
    assert manifest is not None
    assert manifest.status == "success"
    assert manifest.transcript_commit_count == 1


@pytest.mark.asyncio
async def test_runner_does_not_require_manifest_for_sqlite_one_shot(tmp_path: Path) -> None:
    session = Session(
        id="sess-one-shot",
        mode="one_shot",
        status="active",
        title="one shot",
        created_at=_now(),
        updated_at=_now(),
        run_ids=["run-one-shot"],
        durable=False,
    )
    store = SessionStore(tmp_path / "sessions")
    store.write_meta(session)
    store.append_message(session.id, "user", "one shot")
    recovery = RecoveryStore(tmp_path / "recovery.sqlite3")
    config = RuntimeConfig()
    config.agent.engine = "graph"
    engine = _ImmediateEngine()

    def resolver(_name: str):
        def build(*args: object, **kwargs: object) -> _ImmediateEngine:
            del args, kwargs
            return engine

        return build

    runner = AgentRunner(
        config,
        provider=_UnusedProvider(),  # type: ignore[arg-type]
        engine_resolver=resolver,
        recovery_store=recovery,
    )
    outcome = await runner.run_and_capture(
        "one shot",
        run_id="run-one-shot",
        session=session,
        store=store,
    )

    assert isinstance(outcome, RunOutcome)
    assert outcome.status == "success"
    assert await recovery.latest_run(session.id) is None
    assert store.read_messages(session.id)[-1] == {
        "role": "assistant",
        "content": "one shot",
    }
