from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any, TypedDict, cast

import pytest

pytest.importorskip("langgraph")

from langgraph.graph import START, StateGraph

from agent_runtime.core.app import CoreApp
from agent_runtime.core.bus.envelope import HandlerError
from agent_runtime.core.config import RuntimeConfig
from agent_runtime.core.engine.base import RunOutcome, RunSuspension
from agent_runtime.core.engine.router import EngineRouter
from agent_runtime.core.events.bus import EventBus
from agent_runtime.core.graph.checkpoint import CheckpointRuntime
from agent_runtime.core.graph.recovery import CapabilityRejectedError, RecoveryStore
from agent_runtime.core.runner import AgentRunner
from agent_runtime.core.session.manager import (
    SESSION_STATE_ERROR,
    SESSION_SUSPENDED,
    SessionManager,
)
from agent_runtime.core.session.store import SessionStore

pytestmark = pytest.mark.recovery


class _CheckpointState(TypedDict):
    value: int


async def _save_checkpoint(runtime: CheckpointRuntime, thread_id: str) -> None:
    builder = StateGraph(_CheckpointState)
    builder.add_node("increment", lambda state: {"value": state["value"] + 1})
    builder.add_edge(START, "increment")
    graph = builder.compile(checkpointer=runtime.saver)
    await graph.ainvoke(  # type: ignore[attr-defined]
        {"value": 1},
        {"configurable": {"thread_id": thread_id}},
    )


class _DurableRunner:
    def __init__(self, recovery: RecoveryStore) -> None:
        self._recovery = recovery
        self.run_calls: list[dict[str, Any]] = []
        self.close_calls: list[str] = []

    async def run_and_capture(self, _goal: str, **kwargs: Any) -> RunOutcome | RunSuspension:
        self.run_calls.append(kwargs)
        session = kwargs["session"]
        run_id = kwargs["run_id"]
        if kwargs.get("resume"):
            run = await self._recovery.get_run(session.id, run_id)
            await self._recovery.mark_run_terminal(
                session.id,
                run_id,
                status="success",
                checkpoint_revision=run.checkpoint_revision,
                event_seq=run.event_seq + 1,
                transcript_commit_count=0,
                transcript_commit_hash=None,
                expected_resume_epoch=run.resume_epoch,
            )
            return RunOutcome(status="success", result="done", reason=None)

        await self._recovery.mark_run_suspended(
            session.id,
            run_id,
            reason="process_recovery",
            checkpoint_revision="revision-1",
            event_seq=2,
            expected_resume_epoch=0,
        )
        return RunSuspension(
            run_id=run_id,
            session_id=session.id,
            reason="process_recovery",
            checkpoint_revision="revision-1",
            interrupt_id=None,
            event_seq=2,
        )

    async def close_suspended(self, **kwargs: Any) -> RunOutcome:
        session = kwargs["session"]
        run_id = kwargs["run_id"]
        self.close_calls.append(run_id)
        run = await self._recovery.get_run(session.id, run_id)
        await self._recovery.mark_run_terminal(
            session.id,
            run_id,
            status="failed",
            checkpoint_revision=kwargs["checkpoint_revision"],
            event_seq=run.event_seq + 1,
            transcript_commit_count=0,
            transcript_commit_hash=None,
            expected_resume_epoch=kwargs["resume_epoch"],
        )
        return RunOutcome(status="failed", result="", reason="session_closed")


async def _suspended_manager(
    tmp_path: Path,
) -> tuple[SessionManager, SessionStore, RecoveryStore, _DurableRunner, str, str]:
    store = SessionStore(tmp_path / "sessions")
    recovery = RecoveryStore(tmp_path / "recovery.sqlite3")
    runner = _DurableRunner(recovery)
    manager = SessionManager(
        store,
        lambda: runner,  # type: ignore[arg-type]
        EventBus(),
        recovery_store=recovery,
    )
    session = await manager.create(mode="chat", durable=True)
    run_id = await manager.send_message(session.id, "resume this")
    return manager, store, recovery, runner, session.id, run_id


async def test_durable_suspension_persists_meta_and_blocks_a_second_run(
    tmp_path: Path,
) -> None:
    manager, store, recovery, runner, session_id, run_id = await _suspended_manager(tmp_path)

    manifest = await recovery.get_run(session_id, run_id)
    assert manifest.status == "suspended"
    assert manifest.checkpoint_revision == "revision-1"
    assert store.read_meta(session_id).status == "suspended"
    assert session_id not in manager._sessions

    manager.hydrate(session_id)
    with pytest.raises(HandlerError) as send_error:
        await manager.send_message(session_id, "must not create another run")
    with pytest.raises(HandlerError) as compact_error:
        await manager.compact(session_id)

    assert send_error.value.code == compact_error.value.code == SESSION_SUSPENDED
    assert len(runner.run_calls) == 1


async def test_resume_uses_the_leased_run_id_and_reaches_terminal_state(tmp_path: Path) -> None:
    manager, store, recovery, runner, session_id, run_id = await _suspended_manager(tmp_path)
    lease = await recovery.acquire_resume_lease(
        session_id,
        run_id,
        expected_checkpoint_revision="revision-1",
        expected_resume_epoch=0,
    )
    manager.hydrate(session_id)

    result = await manager.resume_run(session_id, lease)

    assert isinstance(result, RunOutcome)
    assert [call["run_id"] for call in runner.run_calls] == [run_id, run_id]
    assert runner.run_calls[-1]["resume"] is True
    assert runner.run_calls[-1]["resume_epoch"] == 1
    assert (await recovery.get_run(session_id, run_id)).status == "success"
    assert store.read_meta(session_id).status == "waiting_for_input"


async def test_disconnected_suspended_session_keeps_capability_and_is_evicted(
    tmp_path: Path,
) -> None:
    manager, store, recovery, _runner, session_id, _run_id = await _suspended_manager(tmp_path)
    grant = await recovery.issue_capability(session_id)

    preserved = await manager.close_disconnected(session_id)
    rotated = await recovery.consume_capability(session_id, grant.token)

    assert preserved is True
    assert rotated.token != grant.token
    assert store.read_meta(session_id).status == "suspended"
    assert session_id not in manager._sessions


async def test_explicit_close_terminalizes_suspended_run_before_resource_callback(
    tmp_path: Path,
) -> None:
    manager, store, recovery, runner, session_id, run_id = await _suspended_manager(tmp_path)
    callback_statuses: list[str] = []

    async def closed_callback(closed_session_id: str) -> None:
        callback_statuses.append((await recovery.get_run(closed_session_id, run_id)).status)

    manager._on_session_closed = closed_callback
    manager.hydrate(session_id)

    await manager.close(session_id)

    assert runner.close_calls == [run_id]
    assert callback_statuses == ["failed"]
    assert await recovery.latest_unfinished_run(session_id) is None
    assert store.read_meta(session_id).status == "closed"


async def test_explicit_close_deletes_sqlite_checkpoint_and_capability_without_task_leaks(
    tmp_path: Path,
) -> None:
    checkpoint = await CheckpointRuntime.open(
        "sqlite",
        sqlite_path=tmp_path / "graph-checkpoints.sqlite3",
    )
    recovery = RecoveryStore(tmp_path / "recovery.sqlite3")
    store = SessionStore(tmp_path / "sessions")
    runner = _DurableRunner(recovery)
    app = CoreApp(EngineRouter(cast(Any, checkpoint)))
    app._recovery_store = recovery
    manager = SessionManager(
        store,
        lambda: runner,  # type: ignore[arg-type]
        EventBus(),
        on_session_closed=app._delete_session_resources,
        recovery_store=recovery,
    )
    session = await manager.create(mode="chat", durable=True)
    run_id = await manager.send_message(session.id, "close this suspended run")
    capability = await recovery.issue_capability(session.id)
    await _save_checkpoint(checkpoint, session.id)
    assert (
        await checkpoint.saver.aget_tuple({"configurable": {"thread_id": session.id}}) is not None
    )
    baseline_tasks = set(asyncio.all_tasks())

    manager.hydrate(session.id)
    await manager.close(session.id)
    await checkpoint.close()
    await asyncio.sleep(0)

    assert runner.close_calls == [run_id]
    assert store.read_meta(session.id).status == "closed"
    assert await recovery.latest_unfinished_run(session.id) is None
    with pytest.raises(CapabilityRejectedError):
        await recovery.validate_capability(session.id, capability.token)
    reopened = await CheckpointRuntime.open(
        "sqlite",
        sqlite_path=tmp_path / "graph-checkpoints.sqlite3",
    )
    try:
        assert await reopened.saver.aget_tuple({"configurable": {"thread_id": session.id}}) is None
    finally:
        await reopened.close()
    leaked = [
        task
        for task in asyncio.all_tasks()
        if task not in baseline_tasks and task is not asyncio.current_task() and not task.done()
    ]
    assert leaked == []


async def test_explicit_close_maps_corrupt_transcript_to_typed_state_error(
    tmp_path: Path,
) -> None:
    manager, store, recovery, _runner, session_id, _run_id = await _suspended_manager(tmp_path)
    callbacks: list[str] = []

    async def closed_callback(closed_session_id: str) -> None:
        callbacks.append(closed_session_id)

    manager._runner_factory = lambda: AgentRunner(
        RuntimeConfig(),
        recovery_store=recovery,
    )
    manager._on_session_closed = closed_callback
    (store.session_dir(session_id) / "thread.jsonl").write_text(
        "{invalid-json}\n",
        encoding="utf-8",
    )
    manager.hydrate(session_id)

    with pytest.raises(HandlerError) as error:
        await manager.close(session_id)

    assert error.value.code == SESSION_STATE_ERROR
    assert error.value.data == {"code": "transcript_corruption"}
    assert callbacks == []
    assert store.read_meta(session_id).status == "suspended"
