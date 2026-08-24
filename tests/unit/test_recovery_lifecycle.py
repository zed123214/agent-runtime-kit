from __future__ import annotations

import asyncio
import json
import threading
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import MagicMock

import pytest

import agent_runtime.core.app as app_module
from agent_runtime.core.app import RECOVERY_CONFLICT, RECOVERY_REJECTED, CoreApp
from agent_runtime.core.bus.envelope import HandlerError
from agent_runtime.core.engine.base import RunOutcome
from agent_runtime.core.graph.event_log import read_event_log
from agent_runtime.core.graph.recovery import CapabilityRejectedError, RecoveryStore
from agent_runtime.core.session.manager import SESSION_NOT_FOUND, SessionManager
from agent_runtime.core.session.store import SessionStore
from agent_runtime.core.transport.ipc_broadcaster import IpcEventBroadcaster

pytestmark = pytest.mark.recovery


def _writer() -> Any:
    return MagicMock()


def _state(
    *,
    revision: str,
    reason: str,
    status: str = "suspended",
    event_seq: int = 0,
    expires_at: str | None = None,
) -> SimpleNamespace:
    return SimpleNamespace(
        checkpoint_revision=revision,
        suspension_reason=reason,
        status=status,
        event_seq=event_seq,
        resumable=status == "suspended" and reason != "outcome_unknown",
        pending_expires_at=expires_at,
    )


class _Runtime:
    def __init__(self, state: Any = None) -> None:
        self.state = state
        self.latest_calls = 0
        self.deleted: list[str] = []

    async def latest_graph_state(self, *_args: Any, **_kwargs: Any) -> Any:
        self.latest_calls += 1
        return self.state

    async def delete_thread(self, thread_id: str) -> None:
        self.deleted.append(thread_id)

    async def close(self) -> None:
        return None


async def _app_session(
    tmp_path: Path,
    runtime: _Runtime,
    recovery: RecoveryStore,
    *,
    runner_factory: Any = None,
) -> tuple[CoreApp, SessionManager, SessionStore, str]:
    app = CoreApp()
    app._engine_router = cast(Any, runtime)
    app._sessions_root = tmp_path / "sessions"
    app._recovery_store = recovery
    app._broadcaster = IpcEventBroadcaster()
    store = SessionStore(app._sessions_root)
    manager = SessionManager(
        store,
        runner_factory or (lambda: cast(Any, None)),
        app._bus,
        on_session_closed=app._delete_session_resources,
        recovery_store=recovery,
    )
    app._sessions = manager
    session = await manager.create(mode="chat", durable=True)
    return app, manager, store, session.id


async def test_process_recovery_accepts_forward_revision_but_permission_does_not(
    tmp_path: Path,
) -> None:
    recovery = RecoveryStore(tmp_path / "recovery.sqlite3")
    app = CoreApp()
    app._sessions_root = tmp_path / "sessions"
    app._recovery_store = recovery
    process_run = await recovery.create_run(
        session_id="sess-process",
        run_id="run-process",
        thread_id="sess-process",
        engine="graph",
        status="suspended",
        suspension_reason="process_recovery",
        checkpoint_revision="rev-old",
    )

    advanced = await app._coordinate_recovery_checkpoint(
        process_run,
        _state(revision="rev-new", reason="process_recovery"),
    )

    assert advanced.checkpoint_revision == "rev-new"
    permission_run = await recovery.create_run(
        session_id="sess-permission",
        run_id="run-permission",
        thread_id="sess-permission",
        engine="graph",
        status="suspended",
        suspension_reason="permission",
        checkpoint_revision="rev-old",
    )
    with pytest.raises(HandlerError) as conflict:
        await app._coordinate_recovery_checkpoint(
            permission_run,
            _state(revision="rev-new", reason="permission"),
        )
    assert conflict.value.code == RECOVERY_CONFLICT


async def test_no_checkpoint_process_loss_is_terminal_and_capability_can_attach(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    recovery = RecoveryStore(tmp_path / "recovery.sqlite3")
    runtime = _Runtime()
    app, _manager, store, session_id = await _app_session(tmp_path, runtime, recovery)
    store.append_message(session_id, "user", "recover after early process loss")
    capability = await recovery.issue_capability(session_id)
    await recovery.create_run(
        session_id=session_id,
        run_id="run-early",
        thread_id=session_id,
        engine="graph",
        status="running",
    )
    interrupted = (await recovery.suspend_interrupted_runs())[0]

    terminal = await app._reconcile_interrupted_run(interrupted)

    assert terminal.status == "failed"
    assert terminal.checkpoint_revision is None
    snapshot = read_event_log(store.runs_dir(session_id) / "run-early" / "events.jsonl")
    assert [event["type"] for event in snapshot.events] == ["run.started", "run.finished"]
    assert snapshot.events[-1]["reason"] == "process_lost_before_checkpoint"
    assert terminal.event_seq == snapshot.last_event_seq == 2

    writer = _writer()
    monkeypatch.setattr(app_module, "get_connection_writer", lambda: writer)
    attached = await app._session_resume_handler(
        {"session_id": session_id, "resume_token": capability.token}
    )
    assert attached.status == "waiting_for_input"
    assert attached.run_id is None
    assert app._broadcaster is not None
    assert app._broadcaster.owns_session(writer, session_id)


async def test_invalid_resume_token_cannot_observe_or_modify_recovery_state(
    tmp_path: Path,
) -> None:
    recovery = RecoveryStore(tmp_path / "recovery.sqlite3")
    runtime = _Runtime(_state(revision="rev-1", reason="permission"))
    app, _manager, store, session_id = await _app_session(tmp_path, runtime, recovery)
    capability = await recovery.issue_capability(session_id)
    run = await recovery.create_run(
        session_id=session_id,
        run_id="run-private",
        thread_id=session_id,
        engine="graph",
        status="suspended",
        suspension_reason="permission",
        checkpoint_revision="rev-1",
    )
    event_path = store.runs_dir(session_id) / run.run_id / "events.jsonl"
    event_path.parent.mkdir(parents=True)
    event_path.write_bytes(b'{"type":"run.started","run_id":"run-private"}\n')
    before_events = event_path.read_bytes()

    with pytest.raises(HandlerError) as rejected:
        await app._session_resume_handler({"session_id": session_id, "resume_token": "wrong-token"})

    assert rejected.value.code == RECOVERY_REJECTED
    assert runtime.latest_calls == 0
    assert event_path.read_bytes() == before_events
    assert await recovery.get_run(session_id, run.run_id) == run
    assert app._resume_attach_inflight == {}
    validation = await recovery.validate_capability(session_id, capability.token)
    assert validation.token_version == 1


async def test_owner_conflict_does_not_consume_capability(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    recovery = RecoveryStore(tmp_path / "recovery.sqlite3")
    runtime = _Runtime()
    app, _manager, _store, session_id = await _app_session(tmp_path, runtime, recovery)
    capability = await recovery.issue_capability(session_id)
    owner = _writer()
    attacker = _writer()
    assert app._broadcaster is not None
    assert app._broadcaster.bind_session(owner, session_id)
    monkeypatch.setattr(app_module, "get_connection_writer", lambda: attacker)

    with pytest.raises(HandlerError) as conflict:
        await app._session_resume_handler(
            {"session_id": session_id, "resume_token": capability.token}
        )

    assert conflict.value.code == SESSION_NOT_FOUND
    assert runtime.latest_calls == 0
    assert (await recovery.validate_capability(session_id, capability.token)).token_version == 1
    assert app._broadcaster.owns_session(owner, session_id)


async def test_pending_attach_does_not_authorize_pipelined_owner_commands(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    recovery = RecoveryStore(tmp_path / "recovery.sqlite3")
    runtime = _Runtime()
    app, _manager, store, session_id = await _app_session(tmp_path, runtime, recovery)
    store.append_message(session_id, "user", "private history")
    capability = await recovery.issue_capability(session_id)
    writer = _writer()
    monkeypatch.setattr(app_module, "get_connection_writer", lambda: writer)
    rotate_started = asyncio.Event()
    release_rotate = asyncio.Event()
    rotate = recovery.rotate_capability

    async def blocked_rotate(*args: Any, **kwargs: Any) -> Any:
        rotate_started.set()
        await release_rotate.wait()
        return await rotate(*args, **kwargs)

    monkeypatch.setattr(recovery, "rotate_capability", blocked_rotate)
    attaching = asyncio.create_task(
        app._session_resume_handler({"session_id": session_id, "resume_token": capability.token})
    )
    await rotate_started.wait()

    with pytest.raises(HandlerError) as hidden:
        await app._session_history_handler({"session_id": session_id})
    assert hidden.value.code == SESSION_NOT_FOUND
    assert app._broadcaster is not None
    assert app._broadcaster.session_ids_for(writer) == frozenset()

    with pytest.raises(HandlerError) as rejected:
        await app._session_resume_handler(
            {"session_id": session_id, "resume_token": "definitely-wrong"}
        )
    assert rejected.value.code == RECOVERY_REJECTED

    with pytest.raises(HandlerError) as concurrent:
        await asyncio.wait_for(
            app._session_resume_handler(
                {"session_id": session_id, "resume_token": capability.token}
            ),
            timeout=0.1,
        )
    assert concurrent.value.code == RECOVERY_CONFLICT
    assert concurrent.value.data == {"code": "resume_in_progress"}

    release_rotate.set()
    await attaching
    history = await app._session_history_handler({"session_id": session_id})
    assert history.messages == [{"role": "user", "content": "private history"}]


async def test_cancel_after_capability_rotation_commit_restores_old_token(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    recovery = RecoveryStore(tmp_path / "recovery.sqlite3")
    app, _manager, _store, session_id = await _app_session(tmp_path, _Runtime(), recovery)
    capability = await recovery.issue_capability(session_id)
    writer = _writer()
    monkeypatch.setattr(app_module, "get_connection_writer", lambda: writer)
    rotation_committed = threading.Event()
    release_worker = threading.Event()
    rotate_sync = recovery._rotate_capability_sync

    def committed_then_block(*args: Any, **kwargs: Any) -> Any:
        grant = rotate_sync(*args, **kwargs)
        rotation_committed.set()
        if not release_worker.wait(timeout=5.0):
            raise TimeoutError("rotation worker gate was not released")
        return grant

    monkeypatch.setattr(recovery, "_rotate_capability_sync", committed_then_block)
    attaching = asyncio.create_task(
        app._session_resume_handler({"session_id": session_id, "resume_token": capability.token})
    )
    assert await asyncio.to_thread(rotation_committed.wait, 5.0)
    assert app._broadcaster is not None
    app._broadcaster.mark_disconnected(writer)
    attaching.cancel()
    release_worker.set()
    [cancelled] = await asyncio.gather(attaching, return_exceptions=True)

    assert isinstance(cancelled, asyncio.CancelledError)
    assert (await recovery.validate_capability(session_id, capability.token)).token_version == 1
    assert not app._broadcaster.owns_session(writer, session_id)
    assert session_id not in app._broadcaster._pending_writer_by_session_id
    assert app._resume_attach_inflight == {}


async def test_closed_session_resume_cleans_capability_before_checkpoint(
    tmp_path: Path,
) -> None:
    recovery = RecoveryStore(tmp_path / "recovery.sqlite3")

    class _ClosedRuntime(_Runtime):
        async def delete_thread(self, thread_id: str) -> None:
            with pytest.raises(CapabilityRejectedError):
                await recovery.validate_capability(thread_id, capability.token)
            await super().delete_thread(thread_id)

    runtime = _ClosedRuntime()
    app, _manager, store, session_id = await _app_session(tmp_path, runtime, recovery)
    capability = await recovery.issue_capability(session_id)
    session = store.read_meta(session_id)
    session.status = "closed"
    store.write_meta(session)

    with pytest.raises(HandlerError) as rejected:
        await app._session_resume_handler(
            {"session_id": session_id, "resume_token": capability.token}
        )

    assert rejected.value.code == RECOVERY_REJECTED
    assert runtime.deleted == [session_id]
    assert runtime.latest_calls == 0
    assert app._broadcaster is not None
    assert app._broadcaster._writer_by_session_id == {}


async def test_close_retry_repeats_resource_cleanup_after_closed_meta(
    tmp_path: Path,
) -> None:
    calls = 0

    async def cleanup(_session_id: str) -> None:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("simulated checkpoint cleanup crash")

    store = SessionStore(tmp_path / "sessions")
    manager = SessionManager(
        store, lambda: cast(Any, None), app_module.EventBus(), on_session_closed=cleanup
    )
    session = await manager.create(mode="chat")

    with pytest.raises(RuntimeError, match="cleanup crash"):
        await manager.close(session.id)
    assert store.read_meta(session.id).status == "closed"

    await manager.close(session.id)
    assert calls == 2


async def test_persisted_session_closed_terminal_wins_and_clears_resources(
    tmp_path: Path,
) -> None:
    recovery = RecoveryStore(tmp_path / "recovery.sqlite3")
    runtime = _Runtime(_state(revision="rev-checkpoint", reason="permission", event_seq=1))
    app, _manager, store, session_id = await _app_session(tmp_path, runtime, recovery)
    capability = await recovery.issue_capability(session_id)
    await recovery.create_run(
        session_id=session_id,
        run_id="run-close",
        thread_id=session_id,
        engine="graph",
        status="running",
        checkpoint_revision="rev-manifest",
        event_seq=1,
    )
    event_path = store.runs_dir(session_id) / "run-close" / "events.jsonl"
    event_path.parent.mkdir(parents=True)
    events = [
        {"type": "run.started", "run_id": "run-close", "goal": "close", "event_seq": 1},
        {
            "type": "run.finished",
            "run_id": "run-close",
            "status": "failed",
            "reason": "session_closed",
            "steps": 0,
            "event_seq": 2,
        },
    ]
    event_path.write_text(
        "".join(json.dumps(event) + "\n" for event in events),
        encoding="utf-8",
    )
    original_events = event_path.read_bytes()
    interrupted = (await recovery.suspend_interrupted_runs())[0]

    terminal = await app._reconcile_interrupted_run(interrupted)

    assert terminal.status == "failed"
    assert terminal.checkpoint_revision == "rev-checkpoint"
    assert terminal.event_seq == 2
    assert await recovery.latest_unfinished_run(session_id) is None
    assert store.read_meta(session_id).status == "closed"
    with pytest.raises(CapabilityRejectedError):
        await recovery.validate_capability(session_id, capability.token)
    assert runtime.deleted == [session_id]
    snapshot = read_event_log(event_path)
    assert event_path.read_bytes() == original_events
    assert [event["type"] for event in snapshot.events].count("run.finished") == 1
    assert snapshot.events[-1]["type"] == "run.finished"


async def test_expired_permission_is_closed_with_timeout_during_attach(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    recovery = RecoveryStore(tmp_path / "recovery.sqlite3")

    class _Runner:
        resume_value: object | None = None

        async def run_and_capture(self, _goal: str, **kwargs: Any) -> RunOutcome:
            self.resume_value = kwargs["resume_value"]
            current = await recovery.get_run(kwargs["session"].id, kwargs["run_id"])
            assert current is not None
            await recovery.mark_run_terminal(
                current.session_id,
                current.run_id,
                status="failed",
                checkpoint_revision=kwargs["expected_checkpoint_revision"],
                event_seq=current.event_seq,
                transcript_commit_count=0,
                transcript_commit_hash=None,
                expected_resume_epoch=kwargs["resume_epoch"],
            )
            return RunOutcome(status="failed", result="", reason="timeout")

    runner = _Runner()
    expired = (datetime.now(UTC) - timedelta(seconds=1)).isoformat()

    class _ExpiryRuntime(_Runtime):
        async def latest_graph_state(self, _thread_id: str, **kwargs: Any) -> Any:
            self.latest_calls += 1
            run = await recovery.get_run(kwargs["session_id"], kwargs["run_id"])
            assert run is not None
            if run.status == "failed":
                return _state(
                    revision="rev-expired",
                    reason="timeout",
                    status="failed",
                    event_seq=run.event_seq,
                )
            return _state(
                revision="rev-expired",
                reason="permission",
                expires_at=expired,
            )

    runtime = _ExpiryRuntime()
    app, _manager, _store, session_id = await _app_session(
        tmp_path,
        runtime,
        recovery,
        runner_factory=lambda: runner,
    )
    capability = await recovery.issue_capability(session_id)
    await recovery.create_run(
        session_id=session_id,
        run_id="run-expired",
        thread_id=session_id,
        engine="graph",
        status="suspended",
        suspension_reason="permission",
        checkpoint_revision="rev-expired",
    )
    writer = _writer()
    monkeypatch.setattr(app_module, "get_connection_writer", lambda: writer)

    result = await app._session_resume_handler(
        {"session_id": session_id, "resume_token": capability.token}
    )

    assert runner.resume_value == "timeout"
    assert result.status == "failed"
    assert result.suspension_reason == "timeout"
    assert await recovery.latest_unfinished_run(session_id) is None
