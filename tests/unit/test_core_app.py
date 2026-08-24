from __future__ import annotations

import asyncio
import json
import signal
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock

import pytest

import agent_runtime.core.app as app_module
import agent_runtime.core.transport.ipc_broadcaster as broadcaster_module
from agent_runtime.core.app import EVENT_LOG_ERROR, CoreApp, _install_shutdown_handlers
from agent_runtime.core.bus.envelope import HandlerError
from agent_runtime.core.bus.events import RunStartedEvent
from agent_runtime.core.config import RuntimeConfig
from agent_runtime.core.engine.router import EngineRouter
from agent_runtime.core.events.bus import EventBus
from agent_runtime.core.permissions.manager import PermissionManager
from agent_runtime.core.session.manager import SESSION_NOT_FOUND, SessionManager
from agent_runtime.core.session.store import SessionStore
from agent_runtime.core.transport.ipc_broadcaster import IpcEventBroadcaster


def _make_writer() -> asyncio.StreamWriter:
    writer = MagicMock(spec=asyncio.StreamWriter)
    writer.drain = AsyncMock()
    return cast(asyncio.StreamWriter, writer)


def _sequenced_run_started(run_id: str, event_seq: int) -> RunStartedEvent:
    return RunStartedEvent(
        run_id=run_id,
        session_id="s1",
        event_seq=event_seq,
        goal="test",
        ts="2026-01-01T00:00:00Z",
    )


class _ShutdownFlag:
    def __init__(self) -> None:
        self.is_set = False

    def set(self) -> None:
        self.is_set = True


class _UnsupportedSignalLoop:
    def __init__(self) -> None:
        self.threadsafe_calls = 0

    def add_signal_handler(self, sig: signal.Signals, callback: Any) -> None:
        raise NotImplementedError

    def call_soon_threadsafe(self, callback: Any) -> None:
        self.threadsafe_calls += 1
        callback()


def test_shutdown_handlers_fall_back_when_asyncio_signal_handlers_are_unsupported(
    monkeypatch: Any,
) -> None:
    registered: dict[signal.Signals, Any] = {}

    def fake_signal(sig: signal.Signals, handler: Any) -> None:
        registered[sig] = handler

    monkeypatch.setattr(signal, "signal", fake_signal)
    shutdown = _ShutdownFlag()
    loop = _UnsupportedSignalLoop()

    _install_shutdown_handlers(loop, shutdown)  # type: ignore[arg-type]

    assert set(registered) == {signal.SIGINT, signal.SIGTERM}

    registered[signal.SIGINT](signal.SIGINT, None)

    assert shutdown.is_set
    assert loop.threadsafe_calls == 1


def test_default_provider_remains_lazy_until_runner_invocation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    constructed = False

    def eager_provider(_model: str) -> Any:
        nonlocal constructed
        constructed = True
        raise AssertionError("default provider must remain lazy")

    monkeypatch.setattr(app_module, "AnthropicProvider", eager_provider)
    app = CoreApp()

    assert app._provider_for_runner(RuntimeConfig()) is None
    assert constructed is False


def test_injected_provider_factory_is_passed_to_runner() -> None:
    provider = object()
    app = CoreApp(provider_factory=lambda _config: cast(Any, provider))

    assert app._provider_for_runner(RuntimeConfig()) is provider


# 功能：验证 session.create 将新 session 绑定到当前 socket 连接
# 设计：注入最小 SessionManager stub 和连接 writer，调用真实 handler 后检查 broadcaster 归属表
async def test_session_create_binds_session_to_current_connection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _Sessions:
        async def create(self, *, mode: str, title: str, before_publish: Any) -> SimpleNamespace:
            assert mode == "chat"
            assert title == "owner"
            created = SimpleNamespace(id="s-owner", status="active")
            before_publish(created)
            return created

    app = CoreApp()
    app._sessions = cast(Any, _Sessions())
    app._broadcaster = IpcEventBroadcaster()
    writer = _make_writer()
    monkeypatch.setattr(app_module, "get_connection_writer", lambda: writer)

    result = await app._session_create_handler({"mode": "chat", "title": "owner"})

    assert result.session_id == "s-owner"
    assert app._broadcaster.session_ids_for(writer) == frozenset({"s-owner"})


# 功能：验证 agent.run 创建的 one-shot session 同样绑定到发起连接
# 设计：用立即完成的 SessionManager stub 运行 handler，确保 CLI run 也能接收自己的权限事件
async def test_agent_run_binds_one_shot_session_to_current_connection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sent = asyncio.Event()

    class _Sessions:
        async def create(self, *, mode: str, title: str, before_publish: Any) -> SimpleNamespace:
            assert mode == "one_shot"
            assert title == "run goal"
            created = SimpleNamespace(id="s-run", status="active")
            before_publish(created)
            return created

        async def send_message(self, session_id: str, goal: str, *, run_id: str) -> str:
            assert session_id == "s-run"
            assert goal == "run goal"
            sent.set()
            return run_id

    app = CoreApp()
    app._sessions = cast(Any, _Sessions())
    app._broadcaster = IpcEventBroadcaster()
    writer = _make_writer()
    monkeypatch.setattr(app_module, "get_connection_writer", lambda: writer)

    result = await app._agent_run_handler({"goal": "run goal"})
    await asyncio.wait_for(sent.wait(), timeout=1.0)

    assert result.run_id
    assert app._broadcaster.session_ids_for(writer) == frozenset({"s-run"})


# 功能：验证 permission.respond handler 拒绝非 owner 连接并允许 owner 连接
# 设计：构造真实挂起 Future，先切换为 s2 writer 响应，再切回 s1 writer 完成审批
async def test_permission_respond_handler_checks_connection_session_owner(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = CoreApp()
    app._broadcaster = IpcEventBroadcaster()
    app._permission_manager = PermissionManager(timeout_s=0)
    owner = _make_writer()
    other = _make_writer()
    app._broadcaster.bind_session(owner, "s1")
    app._broadcaster.bind_session(other, "s2")
    emitted = asyncio.Event()

    async def emitter(_event: dict[str, Any]) -> None:
        emitted.set()

    pending = asyncio.create_task(
        app._permission_manager.check_and_wait(
            tool_use_id="tool-s1",
            tool_name="bash",
            params={"command": "echo safe"},
            session_id="s1",
            event_emitter=emitter,
        )
    )
    await asyncio.wait_for(emitted.wait(), timeout=1.0)

    monkeypatch.setattr(app_module, "get_connection_writer", lambda: other)
    rejected = await app._permission_respond_handler(
        {"tool_use_id": "tool-s1", "decision": "allow_once"}
    )
    assert rejected.ok is False
    await asyncio.sleep(0)
    assert pending.done() is False

    monkeypatch.setattr(app_module, "get_connection_writer", lambda: owner)
    accepted = await app._permission_respond_handler(
        {"tool_use_id": "tool-s1", "decision": "allow_once"}
    )
    assert accepted.ok is True
    assert await asyncio.wait_for(pending, timeout=1.0) == (True, "allow_once")


async def test_permission_respond_handler_routes_full_run_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = CoreApp()
    app._broadcaster = IpcEventBroadcaster()
    app._permission_manager = PermissionManager(timeout_s=0)
    owner = _make_writer()
    app._broadcaster.bind_session(owner, "s1")
    monkeypatch.setattr(app_module, "get_connection_writer", lambda: owner)
    emitted_count = 0
    both_emitted = asyncio.Event()

    async def emitter(_event: dict[str, Any]) -> None:
        nonlocal emitted_count
        emitted_count += 1
        if emitted_count == 2:
            both_emitted.set()

    root = asyncio.create_task(
        app._permission_manager.check_and_wait(
            tool_use_id="same-id",
            tool_name="bash",
            params={"command": "echo root"},
            session_id="s1",
            run_id="root-run",
            event_emitter=emitter,
        )
    )
    child = asyncio.create_task(
        app._permission_manager.check_and_wait(
            tool_use_id="same-id",
            tool_name="bash",
            params={"command": "echo child"},
            session_id="s1",
            run_id="child-run",
            event_emitter=emitter,
        )
    )
    await asyncio.wait_for(both_emitted.wait(), timeout=1.0)

    ambiguous = await app._permission_respond_handler(
        {"tool_use_id": "same-id", "decision": "allow_once"}
    )
    assert ambiguous.ok is False
    assert root.done() is False
    assert child.done() is False

    precise = await app._permission_respond_handler(
        {
            "session_id": "s1",
            "run_id": "root-run",
            "tool_use_id": "same-id",
            "decision": "allow_once",
        }
    )
    assert precise.ok is True
    assert await asyncio.wait_for(root, timeout=1.0) == (True, "allow_once")
    assert child.done() is False

    denied = await app._permission_respond_handler(
        {
            "session_id": "s1",
            "run_id": "child-run",
            "tool_use_id": "same-id",
            "decision": "deny_once",
        }
    )
    assert denied.ok is True
    assert await asyncio.wait_for(child, timeout=1.0) == (False, "deny_once")


async def test_disconnect_cancels_owned_run_and_pending_permission() -> None:
    deleted_threads: list[str] = []
    run_task: asyncio.Task[object] | None = None

    class _Runtime:
        async def delete_thread(self, thread_id: str) -> None:
            assert run_task is not None and run_task.done()
            deleted_threads.append(thread_id)

        async def close(self) -> None:
            return None

    app = CoreApp(EngineRouter(_Runtime()))
    app._permission_manager = PermissionManager(timeout_s=0)
    app._broadcaster = IpcEventBroadcaster(on_disconnect=app._on_client_disconnect)
    writer = _make_writer()
    app._broadcaster.bind_session(writer, "s1")
    permission_emitted = asyncio.Event()

    async def emitter(_event: dict[str, Any]) -> None:
        permission_emitted.set()

    pending = asyncio.create_task(
        app._permission_manager.check_and_wait(
            tool_use_id="tool-s1",
            tool_name="bash",
            params={"command": "echo safe"},
            session_id="s1",
            event_emitter=emitter,
        )
    )
    run_task = asyncio.create_task(asyncio.Event().wait())
    app._track_run_task("s1", run_task)
    await asyncio.wait_for(permission_emitted.wait(), timeout=1.0)

    app._broadcaster.disconnect(writer)

    assert await asyncio.wait_for(pending, timeout=1.0) == (False, "deny_once")
    await asyncio.wait_for(app._wait_for_cleanup_tasks(), timeout=1.0)
    assert run_task.cancelled()
    assert deleted_threads == ["s1"]
    assert app._broadcaster.session_ids_for(writer) == frozenset()
    assert "s1" not in app._run_tasks_by_session


async def test_disconnect_and_shutdown_cancel_wait_for_the_same_terminal_cleanup() -> None:
    terminal_started = asyncio.Event()
    release_terminal = asyncio.Event()
    terminal_finished = asyncio.Event()
    deleted_threads: list[str] = []
    run_task: asyncio.Task[None] | None = None

    class _Runtime:
        async def delete_thread(self, thread_id: str) -> None:
            assert run_task is not None and run_task.done()
            assert terminal_finished.is_set()
            deleted_threads.append(thread_id)

        async def close(self) -> None:
            return None

    async def cancel_resistant_terminal_cleanup() -> None:
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError as original_cancel:
            terminal_started.set()
            while not release_terminal.is_set():
                try:
                    await asyncio.shield(release_terminal.wait())
                except asyncio.CancelledError:
                    continue
            terminal_finished.set()
            raise original_cancel

    app = CoreApp(EngineRouter(_Runtime()))
    app._broadcaster = IpcEventBroadcaster(on_disconnect=app._on_client_disconnect)
    writer = _make_writer()
    app._broadcaster.bind_session(writer, "s1")
    run_task = asyncio.create_task(cancel_resistant_terminal_cleanup())
    app._track_run_task("s1", run_task)

    app._broadcaster.disconnect(writer)
    await asyncio.wait_for(terminal_started.wait(), timeout=1.0)
    run_task.cancel()  # CoreApp shutdown races the disconnect cancellation.
    release_terminal.set()

    await asyncio.wait_for(app._wait_for_cleanup_tasks(), timeout=1.0)
    assert run_task.cancelled()
    assert terminal_finished.is_set()
    assert deleted_threads == ["s1"]
    assert "s1" not in app._run_tasks_by_session


async def test_disconnect_closes_persisted_session_and_reclaims_memory(tmp_path: Path) -> None:
    sequence: list[str] = []
    run_task: asyncio.Task[object] | None = None
    app: CoreApp

    class _RecordingStore(SessionStore):
        def write_meta(self, session: Any) -> None:
            super().write_meta(session)
            if session.status == "closed":
                sequence.append("meta_closed")

    class _Runtime:
        async def delete_thread(self, thread_id: str) -> None:
            assert run_task is not None and run_task.done()
            assert app._permission_manager is not None
            assert not any(key[0] == thread_id for key in app._permission_manager._pending)
            sequence.append("thread_deleted")

        async def close(self) -> None:
            return None

    app = CoreApp(EngineRouter(_Runtime()))
    app._permission_manager = PermissionManager(timeout_s=0)
    store = _RecordingStore(tmp_path / "sessions")
    app._sessions = SessionManager(
        store,
        lambda: cast(Any, None),
        app._bus,
    )
    writer = _make_writer()

    def on_disconnect(session_ids: frozenset[str]) -> None:
        assert app._broadcaster is not None
        assert app._broadcaster.session_ids_for(writer) == frozenset()
        app._on_client_disconnect(session_ids)

    app._broadcaster = IpcEventBroadcaster(on_disconnect=on_disconnect)
    session = await app._sessions.create(
        mode="chat",
        title="disconnect",
        before_publish=lambda created: app._broadcaster.bind_session(writer, created.id),
    )
    store.append_message(session.id, "user", "preserve me")
    event_path = store.runs_dir(session.id) / "root-run" / "events.jsonl"
    event_path.parent.mkdir(parents=True)
    event_path.write_text('{"type":"run.started","run_id":"root-run"}\n', encoding="utf-8")
    permission_emitted = asyncio.Event()

    async def emitter(_event: dict[str, Any]) -> None:
        permission_emitted.set()

    pending = asyncio.create_task(
        app._permission_manager.check_and_wait(
            tool_use_id="shared-id",
            tool_name="bash",
            params={"command": "echo waiting"},
            session_id=session.id,
            run_id="root-run",
            event_emitter=emitter,
        )
    )

    async def active_run() -> None:
        try:
            await asyncio.Event().wait()
        finally:
            assert app._permission_manager is not None
            assert not any(key[0] == session.id for key in app._permission_manager._pending)
            sequence.append("run_finished")

    run_task = asyncio.create_task(active_run())
    app._track_run_task(session.id, run_task)
    await asyncio.wait_for(permission_emitted.wait(), timeout=1.0)

    assert app._broadcaster.disconnect(writer) == frozenset({session.id})
    assert await asyncio.wait_for(pending, timeout=1.0) == (False, "deny_once")
    await asyncio.wait_for(app._wait_for_cleanup_tasks(), timeout=1.0)

    assert run_task.cancelled()
    assert sequence == ["run_finished", "meta_closed", "thread_deleted"]
    assert store.read_meta(session.id).status == "closed"
    assert store.read_messages(session.id) == [{"role": "user", "content": "preserve me"}]
    assert event_path.read_text(encoding="utf-8") == (
        '{"type":"run.started","run_id":"root-run"}\n'
    )
    assert session.id not in app._sessions._sessions
    assert session.id not in app._sessions._locks


async def test_agent_run_does_not_start_after_disconnect_during_session_create(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = CoreApp()
    app._broadcaster = IpcEventBroadcaster(on_disconnect=app._on_client_disconnect)
    writer = _make_writer()
    send_message = AsyncMock()

    class _Sessions:
        def __init__(self) -> None:
            self.send_message = send_message

        async def create(self, *, mode: str, title: str, before_publish: Any) -> SimpleNamespace:
            created = SimpleNamespace(id="s-disconnected", status="active")
            before_publish(created)
            assert app._broadcaster is not None
            app._broadcaster.disconnect(writer)
            return created

    app._sessions = cast(Any, _Sessions())
    monkeypatch.setattr(app_module, "get_connection_writer", lambda: writer)

    with pytest.raises(HandlerError) as error:
        await app._agent_run_handler({"goal": "do not start"})

    assert error.value.code == SESSION_NOT_FOUND
    send_message.assert_not_called()
    assert app._running_runs == set()
    assert app._run_tasks_by_session == {}


# 功能：验证安全 run_id 可作为只读历史 capability，但不会绑定 live ownership
# 设计：同一 JSONL 含 s1 run 与权限事件，s2 持有精确 run_id 时可回放，仍不拥有 s1
async def test_replay_is_read_only_capability_without_live_ownership(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run_id = "20260820-120000-0123456789abcdef0123456789abcdef"
    run_root = tmp_path / "runs"
    event_path = run_root / run_id / "events.jsonl"
    event_path.parent.mkdir(parents=True)
    event_path.write_text(
        "\n".join(
            [
                json.dumps(
                    {
                        "type": "run.started",
                        "run_id": run_id,
                        "session_id": "s1",
                        "goal": "test",
                        "ts": "2026-01-01T00:00:00Z",
                    }
                ),
                json.dumps(
                    {
                        "type": "permission.requested",
                        "run_id": run_id,
                        "tool_use_id": "tool-s1",
                        "tool_name": "bash",
                        "params": {"command": "echo safe"},
                        "param_preview": "echo safe",
                        "session_id": "s1",
                        "ts": "2026-01-01T00:00:00Z",
                    }
                ),
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    app = CoreApp()
    app._runs_root = run_root
    app._sessions_root = tmp_path / "sessions"
    app._broadcaster = IpcEventBroadcaster()
    other = _make_writer()
    app._broadcaster.bind_session(other, "s2")

    replayed = await app._replay_events(
        run_id,
        other,
        ["run.*", "permission.*"],
        "global",
    )

    assert replayed == 2
    assert other.write.call_count == 2  # type: ignore[attr-defined]
    assert app._broadcaster.owns_session(other, "s1") is False


@pytest.mark.asyncio
async def test_subscribe_replay_handoff_has_no_duplicate_or_live_gap(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run_id = "20260820-120000-0123456789abcdef0123456789abcdef"
    run_root = tmp_path / "runs"
    event_path = run_root / run_id / "events.jsonl"
    event_path.parent.mkdir(parents=True)
    event_path.write_text(
        json.dumps(_sequenced_run_started(run_id, 1).model_dump()) + "\n",
        encoding="utf-8",
    )

    app = CoreApp()
    app._runs_root = run_root
    app._sessions_root = tmp_path / "sessions"
    app._broadcaster = IpcEventBroadcaster()
    writer = _make_writer()
    app._broadcaster.bind_session(writer, "s1")
    monkeypatch.setattr(app_module, "get_connection_writer", lambda: writer)

    drain_calls = 0

    async def publish_during_replay_drain() -> None:
        nonlocal drain_calls
        drain_calls += 1
        if drain_calls == 1:
            assert app._broadcaster is not None
            await app._broadcaster.handle(_sequenced_run_started(run_id, 1))
            await app._broadcaster.handle(_sequenced_run_started(run_id, 2))

    writer.drain = AsyncMock(side_effect=publish_during_replay_drain)  # type: ignore[method-assign]

    result = await app._subscribe_handler(
        {
            "type": "event.subscribe",
            "topics": ["run.*"],
            "scope": "global",
            "replay_from_run": run_id,
            "after_event_seq": 0,
        }
    )
    await app._broadcaster.handle(_sequenced_run_started(run_id, 3))

    delivered = [
        json.loads(call.args[0].rstrip(b"\n"))["event"]["event_seq"]
        for call in writer.write.call_args_list  # type: ignore[attr-defined]
    ]
    assert result.replayed_count == 1
    assert delivered == [1, 2, 3]


@pytest.mark.asyncio
async def test_subscribe_replay_limit_fails_before_live_activation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run_id = "20260820-120000-00112233445566778899aabbccddeeff"
    run_root = tmp_path / "runs"
    event_path = run_root / run_id / "events.jsonl"
    event_path.parent.mkdir(parents=True)
    event_path.write_text(
        "\n".join(
            json.dumps(_sequenced_run_started(run_id, event_seq).model_dump())
            for event_seq in (1, 2)
        )
        + "\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(app_module, "_MAX_REPLAY_EVENTS", 1)

    app = CoreApp()
    app._runs_root = run_root
    app._sessions_root = tmp_path / "sessions"
    app._broadcaster = IpcEventBroadcaster()
    writer = _make_writer()
    app._broadcaster.bind_session(writer, "s1")
    monkeypatch.setattr(app_module, "get_connection_writer", lambda: writer)

    with pytest.raises(HandlerError) as error:
        await app._subscribe_handler(
            {
                "type": "event.subscribe",
                "topics": ["run.*"],
                "scope": "global",
                "replay_from_run": run_id,
                "after_event_seq": 0,
            }
        )

    assert error.value.code == EVENT_LOG_ERROR
    assert error.value.data == {"code": "replay_limit_exceeded"}
    writer.write.assert_not_called()  # type: ignore[attr-defined]
    await app._broadcaster.handle(_sequenced_run_started(run_id, 3))
    writer.write.assert_not_called()  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_subscribe_replay_slow_drain_disconnects_within_bound(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run_id = "20260820-120000-fedcba9876543210fedcba9876543210"
    run_root = tmp_path / "runs"
    event_path = run_root / run_id / "events.jsonl"
    event_path.parent.mkdir(parents=True)
    event_path.write_text(
        json.dumps(_sequenced_run_started(run_id, 1).model_dump()) + "\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(broadcaster_module, "_DRAIN_TIMEOUT_S", 0.01)

    app = CoreApp()
    app._runs_root = run_root
    app._sessions_root = tmp_path / "sessions"
    app._broadcaster = IpcEventBroadcaster()
    writer = _make_writer()
    app._broadcaster.bind_session(writer, "s1")
    monkeypatch.setattr(app_module, "get_connection_writer", lambda: writer)
    never_release = asyncio.Event()

    async def blocked_drain() -> None:
        await never_release.wait()

    writer.drain = AsyncMock(side_effect=blocked_drain)  # type: ignore[method-assign]
    result = await asyncio.wait_for(
        app._subscribe_handler(
            {
                "type": "event.subscribe",
                "topics": ["run.*"],
                "scope": "global",
                "replay_from_run": run_id,
                "after_event_seq": 0,
            }
        ),
        timeout=0.2,
    )

    assert result.replayed_count == 1
    assert app._broadcaster.session_ids_for(writer) == frozenset()
    assert not app._broadcaster.can_receive(
        writer,
        {"type": "run.started", "run_id": run_id, "session_id": "s1"},
    )


@pytest.mark.asyncio
async def test_replay_accepts_legacy_event_without_optional_metadata(
    tmp_path: Path,
) -> None:
    run_id = "20260819-120000-a1b2c3"
    sessions_root = tmp_path / "sessions"
    run_root = sessions_root / "s1" / "runs"
    event_path = run_root / run_id / "events.jsonl"
    event_path.parent.mkdir(parents=True)
    event_path.write_text(
        json.dumps(
            {
                "type": "run.started",
                "run_id": run_id,
                "goal": "legacy payload",
                "ts": "2026-01-01T00:00:00Z",
            }
        )
        + "\n",
        encoding="utf-8",
    )
    app = CoreApp()
    app._runs_root = tmp_path / "runs"
    app._sessions_root = sessions_root
    app._broadcaster = IpcEventBroadcaster()
    reader = _make_writer()
    app._broadcaster.bind_session(reader, "s1")

    replayed = await app._replay_events(run_id, reader, ["run.*"], f"run:{run_id}")

    assert replayed == 1
    payload = json.loads(reader.write.call_args.args[0])  # type: ignore[attr-defined]
    assert payload["event"]["run_id"] == run_id
    assert "session_id" not in payload["event"]


async def test_legacy_weak_run_id_cannot_replay_across_session_owners(
    tmp_path: Path,
) -> None:
    run_id = "20260819-120000-a1b2c3"
    sessions_root = tmp_path / "sessions"
    event_path = sessions_root / "s1" / "runs" / run_id / "events.jsonl"
    event_path.parent.mkdir(parents=True)
    event_path.write_text(
        json.dumps({"type": "run.started", "run_id": run_id, "goal": "private"}),
        encoding="utf-8",
    )
    app = CoreApp()
    app._sessions_root = sessions_root
    app._runs_root = tmp_path / "runs"
    app._broadcaster = IpcEventBroadcaster()
    other = _make_writer()
    app._broadcaster.bind_session(other, "s2")

    replayed = await app._replay_events(run_id, other, ["run.*"], "global")

    assert replayed == 0
    other.write.assert_not_called()  # type: ignore[attr-defined]


async def test_session_created_is_bound_before_publish_and_hidden_from_other(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = CoreApp()
    app._broadcaster = IpcEventBroadcaster()
    app._bus = EventBus()
    app._bus.subscribe(app._broadcaster.handle)
    app._sessions = SessionManager(
        SessionStore(tmp_path / "sessions"),
        lambda: cast(Any, None),
        app._bus,
    )
    owner = _make_writer()
    other = _make_writer()
    app._broadcaster.subscribe(owner, ["session.*"])
    app._broadcaster.subscribe(other, ["session.*"])
    monkeypatch.setattr(app_module, "get_connection_writer", lambda: owner)

    result = await asyncio.wait_for(
        app._session_create_handler({"mode": "chat", "title": "private"}),
        timeout=1.0,
    )

    assert app._broadcaster.owns_session(owner, result.session_id)
    owner.write.assert_called_once()  # type: ignore[attr-defined]
    other.write.assert_not_called()  # type: ignore[attr-defined]
    pushed = json.loads(owner.write.call_args.args[0])  # type: ignore[attr-defined]
    assert pushed["event"]["type"] == "session.created"
    assert pushed["event"]["session_id"] == result.session_id


@pytest.mark.parametrize(
    ("handler_name", "params"),
    [
        ("_session_send_handler", {"session_id": "s1", "content": "secret"}),
        ("_session_history_handler", {"session_id": "s1"}),
        ("_session_close_handler", {"session_id": "s1"}),
        ("_session_compact_handler", {"session_id": "s1", "focus": "secret"}),
    ],
)
async def test_session_commands_hide_unowned_and_missing_sessions(
    handler_name: str,
    params: dict[str, Any],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _Sessions:
        def __getattr__(self, name: str) -> Any:
            raise AssertionError(f"manager operation must not be called: {name}")

    app = CoreApp()
    app._sessions = cast(Any, _Sessions())
    app._broadcaster = IpcEventBroadcaster()
    owner = _make_writer()
    attacker = _make_writer()
    app._broadcaster.bind_session(owner, "s1")
    app._broadcaster.bind_session(attacker, "s2")
    monkeypatch.setattr(app_module, "get_connection_writer", lambda: attacker)

    handler = getattr(app, handler_name)
    with pytest.raises(HandlerError) as unauthorized:
        await handler(params)

    params["session_id"] = "missing"
    with pytest.raises(HandlerError) as missing:
        await handler(params)

    assert unauthorized.value.code == missing.value.code == SESSION_NOT_FOUND
    assert str(unauthorized.value) == str(missing.value) == "session not found"


@pytest.mark.parametrize("run_id", ["../secret", "..\\secret", "C:\\secret", "*.jsonl"])
async def test_replay_rejects_unsafe_run_ids(
    run_id: str,
    tmp_path: Path,
) -> None:
    app = CoreApp()
    app._runs_root = tmp_path
    app._sessions_root = tmp_path / "sessions"
    app._broadcaster = IpcEventBroadcaster()
    writer = _make_writer()

    replayed = await app._replay_events(run_id, writer, ["*"], "global")

    assert replayed == 0
    writer.write.assert_not_called()  # type: ignore[attr-defined]


async def test_replay_requires_exact_run_id_and_applies_limits(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run_id = "20260820-120000-abcdef0123456789abcdef0123456789"
    session_id = "s1"
    sessions_root = tmp_path / "sessions"
    event_path = sessions_root / session_id / "runs" / run_id / "events.jsonl"
    event_path.parent.mkdir(parents=True)
    oversized = json.dumps({"type": "run.started", "run_id": run_id, "padding": "x" * 200})
    event_path.write_text(
        "\n".join(
            [
                oversized,
                json.dumps(
                    {
                        "type": "run.started",
                        "run_id": "different-run",
                        "session_id": session_id,
                    }
                ),
                json.dumps(
                    {
                        "type": "run.started",
                        "run_id": run_id,
                        "session_id": session_id,
                    }
                ),
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(app_module, "_MAX_REPLAY_LINE_BYTES", 150)
    app = CoreApp()
    app._sessions_root = sessions_root
    app._runs_root = tmp_path / "global-runs"
    app._broadcaster = IpcEventBroadcaster()
    owner = _make_writer()
    other = _make_writer()
    app._broadcaster.bind_session(owner, session_id)

    with pytest.raises(HandlerError) as owner_error:
        await app._replay_events(run_id, owner, ["run.*"], f"run:{run_id}")
    with pytest.raises(HandlerError) as other_error:
        await app._replay_events(run_id, other, ["run.*"], f"run:{run_id}")

    assert owner_error.value.code == other_error.value.code == EVENT_LOG_ERROR
    assert owner_error.value.data == other_error.value.data == {"code": "replay_limit_exceeded"}
    owner.write.assert_not_called()  # type: ignore[attr-defined]
    other.write.assert_not_called()  # type: ignore[attr-defined]
    assert app._broadcaster.owns_session(other, session_id) is False
