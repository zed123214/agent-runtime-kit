"""M0 ownership and lifecycle regression cases; added but not executed.

These cases use deterministic providers and Local lifecycle handles. They do
not establish physical isolation or Kubernetes behavior.
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest

import agent_runtime.core.app as app_module
from agent_runtime.core.app import CoreApp
from agent_runtime.core.bus.envelope import HandlerError
from agent_runtime.core.config import RuntimeConfig
from agent_runtime.core.context import ExecutionContext
from agent_runtime.core.engine.base import EngineRunConfig, RunOutcome, RunSuspension
from agent_runtime.core.engine.router import EngineRouter
from agent_runtime.core.events.bus import EventBus
from agent_runtime.core.llm.types import LlmResponse, ToolCallBlock
from agent_runtime.core.runner import AgentRunner
from agent_runtime.core.sandbox import (
    ExecRequest,
    ExecResult,
    FileResult,
    ListRequest,
    ListResult,
    LocalSandboxBackend,
    ReadRequest,
    SandboxClosedError,
    SandboxHandle,
    SandboxKey,
    SandboxManager,
    SandboxSpec,
    WriteRequest,
)
from agent_runtime.core.session.manager import SESSION_CLOSED, SessionManager
from agent_runtime.core.session.model import Session
from agent_runtime.core.session.store import SessionStore
from agent_runtime.core.tools.builtin.read_file import ReadFileTool
from agent_runtime.core.tools.registry import ToolRegistry


class _Backend(LocalSandboxBackend):
    def __init__(self) -> None:
        super().__init__()
        self.ensured: list[SandboxKey] = []
        self.destroyed: list[SandboxKey] = []
        self.destroy_started = asyncio.Event()
        self.destroy_gate: asyncio.Event | None = None

    async def ensure(self, key: SandboxKey, spec: SandboxSpec) -> SandboxHandle:
        self.ensured.append(key)
        return await super().ensure(key, spec)

    async def destroy(self, handle: SandboxHandle, reason: str) -> None:
        self.destroyed.append(handle.key)
        self.destroy_started.set()
        if self.destroy_gate is not None:
            await self.destroy_gate.wait()
        await super().destroy(handle, reason)


class _Runtime:
    def __init__(self) -> None:
        self.reads: list[ReadRequest] = []
        self.read_started = asyncio.Event()
        self.read_gate: asyncio.Event | None = None

    async def read_text(self, request: ReadRequest) -> FileResult:
        self.reads.append(request)
        self.read_started.set()
        if self.read_gate is not None:
            await self.read_gate.wait()
        return FileResult(content="shared contents")

    async def exec(self, request: ExecRequest) -> ExecResult:
        return ExecResult(content="ok")

    async def write_text(self, request: WriteRequest) -> FileResult:
        return FileResult(content="ok")

    async def list_dir(self, request: ListRequest) -> ListResult:
        return ListResult(content=".")


class _ReadProvider:
    def __init__(self) -> None:
        self.calls: dict[str, int] = {}

    async def chat(
        self,
        messages: list[dict[str, Any]],
        tool_schemas: list[dict[str, Any]],
        bus: EventBus,
        run_id: str,
        **kwargs: Any,
    ) -> LlmResponse:
        count = self.calls.get(run_id, 0)
        self.calls[run_id] = count + 1
        if count == 0:
            return LlmResponse(
                stop_reason="tool_use",
                tool_calls=[ToolCallBlock(id="same-call-id", name="read_file", input={"path": "x"})],
            )
        return LlmResponse(stop_reason="end_turn", text="done")


class _NestedProvider(_ReadProvider):
    def __init__(self, backend: _Backend, *, background: bool) -> None:
        super().__init__()
        self.backend = backend
        self.background = background
        self.roles: dict[str, str] = {"root": "root"}
        self.child_finished = asyncio.Event()

    async def chat(
        self,
        messages: list[dict[str, Any]],
        tool_schemas: list[dict[str, Any]],
        bus: EventBus,
        run_id: str,
        **kwargs: Any,
    ) -> LlmResponse:
        if run_id not in self.roles:
            self.roles[run_id] = "child" if len(self.roles) == 1 else "nested"
        role = self.roles[run_id]
        count = self.calls.get(run_id, 0)
        self.calls[run_id] = count + 1
        if count == 0:
            calls = [ToolCallBlock(id="same-call-id", name="read_file", input={"path": role})]
            if role != "nested":
                calls.append(
                    ToolCallBlock(
                        id="spawn",
                        name="spawn_agent",
                        input={
                            "description": "shared child",
                            "prompt": "continue",
                            "run_in_background": self.background and role == "root",
                        },
                    )
                )
            return LlmResponse(stop_reason="tool_use", tool_calls=calls)
        if role == "root":
            await asyncio.wait_for(self.child_finished.wait(), timeout=2)
            assert self.backend.destroyed == []
        return LlmResponse(stop_reason="end_turn", text="done")


@pytest.mark.parametrize("background", [False, True])
async def test_root_foreground_background_and_nested_share_ownership(
    tmp_path: Path, background: bool
) -> None:
    backend = _Backend()
    runtime = _Runtime()
    manager = SandboxManager(backend, runtime_factory=lambda _handle: runtime)
    provider = _NestedProvider(backend, background=background)

    async def observe(event: Any) -> None:
        if event.type == "subagent.finished" and provider.roles.get(event.run_id) == "child":
            provider.child_finished.set()

    runner = AgentRunner(
        RuntimeConfig(),
        provider=provider,
        sandbox_manager=manager,
        runs_dir=tmp_path,
        extra_handlers=[observe],
    )
    outcome = await asyncio.wait_for(runner.run_and_capture("start", run_id="root"), timeout=5)

    assert isinstance(outcome, RunOutcome) and outcome.status == "success"
    key = SandboxKey("direct_run", "root")
    assert backend.ensured == [key]
    assert backend.destroyed == [key]
    assert {request.context.key for request in runtime.reads} == {key}
    assert {request.context.run_id for request in runtime.reads} == set(provider.roles)
    assert len(runtime.reads) == 3
    assert {request.context.tool_call_id for request in runtime.reads} == {"same-call-id"}
    assert {request.context.attempt for request in runtime.reads} == {1}


async def test_chat_turns_share_until_explicit_close(tmp_path: Path) -> None:
    backend = _Backend()
    runtime = _Runtime()
    manager = SandboxManager(backend, runtime_factory=lambda _handle: runtime)
    store = SessionStore(tmp_path)

    async def closed(sid: str) -> None:
        await manager.release(SandboxKey("session", sid), "session_closed")

    sessions = SessionManager(
        store,
        lambda: AgentRunner(RuntimeConfig(), provider=_ReadProvider(), sandbox_manager=manager),
        EventBus(),
        on_session_closed=closed,
    )
    session = await sessions.create("chat")
    key = SandboxKey("session", session.id)
    await sessions.send_message(session.id, "first", run_id="turn-one")
    await sessions.send_message(session.id, "second", run_id="turn-two")

    assert backend.ensured == [key]
    assert backend.destroyed == []
    assert {request.context.run_id for request in runtime.reads} == {"turn-one", "turn-two"}
    assert {request.context.session_id for request in runtime.reads} == {session.id}
    await sessions.close(session.id)
    await sessions.close(session.id)
    assert backend.destroyed == [key]


async def test_direct_runner_releases_only_its_own_key(tmp_path: Path) -> None:
    backend = _Backend()
    runtime = _Runtime()
    manager = SandboxManager(backend, runtime_factory=lambda _handle: runtime)
    other = SandboxKey("session", "someone-else")
    other_tool = ReadFileTool(manager.runtime_for(other), sandbox_key=other)
    await other_tool.invoke({"path": "other"})
    runner = AgentRunner(
        RuntimeConfig(), provider=_ReadProvider(), sandbox_manager=manager, runs_dir=tmp_path
    )
    await runner.run_and_capture("root", run_id="direct")
    assert backend.destroyed == [SandboxKey("direct_run", "direct")]
    assert not (await other_tool.invoke({"path": "still-live"})).is_error
    assert backend.ensured.count(other) == 1
    await manager.close()


async def test_early_runner_failure_closes_without_ensure(tmp_path: Path) -> None:
    backend = _Backend()
    manager = SandboxManager(backend)
    blocked_path = tmp_path / "not-a-directory"
    blocked_path.write_text("user marker", encoding="utf-8")
    runner = AgentRunner(RuntimeConfig(), sandbox_manager=manager, runs_dir=blocked_path)
    key = SandboxKey("direct_run", "early")
    facade = manager.runtime_for(key)

    with pytest.raises(OSError):
        await runner.run_and_capture("root", run_id="early")

    assert backend.ensured == backend.destroyed == []
    with pytest.raises(SandboxClosedError):
        await ReadFileTool(facade, sandbox_key=key).invoke({"path": "x"})
    assert backend.ensured == []
    assert blocked_path.read_text(encoding="utf-8") == "user marker"


async def test_repeated_runner_cancellation_finishes_release(tmp_path: Path) -> None:
    backend = _Backend()
    backend.destroy_gate = asyncio.Event()
    runtime = _Runtime()
    runtime.read_gate = asyncio.Event()
    manager = SandboxManager(backend, runtime_factory=lambda _handle: runtime)
    runner = AgentRunner(
        RuntimeConfig(), provider=_ReadProvider(), sandbox_manager=manager, runs_dir=tmp_path
    )
    task = asyncio.create_task(runner.run_and_capture("root", run_id="cancel-root"))
    await asyncio.wait_for(runtime.read_started.wait(), timeout=1)
    task.cancel()
    await asyncio.wait_for(backend.destroy_started.wait(), timeout=1)
    task.cancel()
    backend.destroy_gate.set()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, timeout=2)
    assert backend.destroyed == [SandboxKey("direct_run", "cancel-root")]


@pytest.mark.parametrize("mode", ["chat", "one_shot"])
async def test_session_close_blocks_turns_and_joins_active_run(tmp_path: Path, mode: str) -> None:
    backend = _Backend()
    runtime = _Runtime()
    runtime.read_gate = asyncio.Event()
    manager = SandboxManager(backend, runtime_factory=lambda _handle: runtime)
    store = SessionStore(tmp_path)
    run_task: asyncio.Task[str] | None = None

    async def closed(sid: str) -> None:
        assert run_task is not None and run_task.done()
        await manager.release(SandboxKey("session", sid), "closed")

    sessions = SessionManager(
        store,
        lambda: AgentRunner(RuntimeConfig(), provider=_ReadProvider(), sandbox_manager=manager),
        EventBus(),
        on_session_closed=closed,
    )
    session = await sessions.create(cast(Any, mode))
    run_task = asyncio.create_task(sessions.send_message(session.id, "work"))
    await asyncio.wait_for(runtime.read_started.wait(), timeout=1)
    sessions.begin_close(session.id)
    with pytest.raises(HandlerError) as rejected:
        await sessions.send_message(session.id, "late")
    assert rejected.value.code == SESSION_CLOSED
    await asyncio.wait_for(sessions.close(session.id), timeout=2)
    assert run_task.cancelled()
    assert backend.destroyed == [SandboxKey("session", session.id)]


async def test_one_shot_factory_failure_still_closes(tmp_path: Path) -> None:
    closed: list[str] = []

    def fail_factory() -> AgentRunner:
        raise RuntimeError("provider construction failed")

    async def on_closed(sid: str) -> None:
        closed.append(sid)

    sessions = SessionManager(
        SessionStore(tmp_path), fail_factory, EventBus(), on_session_closed=on_closed
    )
    session = await sessions.create("one_shot")
    with pytest.raises(RuntimeError, match="provider construction failed"):
        await sessions.send_message(session.id, "work")
    assert closed == [session.id]
    assert session.status == "closed"


@pytest.mark.parametrize("disconnected", [False, True])
async def test_session_metadata_failure_still_releases(
    tmp_path: Path, disconnected: bool
) -> None:
    class _BrokenStore(SessionStore):
        def write_meta(self, session: Session) -> None:
            if session.status == "closed":
                raise OSError("closed metadata unavailable")
            super().write_meta(session)

    backend = _Backend()
    runtime = _Runtime()
    manager = SandboxManager(backend, runtime_factory=lambda _handle: runtime)

    async def closed(sid: str) -> None:
        await manager.release(SandboxKey("session", sid))

    sessions = SessionManager(
        _BrokenStore(tmp_path),
        lambda: AgentRunner(RuntimeConfig(), sandbox_manager=manager),
        EventBus(),
        on_session_closed=closed,
    )
    session = await sessions.create("chat")
    key = SandboxKey("session", session.id)
    await ReadFileTool(manager.runtime_for(key), sandbox_key=key).invoke({"path": "x"})

    with pytest.raises(OSError, match="closed metadata unavailable"):
        if disconnected:
            await sessions.close_disconnected(session.id)
        else:
            await sessions.close(session.id)
    assert backend.destroyed == [key]


async def test_disconnect_attempts_every_session_when_closed_metadata_fails(tmp_path: Path) -> None:
    class _BrokenStore(SessionStore):
        def write_meta(self, session: Session) -> None:
            if session.status == "closed":
                raise OSError("closed metadata unavailable")
            super().write_meta(session)

    backend = _Backend()
    runtime = _Runtime()
    manager = SandboxManager(backend, runtime_factory=lambda _handle: runtime)
    app = CoreApp(sandbox_manager=manager)
    sessions = SessionManager(
        _BrokenStore(tmp_path),
        lambda: AgentRunner(RuntimeConfig(), sandbox_manager=manager),
        EventBus(),
        on_session_closed=app._delete_session_resources,
    )
    app._sessions = sessions
    first = await sessions.create("chat")
    second = await sessions.create("chat")
    keys = {SandboxKey("session", first.id), SandboxKey("session", second.id)}
    for key in keys:
        await ReadFileTool(manager.runtime_for(key), sandbox_key=key).invoke({"path": "x"})

    with pytest.raises(ExceptionGroup, match="session disconnect cleanup failed") as failed:
        await app._cleanup_disconnected_sessions(frozenset({first.id, second.id}))

    assert len(failed.value.exceptions) == 2
    assert all(isinstance(exc, OSError) for exc in failed.value.exceptions)
    assert set(backend.destroyed) == keys
    assert len(backend.destroyed) == 2
    await manager.close()


class _SuspendThenFinishEngine:
    name = "graph"

    async def run(
        self, context: ExecutionContext, *, tools: ToolRegistry,
        events: EventBus, config: EngineRunConfig,
    ) -> RunSuspension:
        context.status = "running"
        return RunSuspension(
            run_id=context.run_id, session_id=config.session_id, reason="permission",
            checkpoint_revision="revision", interrupt_id="permission", event_seq=0,
        )

    async def resume(
        self, context: ExecutionContext, *, tools: ToolRegistry,
        events: EventBus, config: EngineRunConfig,
    ) -> RunOutcome:
        context.result = "resumed"
        context.mark_success()
        return RunOutcome.from_context(context)


async def test_session_suspension_and_resume_keep_local_ownership(tmp_path: Path) -> None:
    backend = _Backend()
    runtime = _Runtime()
    manager = SandboxManager(backend, runtime_factory=lambda _handle: runtime)
    session = Session(
        id="durable-local", mode="chat", status="active", title="",
        created_at="now", updated_at="now", run_ids=[], durable=True,
    )
    store = SessionStore(tmp_path)
    store.write_meta(session)
    key = SandboxKey("session", session.id)
    tool = ReadFileTool(manager.runtime_for(key), sandbox_key=key)
    await tool.invoke({"path": "before"})
    runner = AgentRunner(
        RuntimeConfig(), provider=_ReadProvider(), sandbox_manager=manager,
        engine_resolver=lambda _name: lambda *_args, **_kwargs: _SuspendThenFinishEngine(),
    )
    suspended = await runner.run_and_capture("goal", run_id="durable-run", session=session, store=store)
    assert isinstance(suspended, RunSuspension)
    assert backend.destroyed == []
    resumed = await runner.run_and_capture(
        "goal", run_id="durable-run", session=session, store=store, resume=True,
        expected_checkpoint_revision="revision", resume_epoch=1,
    )
    assert isinstance(resumed, RunOutcome) and resumed.status == "success"
    assert not (await tool.invoke({"path": "after"})).is_error
    assert backend.ensured == [key]
    assert backend.destroyed == []
    await manager.release(key, "session_closed")


async def test_durable_disconnect_preserves_suspended_facade(tmp_path: Path) -> None:
    backend = _Backend()
    runtime = _Runtime()
    manager = SandboxManager(backend, runtime_factory=lambda _handle: runtime)
    app = CoreApp(sandbox_manager=manager)

    class _SuspendedIndex:
        async def latest_unfinished_run(self, sid: str) -> Any:
            return SimpleNamespace(status="suspended", run_id="suspended-run")

    sessions = SessionManager(
        SessionStore(tmp_path),
        lambda: AgentRunner(RuntimeConfig(), sandbox_manager=manager),
        EventBus(),
        on_session_closed=app._delete_session_resources,
        recovery_store=cast(Any, _SuspendedIndex()),
    )
    app._sessions = sessions
    session = await sessions.create("chat", durable=True)
    key = SandboxKey("session", session.id)
    facade = manager.runtime_for(key)
    await ReadFileTool(facade, sandbox_key=key).invoke({"path": "before"})

    await app._cleanup_disconnected_sessions(frozenset({session.id}))

    assert backend.destroyed == []
    assert sessions.hydrate(session.id).status == "suspended"
    assert manager.runtime_for(key) is facade
    assert not (await ReadFileTool(facade, sandbox_key=key).invoke({"path": "after"})).is_error
    assert backend.ensured == [key]
    await manager.close()


async def test_shutdown_joins_pending_session_resource_cleanup() -> None:
    started = asyncio.Event()
    finish = asyncio.Event()
    order: list[str] = []

    class _GraphRuntime:
        async def delete_thread(self, thread_id: str) -> None:
            started.set()
            await finish.wait()
            order.append("thread_deleted")

        async def close(self) -> None:
            assert "thread_deleted" in order
            order.append("graph_closed")

    class _Manager(SandboxManager):
        async def close(self) -> None:
            assert "thread_deleted" in order
            await super().close()
            order.append("sandbox_closed")

    app = CoreApp(EngineRouter(cast(Any, _GraphRuntime())), sandbox_manager=_Manager())
    release = asyncio.create_task(app._delete_session_resources("session"))
    await asyncio.wait_for(started.wait(), timeout=1)
    shutdown = asyncio.create_task(app._shutdown())
    await asyncio.sleep(0)
    assert not shutdown.done()
    finish.set()
    await asyncio.wait_for(asyncio.gather(release, shutdown), timeout=2)
    assert order == ["thread_deleted", "sandbox_closed", "graph_closed"]


def test_runner_rejects_programmatic_kubernetes_config() -> None:
    config = RuntimeConfig()
    config.sandbox.backend = "kubernetes"
    with pytest.raises(SystemExit, match="not implemented"):
        AgentRunner(config)


async def test_core_rejects_programmatic_kubernetes_before_listener(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = RuntimeConfig()
    config.sandbox.backend = "kubernetes"
    monkeypatch.setattr(app_module, "get_config", lambda: config)

    def forbidden_listener(*args: Any, **kwargs: Any) -> None:
        raise AssertionError("listener constructed before sandbox validation")

    monkeypatch.setattr(app_module, "SocketServer", forbidden_listener)
    with pytest.raises(SystemExit, match="not implemented"):
        await CoreApp().run()
