"""M0 governance and identity cases; added but not executed for this delivery."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from unittest.mock import AsyncMock

import pytest
from pydantic import BaseModel

from agent_runtime.core.events.bus import EventBus
from agent_runtime.core.graph.recovery import RecoveryStore, hash_tool_input
from agent_runtime.core.llm.types import ToolCallBlock
from agent_runtime.core.permissions.manager import PermissionManager
from agent_runtime.core.permissions.policy import PermissionDecision, ToolPolicy
from agent_runtime.core.sandbox import (
    ExecRequest,
    ExecResult,
    LocalSandboxBackend,
    LocalSandboxRuntime,
    SandboxHandle,
    SandboxKey,
    SandboxManager,
    SandboxSpec,
)
from agent_runtime.core.tools import invocation as invocation_module
from agent_runtime.core.tools.base import ToolResult
from agent_runtime.core.tools.builtin.bash import BashTool
from agent_runtime.core.tools.builtin.write_file import WriteFileTool
from agent_runtime.core.tools.invocation import ToolOutcomeUnknownError, invoke_tool
from agent_runtime.core.tools.registry import ToolRegistry


class CountingBackend(LocalSandboxBackend):
    def __init__(self) -> None:
        super().__init__()
        self.ensures = 0

    async def ensure(self, key: SandboxKey, spec: SandboxSpec) -> SandboxHandle:
        self.ensures += 1
        return await super().ensure(key, spec)


class RecordingRuntime(LocalSandboxRuntime):
    def __init__(self, key: SandboxKey, *, failures: int = 0, raises: bool = False) -> None:
        super().__init__(key)
        self.requests: list[ExecRequest] = []
        self.failures = failures
        self.raises = raises

    async def exec(self, request: ExecRequest) -> ExecResult:
        self.requests.append(request)
        if len(self.requests) <= self.failures:
            if self.raises:
                raise RuntimeError("transport failed")
            return ExecResult(content="[exit 9]\n", is_error=True, error_type="runtime_error")
        return ExecResult(content="ok", exit_code=0)


def write_registry(manager: SandboxManager, key: SandboxKey) -> ToolRegistry:
    registry = ToolRegistry()
    registry.register(WriteFileTool(manager.runtime_for(key), sandbox_key=key))
    return registry


def bash_registry(manager: SandboxManager, key: SandboxKey) -> ToolRegistry:
    registry = ToolRegistry()
    registry.register(BashTool(manager.runtime_for(key), sandbox_key=key))
    return registry


def collect_events(bus: EventBus) -> list[str]:
    events: list[str] = []

    async def collect(event: BaseModel) -> None:
        events.append(str(event.model_dump()["type"]))

    bus.subscribe(collect)
    return events


async def test_schema_error_and_permission_denial_never_ensure() -> None:
    backend = CountingBackend()
    manager = SandboxManager(backend=backend)
    key = SandboxKey("session", "session-1")
    registry = write_registry(manager, key)
    assert backend.ensures == 0
    bus = EventBus()
    events = collect_events(bus)
    permissions = PermissionManager({"write_file": ToolPolicy(PermissionDecision.DENY)})
    try:
        invalid = await invoke_tool(
            registry,
            ToolCallBlock(id="invalid", name="write_file", input={"path": "marker"}),
            bus,
            run_id="root",
            session_id=key.id,
            permission_manager=permissions,
        )
        denied = await invoke_tool(
            registry,
            ToolCallBlock(
                id="denied", name="write_file", input={"path": "marker", "content": "unused"}
            ),
            bus,
            run_id="root",
            session_id=key.id,
            permission_manager=permissions,
        )
        assert invalid.error_type == "schema_error"
        assert denied.error_type == "permission_denied"
        assert backend.ensures == 0
        assert events == ["tool.call_started", "tool.call_failed"] * 2
    finally:
        await manager.close()
    assert backend.ensures == 0


async def test_permission_wait_creates_only_after_approval(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    backend = CountingBackend()
    manager = SandboxManager(backend=backend)
    key = SandboxKey("session", "session-1")
    registry = write_registry(manager, key)
    bus = EventBus()
    events = collect_events(bus)
    requested = asyncio.Event()

    async def on_event(event: BaseModel) -> None:
        if event.model_dump()["type"] == "permission.requested":
            requested.set()

    bus.subscribe(on_event)
    permissions = PermissionManager(timeout_s=0)
    task = asyncio.create_task(
        invoke_tool(
            registry,
            ToolCallBlock(id="write", name="write_file", input={"path": "marker", "content": "ok"}),
            bus,
            run_id="child-run",
            session_id=key.id,
            permission_manager=permissions,
        )
    )
    try:
        await asyncio.wait_for(requested.wait(), timeout=1)
        assert backend.ensures == 0
        assert not (tmp_path / "marker").exists()
        assert permissions.respond(
            "write",
            "allow_once",
            authorized_session_ids={key.id},
            session_id=key.id,
            run_id="child-run",
        )
        result = await asyncio.wait_for(task, timeout=1)
        assert result == ToolResult("wrote 2 bytes to marker")
        assert backend.ensures == 1
        assert events == [
            "tool.call_started",
            "permission.requested",
            "permission.granted",
            "tool.call_finished",
        ]
    finally:
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await manager.close()
    assert (tmp_path / "marker").read_text() == "ok"


@pytest.mark.parametrize("claim_race", [False, True])
async def test_completed_journal_reuse_never_ensures(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, claim_race: bool
) -> None:
    backend = CountingBackend()
    manager = SandboxManager(backend=backend)
    key = SandboxKey("session", "session-1")
    registry = write_registry(manager, key)
    store = RecoveryStore(tmp_path / "recovery.sqlite")
    call = ToolCallBlock(
        id="already-done", name="write_file", input={"path": "marker", "content": "do not repeat"}
    )
    identity = {
        "session_id": key.id,
        "run_id": "root",
        "tool_use_id": call.id,
        "tool_name": call.name,
        "input_hash": hash_tool_input(call.input),
    }
    await store.claim_tool(**identity)
    await store.complete_tool(
        **identity,
        serialized_result=json.dumps({"content": "cached", "is_error": False, "error_type": None}),
    )
    if claim_race:
        monkeypatch.setattr(store, "get_tool", AsyncMock(return_value=None))
    bus = EventBus()
    events = collect_events(bus)
    try:
        result = await invoke_tool(
            registry,
            call,
            bus,
            run_id="root",
            session_id=key.id,
            recovery_store=store,
            durable_permission=lambda: (True, "auto_allow"),
        )
        assert result == ToolResult("cached")
        assert backend.ensures == 0
        assert events == []
    finally:
        await manager.close()


async def test_non_durable_retry_keeps_key_and_call_identity_with_distinct_attempts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(invocation_module, "_RETRY_BASE_S", 0.0)
    key = SandboxKey("direct_run", "parent-run")
    backend = CountingBackend()
    runtime = RecordingRuntime(key, failures=2)
    manager = SandboxManager(backend=backend, runtime_factory=lambda handle: runtime)
    registry = bash_registry(manager, key)
    bus = EventBus()
    events = collect_events(bus)
    try:
        result = await invoke_tool(
            registry,
            ToolCallBlock(id="same-call", name="bash", input={"command": "unused", "run_id": "fake"}),
            bus,
            run_id="child-run",
        )
        assert result == ToolResult("ok")
        assert backend.ensures == 1
        assert [request.context.attempt for request in runtime.requests] == [1, 2, 3]
        assert {request.context.run_id for request in runtime.requests} == {"child-run"}
        assert {request.context.tool_call_id for request in runtime.requests} == {"same-call"}
        assert {request.context.key for request in runtime.requests} == {key}
        assert events == [
            "tool.call_started",
            "tool.call_failed",
            "tool.call_failed",
            "tool.call_finished",
        ]
    finally:
        await manager.close()


@pytest.mark.parametrize("raises", [False, True])
async def test_durable_error_does_not_retry_and_preserves_unknown_boundary(
    tmp_path: Path, raises: bool
) -> None:
    key = SandboxKey("session", "durable-session")
    runtime = RecordingRuntime(key, failures=3, raises=raises)
    manager = SandboxManager(runtime_factory=lambda handle: runtime)
    registry = bash_registry(manager, key)
    store = RecoveryStore(tmp_path / "recovery.sqlite")
    call = ToolCallBlock(id="call", name="bash", input={"command": "unused"})
    bus = EventBus()
    events = collect_events(bus)
    try:
        if raises:
            with pytest.raises(ToolOutcomeUnknownError):
                await invoke_tool(
                    registry, call, bus, run_id="root", session_id=key.id, recovery_store=store
                )
        else:
            result = await invoke_tool(
                registry, call, bus, run_id="root", session_id=key.id, recovery_store=store
            )
            assert result == ToolResult("[exit 9]\n", True, "runtime_error")
        record = await store.get_tool(key.id, "root", call.id)
        assert record is not None
        assert record.status == ("outcome_unknown" if raises else "completed")
        assert len(runtime.requests) == 1
        assert runtime.requests[0].context.attempt == 1
        assert events == ["tool.call_started", "tool.call_failed"]
    finally:
        await manager.close()


@pytest.mark.parametrize("failure_site", ["journal", "event_sink"])
async def test_result_persistence_and_event_failures_do_not_repeat_runtime_operation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failure_site: str
) -> None:
    key = SandboxKey("session", "session-1")
    runtime = RecordingRuntime(key)
    manager = SandboxManager(runtime_factory=lambda handle: runtime)
    registry = bash_registry(manager, key)
    store = RecoveryStore(tmp_path / "recovery.sqlite")
    bus = EventBus()
    if failure_site == "journal":
        monkeypatch.setattr(store, "complete_tool", AsyncMock(side_effect=RuntimeError("sink failed")))
    else:

        async def failing_sink(event: BaseModel) -> None:
            if event.model_dump()["type"] == "tool.call_finished":
                raise RuntimeError("sink failed")

        bus.subscribe(failing_sink)
    try:
        with pytest.raises(RuntimeError, match="sink failed"):
            await invoke_tool(
                registry,
                ToolCallBlock(id="call", name="bash", input={"command": "unused"}),
                bus,
                run_id="root",
                session_id=key.id,
                recovery_store=store,
            )
        assert len(runtime.requests) == 1
        record = await store.get_tool(key.id, "root", "call")
        assert record is not None
        assert record.status == ("started" if failure_site == "journal" else "completed")
    finally:
        await manager.close()


async def test_root_and_children_sharing_a_runtime_keep_independent_run_identity() -> None:
    key = SandboxKey("direct_run", "root-run")
    runtime = RecordingRuntime(key)
    backend = CountingBackend()
    manager = SandboxManager(backend=backend, runtime_factory=lambda handle: runtime)
    registry = bash_registry(manager, key)
    try:
        await asyncio.gather(
            *(
                invoke_tool(
                    registry,
                    ToolCallBlock(
                        id="shared-model-call-id", name="bash", input={"command": "unused"}
                    ),
                    EventBus(),
                    run_id=run_id,
                )
                for run_id in ("root-run", "foreground-child", "background-child", "nested-child")
            )
        )
        assert backend.ensures == 1
        assert {request.context.key for request in runtime.requests} == {key}
        assert {request.context.run_id for request in runtime.requests} == {
            "root-run", "foreground-child", "background-child", "nested-child"
        }
        assert len(runtime.requests) == 4
    finally:
        await manager.close()
