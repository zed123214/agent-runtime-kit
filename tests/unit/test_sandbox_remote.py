"""Remote retry boundary and deadline regressions. No execution in this delivery."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any

import pytest

from agent_runtime.core.events.bus import EventBus
from agent_runtime.core.events.writer import EventWriter
from agent_runtime.core.llm.types import ToolCallBlock
from agent_runtime.core.permissions.manager import PermissionManager
from agent_runtime.core.permissions.policy import PermissionDecision, ToolPolicy
from agent_runtime.core.sandbox import (
    ExecRequest,
    ExecResult,
    ReadRequest,
    ReconcileReport,
    SandboxCallContext,
    SandboxHandle,
    SandboxKey,
    SandboxManager,
    SandboxSpec,
)
from agent_runtime.core.sandbox.models import (
    SandboxOutcomeUnknownError,
    SandboxProvisionError,
    SandboxQueueTimeout,
    WorkspaceLostError,
)
from agent_runtime.core.tools.builtin.bash import BashTool
from agent_runtime.core.tools.invocation import invoke_tool
from agent_runtime.core.tools.registry import ToolRegistry


class Backend:
    def __init__(self) -> None:
        self.created = 0
        self.destroyed = 0
        self.calls = 0
        self.started = asyncio.Event()
        self.gate = asyncio.Event()
        self.gate.set()
        self.unknown = False

    async def ensure(self, key: SandboxKey, spec: SandboxSpec) -> SandboxHandle:
        self.created += 1
        return SandboxHandle(key, "b" * 32, backend="kubernetes")

    async def status(self, handle: Any) -> Any:
        return handle.status

    async def destroy(self, handle: Any, reason: str) -> None:
        self.destroyed += 1

    async def reconcile(self) -> ReconcileReport:
        return ReconcileReport()

    def runtime_for(self, handle: Any) -> Any:
        return self

    async def exec(self, request: ExecRequest) -> ExecResult:
        self.calls += 1
        self.started.set()
        await self.gate.wait()
        if self.unknown:
            raise SandboxOutcomeUnknownError("outcome_unknown")
        return ExecResult(content="exit 9", is_error=True, error_type="runtime_error", exit_code=9)


def setup(backend: Backend, **kwargs: Any) -> tuple[SandboxManager, ToolRegistry, SandboxKey]:
    manager = SandboxManager(backend, spec=SandboxSpec(backend="kubernetes"), **kwargs)
    key = SandboxKey("session", "remote-session")
    registry = ToolRegistry()
    registry.register(BashTool(manager.runtime_for(key), sandbox_key=key))
    return manager, registry, key


@pytest.mark.parametrize("unknown", [False, True])
async def test_remote_failure_never_replays_nonzero_or_unknown(unknown: bool) -> None:
    backend = Backend()
    backend.unknown = unknown
    manager, registry, key = setup(backend)
    try:
        result = await invoke_tool(
            registry,
            ToolCallBlock(id="side-effect", name="bash", input={"command": "x"}),
            EventBus(),
            "root",
            session_id=key.id,
        )
        assert result.is_error
        assert backend.calls == 1
        if unknown:
            assert result.error_type == "outcome_unknown"
            with pytest.raises(WorkspaceLostError):
                manager.runtime_for(key)
    finally:
        await manager.close()


async def test_remote_uses_its_own_deadlines_not_local_outer_timeout() -> None:
    backend = Backend()
    backend.gate.clear()
    manager, registry, key = setup(backend)
    task = asyncio.create_task(
        invoke_tool(
            registry,
            ToolCallBlock(id="cold", name="bash", input={"command": "x"}),
            EventBus(),
            "root",
            timeout=0.001,
            session_id=key.id,
        )
    )
    try:
        await backend.started.wait()
        await asyncio.sleep(0.02)
        assert not task.done()
        backend.gate.set()
        await task
    finally:
        await manager.close()


async def test_creation_error_is_preserved_while_partial_cleanup_is_joined() -> None:
    class FailedBackend(Backend):
        cleaned = 0

        async def ensure(self, key: SandboxKey, spec: SandboxSpec) -> SandboxHandle:
            self.created += 1
            raise SandboxProvisionError("creation rejected")

        async def cleanup_key(self, key: SandboxKey) -> None:
            self.cleaned += 1

    backend = FailedBackend()
    manager, registry, key = setup(backend)
    try:
        result = await invoke_tool(
            registry,
            ToolCallBlock(id="create", name="bash", input={"command": "x"}),
            EventBus(),
            "root",
            session_id=key.id,
        )
        assert result.is_error and result.error_type == "provision_failed"
        assert backend.created == 1 and backend.calls == 0 and backend.cleaned == 1
        with pytest.raises(WorkspaceLostError):
            manager.runtime_for(key)
    finally:
        await manager.close()


async def test_permission_denial_and_bad_params_do_not_provision() -> None:
    backend = Backend()
    manager, registry, key = setup(backend)
    permissions = PermissionManager({"bash": ToolPolicy(PermissionDecision.DENY)})
    try:
        for params in ({}, {"command": "forbidden"}):
            result = await invoke_tool(
                registry,
                ToolCallBlock(id=str(params), name="bash", input=params),
                EventBus(),
                "root",
                session_id=key.id,
                permission_manager=permissions,
            )
            assert result.is_error
        assert backend.created == backend.calls == 0
    finally:
        await manager.close()


async def test_idle_ttl_does_not_reap_busy_or_queued_and_never_revives(tmp_path: Path) -> None:
    backend = Backend()
    backend.gate.clear()
    manager, _, key = setup(
        backend,
        idle_timeout_s=900,
        queue_timeout_s=0.01,
        lifecycle_path=tmp_path / "lifecycle.jsonl",
    )
    runtime = manager.runtime_for(key)
    first = asyncio.create_task(
        runtime.exec(ExecRequest(SandboxCallContext(key, "root", "first"), "x"))
    )
    try:
        await backend.started.wait()
        manager._entries[key].last_completed -= 901
        second = asyncio.create_task(
            runtime.exec(ExecRequest(SandboxCallContext(key, "child", "queued"), "x"))
        )
        await asyncio.sleep(0)
        await manager.reap_idle()
        assert backend.destroyed == 0
        with pytest.raises(SandboxQueueTimeout):
            await second
        backend.gate.set()
        await first
        manager._entries[key].last_completed -= 901
        await manager.reap_idle()
        assert backend.destroyed == 1
        with pytest.raises(WorkspaceLostError):
            await runtime.read_text(ReadRequest(SandboxCallContext(key, "later", "read"), "."))
        records = [
            json.loads(line) for line in (tmp_path / "lifecycle.jsonl").read_text().splitlines()
        ]
        reclaimed = [item for item in records if item.get("terminal_reason") == "idle_ttl"]
        assert reclaimed and all(item["run_id"] is None for item in reclaimed)
    finally:
        await manager.close()


async def test_child_creation_events_reach_only_child_writer(tmp_path: Path) -> None:
    backend = Backend()
    manager, _, key = setup(backend)
    parent, child = EventBus(correlation_id="parent"), EventBus(correlation_id="parent")
    try:
        async with EventWriter(tmp_path / "parent.jsonl", run_id="parent") as pw:
            async with EventWriter(tmp_path / "child.jsonl", run_id="child") as cw:
                pw.subscribe(parent)
                cw.subscribe(child)
                child.subscribe(parent.publish)
                runtime = manager.runtime_for(key)
                await runtime.exec(
                    ExecRequest(
                        SandboxCallContext(key, "child", "tool", event_sink=child.publish), "x"
                    )
                )
        assert (tmp_path / "parent.jsonl").read_text() == ""
        rows = [json.loads(line) for line in (tmp_path / "child.jsonl").read_text().splitlines()]
        assert [row["type"] for row in rows] == ["sandbox.creating", "sandbox.ready"]
        assert all(row["run_id"] == "child" for row in rows)
    finally:
        await manager.close()
