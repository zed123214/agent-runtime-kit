"""M0 review regression and M1 cleanup retries. Written, not executed."""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any, cast

import pytest

from agent_runtime.core.app import CoreApp
from agent_runtime.core.config import RuntimeConfig
from agent_runtime.core.events.bus import EventBus
from agent_runtime.core.graph.recovery import RecoveryStoreError
from agent_runtime.core.runner import AgentRunner
from agent_runtime.core.sandbox import (
    ReadRequest,
    SandboxCallContext,
    SandboxClosedError,
    SandboxKey,
    SandboxManager,
)
from agent_runtime.core.session.manager import SessionManager
from agent_runtime.core.session.store import SessionStore


@pytest.mark.parametrize("disconnected", [False, True])
async def test_transient_recovery_lookup_does_not_permanently_close_admission(
    tmp_path: Path,
    disconnected: bool,
) -> None:
    class Recovery:
        count = 0

        async def latest_unfinished_run(self, sid: str) -> None:
            self.count += 1
            if self.count == 1:
                raise RecoveryStoreError("temporarily unavailable")
            return None

    recovery = Recovery()
    released: list[str] = []

    async def release(sid: str) -> None:
        released.append(sid)

    sessions = SessionManager(
        SessionStore(tmp_path),
        lambda: AgentRunner(RuntimeConfig()),
        EventBus(),
        recovery_store=cast(Any, recovery),
        on_session_closed=release,
    )
    session = await sessions.create("chat", durable=True)
    with pytest.raises(RecoveryStoreError):
        if disconnected:
            await sessions.close_disconnected(session.id)
        else:
            await sessions.close(session.id)
    assert session.status != "closed"
    assert session.id not in sessions._closing
    assert released == []
    await sessions.close(session.id)
    assert recovery.count == 2
    assert session.status == "closed"
    assert released == [session.id]


async def test_concurrent_failed_close_is_one_attempt_and_can_be_retried(tmp_path: Path) -> None:
    started, finish = asyncio.Event(), asyncio.Event()

    class Recovery:
        count = 0

        async def latest_unfinished_run(self, sid: str) -> None:
            self.count += 1
            started.set()
            await finish.wait()
            if self.count == 1:
                raise RecoveryStoreError("one failure")

    recovery = Recovery()
    sessions = SessionManager(
        SessionStore(tmp_path),
        lambda: AgentRunner(RuntimeConfig()),
        EventBus(),
        recovery_store=cast(Any, recovery),
    )
    session = await sessions.create("chat", durable=True)
    first = asyncio.create_task(sessions.close(session.id))
    await started.wait()
    second = asyncio.create_task(sessions.close(session.id))
    await asyncio.sleep(0)
    finish.set()
    results = await asyncio.gather(first, second, return_exceptions=True)
    assert all(isinstance(item, RecoveryStoreError) for item in results)
    assert recovery.count == 1
    await sessions.close(session.id)
    assert recovery.count == 2


async def test_closed_session_cleanup_retry_never_revives_runtime(tmp_path: Path) -> None:
    from agent_runtime.core.sandbox.local import LocalSandboxBackend

    class Backend(LocalSandboxBackend):
        attempts = 0

        async def destroy(self, handle: Any, reason: str) -> None:
            self.attempts += 1
            if self.attempts == 1:
                raise OSError("temporary deletion error")
            await super().destroy(handle, reason)

    backend = Backend()
    manager = SandboxManager(backend)
    app = CoreApp(sandbox_manager=manager)
    sessions = SessionManager(
        SessionStore(tmp_path),
        lambda: AgentRunner(RuntimeConfig()),
        EventBus(),
        on_session_closed=app._delete_session_resources,
    )
    session = await sessions.create("chat")
    key = SandboxKey("session", session.id)
    runtime = manager.runtime_for(key)
    marker = tmp_path / "marker"
    marker.write_text("owned workspace", encoding="utf-8")
    await runtime.read_text(ReadRequest(SandboxCallContext(key, "run", "read"), str(marker)))
    with pytest.raises(OSError):
        await sessions.close(session.id)
    assert session.status == "closed"
    with pytest.raises(SandboxClosedError):
        manager.runtime_for(key)
    await sessions.close(session.id)
    await sessions.close(session.id)
    assert backend.attempts == 2
    assert session.id in sessions._closing
    assert marker.read_text(encoding="utf-8") == "owned workspace"
    await manager.close()


async def test_cancelled_close_waiter_does_not_cancel_shared_close(tmp_path: Path) -> None:
    entered, allowed = asyncio.Event(), asyncio.Event()

    async def cleanup(sid: str) -> None:
        entered.set()
        await allowed.wait()

    sessions = SessionManager(
        SessionStore(tmp_path),
        lambda: AgentRunner(RuntimeConfig()),
        EventBus(),
        on_session_closed=cleanup,
    )
    session = await sessions.create("chat")
    closing = asyncio.create_task(sessions.close(session.id))
    await entered.wait()
    closing.cancel()
    await asyncio.sleep(0)
    assert not closing.done()
    allowed.set()
    with pytest.raises(asyncio.CancelledError):
        await closing
    await sessions.close(session.id)
    assert session.status == "closed"
