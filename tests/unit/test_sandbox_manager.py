"""M0 lifecycle cases. Added for later acceptance; not executed in this delivery."""

from __future__ import annotations

import asyncio

import pytest

from agent_runtime.core.sandbox import (
    ExecRequest,
    ExecResult,
    FileResult,
    ListRequest,
    ListResult,
    ReadRequest,
    ReconcileReport,
    SandboxCallContext,
    SandboxClosedError,
    SandboxHandle,
    SandboxKey,
    SandboxManager,
    SandboxSpec,
    SandboxStatus,
    WriteRequest,
)


class RecordingRuntime:
    def __init__(self) -> None:
        self.calls: list[SandboxCallContext] = []
        self.started = asyncio.Event()
        self.allowed = asyncio.Event()
        self.allowed.set()
        self.active = 0
        self.peak_active = 0
        self.fail = False

    async def _call(self, context: SandboxCallContext) -> str:
        self.calls.append(context)
        self.active += 1
        self.peak_active = max(self.peak_active, self.active)
        self.started.set()
        try:
            await self.allowed.wait()
            if self.fail:
                raise OSError("operation failed")
            return context.tool_call_id
        finally:
            self.active -= 1

    async def exec(self, request: ExecRequest) -> ExecResult:
        return ExecResult(content=await self._call(request.context))

    async def read_text(self, request: ReadRequest) -> FileResult:
        return FileResult(content=await self._call(request.context))

    async def write_text(self, request: WriteRequest) -> FileResult:
        return FileResult(content=await self._call(request.context))

    async def list_dir(self, request: ListRequest) -> ListResult:
        return ListResult(content=await self._call(request.context))


class RecordingBackend:
    def __init__(self) -> None:
        self.ensured: list[SandboxKey] = []
        self.destroyed: list[tuple[SandboxHandle, str]] = []
        self.runtimes: dict[SandboxKey, RecordingRuntime] = {}
        self.creating = asyncio.Event()
        self.create_allowed = asyncio.Event()
        self.create_allowed.set()
        self.cancel_seen = asyncio.Event()
        self.destroying = asyncio.Event()
        self.destroy_allowed = asyncio.Event()
        self.destroy_allowed.set()
        self.ignore_create_cancel = False
        self.fail_create = False

    async def ensure(self, key: SandboxKey, spec: SandboxSpec) -> SandboxHandle:
        self.ensured.append(key)
        self.creating.set()
        try:
            await self.create_allowed.wait()
        except asyncio.CancelledError:
            self.cancel_seen.set()
            if not self.ignore_create_cancel:
                raise
            await self.create_allowed.wait()
        if self.fail_create:
            raise RuntimeError("creation failed")
        return SandboxHandle(key, f"{key.kind}:{key.id}")

    def runtime_for(self, handle: SandboxHandle) -> RecordingRuntime:
        return self.runtimes.setdefault(handle.key, RecordingRuntime())

    async def status(self, handle: SandboxHandle) -> SandboxStatus:
        return handle.status

    async def destroy(self, handle: SandboxHandle, reason: str) -> None:
        self.destroyed.append((handle, reason))
        self.destroying.set()
        await self.destroy_allowed.wait()
        handle.status = SandboxStatus.TERMINATED

    async def reconcile(self) -> ReconcileReport:
        return ReconcileReport()


def context(key: SandboxKey, call: str = "call", run: str = "root") -> SandboxCallContext:
    return SandboxCallContext(
        key, run, call, session_id=key.id if key.kind == "session" else ""
    )


async def test_facade_is_lazy_and_absent_release_never_creates() -> None:
    backend = RecordingBackend()
    manager = SandboxManager(backend)
    key = SandboxKey("session", "unused")
    runtime = manager.runtime_for(key)
    assert manager.runtime_for(key) is runtime
    assert backend.ensured == []
    assert manager.status(key) == SandboxStatus.ABSENT
    await manager.release(key)
    await manager.release(key)
    assert backend.ensured == []
    assert backend.destroyed == []
    with pytest.raises(SandboxClosedError):
        await runtime.read_text(ReadRequest(context(key), "ignored"))
    with pytest.raises(SandboxClosedError):
        manager.runtime_for(key)
    await manager.close()
    await manager.close()


async def test_parallel_calls_create_once_and_serialize_all_four_operations() -> None:
    backend = RecordingBackend()
    manager = SandboxManager(backend)
    key = SandboxKey("session", "shared")
    recording = RecordingRuntime()
    recording.allowed.clear()
    backend.runtimes[key] = recording
    runtime = manager.runtime_for(key)
    calls = [
        asyncio.create_task(runtime.exec(ExecRequest(context(key, "exec"), "ignored"))),
        asyncio.create_task(runtime.read_text(ReadRequest(context(key, "read"), "ignored"))),
        asyncio.create_task(runtime.write_text(WriteRequest(context(key, "write"), "x", "y"))),
        asyncio.create_task(runtime.list_dir(ListRequest(context(key, "list")))),
    ]
    await recording.started.wait()
    assert manager.status(key) == SandboxStatus.BUSY
    assert len(recording.calls) == 1
    recording.allowed.set()
    results = await asyncio.gather(*calls)
    assert {result.content for result in results} == {"exec", "read", "write", "list"}
    assert recording.peak_active == 1
    assert backend.ensured == [key]
    assert manager.status(key) == SandboxStatus.READY
    await manager.close()
    assert len(backend.destroyed) == 1


async def test_key_kind_prevents_collision_and_other_keys_can_execute_concurrently() -> None:
    backend = RecordingBackend()
    manager = SandboxManager(backend)
    session = SandboxKey("session", "same-id")
    direct = SandboxKey("direct_run", "same-id")
    for key in (session, direct):
        backend.runtimes[key] = RecordingRuntime()
        backend.runtimes[key].allowed.clear()
    tasks = [
        asyncio.create_task(manager.runtime_for(key).read_text(ReadRequest(context(key), "x")))
        for key in (session, direct)
    ]
    await asyncio.gather(*(runtime.started.wait() for runtime in backend.runtimes.values()))
    assert manager.handle_for(session) is not manager.handle_for(direct)
    assert set(backend.ensured) == {session, direct}
    for runtime in backend.runtimes.values():
        runtime.allowed.set()
    await asyncio.gather(*tasks)
    await manager.release(direct, "direct_run_finished")
    assert manager.status(session) == SandboxStatus.READY
    assert manager.status(direct) == SandboxStatus.TERMINATED
    await manager.close()


async def test_failed_creation_and_failed_operation_remain_releasable() -> None:
    backend = RecordingBackend()
    manager = SandboxManager(backend)
    bad = SandboxKey("session", "creation-fails")
    backend.fail_create = True
    with pytest.raises(RuntimeError, match="creation failed"):
        await manager.runtime_for(bad).read_text(ReadRequest(context(bad), "x"))
    assert manager.status(bad) == SandboxStatus.FAILED
    await manager.release(bad)
    assert backend.destroyed == []
    backend.fail_create = False
    key = SandboxKey("session", "operation-fails")
    recording = RecordingRuntime()
    recording.fail = True
    backend.runtimes[key] = recording
    runtime = manager.runtime_for(key)
    with pytest.raises(OSError, match="operation failed"):
        await runtime.read_text(ReadRequest(context(key), "x"))
    assert manager.status(key) == SandboxStatus.READY
    recording.fail = False
    assert (await runtime.read_text(ReadRequest(context(key), "x"))).content == "call"
    await manager.close()
    assert len(backend.destroyed) == 1


async def test_close_during_creation_destroys_even_a_late_handle() -> None:
    backend = RecordingBackend()
    backend.create_allowed.clear()
    backend.ignore_create_cancel = True
    manager = SandboxManager(backend)
    key = SandboxKey("session", "late")
    runtime = manager.runtime_for(key)
    call = asyncio.create_task(runtime.read_text(ReadRequest(context(key), "x")))
    await backend.creating.wait()
    closing = asyncio.create_task(manager.close())
    await backend.cancel_seen.wait()
    assert manager.status(key) == SandboxStatus.TERMINATING
    backend.create_allowed.set()
    await closing
    with pytest.raises(asyncio.CancelledError):
        await call
    assert len(backend.destroyed) == 1
    assert manager.status(key) == SandboxStatus.TERMINATED
    assert key not in backend.runtimes
    with pytest.raises(SandboxClosedError):
        await runtime.read_text(ReadRequest(context(key), "x"))


async def test_cancelled_creation_cleans_up_without_an_explicit_owner_close() -> None:
    backend = RecordingBackend()
    backend.create_allowed.clear()
    backend.ignore_create_cancel = True
    manager = SandboxManager(backend)
    key = SandboxKey("session", "cancelled-create")
    runtime = manager.runtime_for(key)
    call = asyncio.create_task(runtime.read_text(ReadRequest(context(key), "x")))
    await backend.creating.wait()
    call.cancel()
    await backend.cancel_seen.wait()
    assert manager.status(key) == SandboxStatus.TERMINATING
    backend.create_allowed.set()
    with pytest.raises(asyncio.CancelledError):
        await call
    assert len(backend.destroyed) == 1
    assert backend.destroyed[0][1] == "creation_cancelled"
    assert manager.status(key) == SandboxStatus.TERMINATED
    with pytest.raises(SandboxClosedError):
        await runtime.read_text(ReadRequest(context(key), "x"))
    await manager.close()


async def test_cancelled_release_waiter_does_not_cancel_shared_cleanup() -> None:
    backend = RecordingBackend()
    manager = SandboxManager(backend)
    key = SandboxKey("session", "release")
    await manager.runtime_for(key).read_text(ReadRequest(context(key), "x"))
    backend.destroy_allowed.clear()
    first = asyncio.create_task(manager.release(key, "first"))
    await backend.destroying.wait()
    first.cancel()
    with pytest.raises(asyncio.CancelledError):
        await first
    second = asyncio.create_task(manager.release(key, "second"))
    backend.destroy_allowed.set()
    await second
    await manager.close()
    assert len(backend.destroyed) == 1
    assert backend.destroyed[0][1] == "first"


async def test_release_cancels_busy_operation_and_preserves_other_owner() -> None:
    backend = RecordingBackend()
    manager = SandboxManager(backend)
    busy = SandboxKey("session", "busy")
    other = SandboxKey("session", "other")
    recording = RecordingRuntime()
    recording.allowed.clear()
    backend.runtimes[busy] = recording
    call = asyncio.create_task(
        manager.runtime_for(busy).exec(ExecRequest(context(busy), "ignored"))
    )
    await recording.started.wait()
    await manager.runtime_for(other).read_text(ReadRequest(context(other), "x"))
    await manager.release(busy)
    with pytest.raises(asyncio.CancelledError):
        await call
    assert recording.active == 0
    assert manager.status(other) == SandboxStatus.READY
    await manager.close()


async def test_bound_facade_rejects_a_different_owner_before_ensure() -> None:
    backend = RecordingBackend()
    manager = SandboxManager(backend)
    key = SandboxKey("session", "owner")
    other = SandboxKey("session", "wrong")
    with pytest.raises(ValueError, match="identity"):
        await manager.runtime_for(key).read_text(ReadRequest(context(other), "x"))
    assert backend.ensured == []
    await manager.close()
