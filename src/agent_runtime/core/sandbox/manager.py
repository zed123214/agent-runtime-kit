"""Lazy owner-scoped runtimes with single-operation locking and durable close tasks."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import TypeVar, cast

from agent_runtime.core.sandbox.base import SandboxBackend, SandboxRuntime
from agent_runtime.core.sandbox.local import LocalSandboxBackend
from agent_runtime.core.sandbox.models import (
    ExecRequest,
    ExecResult,
    FileResult,
    ListRequest,
    ListResult,
    ReadRequest,
    SandboxCallContext,
    SandboxClosedError,
    SandboxHandle,
    SandboxKey,
    SandboxSpec,
    SandboxStatus,
    WriteRequest,
)

_T = TypeVar("_T")


def _observe_task(task: asyncio.Task[_T]) -> None:
    # A caller can disappear while a shielded create/close continues. Retrieve its
    # exception without changing the result seen by later awaiters of this task.
    if not task.cancelled():
        task.exception()


@dataclass
class _Entry:
    key: SandboxKey
    status: SandboxStatus = SandboxStatus.ABSENT
    closed: bool = False
    handle: SandboxHandle | None = None
    runtime: SandboxRuntime | None = None
    facade: _ManagedRuntime | None = None
    creation: asyncio.Task[SandboxRuntime] | None = None
    release: asyncio.Task[None] | None = None
    operation_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    operations: set[asyncio.Task[object]] = field(default_factory=set)

    def set_status(self, status: SandboxStatus) -> None:
        self.status = status
        if self.handle is not None:
            self.handle.status = status


class _ManagedRuntime:
    def __init__(self, manager: SandboxManager, entry: _Entry) -> None:
        self.key = entry.key
        self._manager = manager
        self._entry = entry

    def _validate_context(self, context: SandboxCallContext) -> None:
        if context.key != self.key:
            raise ValueError("sandbox request identity does not match its bound owner")

    async def exec(self, request: ExecRequest) -> ExecResult:
        self._validate_context(request.context)
        return await self._manager._invoke(self._entry, lambda runtime: runtime.exec(request))

    async def read_text(self, request: ReadRequest) -> FileResult:
        self._validate_context(request.context)
        return await self._manager._invoke(self._entry, lambda runtime: runtime.read_text(request))

    async def write_text(self, request: WriteRequest) -> FileResult:
        self._validate_context(request.context)
        return await self._manager._invoke(self._entry, lambda runtime: runtime.write_text(request))

    async def list_dir(self, request: ListRequest) -> ListResult:
        self._validate_context(request.context)
        return await self._manager._invoke(self._entry, lambda runtime: runtime.list_dir(request))


class SandboxManager:
    """Share resources by owner without storing any mutable current run identity.

    A backend owns lifecycle only. Runtime construction is an explicit injectable
    factory; Local also supplies a convenience runtime_for(handle) binding hook.
    All calls belong to one asyncio event loop, matching Core/Runner ownership.
    """

    def __init__(
        self,
        backend: SandboxBackend | None = None,
        *,
        spec: SandboxSpec | None = None,
        runtime_factory: Callable[[SandboxHandle], SandboxRuntime] | None = None,
    ) -> None:
        self._backend = backend if backend is not None else LocalSandboxBackend()
        self._spec = spec or SandboxSpec()
        if runtime_factory is None:
            binding = getattr(self._backend, "runtime_for", None)
            if not callable(binding):
                raise TypeError("sandbox backend requires an explicit runtime_factory")
            runtime_factory = cast(Callable[[SandboxHandle], SandboxRuntime], binding)
        self._runtime_factory: Callable[[SandboxHandle], SandboxRuntime] = runtime_factory
        self._entries: dict[SandboxKey, _Entry] = {}
        self._closed = False
        self._close_task: asyncio.Task[None] | None = None

    def runtime_for(self, key: SandboxKey) -> SandboxRuntime:
        """Build/reuse a lazy facade. Never ensures a backend or captures cwd."""
        if self._closed:
            raise SandboxClosedError("sandbox manager is closed")
        entry = self._entry_for(key)
        self._assert_open(entry)
        if entry.facade is None:
            entry.facade = _ManagedRuntime(self, entry)
        return entry.facade

    def status(self, key: SandboxKey) -> SandboxStatus:
        entry = self._entries.get(key)
        return entry.status if entry is not None else SandboxStatus.ABSENT

    def handle_for(self, key: SandboxKey) -> SandboxHandle | None:
        entry = self._entries.get(key)
        return entry.handle if entry is not None else None

    def _entry_for(self, key: SandboxKey) -> _Entry:
        entry = self._entries.get(key)
        if entry is None:
            entry = _Entry(key)
            self._entries[key] = entry
        return entry

    def _assert_open(self, entry: _Entry) -> None:
        if self._closed or entry.closed:
            raise SandboxClosedError(f"sandbox is closed: {entry.key.kind}:{entry.key.id}")

    async def _ensure(self, entry: _Entry) -> SandboxRuntime:
        self._assert_open(entry)
        if entry.creation is None:
            entry.set_status(SandboxStatus.CREATING)
            entry.creation = asyncio.create_task(self._create(entry))
            entry.creation.add_done_callback(_observe_task)
        runtime = await asyncio.shield(entry.creation)
        self._assert_open(entry)
        return runtime

    async def _create(self, entry: _Entry) -> SandboxRuntime:
        try:
            handle = await self._backend.ensure(entry.key, self._spec)
            # Capture even a late handle returned by a cancellation-resistant
            # backend. Release awaits this task, then destroys that exact handle.
            entry.handle = handle
            self._assert_open(entry)
            runtime = self._runtime_factory(handle)
            entry.runtime = runtime
            entry.set_status(SandboxStatus.READY)
            return runtime
        except BaseException:
            if not entry.closed:
                entry.set_status(SandboxStatus.FAILED)
                if entry.handle is not None:
                    await self._backend.destroy(entry.handle, "creation_failed")
                    entry.handle = None
            raise

    async def _invoke(
        self, entry: _Entry, operation: Callable[[SandboxRuntime], Awaitable[_T]]
    ) -> _T:
        self._assert_open(entry)
        task = asyncio.create_task(self._run_operation(entry, operation))
        entry.operations.add(task)
        try:
            return await task
        except asyncio.CancelledError:
            # The operation task is finished now, so cleanup can safely join it.
            # In particular, cancellation while creating cannot leave an orphan
            # create task that later promotes an abandoned key to READY.
            if entry.release is not None:
                try:
                    await asyncio.shield(entry.release)
                except Exception:
                    # Release keeps its own failed result for its owner to inspect.
                    # Preserve the caller's cancellation rather than replacing it.
                    pass
            raise
        finally:
            entry.operations.discard(task)

    async def _run_operation(
        self, entry: _Entry, operation: Callable[[SandboxRuntime], Awaitable[_T]]
    ) -> _T:
        # The lock is acquired for one runtime operation, never a run or spawn.
        async with entry.operation_lock:
            try:
                runtime = await self._ensure(entry)
            except asyncio.CancelledError:
                if entry.creation is not None and not entry.creation.done() and not entry.closed:
                    self._start_release(entry, "creation_cancelled")
                raise
            self._assert_open(entry)
            entry.set_status(SandboxStatus.BUSY)
            try:
                return await operation(runtime)
            finally:
                entry.set_status(
                    SandboxStatus.TERMINATING if entry.closed else SandboxStatus.READY
                )

    def _start_release(self, entry: _Entry, reason: str) -> asyncio.Task[None]:
        if entry.release is None:
            # Tombstone synchronously, before cleanup yields to a racing creator.
            entry.closed = True
            entry.set_status(SandboxStatus.TERMINATING)
            entry.release = asyncio.create_task(self._release(entry, reason))
            entry.release.add_done_callback(_observe_task)
        return entry.release

    async def release(self, key: SandboxKey, reason: str = "released") -> None:
        """Release only this owner; even an absent key becomes a closed tombstone."""
        task = self._start_release(self._entry_for(key), reason)
        await asyncio.shield(task)

    async def _release(self, entry: _Entry, reason: str) -> None:
        try:
            operations = tuple(entry.operations)
            for operation in operations:
                operation.cancel()
            if entry.creation is not None and not entry.creation.done():
                entry.creation.cancel()
            pending: list[asyncio.Task[object]] = list(operations)
            if entry.creation is not None:
                pending.append(entry.creation)
            if pending:
                await asyncio.gather(*pending, return_exceptions=True)
            if entry.handle is not None:
                await self._backend.destroy(entry.handle, reason)
            entry.runtime = None
            entry.set_status(SandboxStatus.TERMINATED)
        except BaseException:
            entry.set_status(SandboxStatus.FAILED)
            raise

    async def close(self) -> None:
        """Idempotent whole-manager close, protected from a caller's cancellation."""
        if self._close_task is None:
            self._closed = True
            releases = [
                self._start_release(entry, "manager_closed") for entry in self._entries.values()
            ]
            self._close_task = asyncio.create_task(self._close(releases))
            self._close_task.add_done_callback(_observe_task)
        await asyncio.shield(self._close_task)

    async def _close(self, releases: list[asyncio.Task[None]]) -> None:
        results = await asyncio.gather(*releases, return_exceptions=True)
        errors = [result for result in results if isinstance(result, BaseException)]
        if errors:
            raise BaseExceptionGroup("sandbox manager cleanup failed", errors)
