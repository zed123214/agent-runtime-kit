"""Lazy owner-scoped runtimes with single-operation locking and durable close tasks."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import time
from collections.abc import Awaitable, Callable
from dataclasses import asdict, dataclass, field, fields
from datetime import UTC, datetime
from pathlib import Path
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
    SandboxConflictError,
    SandboxHandle,
    SandboxKey,
    SandboxOutcomeUnknownError,
    SandboxProvisionError,
    SandboxQueueTimeout,
    SandboxSpec,
    SandboxStatus,
    WorkspaceLostError,
    WriteRequest,
)

_T = TypeVar("_T")
logger = logging.getLogger(__name__)


def _observe_task[T](task: asyncio.Task[T]) -> None:
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
    last_completed: float = field(default_factory=time.monotonic)
    workspace_lost: bool = False
    phase_started: float = field(default_factory=time.monotonic)
    create_time: str | None = None

    def set_status(self, status: SandboxStatus) -> None:
        self.status = status
        if self.handle is not None:
            self.handle.status = status


class _ManagedRuntime:
    def __init__(self, manager: SandboxManager, entry: _Entry) -> None:
        self.key = entry.key
        self._manager = manager
        self._entry = entry

    @property
    def remote_execution(self) -> bool:
        return self._manager.remote_execution

    def _validate_context(self, context: SandboxCallContext) -> None:
        if context.key != self.key:
            raise ValueError("sandbox request identity does not match its bound owner")

    async def exec(self, request: ExecRequest) -> ExecResult:
        self._validate_context(request.context)
        return await self._manager._invoke(
            self._entry, lambda runtime: runtime.exec(request), request.context, "exec", request
        )

    async def read_text(self, request: ReadRequest) -> FileResult:
        self._validate_context(request.context)
        return await self._manager._invoke(
            self._entry,
            lambda runtime: runtime.read_text(request),
            request.context,
            "read_text",
            request,
        )

    async def write_text(self, request: WriteRequest) -> FileResult:
        self._validate_context(request.context)
        return await self._manager._invoke(
            self._entry,
            lambda runtime: runtime.write_text(request),
            request.context,
            "write_text",
            request,
        )

    async def list_dir(self, request: ListRequest) -> ListResult:
        self._validate_context(request.context)
        return await self._manager._invoke(
            self._entry,
            lambda runtime: runtime.list_dir(request),
            request.context,
            "list_dir",
            request,
        )


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
        queue_timeout_s: float | None = None,
        provision_timeout_s: float | None = None,
        idle_timeout_s: float | None = None,
        lifecycle_path: Path | None = None,
        trace_sink: Callable[[dict[str, object]], None] | None = None,
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
        self.remote_execution = self._spec.backend == "kubernetes"
        self._queue_timeout_s = queue_timeout_s
        self._provision_timeout_s = provision_timeout_s
        self._idle_timeout_s = idle_timeout_s
        self._lifecycle_path = lifecycle_path
        self._trace_sink = trace_sink
        self._maintenance: asyncio.Task[None] | None = None
        self._start_lock = asyncio.Lock()
        self._started = False
        set_sink = getattr(self._backend, "set_record_sink", None)
        if callable(set_sink):
            set_sink(self._record)

    def _record(self, record: dict[str, object]) -> None:
        try:
            if self._lifecycle_path is not None:
                self._lifecycle_path.parent.mkdir(parents=True, exist_ok=True)
                with self._lifecycle_path.open("a", encoding="utf-8") as stream:
                    stream.write(json.dumps(record, ensure_ascii=False) + "\n")
            if self._trace_sink is not None:
                self._trace_sink(record)
        except Exception:
            logger.exception("sandbox internal record failed")

    async def start(self) -> None:
        if not self.remote_execution:
            return
        async with self._start_lock:
            if self._started:
                return
            start = getattr(self._backend, "start", None)
            if callable(start):
                await start()
            report = await self._backend.reconcile()
            logger.info("sandbox startup reconcile: %s", report)
            self._record(
                {
                    "type": "sandbox.reconcile",
                    "run_id": None,
                    "ts": datetime.now(UTC).isoformat(),
                    **asdict(report),
                }
            )
            if self._closed:
                raise SandboxClosedError("sandbox manager closed during startup")
            self._started = True
            self._maintenance = asyncio.create_task(self._maintain())

    async def stop_if_idle(self) -> None:
        """Release transports/tasks of a direct Runner without closing other keys."""
        if not self.remote_execution:
            return
        async with self._start_lock:
            if any(not entry.closed for entry in self._entries.values()):
                return
            if self._maintenance is not None:
                self._maintenance.cancel()
                await asyncio.gather(self._maintenance, return_exceptions=True)
                self._maintenance = None
            aclose = getattr(self._backend, "aclose", None)
            if callable(aclose):
                await aclose()
            self._started = False

    async def _maintain(self) -> None:
        interval = min(30.0, (self._idle_timeout_s or 900) / 2)
        while True:
            await asyncio.sleep(interval)
            try:
                await self.reap_idle()
                report = await self._backend.reconcile()
                if report.destroyed or report.errors:
                    logger.info("sandbox reconcile: %s", report)
                    self._record(
                        {
                            "type": "sandbox.reconcile",
                            "run_id": None,
                            "ts": datetime.now(UTC).isoformat(),
                            **asdict(report),
                        }
                    )
            except Exception:
                logger.exception("sandbox maintenance failed; will reconcile again")

    async def reap_idle(self) -> None:
        now = time.monotonic()
        tasks = []
        for entry in self._entries.values():
            if (
                self._idle_timeout_s is not None
                and not entry.closed
                and entry.status == SandboxStatus.READY
                and not entry.operations
                and now - entry.last_completed >= self._idle_timeout_s
            ):
                # Admission and the TTL tombstone compete synchronously on this
                # event loop, including operations queued but not yet executing.
                entry.workspace_lost = self.remote_execution
                tasks.append(self._start_release(entry, "idle_ttl"))
            elif (
                entry.closed
                and entry.release is not None
                and entry.release.done()
                and (entry.release.cancelled() or entry.release.exception() is not None)
            ):
                tasks.append(self._start_release(entry, "cleanup_retry"))
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _emit(
        self,
        entry: _Entry,
        state: str,
        context: SandboxCallContext | None = None,
        reason: str | None = None,
    ) -> None:
        if not self.remote_execution:
            return
        from agent_runtime.core.bus.events import SandboxLifecycleEvent

        handle = entry.handle
        identity = getattr(self._backend, "identity_for", None)
        sid = (
            handle.sandbox_id
            if handle
            else (identity(entry.key) if callable(identity) else entry.key.id)
        )
        record: dict[str, object] = {
            "type": "sandbox." + state,
            "sandbox_id": sid,
            "backend": self._spec.backend,
            "key_kind": entry.key.kind,
            "key_id": entry.key.id,
            "session_id": entry.key.id if entry.key.kind == "session" else None,
            "run_id": context.run_id if context else None,
            "namespace": handle.namespace if handle else None,
            "pod_uid": handle.pod_uid if handle else None,
            "image_digest": handle.image_digest if handle else None,
            "policy_version": self._spec.policy_version,
            "resource_profile": handle.resource_profile if handle else None,
            "terminal_reason": reason,
            "ts": datetime.now(UTC).isoformat(),
        }
        try:
            now = time.monotonic()
            if state in {"creating", "terminating"}:
                entry.phase_started = now
            if state == "creating":
                entry.create_time = str(record["ts"])
            self._record(
                {
                    **record,
                    "schema_version": 1,
                    "tool_call_id": context.tool_call_id if context else None,
                    "monotonic_ns": time.monotonic_ns(),
                    "phase_duration_ms": (now - entry.phase_started) * 1000,
                    "create_time": entry.create_time,
                    "ready_ms": (now - entry.phase_started) * 1000 if state == "ready" else None,
                }
            )
            if context is not None and context.event_sink is not None:
                event = SandboxLifecycleEvent.model_validate(record)
                await context.event_sink(event)
        except Exception:
            # Lifecycle sinks cannot bypass cleanup or replay dispatched work.
            logger.exception("sandbox lifecycle record failed")

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
        if entry.workspace_lost:
            raise WorkspaceLostError("workspace_lost; create a new Session")
        if self._closed or entry.closed:
            raise SandboxClosedError(f"sandbox is closed: {entry.key.kind}:{entry.key.id}")

    async def _ensure(self, entry: _Entry, context: SandboxCallContext) -> SandboxRuntime:
        self._assert_open(entry)
        if entry.creation is None:
            entry.set_status(SandboxStatus.CREATING)
            entry.creation = asyncio.create_task(self._create(entry, context))
            entry.creation.add_done_callback(_observe_task)
        runtime = await asyncio.shield(entry.creation)
        self._assert_open(entry)
        return runtime

    async def _provision(self, entry: _Entry, context: SandboxCallContext) -> SandboxHandle:
        await self._emit(entry, "creating", context)
        try:
            async with asyncio.timeout(self._provision_timeout_s):
                await self.start()
                return await self._backend.ensure(entry.key, self._spec)
        except TimeoutError:
            if not self.remote_execution:
                raise
            raise SandboxProvisionError("Sandbox provisioning deadline expired") from None

    async def _create(self, entry: _Entry, context: SandboxCallContext) -> SandboxRuntime:
        try:
            handle = await self._provision(entry, context)
            # Capture even a late handle returned by a cancellation-resistant
            # backend. Release awaits this task, then destroys that exact handle.
            entry.handle = handle
            self._assert_open(entry)
            runtime = self._runtime_factory(handle)
            entry.runtime = runtime
            entry.set_status(SandboxStatus.READY)
            await self._emit(entry, "ready", context)
            return runtime
        except BaseException as exc:
            if not entry.closed:
                entry.set_status(SandboxStatus.FAILED)
                await self._emit(entry, "failed", context, getattr(exc, "code", "creation_failed"))
                if self.remote_execution:
                    # The operation waiter starts release after receiving this
                    # failure. Starting it here can cancel that waiter before it
                    # observes the actual provisioning error.
                    entry.workspace_lost = True
                elif entry.handle is not None:
                    await self._backend.destroy(entry.handle, "creation_failed")
                    entry.handle = None
            raise

    async def _invoke(
        self,
        entry: _Entry,
        operation: Callable[[SandboxRuntime], Awaitable[_T]],
        context: SandboxCallContext,
        operation_name: str = "unknown",
        request: ExecRequest | ReadRequest | WriteRequest | ListRequest | None = None,
    ) -> _T:
        started = time.monotonic()
        started_at = datetime.now(UTC).isoformat()
        cold = entry.creation is None
        timings: dict[str, float] = {}
        task: asyncio.Task[_T] | None = None
        result: object = None
        reason = "completed"
        error_type: str | None = None
        try:
            self._assert_open(entry)
            task = asyncio.create_task(self._run_operation(entry, operation, context, timings))
            entry.operations.add(task)
            result = await task
            reason = getattr(result, "terminal_reason", "completed")
            error_type = getattr(result, "error_type", None)
            return result
        except (asyncio.CancelledError, Exception) as exc:
            reason = getattr(
                exc,
                "terminal_reason",
                "cancelled" if isinstance(exc, asyncio.CancelledError) else "failed",
            )
            error_type = getattr(exc, "code", type(exc).__name__)
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
            if task is not None:
                entry.operations.discard(task)
            if self.remote_execution:
                handle = entry.handle
                total_ms = (time.monotonic() - started) * 1000
                worker_ms = getattr(result, "duration_ms", None)
                raw_input = (
                    {
                        f.name: getattr(request, f.name)
                        for f in fields(request)
                        if f.name != "context"
                    }
                    if request is not None
                    else {}
                )
                input_hash = hashlib.sha256(
                    json.dumps(
                        {"operation": operation_name, **raw_input},
                        ensure_ascii=True,
                        sort_keys=True,
                        separators=(",", ":"),
                    ).encode()
                ).hexdigest()
                stdout, stderr = getattr(result, "stdout", None), getattr(result, "stderr", None)
                self._record(
                    {
                        "schema_version": 1,
                        "type": "sandbox.execution",
                        "operation": operation_name,
                        "input_hash": input_hash,
                        "input_hash_version": "request-arguments-v1",
                        "stdout_hash": hashlib.sha256(stdout.encode()).hexdigest()
                        if isinstance(stdout, str)
                        else None,
                        "stderr_hash": hashlib.sha256(stderr.encode()).hexdigest()
                        if isinstance(stderr, str)
                        else None,
                        "session_id": entry.key.id if entry.key.kind == "session" else None,
                        "key_kind": entry.key.kind,
                        "key_id": entry.key.id,
                        "run_id": context.run_id,
                        "tool_call_id": context.tool_call_id,
                        "attempt": context.attempt,
                        "sandbox_id": handle.sandbox_id if handle else None,
                        "pod_uid": handle.pod_uid if handle else None,
                        "generation": handle.generation if handle else None,
                        "image_digest": handle.image_digest if handle else None,
                        "policy_version": self._spec.policy_version,
                        "resource_profile": handle.resource_profile if handle else None,
                        "started_at": started_at,
                        "finished_at": datetime.now(UTC).isoformat(),
                        "cold": cold,
                        "total_ms": total_ms,
                        "duration_ms": total_ms,
                        **timings,
                        "worker_reported_duration_ms": worker_ms,
                        "ready_overhead_ms": total_ms - worker_ms
                        if not cold and isinstance(worker_ms, (float, int))
                        else None,
                        "terminal_reason": reason,
                        "error_type": error_type,
                        "is_error": bool(error_type) or bool(getattr(result, "is_error", False)),
                        "exit_code": getattr(result, "exit_code", None),
                        "signal": getattr(result, "signal", None),
                        "resource_observations": getattr(result, "resource_observations", None),
                        "truncated": getattr(result, "truncated", False),
                        "status_after": entry.status.value,
                    }
                )

    async def _run_operation(
        self,
        entry: _Entry,
        operation: Callable[[SandboxRuntime], Awaitable[_T]],
        context: SandboxCallContext,
        timings: dict[str, float] | None = None,
    ) -> _T:
        if timings is None:
            timings = {}
        queued_at = time.monotonic()
        # The lock is acquired for one runtime operation, never a run or spawn.
        try:
            if self._queue_timeout_s is None:
                await entry.operation_lock.acquire()
            else:
                try:
                    async with asyncio.timeout(self._queue_timeout_s):
                        await entry.operation_lock.acquire()
                except TimeoutError:
                    raise SandboxQueueTimeout("Sandbox operation queue deadline expired") from None
        finally:
            timings["queue_ms"] = (time.monotonic() - queued_at) * 1000
        try:
            ensure_started = time.monotonic()
            try:
                runtime = await self._ensure(entry, context)
            except BaseException as exc:
                cancelled = isinstance(exc, asyncio.CancelledError)
                pending_creation = entry.creation is not None and not entry.creation.done()
                if not entry.closed and (self.remote_execution or (cancelled and pending_creation)):
                    entry.workspace_lost = self.remote_execution
                    reason = "creation_cancelled" if cancelled else "creation_failed"
                    self._start_release(entry, reason, context)
                raise
            finally:
                timings["ensure_ms"] = (time.monotonic() - ensure_started) * 1000
            self._assert_open(entry)
            entry.set_status(SandboxStatus.BUSY)
            operation_started = time.monotonic()
            try:
                return await operation(runtime)
            except (WorkspaceLostError, SandboxOutcomeUnknownError, SandboxConflictError) as exc:
                entry.workspace_lost = True
                self._start_release(
                    entry, getattr(exc, "terminal_reason", exc.code), context, failed=True
                )
                raise
            except asyncio.CancelledError:
                if self.remote_execution:
                    entry.workspace_lost = True
                    self._start_release(entry, "operation_cancelled", context)
                raise
            finally:
                timings["operation_ms"] = (time.monotonic() - operation_started) * 1000
                entry.last_completed = time.monotonic()
                entry.set_status(SandboxStatus.TERMINATING if entry.closed else SandboxStatus.READY)
        finally:
            entry.operation_lock.release()

    def _start_release(
        self,
        entry: _Entry,
        reason: str,
        context: SandboxCallContext | None = None,
        *,
        failed: bool = False,
    ) -> asyncio.Task[None]:
        if entry.release is None or (
            entry.release.done()
            and (entry.release.cancelled() or entry.release.exception() is not None)
        ):
            # Tombstone synchronously, before cleanup yields to a racing creator.
            entry.closed = True
            entry.set_status(SandboxStatus.TERMINATING)
            entry.release = asyncio.create_task(
                self._release(entry, reason, context, failed=failed)
            )
            entry.release.add_done_callback(_observe_task)
        return entry.release

    async def release(self, key: SandboxKey, reason: str = "released") -> None:
        """Release only this owner; even an absent key becomes a closed tombstone."""
        task = self._start_release(self._entry_for(key), reason)
        await asyncio.shield(task)

    async def _release(
        self,
        entry: _Entry,
        reason: str,
        context: SandboxCallContext | None = None,
        *,
        failed: bool = False,
    ) -> None:
        try:
            if failed:
                await self._emit(entry, "failed", context, reason)
            await self._emit(entry, "terminating", context, reason)
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
            elif self.remote_execution:
                cleanup_key = getattr(self._backend, "cleanup_key", None)
                if callable(cleanup_key):
                    await cleanup_key(entry.key)
            entry.runtime = None
            entry.set_status(SandboxStatus.TERMINATED)
            await self._emit(entry, "terminated", context, reason)
        except BaseException:
            entry.set_status(SandboxStatus.FAILED)
            raise

    async def close(self) -> None:
        """Idempotent whole-manager close, protected from a caller's cancellation."""
        if self._close_task is None or (
            self._close_task.done()
            and (self._close_task.cancelled() or self._close_task.exception() is not None)
        ):
            self._closed = True
            releases = [
                self._start_release(entry, "manager_closed") for entry in self._entries.values()
            ]
            self._close_task = asyncio.create_task(self._close(releases))
            self._close_task.add_done_callback(_observe_task)
        await asyncio.shield(self._close_task)

    async def _close(self, releases: list[asyncio.Task[None]]) -> None:
        if self._maintenance is not None:
            self._maintenance.cancel()
            await asyncio.gather(self._maintenance, return_exceptions=True)
            self._maintenance = None
        results = await asyncio.gather(*releases, return_exceptions=True)
        errors = [result for result in results if isinstance(result, BaseException)]
        if errors:
            raise BaseExceptionGroup("sandbox manager cleanup failed", errors)
        aclose = getattr(self._backend, "aclose", None)
        if callable(aclose):
            await aclose()
