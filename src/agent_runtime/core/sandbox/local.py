"""Host execution adapter. This is not filesystem, process, or network isolation."""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import signal
import time
import uuid
from collections.abc import Coroutine
from pathlib import Path

from agent_runtime.core.sandbox.models import (
    ExecRequest,
    ExecResult,
    FileResult,
    ListRequest,
    ListResult,
    ReadRequest,
    ReconcileReport,
    SandboxClosedError,
    SandboxHandle,
    SandboxKey,
    SandboxSpec,
    SandboxStatus,
    WriteRequest,
)

_MAX_OUTPUT_BYTES = 64 * 1024
_MAX_READ_BYTES = 512 * 1024
_MAX_WRITE_BYTES = 1024 * 1024
_MAX_ENTRIES = 200


async def _finish_cleanup(cleanup: Coroutine[object, object, None]) -> None:
    """Finish owned process cleanup even if another cancellation arrives."""
    task = asyncio.create_task(cleanup)
    cancelled = False
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            cancelled = True
    task.result()
    if cancelled:
        raise asyncio.CancelledError


class LocalSandboxRuntime:
    """Keep the original cwd/path and presentation behavior of the four tools.

    Cleanup owns active executions, including late subprocess creation. A shell
    that has already returned can leave redirected or detached background work;
    Local does not track those descendants or promise process isolation. Do not
    retain exited process-group IDs for later signaling: IDs can be reused.
    """

    def __init__(self, key: SandboxKey | None = None) -> None:
        self.key = key or SandboxKey("direct_run", uuid.uuid4().hex)
        self._closed = False
        self._processes: set[asyncio.subprocess.Process] = set()
        self._calls: set[asyncio.Task[ExecResult]] = set()
        self._close_task: asyncio.Task[None] | None = None

    def _check_open(self) -> None:
        if self._closed:
            raise SandboxClosedError(f"sandbox is closed: {self.key.kind}:{self.key.id}")

    async def _terminate(self, proc: asyncio.subprocess.Process) -> None:
        # Every shell has its own POSIX session. Never signal the host process group.
        try:
            os.killpg(proc.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        await proc.communicate()

    async def _cleanup_spawn(self, task: asyncio.Task[asyncio.subprocess.Process]) -> None:
        try:
            proc = await task
        except Exception:
            return
        await self._terminate(proc)

    async def exec(self, request: ExecRequest) -> ExecResult:
        self._check_open()
        task = asyncio.create_task(self._exec(request))
        self._calls.add(task)
        try:
            return await task
        finally:
            self._calls.discard(task)

    async def _exec(self, request: ExecRequest) -> ExecResult:
        self._check_open()
        started = time.monotonic()
        proc: asyncio.subprocess.Process | None = None
        spawn = asyncio.create_task(
            asyncio.create_subprocess_shell(
                request.command,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
                start_new_session=True,
            )
        )
        try:
            try:
                proc = await asyncio.shield(spawn)
            except asyncio.CancelledError:
                await _finish_cleanup(self._cleanup_spawn(spawn))
                raise
            self._processes.add(proc)
            if self._closed:
                await _finish_cleanup(self._terminate(proc))
                raise SandboxClosedError(f"sandbox is closed: {self.key.kind}:{self.key.id}")
            try:
                stdout_bytes, _ = await asyncio.wait_for(
                    proc.communicate(), timeout=request.timeout_s
                )
            except TimeoutError:
                await _finish_cleanup(self._terminate(proc))
                return ExecResult(
                    content=f"[timeout after {request.timeout_s}s]",
                    is_error=True,
                    error_type="timeout",
                    exit_code=proc.returncode,
                    terminal_reason="timeout",
                    duration_ms=(time.monotonic() - started) * 1000,
                )
            except asyncio.CancelledError:
                await _finish_cleanup(self._terminate(proc))
                raise
        except SandboxClosedError:
            raise
        except Exception as exc:
            if proc is not None:
                await _finish_cleanup(self._terminate(proc))
            return ExecResult(
                content=str(exc),
                is_error=True,
                error_type="runtime_error",
                terminal_reason="runtime_error",
                duration_ms=(time.monotonic() - started) * 1000,
            )
        finally:
            if proc is not None:
                self._processes.discard(proc)

        output = stdout_bytes.decode("utf-8", errors="replace")
        truncated = len(stdout_bytes) > _MAX_OUTPUT_BYTES
        if truncated:
            # Compatibility: the old tool reads fully, then slices characters.
            # This is presentation compatibility, not a bounded-memory guarantee.
            output = output[:_MAX_OUTPUT_BYTES] + "\n[truncated]"
        returncode = proc.returncode or 0
        return ExecResult(
            content=(f"[exit {returncode}]\n{output}" if returncode else output or "[no output]"),
            is_error=returncode != 0,
            error_type="runtime_error" if returncode else None,
            output=output,
            exit_code=returncode,
            truncated=truncated,
            duration_ms=(time.monotonic() - started) * 1000,
        )

    async def read_text(self, request: ReadRequest) -> FileResult:
        self._check_open()
        path = self._path(request.path)
        raw = path.read_bytes()
        truncated = len(raw) > _MAX_READ_BYTES
        content = raw[:_MAX_READ_BYTES].decode("utf-8", errors="replace")
        if truncated:
            content += "\n[truncated]"
        return FileResult(content=content, truncated=truncated)

    async def write_text(self, request: WriteRequest) -> FileResult:
        self._check_open()
        path = self._path(request.path)
        encoded = request.content.encode("utf-8")
        if len(encoded) > _MAX_WRITE_BYTES:
            return FileResult(
                content=f"content too large: {len(encoded)} bytes (limit 1 MB)",
                is_error=True,
                error_type="runtime_error",
            )
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(request.content, encoding="utf-8")
        return FileResult(content=f"wrote {len(encoded)} bytes to {request.path}")

    async def list_dir(self, request: ListRequest) -> ListResult:
        self._check_open()
        root = self._path(request.path)
        if not root.exists():
            raise FileNotFoundError(f"no such directory: {request.path}")
        if not root.is_dir():
            raise NotADirectoryError(f"not a directory: {request.path}")
        lines: list[str] = [str(root) + "/"]
        count = 0
        truncated = False

        def walk(directory: Path, depth: int, prefix: str) -> None:
            nonlocal count, truncated
            if depth > request.max_depth or count >= _MAX_ENTRIES:
                return
            entries = sorted(directory.iterdir(), key=lambda entry: (entry.is_file(), entry.name))
            for index, entry in enumerate(entries):
                if count >= _MAX_ENTRIES:
                    lines.append(f"{prefix}... (truncated)")
                    truncated = True
                    return
                connector = "└── " if index == len(entries) - 1 else "├── "
                suffix = "/" if entry.is_dir() else ""
                lines.append(f"{prefix}{connector}{entry.name}{suffix}")
                count += 1
                if entry.is_dir() and depth < request.max_depth:
                    extension = "    " if index == len(entries) - 1 else "│   "
                    walk(entry, depth + 1, prefix + extension)

        walk(root, 1, "")
        return ListResult(content="\n".join(lines), truncated=truncated)

    @staticmethod
    def _path(raw: str) -> Path:
        path = Path(raw)
        if ".." in path.parts:
            raise PermissionError(f"path traversal not allowed: {raw}")
        # Resolve neither here nor at construction: cwd belongs to the host caller.
        return path

    async def close(self) -> None:
        if self._close_task is None:
            self._closed = True
            self._close_task = asyncio.create_task(self._close())
        await asyncio.shield(self._close_task)

    async def _close(self) -> None:
        # Each execution cleans its own pipes/group, including late OS creation.
        # Do not concurrently call communicate() from close and exec.
        calls = tuple(self._calls)
        for call in calls:
            call.cancel()
        await asyncio.gather(*calls, return_exceptions=True)


class LocalSandboxBackend:
    """Lifecycle bookkeeping only; destroy never removes host paths."""

    def __init__(self) -> None:
        self._handles: dict[SandboxKey, SandboxHandle] = {}
        self._runtimes: dict[str, LocalSandboxRuntime] = {}

    async def ensure(self, key: SandboxKey, spec: SandboxSpec) -> SandboxHandle:
        if spec.backend != "local" or spec.workspace_policy != "host_cwd":
            raise ValueError("Local sandbox only supports backend=local, workspace_policy=host_cwd")
        handle = self._handles.get(key)
        if handle is None:
            identity = json.dumps(["local", key.kind, key.id], separators=(",", ":"))
            handle = SandboxHandle(
                key=key,
                sandbox_id=hashlib.sha256(identity.encode("utf-8")).hexdigest()[:32],
            )
            self._handles[key] = handle
        return handle

    def runtime_for(self, handle: SandboxHandle) -> LocalSandboxRuntime:
        if self._handles.get(handle.key) is not handle or handle.status in (
            SandboxStatus.TERMINATING,
            SandboxStatus.TERMINATED,
        ):
            raise SandboxClosedError("cannot bind a released Local sandbox handle")
        runtime = self._runtimes.get(handle.sandbox_id)
        if runtime is None:
            runtime = LocalSandboxRuntime(handle.key)
            self._runtimes[handle.sandbox_id] = runtime
        return runtime

    async def status(self, handle: SandboxHandle) -> SandboxStatus:
        return handle.status

    async def destroy(self, handle: SandboxHandle, reason: str) -> None:
        if self._handles.get(handle.key) is not handle:
            # An old handle must not close a later backend lifecycle with the
            # same deterministic ID. Manager tombstones additionally prevent
            # recreating released keys through old facades.
            handle.status = SandboxStatus.TERMINATED
            return
        runtime = self._runtimes.get(handle.sandbox_id)
        if runtime is not None:
            await runtime.close()
            self._runtimes.pop(handle.sandbox_id, None)
        if self._handles.get(handle.key) is handle:
            self._handles.pop(handle.key, None)
        handle.status = SandboxStatus.TERMINATED

    async def reconcile(self) -> ReconcileReport:
        return ReconcileReport()
