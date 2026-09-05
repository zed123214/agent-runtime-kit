"""Lifecycle and execution contracts intentionally have separate dependencies."""

from __future__ import annotations

from typing import Protocol

from agent_runtime.core.sandbox.models import (
    ExecRequest,
    ExecResult,
    FileResult,
    ListRequest,
    ListResult,
    ReadRequest,
    ReconcileReport,
    SandboxHandle,
    SandboxKey,
    SandboxSpec,
    SandboxStatus,
    WriteRequest,
)


class SandboxBackend(Protocol):
    """Own resource creation/deletion; failed ensure must clean partial resources."""

    async def ensure(self, key: SandboxKey, spec: SandboxSpec) -> SandboxHandle: ...

    async def status(self, handle: SandboxHandle) -> SandboxStatus: ...

    async def destroy(self, handle: SandboxHandle, reason: str) -> None: ...

    async def reconcile(self) -> ReconcileReport: ...


class SandboxRuntime(Protocol):
    async def exec(self, request: ExecRequest) -> ExecResult: ...

    async def read_text(self, request: ReadRequest) -> FileResult: ...

    async def write_text(self, request: WriteRequest) -> FileResult: ...

    async def list_dir(self, request: ListRequest) -> ListResult: ...
