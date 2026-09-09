"""Worker client with a conservative, explicit remote dispatch boundary."""

from __future__ import annotations

import asyncio
import json
from typing import TYPE_CHECKING, Any, TypeVar

import httpx
from pydantic import TypeAdapter, ValidationError

from agent_runtime.core.sandbox.models import (
    ExecRequest,
    ExecResult,
    FileResult,
    ListRequest,
    ListResult,
    ReadRequest,
    SandboxHandle,
    SandboxOutcomeUnknownError,
    SandboxProvisionError,
    WorkspaceLostError,
    WriteRequest,
)
from agent_runtime.sandbox_server.protocol import MAX_RESULT_BYTES, Identity, Operation

if TYPE_CHECKING:
    from agent_runtime.core.sandbox.kubernetes import KubernetesBackend

_T = TypeVar("_T", ExecResult, FileResult, ListResult)


class KubernetesRuntime:
    remote_execution = True

    def __init__(self, backend: KubernetesBackend, handle: SandboxHandle) -> None:
        self.backend = backend
        self.handle = handle
        self.key = handle.key

    async def exec(self, request: ExecRequest) -> ExecResult:
        return await self._call(
            request,
            "exec",
            ExecResult,
            command=request.command,
            timeout_s=float(min(request.timeout_s, self.backend.config.command_timeout_s)),
        )

    async def read_text(self, request: ReadRequest) -> FileResult:
        return await self._call(request, "read_text", FileResult, path=request.path)

    async def write_text(self, request: WriteRequest) -> FileResult:
        return await self._call(
            request, "write_text", FileResult, path=request.path, content=request.content
        )

    async def list_dir(self, request: ListRequest) -> ListResult:
        return await self._call(
            request, "list_dir", ListResult, path=request.path, max_depth=request.max_depth
        )

    async def _cancel(self, op: Operation) -> None:
        async with asyncio.timeout(self.backend.config.cleanup_margin_s):
            response = await self.backend.http.post(
                str(self.handle.endpoint) + "/v1/cancel",
                json=op.model_dump(),
                headers={"Authorization": "Bearer " + self.backend.credential(self.handle)},
                timeout=self.backend.config.cleanup_margin_s,
            )
            if response.status_code != 200:
                raise WorkspaceLostError("Worker cancellation was not acknowledged")

    async def _call(
        self,
        request: ExecRequest | ReadRequest | WriteRequest | ListRequest,
        operation: str,
        result_type: type[_T],
        **params: Any,
    ) -> _T:
        if request.context.key != self.key:
            raise ValueError("request owner mismatch")
        # This whole phase precedes dispatch. Permission was already granted by
        # invoke_tool before the lazy Manager was entered.
        await self.backend.validate_handle(self.handle)
        assert self.handle.pod_uid is not None and self.handle.generation is not None
        context = request.context
        op = Operation.model_validate(
            {
                "identity": Identity(
                    sandbox_id=self.handle.sandbox_id,
                    run_id=context.run_id,
                    tool_call_id=context.tool_call_id,
                    attempt=context.attempt,
                ),
                "pod_uid": self.handle.pod_uid,
                "generation": self.handle.generation,
                "operation": operation,
                **params,
            }
        )
        method, path = {
            "exec": ("POST", "/v1/exec"),
            "read_text": ("GET", "/v1/files"),
            "write_text": ("PUT", "/v1/files"),
            "list_dir": ("GET", "/v1/dirs"),
        }[operation]
        budget = op.response_timeout_s(self.backend.config.cleanup_margin_s)
        try:
            # From this point a write, read or response timeout may follow a
            # partial/full dispatch. Never convert it to a replayable runtime_error.
            async with asyncio.timeout(budget):
                async with self.backend.http.stream(
                    method,
                    str(self.handle.endpoint) + path,
                    params={"path": op.path} if operation != "exec" else None,
                    json=op.model_dump(),
                    timeout=budget,
                    headers={"Authorization": "Bearer " + self.backend.credential(self.handle)},
                ) as response:
                    raw = bytearray()
                    async for chunk in response.aiter_bytes(chunk_size=8192):
                        raw.extend(chunk)
                        if len(raw) > MAX_RESULT_BYTES:
                            raise SandboxOutcomeUnknownError(
                                "Worker response exceeded the protocol bound"
                            )
                    try:
                        value = json.loads(raw)
                    except ValueError:
                        raise SandboxOutcomeUnknownError(
                            "Invalid Worker response after dispatch"
                        ) from None
                    if not isinstance(value, dict):
                        raise SandboxOutcomeUnknownError("Invalid Worker result after dispatch")
                    code = value.get("error_type")
                    if code == "workspace_lost":
                        raise WorkspaceLostError("Worker generation is no longer valid")
                    if code == "outcome_unknown":
                        raise SandboxOutcomeUnknownError("Worker execution outcome is unknown")
                    if response.status_code != 200:
                        if response.status_code == 409 and code in (
                            "idempotency_conflict",
                            "idempotency_capacity",
                        ):
                            return TypeAdapter(result_type).validate_python(value)
                        raise SandboxOutcomeUnknownError(
                            "Worker rejected or lost the dispatched request"
                        )
                    try:
                        return TypeAdapter(result_type).validate_python(value)
                    except ValidationError:
                        raise SandboxOutcomeUnknownError(
                            "Malformed Worker result after dispatch"
                        ) from None
        except (httpx.ConnectError, httpx.ConnectTimeout, httpx.PoolTimeout):
            raise SandboxProvisionError("Worker connection failed before dispatch") from None
        except asyncio.CancelledError:
            cleanup = asyncio.create_task(self._cancel(op))
            try:
                await asyncio.shield(cleanup)
            except BaseException:
                cleanup.add_done_callback(
                    lambda task: None if task.cancelled() else task.exception()
                )
            # Manager also closes this Pod ownership. Cancellation cannot leave
            # remote work outliving the Core lifecycle even if this RPC was lost.
            raise
        except (httpx.HTTPError, TimeoutError, SandboxOutcomeUnknownError):
            terminal = await self.backend.terminal_failure(self.handle)
            if terminal is not None:
                raise terminal from None
            raise SandboxOutcomeUnknownError(
                "outcome_unknown: connection lost after dispatch; command was not replayed"
            ) from None
