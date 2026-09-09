"""HTTP boundary tests, written but not executed for M1."""

from __future__ import annotations

from typing import Any, cast

import httpx
import pytest

from agent_runtime.core.sandbox.config import SandboxConfig
from agent_runtime.core.sandbox.models import (
    ExecRequest,
    SandboxCallContext,
    SandboxHandle,
    SandboxKey,
    SandboxOutcomeUnknownError,
    SandboxProvisionError,
    WorkspaceLostError,
)
from agent_runtime.core.sandbox.remote import KubernetesRuntime


class Backend:
    def __init__(self, http: httpx.AsyncClient) -> None:
        self.http = http
        self.config = SandboxConfig()
        self.invalid = False
        self.terminal: WorkspaceLostError | None = None

    async def validate_handle(self, handle: SandboxHandle) -> None:
        if self.invalid:
            raise WorkspaceLostError("generation lost")

    def credential(self, handle: SandboxHandle) -> str:
        return "secret-token-not-for-logs"

    async def terminal_failure(self, handle: SandboxHandle) -> WorkspaceLostError | None:
        return self.terminal


def runtime(backend: Backend) -> tuple[KubernetesRuntime, ExecRequest]:
    key = SandboxKey("session", "owner")
    handle = SandboxHandle(
        key,
        "a" * 32,
        backend="kubernetes",
        pod_uid="pod-uid",
        generation="generation",
        endpoint="http://worker",
    )
    return KubernetesRuntime(cast(Any, backend), handle), ExecRequest(
        SandboxCallContext(key, "child", "tool"), "printf x >> marker"
    )


@pytest.mark.parametrize("connected", [False, True])
async def test_transport_failure_classification_does_not_replay(connected: bool) -> None:
    calls = 0

    async def response(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        assert b'"run_id":"child"' in request.content
        if connected:
            raise httpx.ReadTimeout("response lost", request=request)
        raise httpx.ConnectError("not connected", request=request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(response)) as http:
        client, request = runtime(Backend(http))
        with pytest.raises(SandboxOutcomeUnknownError if connected else SandboxProvisionError):
            await client.exec(request)
    assert calls == 1


async def test_pre_dispatch_generation_loss_sends_no_command() -> None:
    async def forbidden(request: httpx.Request) -> httpx.Response:
        raise AssertionError("command dispatched to a new generation")

    async with httpx.AsyncClient(transport=httpx.MockTransport(forbidden)) as http:
        backend = Backend(http)
        backend.invalid = True
        client, request = runtime(backend)
        with pytest.raises(WorkspaceLostError):
            await client.exec(request)


async def test_lost_worker_response_preserves_observed_oom_without_replay() -> None:
    calls = 0

    async def response(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(500, json={"error_type": "outcome_unknown"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(response)) as http:
        backend = Backend(http)
        backend.terminal = WorkspaceLostError("workspace lost", terminal_reason="OOMKilled")
        client, request = runtime(backend)
        with pytest.raises(WorkspaceLostError) as result:
            await client.exec(request)
        assert result.value.terminal_reason == "OOMKilled"
        assert result.value.code == "workspace_lost"
    assert calls == 1


async def test_nonzero_and_streams_are_preserved() -> None:
    async def response(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "content": "out\nerr\n[exit 9]",
                "exit_code": 9,
                "is_error": True,
                "error_type": "command_failed",
                "stdout": "out",
                "stderr": "err",
                "merged_output": "outerr",
                "output": "outerr",
                "terminal_reason": "completed",
            },
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(response)) as http:
        client, request = runtime(Backend(http))
        result = await client.exec(request)
        assert result.exit_code == 9 and result.error_type == "command_failed"
        assert (
            result.stdout == "out" and result.stderr == "err" and result.merged_output == "outerr"
        )
