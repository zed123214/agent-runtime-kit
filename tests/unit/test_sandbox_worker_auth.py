"""Broker authentication boundaries; written only, not run for M1 delivery."""

from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING, Any
from unittest.mock import AsyncMock, Mock

import pytest

from agent_runtime.sandbox_server.protocol import Identity, Operation

if TYPE_CHECKING:
    from agent_runtime.sandbox_server.server import WorkerServer

web = pytest.importorskip("aiohttp.web")

TOKEN = "b" * 43


@pytest.fixture
def broker(monkeypatch: pytest.MonkeyPatch) -> WorkerServer:
    from agent_runtime.sandbox_server.server import WorkerServer

    monkeypatch.setenv("SANDBOX_ID", "a" * 32)
    monkeypatch.setenv("POD_UID", "pod-uid")

    def credential(path: Path, **kwargs: Any) -> str:
        assert path == Path("/run/credential/token")
        return TOKEN

    monkeypatch.setattr(Path, "read_text", credential)
    return WorkerServer(broker=True)


def request(method: str, path: str, authorization: str = "") -> Mock:
    result = Mock(spec=web.Request)
    result.headers = {"Authorization": authorization}
    result.method = method
    result.path = path
    result.query = {}
    result.read = AsyncMock(side_effect=AssertionError("unauthenticated body was read"))
    return result


@pytest.mark.parametrize("authorization", ["", "Bearer wrong", "Bearer 非ASCII凭据"])
@pytest.mark.parametrize(
    "handler,method,path",
    [
        ("health", "GET", "/healthz"),
        ("operate", "POST", "/v1/exec"),
        ("operate", "GET", "/v1/files"),
        ("operate", "PUT", "/v1/files"),
        ("operate", "GET", "/v1/dirs"),
        ("cancel", "POST", "/v1/cancel"),
    ],
)
async def test_every_endpoint_authenticates_before_body_or_executor_access(
    broker: WorkerServer,
    monkeypatch: pytest.MonkeyPatch,
    authorization: str,
    handler: str,
    method: str,
    path: str,
) -> None:
    health = AsyncMock(side_effect=AssertionError("unauthenticated executor access"))
    execute = AsyncMock(side_effect=AssertionError("unauthenticated journal mutation"))
    cancel = AsyncMock(side_effect=AssertionError("unauthenticated cancellation"))
    monkeypatch.setattr(broker, "_executor_health", health)
    monkeypatch.setattr(broker.journal, "execute", execute)
    monkeypatch.setattr(broker.journal, "cancel", cancel)
    incoming = request(method, path, authorization)

    with pytest.raises(web.HTTPUnauthorized) as error:
        await getattr(broker, handler)(incoming)

    assert error.value.text == "unauthorized"
    incoming.read.assert_not_awaited()
    health.assert_not_awaited()
    execute.assert_not_awaited()
    cancel.assert_not_awaited()


@pytest.mark.parametrize("mismatch", ["sandbox_id", "pod_uid", "generation"])
async def test_valid_token_cannot_dispatch_for_another_workspace_generation(
    broker: WorkerServer,
    monkeypatch: pytest.MonkeyPatch,
    mismatch: str,
) -> None:
    op = Operation(
        identity=Identity(sandbox_id="a" * 32, run_id="root", tool_call_id="call"),
        pod_uid="pod-uid",
        generation=broker.generation,
        operation="exec",
        command="true",
    )
    if mismatch == "sandbox_id":
        op = op.model_copy(
            update={
                "identity": op.identity.model_copy(
                    update={"sandbox_id": "c" * 32},
                )
            }
        )
    else:
        op = op.model_copy(update={mismatch: "other-generation"})
    incoming = request("POST", "/v1/exec", "Bearer " + TOKEN)
    incoming.read = AsyncMock(return_value=op.model_dump_json().encode())
    execute = AsyncMock(side_effect=AssertionError("mismatched identity was dispatched"))
    monkeypatch.setattr(broker.journal, "execute", execute)

    with pytest.raises(web.HTTPConflict) as error:
        await broker.operate(incoming)

    assert json.loads(error.value.text)["error_type"] == "workspace_lost"
    execute.assert_not_awaited()


async def test_authorized_health_returns_identity_without_credentials(
    broker: WorkerServer,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(broker, "_executor_health", AsyncMock(return_value={}))
    response = await broker.health(request("GET", "/healthz", "Bearer " + TOKEN))
    assert response.status == 200
    assert response.text is not None
    assert json.loads(response.text) == {
        "protocol": "1",
        "sandbox_id": "a" * 32,
        "pod_uid": "pod-uid",
        "generation": broker.generation,
    }
    assert TOKEN not in response.text


async def test_lower_command_deadline_does_not_reject_file_operations(
    broker: WorkerServer,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    broker.command_timeout_s = 10
    op = Operation(
        identity=Identity(sandbox_id="a" * 32, run_id="root", tool_call_id="read"),
        pod_uid="pod-uid",
        generation=broker.generation,
        operation="read_text",
        path="value",
    )
    incoming = request("GET", "/v1/files", "Bearer " + TOKEN)
    incoming.query = {"path": "value"}
    incoming.read = AsyncMock(return_value=op.model_dump_json().encode())
    execute = AsyncMock(return_value=b'{"content":"value","is_error":false}')
    monkeypatch.setattr(broker.journal, "execute", execute)

    response = await broker.operate(incoming)

    assert response.status == 200
    execute.assert_awaited_once()
