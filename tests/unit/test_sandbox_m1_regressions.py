"""Behavioral regressions for the M1 review, required before M2 validation."""

from __future__ import annotations

import asyncio
import json
import runpy
import shlex
import sys
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, Mock

import pytest

from agent_runtime.core.app import CoreApp
from agent_runtime.core.config import RuntimeConfig
from agent_runtime.core.events.bus import EventBus
from agent_runtime.core.llm.types import LlmResponse, ToolCallBlock
from agent_runtime.core.runner import AgentRunner
from agent_runtime.core.sandbox import SandboxKey, SandboxManager, SandboxSpec
from agent_runtime.core.session.manager import SessionManager
from agent_runtime.core.session.store import SessionStore
from agent_runtime.core.transport.socket_server import SocketServer
from agent_runtime.sandbox_server.executor import Executor
from agent_runtime.sandbox_server.files import Workspace
from agent_runtime.sandbox_server.protocol import Identity, Operation
from tests.unit.test_sandbox_remote import Backend


def operation(**changes: Any) -> Operation:
    values = {
        "identity": Identity(sandbox_id="a" * 32, run_id="root", tool_call_id="call"),
        "pod_uid": "pod-uid",
        "generation": "generation",
        "operation": "exec",
        "command": "true",
    }
    values.update(changes)
    return Operation.model_validate(values)


@pytest.fixture
def executor(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Any:
    real_spawn = asyncio.create_subprocess_exec

    async def spawn(*args: Any, **kwargs: Any) -> asyncio.subprocess.Process:
        assert kwargs["cwd"] == "/workspace"
        kwargs["cwd"] = str(tmp_path)
        return await real_spawn(*args, **kwargs)

    monkeypatch.setattr(asyncio, "create_subprocess_exec", spawn)
    workspace = Workspace(str(tmp_path))
    try:
        yield Executor(workspace)
    finally:
        workspace.close()


async def test_exited_shell_does_not_wait_for_background_pipe(executor: Executor) -> None:
    result = await executor.execute(operation(command="sleep 60 &", timeout_s=0.3))
    assert result["terminal_reason"] == "completed"
    assert result["exit_code"] == 0


async def test_exited_shell_cannot_leave_a_delayed_writer(
    executor: Executor,
    tmp_path: Path,
) -> None:
    result = await executor.execute(
        operation(
            command="(sleep 0.4; printf leaked > marker) &",
            timeout_s=1.0,
        )
    )
    assert result["terminal_reason"] == "completed"
    await asyncio.sleep(0.5)
    assert not (tmp_path / "marker").exists()


@pytest.mark.parametrize("stream", ["stdout", "stderr"])
async def test_replacement_expansion_is_reported_as_truncation(
    executor: Executor,
    stream: str,
) -> None:
    script = f"import sys; sys.{stream}.buffer.write(bytes([255]) * 30000)"
    result = await executor.execute(
        operation(
            command=f"{shlex.quote(sys.executable)} -c {shlex.quote(script)}",
        )
    )
    assert result["exit_code"] == 0
    assert len(result[stream].encode("utf-8")) <= 65536
    assert result["truncated"] is True
    assert "[output truncated at 64 KiB]" in result["content"]


@pytest.mark.parametrize("file_operation", ["read_text", "write_text", "list_dir"])
async def test_short_command_budget_does_not_cancel_file_dispatch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    file_operation: str,
) -> None:
    web = pytest.importorskip("aiohttp.web")
    from aiohttp import UnixConnector

    from agent_runtime.sandbox_server import server as server_module

    monkeypatch.setenv("SANDBOX_ID", "a" * 32)
    monkeypatch.setenv("POD_UID", "pod-uid")
    monkeypatch.setenv("COMMAND_TIMEOUT_S", "0.1")
    monkeypatch.setenv("CLEANUP_MARGIN_S", "0.1")
    token = "b" * 43
    read_text = Path.read_text

    def credential(path: Path, *args: Any, **kwargs: Any) -> str:
        if path == Path("/run/credential/token"):
            return token
        return read_text(path, *args, **kwargs)

    monkeypatch.setattr(Path, "read_text", credential)
    socket_path = tmp_path / "executor.sock"

    def connector(**kwargs: Any) -> UnixConnector:
        kwargs["path"] = str(socket_path)
        return UnixConnector(**kwargs)

    monkeypatch.setattr(server_module, "UnixConnector", connector)

    async def health(request: Any) -> Any:
        return web.json_response(
            {"sandbox_id": "a" * 32, "pod_uid": "pod-uid", "generation": "executor-generation"}
        )

    async def delayed_file(request: Any) -> Any:
        forwarded = Operation.model_validate(await request.json())
        assert forwarded.operation == file_operation
        assert forwarded.generation == "executor-generation"
        # Exceeds the old 0.1 + 0.1 + 2 command-derived transport deadline,
        # while remaining well inside the actual file operation budget.
        await asyncio.sleep(2.4)
        return web.json_response({"content": "file-result", "is_error": False})

    async def cancel(request: Any) -> Any:
        return web.json_response({"cancelled": True})

    executor_app = web.Application()
    executor_app.router.add_get("/healthz", health)
    executor_app.router.add_post("/operate", delayed_file)
    executor_app.router.add_post("/cancel", cancel)
    server_runner = web.AppRunner(executor_app, handler_cancellation=True)
    await server_runner.setup()
    broker = server_module.WorkerServer(broker=True)
    broker_app = broker.app()
    try:
        await web.UnixSite(server_runner, str(socket_path)).start()
        await broker.start(broker_app)
        op = operation(operation=file_operation, generation=broker.generation, path="value")
        method, endpoint = {
            "read_text": ("GET", "/v1/files"),
            "write_text": ("PUT", "/v1/files"),
            "list_dir": ("GET", "/v1/dirs"),
        }[file_operation]
        incoming = Mock(spec=web.Request)
        incoming.headers = {"Authorization": "Bearer " + token}
        incoming.method, incoming.path = method, endpoint
        incoming.query = {"path": "value"}
        incoming.read = AsyncMock(return_value=op.model_dump_json().encode())
        response = await broker.operate(incoming)
        payload = json.loads(response.body)
        assert payload == {"content": "file-result", "is_error": False}
        assert not broker.journal.closed
    finally:
        await broker.close(broker_app)
        await server_runner.cleanup()


@pytest.mark.parametrize("scope", ["123", "true", "null", "1e3"])
def test_rendered_scope_keeps_its_string_type(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    scope: str,
) -> None:
    yaml = pytest.importorskip("yaml")
    script = Path(__file__).resolve().parents[2] / "scripts" / "render_kubernetes.py"
    image = "example/worker@sha256:" + "a" * 64
    monkeypatch.setattr(
        sys,
        "argv",
        [
            str(script),
            "--scope",
            scope,
            "--worker-image",
            image,
            "--core-image",
            image,
            "--validation-image",
            image,
            "--output",
            str(tmp_path),
        ],
    )
    runpy.run_path(str(script), run_name="__main__")
    for name in ("core.yaml", "validation/job.yaml"):
        resource = next(yaml.safe_load_all((tmp_path / name).read_text()))
        pod = resource["spec"]["template"]
        assert pod["metadata"]["labels"]["sandbox.agentrt.dev/scope"] == scope
        env = {item["name"]: item.get("value") for item in pod["spec"]["containers"][0]["env"]}
        assert env["AGENTRT_SANDBOX_DEPLOYMENT_SCOPE"] == scope
    policies = list(yaml.safe_load_all((tmp_path / "network-policy.yaml").read_text()))
    peer = policies[1]["spec"]["ingress"][0]["from"][0]
    assert peer["podSelector"]["matchLabels"]["sandbox.agentrt.dev/scope"] == scope


async def test_chat_after_ttl_reports_workspace_lost_and_finishes_run(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    backend = Backend()
    manager = SandboxManager(backend, spec=SandboxSpec(backend="kubernetes"), idle_timeout_s=900)
    calls: list[str] = []

    class Provider:
        async def chat(
            self, messages: Any, tool_schemas: Any, bus: Any, run_id: str, **kwargs: Any
        ) -> LlmResponse:
            first = run_id not in calls
            calls.append(run_id)
            if first:
                return LlmResponse(
                    stop_reason="tool_use",
                    tool_calls=[
                        ToolCallBlock(id="command", name="bash", input={"command": "true"}),
                    ],
                )
            return LlmResponse(stop_reason="end_turn", text="done")

    store = SessionStore(tmp_path / "sessions")
    sessions = SessionManager(
        store,
        lambda: AgentRunner(RuntimeConfig(), provider=Provider(), sandbox_manager=manager),
        EventBus(),
        on_session_closed=lambda sid: manager.release(SandboxKey("session", sid)),
    )
    app = CoreApp(sandbox_manager=manager)
    app._sessions = sessions
    monkeypatch.setattr(app, "_require_current_connection_owns", lambda sid: None)
    server = SocketServer("127.0.0.1", 0)
    server.register("session.send_message", app._session_send_handler)
    session = await sessions.create("chat")
    try:
        await sessions.send_message(session.id, "first", run_id="first-run")
        key = SandboxKey("session", session.id)
        manager._entries[key].last_completed -= 901
        await manager.reap_idle()
        writer = Mock()
        writer.drain = AsyncMock()
        await server._handle_line(
            json.dumps(
                {
                    "jsonrpc": "2.0",
                    "id": "next-turn",
                    "method": "session.send_message",
                    "params": {"session_id": session.id, "content": "after TTL"},
                }
            ).encode(),
            writer,
        )
        reply = json.loads(writer.write.call_args.args[0])
        assert "error" not in reply, reply
        run_id = reply["result"]["run_id"]
        rows = [
            json.loads(line)
            for line in (store.runs_dir(session.id) / run_id / "events.jsonl")
            .read_text()
            .splitlines()
        ]
        terminal = [row for row in rows if row["type"] == "run.finished"]
        assert len(terminal) == 1
        assert terminal[0]["status"] == "failed"
        assert terminal[0]["error"]["code"] == "workspace_lost"
        assert "new Session" in terminal[0]["error"]["message"]
        assert session.active_run_id is None
        assert store.read_meta(session.id).active_run_id is None
        assert session.status == "waiting_for_input"
        assert backend.created == 1 and calls == ["first-run", "first-run"]
        assert not any(row["type"] == "sandbox.creating" for row in rows)
    finally:
        await sessions.close(session.id)
        await manager.close()
