from __future__ import annotations

import asyncio
import json
import socket

from agent_runtime.core.transport.socket_server import (
    SocketServer,
    get_connection_writer,
    redact_sensitive_fields,
)


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def test_resume_capabilities_are_recursively_redacted_from_trace_payloads() -> None:
    raw = {
        "params": {"resume_token": "secret-current"},
        "result": [{"next_resume_token": "secret-next", "token": "visible-llm-token"}],
    }

    redacted = redact_sensitive_fields(raw)

    assert redacted == {
        "params": {"resume_token": "[REDACTED]"},
        "result": [{"next_resume_token": "[REDACTED]", "token": "visible-llm-token"}],
    }
    assert raw["params"]["resume_token"] == "secret-current"


def test_tool_credentials_are_redacted_from_trace_payloads() -> None:
    raw = {
        "params": {
            "api_key": "SECRET-KEY",
            "command": "curl -H 'Authorization: Bearer SECRET-TOKEN' example.test",
            "nested": {"password": "SECRET-PASSWORD"},
        }
    }

    redacted = redact_sensitive_fields(raw)

    rendered = repr(redacted)
    assert "SECRET-KEY" not in rendered
    assert "SECRET-TOKEN" not in rendered
    assert "SECRET-PASSWORD" not in rendered
    assert rendered.count("[REDACTED]") == 3


# 功能：验证客户端断开后 SocketServer 调用 broadcaster.disconnect(writer) 清理订阅
# 设计：用内联 MockBroadcaster 捕获 disconnect 调用并设置 asyncio.Event，避免 sleep 轮询；
#       等待 Event 而非断言调用次数，确保时序正确性而不依赖竞态假设
async def test_broadcaster_unsubscribe_called_on_disconnect() -> None:
    unsubscribed = asyncio.Event()

    class MockBroadcaster:
        def mark_disconnected(self, writer: object) -> None:
            pass

        def disconnect(self, writer: object) -> None:
            unsubscribed.set()

    port = _free_port()
    server = SocketServer("127.0.0.1", port, broadcaster=MockBroadcaster())  # type: ignore[arg-type]
    await server.start()

    try:
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        writer.close()
        await writer.wait_closed()

        await asyncio.wait_for(unsubscribed.wait(), timeout=2.0)
    finally:
        await server.stop()


# 功能：验证不传入 broadcaster 时 SocketServer 仍可正常启动和停止（backward-compatible 默认值）
# 设计：直接实例化 SocketServer(host, port)（无 broadcaster），start/stop 不抛异常即为通过；
#       回归测试确保新参数的默认值 None 不破坏现有调用方
async def test_no_broadcaster_server_starts_and_stops() -> None:
    port = _free_port()
    server = SocketServer("127.0.0.1", port)
    await server.start()
    await server.stop()


async def test_disconnect_cancels_handlers_before_cleanup_and_blocks_late_bind() -> None:
    from agent_runtime.core.transport.ipc_broadcaster import IpcEventBroadcaster

    order: list[str] = []
    handler_entered = asyncio.Event()
    handler_cancelled = asyncio.Event()
    disconnected = asyncio.Event()

    class RecordingBroadcaster(IpcEventBroadcaster):
        def disconnect(self, writer: asyncio.StreamWriter) -> frozenset[str]:
            order.append("disconnect")
            session_ids = super().disconnect(writer)
            disconnected.set()
            return session_ids

    broadcaster = RecordingBroadcaster()

    async def blocking_handler(params: dict[str, object]) -> dict[str, bool]:
        writer = get_connection_writer()
        assert broadcaster.bind_session(writer, "owned") is True
        handler_entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            order.append("cancelled")
            assert broadcaster.bind_session(writer, "late") is False
            handler_cancelled.set()

    port = _free_port()
    server = SocketServer("127.0.0.1", port, broadcaster=broadcaster)
    server.register("block", blocking_handler)
    await server.start()

    try:
        _reader, writer = await asyncio.open_connection("127.0.0.1", port)
        writer.write(
            (
                json.dumps({"jsonrpc": "2.0", "id": "1", "method": "block", "params": {}}) + "\n"
            ).encode()
        )
        await writer.drain()
        await asyncio.wait_for(handler_entered.wait(), timeout=2.0)

        writer.close()
        await asyncio.wait_for(writer.wait_closed(), timeout=2.0)
        await asyncio.wait_for(handler_cancelled.wait(), timeout=2.0)
        await asyncio.wait_for(disconnected.wait(), timeout=2.0)
        assert order == ["cancelled", "disconnect"]
    finally:
        await server.stop()
