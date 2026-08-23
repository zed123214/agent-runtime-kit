from __future__ import annotations

import asyncio
import json
import subprocess


# 发送一条 JSON-RPC 请求并返回响应对象
async def _send_recv(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    method: str,
    params: dict,
    req_id: str = "1",
) -> dict:
    req = {"jsonrpc": "2.0", "id": req_id, "method": method, "params": params}
    writer.write((json.dumps(req) + "\n").encode())
    await writer.drain()
    line = await asyncio.wait_for(reader.readline(), timeout=5.0)
    return json.loads(line)


# 功能：验证 daemon 暴露 session.create、session.get_history、session.close 三个 S4 IPC 命令
# 设计：不触发 session.send_message，避免真实 LLM 依赖；只验证 CoreApp handler 注册、协议序列化和 session 状态持久化
async def test_session_create_history_close_over_ipc(
    running_daemon: subprocess.Popen[bytes],
    free_port: int,
) -> None:
    reader, writer = await asyncio.open_connection("127.0.0.1", free_port)

    created = await _send_recv(
        reader,
        writer,
        "session.create",
        {"mode": "chat", "title": "ipc test"},
        req_id="create",
    )
    assert "result" in created, created
    session_id = created["result"]["session_id"]
    assert created["result"]["status"] == "active"

    history = await _send_recv(
        reader,
        writer,
        "session.get_history",
        {"session_id": session_id},
        req_id="history",
    )
    assert history["result"]["messages"] == []

    closed = await _send_recv(
        reader,
        writer,
        "session.close",
        {"session_id": session_id},
        req_id="close",
    )
    assert closed["result"]["status"] == "closed"

    writer.close()
    await writer.wait_closed()


async def test_two_clients_cannot_operate_on_each_others_session(
    running_daemon: subprocess.Popen[bytes],
    free_port: int,
) -> None:
    owner_reader, owner_writer = await asyncio.open_connection("127.0.0.1", free_port)
    other_reader, other_writer = await asyncio.open_connection("127.0.0.1", free_port)

    try:
        created = await _send_recv(
            owner_reader,
            owner_writer,
            "session.create",
            {"mode": "chat", "title": "private"},
            req_id="create-private",
        )
        session_id = created["result"]["session_id"]

        attacks = [
            ("session.get_history", {"session_id": session_id}),
            ("session.send_message", {"session_id": session_id, "content": "steal"}),
            ("session.close", {"session_id": session_id}),
            ("session.compact", {"session_id": session_id, "focus": "steal"}),
        ]
        for index, (method, params) in enumerate(attacks):
            response = await _send_recv(
                other_reader,
                other_writer,
                method,
                params,
                req_id=f"attack-{index}",
            )
            assert response["error"]["code"] == -32010
            assert response["error"]["message"] == "session not found"

        missing = await _send_recv(
            other_reader,
            other_writer,
            "session.get_history",
            {"session_id": "missing"},
            req_id="missing",
        )
        assert missing["error"] == response["error"]

        owner_history = await _send_recv(
            owner_reader,
            owner_writer,
            "session.get_history",
            {"session_id": session_id},
            req_id="owner-history",
        )
        assert owner_history["result"]["messages"] == []
    finally:
        owner_writer.close()
        other_writer.close()
        await asyncio.wait_for(owner_writer.wait_closed(), timeout=2.0)
        await asyncio.wait_for(other_writer.wait_closed(), timeout=2.0)
