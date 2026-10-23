import asyncio
from typing import Any

import pytest

from agent_runtime.core.transport.socket_client import SocketClient
from agent_runtime.web.session import WebSession


class DelayedResultClient(SocketClient):
    def __init__(self) -> None:
        super().__init__("127.0.0.1", 1)
        self.reading = asyncio.Event()
        self.release = asyncio.Event()
        self.history_reads = 0

    async def send_command(self, method: str, params: dict[str, Any]) -> dict[str, Any]:
        if method == "run.get_result":
            return {
                "run_id": params["run_id"],
                "commit_state": "committed",
                "final_answer": "answer",
            }
        assert method == "session.get_history"
        self.history_reads += 1
        self.reading.set()
        await self.release.wait()
        return {"messages": [{"role": "assistant", "content": "answer"}]}


def session_for(client: SocketClient) -> WebSession:
    session = WebSession("web", "instance", client, {})
    session.core_id = "core"
    session.projection.meta(current_run_id="run", state="sync_error")
    return session


async def test_concurrent_sync_is_shared_and_http_cancellation_does_not_cancel_it() -> None:
    client = DelayedResultClient()
    session = session_for(client)
    first = asyncio.create_task(session.synchronize("run"))
    await client.reading.wait()
    second = asyncio.create_task(session.synchronize("run"))
    await asyncio.sleep(0)
    assert session.state == "syncing"
    first.cancel()
    with pytest.raises(asyncio.CancelledError):
        await first
    client.release.set()
    await second
    assert session.state == "ready"
    assert client.history_reads == 1
    assert session.projection.snapshot()["reports"]["run"]["result"]["final_answer"] == "answer"


async def test_late_sync_does_not_overwrite_closing_state() -> None:
    client = DelayedResultClient()
    session = session_for(client)
    sync = asyncio.create_task(session.synchronize("run"))
    await client.reading.wait()
    session.projection.meta(state="closing")
    client.release.set()
    await sync
    assert session.state == "closing"
    assert session.projection.snapshot()["reports"] == {}


async def test_lost_permission_change_response_reads_actual_core_state() -> None:
    class LostReplyClient(SocketClient):
        async def send_command(self, method: str, params: dict[str, Any]) -> dict[str, Any]:
            if method == "session.set_permissions":
                raise TimeoutError("response lost after Core accepted change")
            assert method == "session.get_permissions"
            return {"mode": "auto", "allowed_tools": [], "revision": 2}

    session = session_for(LostReplyClient("127.0.0.1", 1))
    with pytest.raises(TimeoutError):
        await session.configure_permissions({"mode": "auto", "expected_revision": 1})
    assert session.projection.snapshot()["permissions"]["mode"] == "auto"
    session._permissions({"mode": "ask", "allowed_tools": [], "revision": 1})
    assert session.projection.snapshot()["permissions"]["mode"] == "auto"
