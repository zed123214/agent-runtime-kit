from __future__ import annotations

import asyncio
import sqlite3
import sys
from contextlib import closing
from pathlib import Path

import pytest

pytest.importorskip("langgraph")

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from recovery_helpers.harness import (  # noqa: E402
    DaemonProcess,
    RecoveryClient,
    _approval_params,
    _read_jsonl,
    guid_data_root,
)
from recovery_helpers.scenario_runtime import TOOL_USE_ID  # noqa: E402

from agent_runtime.core.graph.recovery import RecoveryStore  # noqa: E402
from agent_runtime.core.graph.runtime import GraphRuntime  # noqa: E402

pytestmark = pytest.mark.recovery


async def test_two_real_clients_keep_durable_sessions_isolated(tmp_path: Path) -> None:
    data_root = guid_data_root(tmp_path)
    control_dir = data_root / "control"
    control_dir.mkdir(parents=True)
    daemon = DaemonProcess(data_root, control_dir, "permission_interrupt", "A")
    clients = [RecoveryClient(daemon.port), RecoveryClient(daemon.port)]
    send_tasks: list[asyncio.Task[dict[str, object]]] = []
    session_ids: list[str] = []
    run_ids: list[str] = []
    final_states: list[dict[str, object]] = []

    try:
        await daemon.start()
        await asyncio.gather(*(client.connect() for client in clients))
        created_sessions = await asyncio.gather(
            *(
                client.send(
                    "session.create",
                    {"mode": "chat", "title": f"isolation-{index}"},
                )
                for index, client in enumerate(clients)
            )
        )
        session_ids = [str(created["session_id"]) for created in created_sessions]
        assert len(set(session_ids)) == 2

        send_tasks = [
            client.send_task(
                "session.send_message",
                {"session_id": session_id, "content": f"isolate {session_id}"},
            )
            for client, session_id in zip(clients, session_ids, strict=True)
        ]
        started_events = await asyncio.gather(
            *(client.wait_event("run.started") for client in clients)
        )
        run_ids = [str(event["run_id"]) for event in started_events]
        assert len(set(run_ids)) == 2
        assert [event.get("session_id") for event in started_events] == session_ids

        await asyncio.gather(
            *(
                client.wait_event("run.suspended", run_id=run_id)
                for client, run_id in zip(clients, run_ids, strict=True)
            )
        )
        await asyncio.gather(*send_tasks)
        states = await asyncio.gather(
            *(
                client.send(
                    "run.get_state",
                    {"session_id": session_id, "run_id": run_id},
                )
                for client, session_id, run_id in zip(
                    clients,
                    session_ids,
                    run_ids,
                    strict=True,
                )
            )
        )
        assert all(state["suspension_reason"] == "permission" for state in states)

        approvals = await asyncio.gather(
            *(
                client.send("permission.respond", _approval_params(state), timeout=40.0)
                for client, state in zip(clients, states, strict=True)
            )
        )
        assert all(response["ok"] is True for response in approvals)
        await asyncio.gather(
            *(
                client.wait_event("run.finished", run_id=run_id)
                for client, run_id in zip(clients, run_ids, strict=True)
            )
        )
        final_states = await asyncio.gather(
            *(
                client.send(
                    "run.get_state",
                    {"session_id": session_id, "run_id": run_id},
                )
                for client, session_id, run_id in zip(
                    clients,
                    session_ids,
                    run_ids,
                    strict=True,
                )
            )
        )
    finally:
        for task in send_tasks:
            if not task.done():
                task.cancel()
        if send_tasks:
            await asyncio.gather(*send_tasks, return_exceptions=True)
        await asyncio.gather(*(client.close() for client in clients))
        if daemon.process is not None and daemon.process.returncode is None:
            await daemon.kill()

    assert all(state["status"] == "success" for state in final_states)
    for client, session_id in zip(clients, session_ids, strict=True):
        owned_session_ids = {
            str(event["session_id"])
            for event in client.events
            if event.get("session_id") is not None
        }
        assert owned_session_ids == {session_id}

    with closing(sqlite3.connect(data_root / "recovery.sqlite3")) as connection:
        journal_rows = connection.execute(
            "SELECT session_id, run_id, tool_use_id, status FROM tool_invocations "
            "ORDER BY session_id, run_id"
        ).fetchall()
    assert set(journal_rows) == {
        (session_id, run_id, TOOL_USE_ID, "completed")
        for session_id, run_id in zip(session_ids, run_ids, strict=True)
    }
    assert len(_read_jsonl(control_dir / "tool_calls.jsonl")) == 2

    recovery_store = RecoveryStore(data_root / "recovery.sqlite3")
    runtime = GraphRuntime(
        checkpoint_backend="sqlite",
        sqlite_path=data_root / "graph-checkpoints.sqlite3",
        recovery_store=recovery_store,
    )
    try:
        checkpoint_states = await asyncio.gather(
            *(
                runtime.latest_state(
                    session_id,
                    session_id=session_id,
                    run_id=run_id,
                )
                for session_id, run_id in zip(session_ids, run_ids, strict=True)
            )
        )
    finally:
        await runtime.close()
    assert all(state is not None and state.status == "success" for state in checkpoint_states)
