from __future__ import annotations

import asyncio
import os
import subprocess
import sys
from contextlib import asynccontextmanager
from pathlib import Path

import httpx
import pytest

pytest.importorskip("fastapi")

from agent_runtime.web.app import create_app
from agent_runtime.web.gateway import Gateway


@pytest.fixture
async def web_core(tmp_path: Path, free_port: int):
    env = os.environ.copy()
    env["PYTHONPATH"] = str(Path(__file__).resolve().parents[2] / "src")
    process = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "agent_runtime.web.demo",
            "--port",
            str(free_port),
            "--directory",
            str(tmp_path / "demo"),
        ],
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
    )
    try:
        for _ in range(200):
            try:
                _, writer = await asyncio.open_connection("127.0.0.1", free_port)
                writer.close()
                await writer.wait_closed()
                break
            except OSError:
                if process.poll() is not None:
                    pytest.fail(process.stderr.read().decode())
                await asyncio.sleep(0.05)
        else:
            pytest.fail("Demo Core did not bind")
        yield free_port, tmp_path / "demo"
    finally:
        process.terminate()
        await asyncio.to_thread(process.wait, 10)


@asynccontextmanager
async def client_for(gateway: Gateway):
    app = create_app(gateway, origins={"http://testserver"})
    client = httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://testserver")
    response = await client.post(
        "/api/bootstrap",
        json={"token": gateway.bootstrap_token},
        headers={"Origin": "http://testserver"},
    )
    assert response.status_code == 200
    client.headers.update({"Origin": "http://testserver", "X-CSRF-Token": response.json()["csrf"]})
    try:
        yield client
    finally:
        await client.aclose()


async def wait_snapshot(client: httpx.AsyncClient, sid: str, predicate):
    for _ in range(300):
        response = await client.get(f"/api/sessions/{sid}/snapshot")
        assert response.status_code == 200
        snapshot = response.json()
        if predicate(snapshot):
            return snapshot
        await asyncio.sleep(0.02)
    pytest.fail(f"Timed out waiting for snapshot: {snapshot['state']}")


async def create_session(client: httpx.AsyncClient, request_id: str = "create") -> str:
    response = await client.post("/api/sessions", json={"request_id": request_id})
    assert response.status_code == 202
    for _ in range(100):
        record = (await client.get(f"/api/requests/{request_id}")).json()
        if record.get("session_id"):
            return record["session_id"]
        assert record["state"] != "failed", record
        await asyncio.sleep(0.02)
    pytest.fail("Create not completed")


async def test_session_tool_grants_survive_turns_and_refresh_then_can_be_revoked(web_core) -> None:
    gateway = Gateway(core_port=web_core[0])
    async with client_for(gateway) as client:
        try:
            sid = await create_session(client)
            await client.post(
                f"/api/sessions/{sid}/messages", json={"request_id": "first", "content": "检查"}
            )
            granted: set[str] = set()
            for _ in range(300):
                snapshot = (await client.get(f"/api/sessions/{sid}/snapshot")).json()
                for aid, approval in snapshot["approvals"].items():
                    if approval["status"] == "pending":
                        assert approval["name"] not in granted
                        result = await client.post(
                            f"/api/sessions/{sid}/approvals/{aid}",
                            json={"decision": "allow_session"},
                        )
                        assert result.json()["ok"]
                        granted.add(approval["name"])
                if snapshot["state"] == "ready":
                    break
                await asyncio.sleep(0.02)
            else:
                pytest.fail("First turn did not complete")
            assert {"bash", "write_file"} <= granted
            assert snapshot["permissions"]["allowed_tools"] == sorted(granted)
            count = len(snapshot["approvals"])
            await client.post(
                f"/api/sessions/{sid}/messages", json={"request_id": "second", "content": "检查"}
            )
            done = await wait_snapshot(client, sid, lambda s: s["state"] == "ready")
            assert len(done["approvals"]) == count
            assert next(reversed(done["reports"].values()))["result"]["status"] == "success"
            refreshed, queue = gateway.session(sid).subscribe()
            assert refreshed["permissions"]["allowed_tools"] == sorted(granted)
            gateway.session(sid).unsubscribe(queue)
            result = await client.post(
                f"/api/sessions/{sid}/permissions",
                json={
                    "mode": "ask",
                    "expected_revision": done["permissions"]["revision"],
                    "revoke_tool": "bash",
                },
            )
            assert result.status_code == 200
            assert "bash" not in result.json()["allowed_tools"]
            await client.post(
                f"/api/sessions/{sid}/messages", json={"request_id": "third", "content": "检查"}
            )
            await wait_snapshot(
                client,
                sid,
                lambda s: any(
                    a["status"] == "pending" and a["name"] == "bash"
                    for a in s["approvals"].values()
                ),
            )
            other = await create_session(client, "other")
            snapshot = (await client.get(f"/api/sessions/{other}/snapshot")).json()
            assert snapshot["permissions"] == {"mode": "ask", "allowed_tools": [], "revision": 0}
        finally:
            await gateway.shutdown()


async def test_mode_change_resolves_waiting_call_and_readonly_blocks_side_effects(web_core) -> None:
    port, directory = web_core
    gateway = Gateway(core_port=port)
    async with client_for(gateway) as client:
        try:
            sid = await create_session(client)
            await client.post(
                f"/api/sessions/{sid}/messages", json={"request_id": "run", "content": "检查"}
            )
            await wait_snapshot(client, sid, lambda s: s["state"] == "approval")
            path = f"/api/sessions/{sid}/permissions"
            response = await client.post(path, json={"mode": "auto", "expected_revision": 0})
            assert response.status_code == 200
            done = await wait_snapshot(client, sid, lambda s: s["state"] == "ready")
            assert next(iter(done["reports"].values()))["result"]["status"] == "success"
            assert any(tool.get("decision") == "session_auto" for tool in done["tools"].values())
            await client.post(
                f"/api/sessions/{sid}/messages",
                json={"request_id": "childrun", "content": "检查，使用子任务"},
            )
            child_done = await wait_snapshot(client, sid, lambda s: s["state"] == "ready")
            assert any(
                tool["name"] == "spawn_agent" and tool.get("decision") == "session_auto"
                for tool in child_done["tools"].values()
            )
            assert not any(a["status"] == "pending" for a in child_done["approvals"].values())
            assert (
                await client.post(path, json={"mode": "ask", "expected_revision": 0})
            ).status_code == 409
            assert (await client.get(f"/api/sessions/{sid}/snapshot")).json()["permissions"][
                "mode"
            ] == "auto"
            readonly = await create_session(client, "readonly")
            await client.post(
                f"/api/sessions/{readonly}/permissions",
                json={"mode": "read_only", "expected_revision": 0},
            )
            before = (directory / "check-count.txt").read_text()
            await client.post(
                f"/api/sessions/{readonly}/messages",
                json={"request_id": "readrun", "content": "检查"},
            )
            done = await wait_snapshot(client, readonly, lambda s: s["state"] == "ready")
            assert done["approvals"] == {}
            assert any(tool.get("decision") == "read_only" for tool in done["tools"].values())
            assert (directory / "check-count.txt").read_text() == before
        finally:
            await gateway.shutdown()


async def test_permission_settings_require_owner_csrf_and_validated_mode(web_core) -> None:
    from agent_runtime.core.transport.socket_client import SocketClient

    gateway = Gateway(core_port=web_core[0])
    async with client_for(gateway) as client:
        try:
            sid = await create_session(client)
            path = f"/api/sessions/{sid}/permissions"
            body = {"mode": "auto", "expected_revision": 0}
            assert (
                await client.post(path, json=body, headers={"X-CSRF-Token": "wrong"})
            ).status_code == 403
            assert (
                await client.post(path, json={**body, "mode": "unrestricted"})
            ).status_code == 422
            assert (
                await client.post("/api/sessions/foreign/permissions", json=body)
            ).status_code == 404
            intruder = SocketClient("127.0.0.1", web_core[0])
            await intruder.connect()
            reader = asyncio.create_task(intruder.run_event_loop())
            try:
                with pytest.raises(Exception, match="session not found"):
                    await intruder.send_command(
                        "session.set_permissions",
                        {**body, "session_id": gateway.session(sid).core_id},
                    )
            finally:
                await intruder.close()
                reader.cancel()
                await asyncio.gather(reader, return_exceptions=True)
            assert (await client.get(f"/api/sessions/{sid}/snapshot")).json()["permissions"][
                "mode"
            ] == "ask"
            session = gateway.session(sid)
            session.projection.put(
                "approvals", "clipped", {"id": "clipped", "status": "pending", "can_allow": False}
            )
            assert not await session.approve("clipped", "allow_session")
        finally:
            await gateway.shutdown()


async def test_denial_and_duplicate_submission_do_not_execute_tools(web_core) -> None:
    port, directory = web_core
    gateway = Gateway(core_port=port)
    async with client_for(gateway) as client:
        try:
            sid = await create_session(client)
            repeated = await client.post("/api/sessions", json={"request_id": "create"})
            assert repeated.json()["session_id"] == sid
            body = {"request_id": "send", "content": "检查样例仓库"}
            await client.post(f"/api/sessions/{sid}/messages", json=body)
            assert (
                await client.post(f"/api/sessions/{sid}/messages", json=body)
            ).status_code == 202
            conflict = await client.post(
                f"/api/sessions/{sid}/messages", json={**body, "content": "changed"}
            )
            assert conflict.status_code == 409
            busy = await client.post(
                f"/api/sessions/{sid}/messages", json={**body, "request_id": "other"}
            )
            assert busy.status_code == 409
            snapshot = await wait_snapshot(client, sid, lambda s: bool(s["approvals"]))
            aid = next(iter(snapshot["approvals"]))
            path = f"/api/sessions/{sid}/approvals/{aid}"
            results = await asyncio.gather(
                client.post(path, json={"decision": "deny_once"}),
                client.post(path, json={"decision": "allow_once"}),
            )
            assert sum(r.json().get("ok", False) for r in results) == 1
            done = await wait_snapshot(client, sid, lambda s: s["state"] == "ready")
            assert len(done["runs"]) == 1
            assert not (directory / "check-count.txt").exists()
        finally:
            await gateway.shutdown()


async def test_auth_origin_csrf_and_input_boundaries(web_core) -> None:
    gateway = Gateway(core_port=web_core[0])
    app = create_app(gateway, origins={"http://testserver"})
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app), base_url="http://testserver"
    ) as raw:
        assert (await raw.get("/api/sessions")).status_code == 401
        assert (
            await raw.post(
                "/api/bootstrap",
                json={"token": gateway.bootstrap_token},
                headers={"Origin": "http://evil.invalid"},
            )
        ).status_code == 403
    async with client_for(gateway) as client:
        try:
            assert (
                await client.post(
                    "/api/sessions", json={"request_id": "a"}, headers={"X-CSRF-Token": "wrong"}
                )
            ).status_code == 403
            assert (
                await client.get("/api/sessions", headers={"Host": "evil.invalid"})
            ).status_code == 403
            sid = await create_session(client)
            empty = await client.post(
                f"/api/sessions/{sid}/messages", json={"request_id": "empty", "content": "  "}
            )
            assert empty.status_code == 422
            assert (await client.get("/api/sessions/not-owned/snapshot")).status_code == 404
            invalid = await client.post(
                f"/api/sessions/{sid}/approvals/fake", json={"decision": "always_allow"}
            )
            assert invalid.status_code == 422
            assert "resume_token" not in (await client.get("/api/status")).text
        finally:
            await gateway.shutdown()


async def test_subscription_refresh_keeps_owner_and_slow_subscriber_isolated(web_core) -> None:
    gateway = Gateway(core_port=web_core[0])
    async with client_for(gateway) as client:
        try:
            sid = await create_session(client)
            session = gateway.session(sid)
            _, old_queue = session.subscribe()
            await client.post(
                f"/api/sessions/{sid}/messages", json={"request_id": "send", "content": "检查"}
            )
            before = await wait_snapshot(client, sid, lambda s: bool(s["approvals"]))
            session.unsubscribe(old_queue)
            restored, new_queue = session.subscribe()
            assert restored["current_run_id"] == before["current_run_id"]
            assert restored["approvals"] == before["approvals"]
            assert not session.disconnected
            _, slow = session.subscribe()
            for index in range(70):
                session.projection.meta(title=f"Update {index}")
                session.flush()
                new_queue.get_nowait()
            assert (await slow.get())["type"] == "resync"
            assert new_queue in session.subscribers
            assert not session.disconnected
            aid = next(iter(restored["approvals"]))
            await client.post(
                f"/api/sessions/{sid}/approvals/{aid}", json={"decision": "deny_once"}
            )
            await wait_snapshot(client, sid, lambda s: s["state"] == "ready")
        finally:
            await gateway.shutdown()


async def complete_turn(client: httpx.AsyncClient, sid: str, index: int, content: str = "检查"):
    await client.post(
        f"/api/sessions/{sid}/messages",
        json={"request_id": f"turn-{sid}-{index}", "content": content},
    )
    for _ in range(400):
        snapshot = (await client.get(f"/api/sessions/{sid}/snapshot")).json()
        for approval in snapshot["approvals"].values():
            if approval["status"] == "pending":
                await client.post(
                    f"/api/sessions/{sid}/approvals/{approval['id']}",
                    json={"decision": "allow_once"},
                )
        if snapshot["state"] == "ready":
            return snapshot
        assert snapshot["state"] not in {"lost", "closed", "sync_error"}, snapshot.get("error")
        await asyncio.sleep(0.015)
    pytest.fail("Turn did not finish")


async def test_twenty_turns_three_sessions_and_real_subagent_remain_isolated(web_core) -> None:
    gateway = Gateway(core_port=web_core[0])
    async with client_for(gateway) as client:
        try:
            ids = [await create_session(client, f"create-{i}") for i in range(3)]
            completed = await asyncio.gather(
                *(
                    complete_turn(client, sid, 0, "检查子任务" if index == 0 else "检查")
                    for index, sid in enumerate(ids)
                )
            )
            assert len({snapshot["current_run_id"] for snapshot in completed}) == 3
            children = [run for run in completed[0]["runs"].values() if run.get("parent_run_id")]
            assert len(children) == 1
            assert all("此回复属于独立" not in m["text"] for m in completed[0]["messages"].values())
            for index in range(1, 18):
                await complete_turn(client, ids[index % 3], index)
            checks = list((web_core[1] / "checks").iterdir())
            assert len(checks) == 20
            assert all(file.read_text() == "1" for file in checks)
            for sid in ids:
                snapshot = (await client.get(f"/api/sessions/{sid}/snapshot")).json()
                root_ids = {
                    run["id"] for run in snapshot["runs"].values() if not run.get("parent_run_id")
                }
                assert all(tool["root_id"] in root_ids for tool in snapshot["tools"].values())
        finally:
            await gateway.shutdown()


async def test_real_core_chat_tool_approval_and_committed_report(web_core) -> None:
    port, directory = web_core
    gateway = Gateway(core_port=port)
    async with client_for(gateway) as client:
        try:
            status = await client.get("/api/status")
            assert status.json()["core"]["engine"] == "loop"
            created = await client.post("/api/sessions", json={"request_id": "create-1"})
            assert created.status_code == 202
            for _ in range(100):
                record = (await client.get("/api/requests/create-1")).json()
                if record.get("session_id"):
                    break
                await asyncio.sleep(0.02)
            sid = record["session_id"]
            sent = await client.post(
                f"/api/sessions/{sid}/messages",
                json={"request_id": "send-1", "content": "检查样例仓库"},
            )
            assert sent.status_code == 202
            snapshot = await wait_snapshot(client, sid, lambda s: bool(s["approvals"]))
            approval = next(iter(snapshot["approvals"].values()))
            assert not (directory / "check-count.txt").exists()
            accepted = await client.post(
                f"/api/sessions/{sid}/approvals/{approval['id']}", json={"decision": "allow_once"}
            )
            assert accepted.json()["ok"] is True
            write_pending = await wait_snapshot(
                client,
                sid,
                lambda s: any(
                    a["status"] == "pending" and a["name"] == "write_file"
                    for a in s["approvals"].values()
                ),
            )
            write_approval = next(
                a for a in write_pending["approvals"].values() if a["status"] == "pending"
            )
            await client.post(
                f"/api/sessions/{sid}/approvals/{write_approval['id']}",
                json={"decision": "allow_once"},
            )
            done = await wait_snapshot(client, sid, lambda s: s["state"] == "ready")
            assert (directory / "check-count.txt").read_text() == "1"
            assert any(
                t["name"] == "bash" and t["status"] == "success" for t in done["tools"].values()
            )
            rid = done["current_run_id"]
            report = await client.get(f"/api/sessions/{sid}/runs/{rid}/report")
            assert report.json()["result"]["commit_state"] == "committed"
            assert "检查报告" in report.json()["result"]["final_answer"]
        finally:
            await gateway.shutdown()


async def test_report_uses_each_runs_request_not_the_conversation_title(web_core) -> None:
    gateway = Gateway(core_port=web_core[0])
    async with client_for(gateway) as client:
        try:
            sid = await create_session(client)
            first_task = "检查样例仓库的项目说明"
            second_task = "重新检查样例仓库并确认报告文件"
            first = await complete_turn(client, sid, 1, first_task)
            first_id = first["current_run_id"]
            first_report = (await client.get(f"/api/sessions/{sid}/runs/{first_id}/report")).json()
            second = await complete_turn(client, sid, 2, second_task)
            second_id = second["current_run_id"]
            second_report = (
                await client.get(f"/api/sessions/{sid}/runs/{second_id}/report")
            ).json()
            exported = await client.get(
                f"/api/sessions/{sid}/runs/{second_id}/report?format=markdown"
            )
            assert first_id != second_id
            assert second["title"] == first_task  # Conversation navigation keeps its title.
            assert first_report["title"] == first_task
            assert second_report["title"] == second_task
            assert second_report["request_id"] == f"turn-{sid}-2"
            assert f"任务：{second_task}" in exported.text
            assert f"任务：{first_task}" not in exported.text
            assert second_id in second_report["result"]["transcript_ref"]
            assert (
                await client.get(f"/api/sessions/{sid}/runs/{first_id}/report")
            ).json() == first_report
        finally:
            await gateway.shutdown()
