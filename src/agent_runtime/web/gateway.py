"""Single-process command ownership and idempotency for the local workbench."""

from __future__ import annotations

import asyncio
import hashlib
import json
import secrets
from contextlib import suppress
from typing import Any
from uuid import uuid4

from agent_runtime.core.transport.socket_client import SocketClient
from agent_runtime.web.projection import now
from agent_runtime.web.session import WebSession


class GatewayError(Exception):
    def __init__(self, status: int, message: str) -> None:
        self.status = status
        super().__init__(message)


class Gateway:
    def __init__(self, core_host: str = "127.0.0.1", core_port: int = 7437) -> None:
        self.core_host, self.core_port = core_host, core_port
        self.instance_id = uuid4().hex
        self.bootstrap_token = secrets.token_urlsafe(32)
        self.cookie = secrets.token_urlsafe(32)
        self.csrf = secrets.token_urlsafe(32)
        self.bootstrapped = False
        self.sessions: dict[str, WebSession] = {}
        self.requests: dict[str, dict[str, Any]] = {}
        self.tasks: set[asyncio.Task[None]] = set()
        self.closing = False

    def authorize(self, cookie: str | None) -> bool:
        return (
            self.bootstrapped and cookie is not None and secrets.compare_digest(cookie, self.cookie)
        )

    async def describe(self) -> dict[str, Any]:
        client = SocketClient(self.core_host, self.core_port)
        reader: asyncio.Task[None] | None = None
        try:
            await asyncio.wait_for(client.connect(), 3)
            reader = asyncio.create_task(client.run_event_loop())
            return await asyncio.wait_for(client.send_command("core.describe", {}), 3)
        except (Exception, asyncio.CancelledError):
            raise GatewayError(503, "Core 不可达或版本不支持 Web 协议。") from None
        finally:
            with suppress(Exception):
                await client.close()
            if reader is not None:
                reader.cancel()
                await asyncio.gather(reader, return_exceptions=True)

    @staticmethod
    def supported(core: dict[str, Any]) -> bool:
        return (
            core.get("web_protocol_version") == 1
            and core.get("engine") == "loop"
            and core.get("sandbox_backend") == "local"
            and not core.get("durable_chat")
        )

    def session(self, sid: str) -> WebSession:
        if sid not in self.sessions:
            raise GatewayError(404, "会话不存在或属于另一个网关实例。")
        return self.sessions[sid]

    def _record(
        self, request_id: str, kind: str, payload: dict[str, Any]
    ) -> tuple[dict[str, Any], bool]:
        digest = hashlib.sha256(json.dumps([kind, payload], sort_keys=True).encode()).hexdigest()
        existing = self.requests.get(request_id)
        if existing is not None:
            if existing["digest"] != digest:
                raise GatewayError(409, "同一 request ID 对应不同内容。")
            return existing, False
        if self.closing:
            raise GatewayError(503, "网关正在退出。")
        if len(self.requests) >= 1000 and kind != "close":
            raise GatewayError(429, "请求记录容量已满，请重启网关。")
        record = {
            "request_id": request_id,
            "kind": kind,
            "digest": digest,
            "state": "accepted",
            "instance_id": self.instance_id,
        }
        self.requests[request_id] = record
        return record, True

    def _launch(self, coroutine: Any) -> asyncio.Task[None]:
        task = asyncio.create_task(coroutine)
        self.tasks.add(task)
        task.add_done_callback(self.tasks.discard)
        return task

    def create(self, request_id: str) -> dict[str, Any]:
        record, fresh = self._record(request_id, "create", {})
        if fresh:
            active = sum(s.state not in {"closed", "lost"} for s in self.sessions.values())
            pending = sum(
                r["kind"] == "create" and r["state"] == "accepted" for r in self.requests.values()
            )
            if active + pending > 5 or len(self.sessions) >= 25:
                record.update(state="failed", error="会话容量已满，请结束会话或重启网关。")
            else:
                self._launch(self._create(record))
        return record

    async def _create(self, record: dict[str, Any]) -> None:
        session: WebSession | None = None
        try:
            client = SocketClient(self.core_host, self.core_port)
            await asyncio.wait_for(client.connect(), 3)
            session = WebSession(uuid4().hex, self.instance_id, client, {})
            client.on_event(session.event)
            session.reader = asyncio.create_task(session.read())
            core = await session.rpc("core.describe")
            if not self.supported(core):
                raise GatewayError(409, "P0 仅支持 Loop + Local。")
            session.projection.data["core"] = core
            await session.rpc("event.subscribe", topics=["*"], scope="global")
            created = await session.rpc("session.create", mode="chat")
            session.core_id = str(created["session_id"])
            if core.get("permission_modes"):
                session.projection.data["permissions"] = await session.rpc(
                    "session.get_permissions", session_id=session.core_id
                )
            self.sessions[session.id] = session
            record.update(state="completed", session_id=session.id)
        except (Exception, asyncio.CancelledError) as exc:
            record.update(
                state="failed",
                error=str(exc) if isinstance(exc, GatewayError) else "创建会话失败。",
            )
            if session is not None:
                await session.start_close()

    def send(self, sid: str, request_id: str, content: str) -> dict[str, Any]:
        session = self.session(sid)
        record, fresh = self._record(request_id, "send", {"session_id": sid, "content": content})
        if not fresh:
            return record
        record["session_id"] = sid
        if session.state != "ready":
            record.update(state="failed", error="会话忙或不可继续。")
            raise GatewayError(409, "会话忙或不可继续。")
        view = session.projection
        if view.data["title"] == "新会话":
            view.meta(title=content[:40])
        mid = f"user:{request_id}"
        view.put("messages", mid, {"id": mid, "role": "user", "text": content, "draft": True})
        view.meta(state="running", error=None, current_run_id=None, updated_at=now())
        session.flush()
        session.run_task = self._launch(session.send(content, record))
        return record

    def close(self, sid: str, request_id: str) -> dict[str, Any]:
        session = self.session(sid)
        if session.close_task is not None and request_id not in self.requests:
            for existing in self.requests.values():
                if existing["kind"] == "close" and existing.get("session_id") == sid:
                    return existing
        record, fresh = self._record(request_id, "close", {"session_id": sid})
        if fresh:
            record["session_id"] = sid
            task = session.start_close()
            task.add_done_callback(lambda _: record.update(state=session.state))
        return record

    async def reap(self) -> None:
        while True:
            await asyncio.sleep(30)
            for session in list(self.sessions.values()):
                if (
                    session.state == "ready"
                    and not session.subscribers
                    and asyncio.get_running_loop().time() - session.last_seen > 1800
                ):
                    session.start_close()

    async def shutdown(self) -> None:
        self.closing = True
        # Finish creates before collecting owned sessions, avoiding a shutdown orphan.
        for task in list(self.tasks):
            if not any(session.run_task is task for session in self.sessions.values()):
                task.cancel()
        await asyncio.gather(
            *(session.start_close() for session in list(self.sessions.values())),
            return_exceptions=True,
        )
        for task in list(self.tasks):
            task.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)
