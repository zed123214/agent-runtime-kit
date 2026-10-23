"""Authenticated same-origin HTTP and WebSocket transport for a local gateway."""

from __future__ import annotations

import asyncio
import secrets
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, Literal
from urllib.parse import urlsplit

from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse, Response
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, ConfigDict, Field, field_validator

from agent_runtime.core.permissions.policy import PermissionMode
from agent_runtime.web.gateway import Gateway, GatewayError
from agent_runtime.web.projection import report_markdown


class Body(BaseModel):
    model_config = ConfigDict(extra="forbid")


class Bootstrap(Body):
    token: str = Field(max_length=128)


class Command(Body):
    request_id: str = Field(min_length=1, max_length=128, pattern=r"^[a-zA-Z0-9_-]+$")


class Message(Command):
    content: str

    @field_validator("content")
    @classmethod
    def validate_content(cls, value: str) -> str:
        if not value.strip() or len(value.encode("utf-8")) > 32768:
            raise ValueError("消息必须为非空文本，且不能超过 32 KiB。")
        return value


class Approval(Body):
    decision: Literal["allow_once", "allow_session", "deny_once"]


class Permissions(Body):
    mode: PermissionMode
    expected_revision: int = Field(ge=0)
    revoke_tool: str | None = Field(default=None, max_length=200)
    reset_grants: bool = False


def public_record(record: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in record.items() if key != "digest"}


def create_app(
    gateway: Gateway | None = None,
    *,
    origins: set[str] | None = None,
    static_directory: Path | None = None,
) -> FastAPI:
    gateway = gateway or Gateway()
    origins = origins or {"http://127.0.0.1:8765", "http://localhost:8765"}
    hosts = {urlsplit(origin).netloc for origin in origins}

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        reaper = asyncio.create_task(gateway.reap())
        try:
            yield
        finally:
            reaper.cancel()
            await asyncio.gather(reaper, return_exceptions=True)
            await gateway.shutdown()

    app = FastAPI(
        title="Agent Runtime Kit Web",
        lifespan=lifespan,
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
    )

    @app.exception_handler(GatewayError)
    async def gateway_error(request: Request, exc: GatewayError) -> JSONResponse:
        return JSONResponse({"error": str(exc)}, status_code=exc.status)

    @app.middleware("http")
    async def guard(request: Request, call_next: Any) -> Response:
        if request.headers.get("host") not in hosts:
            return JSONResponse({"error": "Host 不允许"}, status_code=403)
        origin = request.headers.get("origin")
        if origin is not None and origin not in origins:
            return JSONResponse({"error": "Origin 不允许"}, status_code=403)
        if request.url.path.startswith("/api/"):
            bootstrap = request.url.path == "/api/bootstrap" and request.method == "POST"
            if not bootstrap and not gateway.authorize(request.cookies.get("agentrt_web")):
                return JSONResponse(
                    {"error": "请使用当前网关的启动链接打开工作台。"}, status_code=401
                )
            if request.method not in {"GET", "HEAD"}:
                if origin not in origins:
                    return JSONResponse({"error": "需要同源请求"}, status_code=403)
                if not bootstrap and not secrets.compare_digest(
                    request.headers.get("x-csrf-token", ""), gateway.csrf
                ):
                    return JSONResponse({"error": "CSRF 校验失败"}, status_code=403)
                length = request.headers.get("content-length", "")
                if not length.isdigit() or int(length) > 65536:
                    return JSONResponse({"error": "请求体必须有界且小于 64 KiB"}, status_code=413)
                if (
                    int(length)
                    and request.headers.get("content-type", "").split(";")[0] != "application/json"
                ):
                    return JSONResponse({"error": "需要 JSON 请求"}, status_code=415)
        response: Response = await call_next(request)
        response.headers["Cache-Control"] = "no-store"
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["Referrer-Policy"] = "no-referrer"
        response.headers["Content-Security-Policy"] = (
            "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; "
            "img-src 'self' data:; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'"
        )
        return response

    @app.post("/api/bootstrap")
    async def bootstrap(body: Bootstrap) -> Response:
        if gateway.bootstrapped or not secrets.compare_digest(body.token, gateway.bootstrap_token):
            raise GatewayError(403, "启动口令已使用或无效。")
        gateway.bootstrapped = True
        response = JSONResponse({"csrf": gateway.csrf, "instance_id": gateway.instance_id})
        response.set_cookie(
            "agentrt_web", gateway.cookie, httponly=True, samesite="strict", path="/"
        )
        return response

    @app.get("/api/status")
    async def status() -> dict[str, Any]:
        try:
            core = await gateway.describe()
            error = None if gateway.supported(core) else "P0 仅支持 Loop + Local。"
        except GatewayError as exc:
            core, error = None, str(exc)
        return {
            "instance_id": gateway.instance_id,
            "csrf": gateway.csrf,
            "core": core,
            "error": error,
            "supported": core is not None and error is None,
        }

    @app.get("/api/sessions")
    async def sessions() -> dict[str, Any]:
        return {
            "instance_id": gateway.instance_id,
            "sessions": [
                {
                    "id": sid,
                    "title": s.projection.data["title"],
                    "state": s.state,
                    "seq": s.projection.data["seq"],
                    "updated_at": s.projection.data["updated_at"],
                    "pending": sum(
                        a["status"] == "pending" for a in s.projection.data["approvals"].values()
                    ),
                }
                for sid, s in gateway.sessions.items()
            ],
        }

    @app.post("/api/sessions", status_code=202)
    async def create(body: Command) -> dict[str, Any]:
        return public_record(gateway.create(body.request_id))

    @app.get("/api/requests/{request_id}")
    async def request_state(request_id: str) -> dict[str, Any]:
        record = gateway.requests.get(request_id)
        if record is None:
            raise GatewayError(404, "当前实例未登记该请求；不会自动重新执行。")
        return public_record(record)

    @app.post("/api/sessions/{sid}/messages", status_code=202)
    async def send(sid: str, body: Message) -> dict[str, Any]:
        return public_record(gateway.send(sid, body.request_id, body.content))

    @app.post("/api/sessions/{sid}/approvals/{aid}")
    async def approve(sid: str, aid: str, body: Approval) -> dict[str, bool]:
        session = gateway.session(sid)
        if session.state not in {"approval", "running"}:
            raise GatewayError(409, "会话当前不可审批。")
        return {"ok": await session.approve(aid, body.decision)}

    @app.post("/api/sessions/{sid}/close", status_code=202)
    async def close(sid: str, body: Command) -> dict[str, Any]:
        return public_record(gateway.close(sid, body.request_id))

    @app.post("/api/sessions/{sid}/permissions")
    async def permissions(sid: str, body: Permissions) -> dict[str, Any]:
        session = gateway.session(sid)
        if session.state in {"closing", "closed", "lost"}:
            raise GatewayError(409, "会话当前无法修改权限。")
        if not session.projection.data["core"].get("permission_modes"):
            raise GatewayError(409, "当前 Core 不支持会话权限，请更新并重启 Core。")
        try:
            return await session.configure_permissions(body.model_dump())
        except Exception:
            raise GatewayError(409, "权限状态已变化或未确认，请检查当前模式后重试。") from None

    @app.post("/api/sessions/{sid}/sync")
    async def synchronize(sid: str) -> dict[str, bool]:
        session = gateway.session(sid)
        if session.state != "sync_error" or not session.projection.data["current_run_id"]:
            raise GatewayError(409, "当前没有可重新同步的已执行任务。")
        try:
            await session.synchronize(session.projection.data["current_run_id"])
        except Exception:
            raise GatewayError(503, "结果仍未能同步；任务没有重新执行。") from None
        return {"ok": True}

    @app.get("/api/sessions/{sid}/snapshot")
    async def snapshot(sid: str) -> dict[str, Any]:
        session = gateway.session(sid)
        session.flush()
        return session.projection.snapshot()

    @app.get("/api/sessions/{sid}/runs/{rid}/report")
    async def report(sid: str, rid: str, format: Literal["json", "markdown"] = "json") -> Any:
        frozen = gateway.session(sid).projection.data["reports"].get(rid)
        if frozen is None:
            raise GatewayError(404, "报告尚未冻结或已不再保留。")
        if format == "markdown":
            return Response(
                report_markdown(frozen),
                media_type="text/markdown; charset=utf-8",
                headers={
                    "Content-Disposition": f'attachment; filename="kitagent-run-{rid[:24]}.md"'
                },
            )
        return frozen

    @app.websocket("/api/sessions/{sid}/events")
    async def events(websocket: WebSocket, sid: str) -> None:
        if (
            websocket.headers.get("host") not in hosts
            or websocket.headers.get("origin") not in origins
            or not gateway.authorize(websocket.cookies.get("agentrt_web"))
            or sid not in gateway.sessions
        ):
            await websocket.close(code=1008)
            return
        session = gateway.session(sid)
        await websocket.accept()
        initial, queue = session.subscribe()

        async def receive() -> None:
            while True:
                await websocket.receive_text()

        async def transmit() -> None:
            await asyncio.wait_for(
                websocket.send_json({"type": "snapshot", "snapshot": initial}), 5
            )
            while True:
                try:
                    frame = await asyncio.wait_for(queue.get(), 15)
                except TimeoutError:
                    frame = {"type": "heartbeat"}
                await asyncio.wait_for(websocket.send_json(frame), 5)
                if frame["type"] == "resync":
                    await websocket.close(code=1013)
                    return

        tasks = [asyncio.create_task(receive()), asyncio.create_task(transmit())]
        try:
            done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                with suppress_websocket_errors():
                    task.result()
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            session.unsubscribe(queue)

    directory = static_directory or Path(__file__).with_name("static")
    if directory.is_dir():
        app.mount("/", StaticFiles(directory=directory, html=True), name="workbench")
    else:

        @app.get("/")
        async def missing_build() -> dict[str, str]:
            return {"message": "前端尚未构建。请在 frontend 目录执行 npm ci && npm run build。"}

    return app


def suppress_websocket_errors() -> Any:
    from contextlib import suppress

    return suppress(WebSocketDisconnect, RuntimeError, TimeoutError, OSError)
