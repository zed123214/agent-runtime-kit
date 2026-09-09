"""HTTP broker and Unix-socket executor, deployed in separate PID/mount namespaces.

The broker is the sole bearer-token and authoritative idempotency owner. It has
no workspace mount, does not fork commands, strips client headers, and never
passes credentials to the executor. Executor responses remain untrusted data.
"""

from __future__ import annotations

import asyncio
import hmac
import json
import os
import uuid
from pathlib import Path
from typing import Any

from aiohttp import ClientSession, ClientTimeout, UnixConnector, web
from pydantic import ValidationError

from agent_runtime.sandbox_server.executor import Executor
from agent_runtime.sandbox_server.files import Workspace
from agent_runtime.sandbox_server.journal import Journal, JournalError
from agent_runtime.sandbox_server.protocol import (
    MAX_REQUEST_BYTES,
    MAX_RESULT_BYTES,
    PROTOCOL_VERSION,
    Operation,
    error_result,
)


class WorkerServer:
    def __init__(self, *, broker: bool) -> None:
        self.broker = broker
        self.sandbox_id = os.environ["SANDBOX_ID"]
        self.pod_uid = os.environ["POD_UID"]
        self.generation = uuid.uuid4().hex
        self.executor_generation: str | None = None
        self.cleanup_s = float(os.environ.get("CLEANUP_MARGIN_S", "5"))
        self.command_timeout_s = float(os.environ.get("COMMAND_TIMEOUT_S", "120"))
        self.journal = Journal(
            int(os.environ.get("JOURNAL_MAX_ENTRIES", "1024")),
            int(os.environ.get("JOURNAL_MAX_BYTES", "16777216")),
        )
        self.token = ""
        self.workspace: Workspace | None = None
        self.executor: Executor | None = None
        self.client: ClientSession | None = None
        if broker:
            self.token = Path("/run/credential/token").read_text(encoding="ascii").strip()
            if len(self.token) < 43:
                raise ValueError("missing per-Sandbox credential")
        else:
            self.workspace = Workspace()
            self.executor = Executor(self.workspace, self.cleanup_s)

    async def start(self, app: web.Application) -> None:
        if self.broker:
            self.client = ClientSession(
                connector=UnixConnector(path="/tmp/control/executor.sock", limit=8),
                # Every health, operation and cancellation request sets its own
                # deadline; one command policy cannot shorten file I/O.
                timeout=ClientTimeout(total=None),
                trust_env=False,
            )

    async def close(self, app: web.Application) -> None:
        await self.journal.close()
        if self.client is not None:
            await self.client.close()
        if self.workspace is not None:
            self.workspace.close()

    def authenticate(self, request: web.Request) -> None:
        if self.broker and not hmac.compare_digest(
            request.headers.get("Authorization", "").encode("utf-8", errors="surrogatepass"),
            ("Bearer " + self.token).encode("ascii"),
        ):
            raise web.HTTPUnauthorized(text="unauthorized")

    async def _executor_health(self) -> dict[str, Any]:
        assert self.client is not None
        async with self.client.get(
            "http://executor/healthz", timeout=ClientTimeout(total=2)
        ) as resp:
            data = await self._bounded_json(resp)
        if data.get("sandbox_id") != self.sandbox_id or data.get("pod_uid") != self.pod_uid:
            raise JournalError("workspace_lost")
        generation = data.get("generation")
        if not isinstance(generation, str) or not generation:
            raise JournalError("workspace_lost")
        if self.executor_generation is None:
            self.executor_generation = generation
        elif self.executor_generation != generation:
            self.journal.closed = True
            raise JournalError("workspace_lost")
        return data

    async def health(self, request: web.Request) -> web.Response:
        self.authenticate(request)
        if self.broker:
            try:
                await self._executor_health()
            except Exception:
                if self.journal.closed:
                    raise web.HTTPGone(text="workspace_lost") from None
                raise web.HTTPServiceUnavailable(text="executor unavailable") from None
        if self.journal.closed:
            raise web.HTTPGone(text="workspace_lost")
        return web.json_response(
            {
                "protocol": PROTOCOL_VERSION,
                "sandbox_id": self.sandbox_id,
                "pod_uid": self.pod_uid,
                "generation": self.generation,
            }
        )

    async def parse(self, request: web.Request) -> Operation:
        self.authenticate(request)
        try:
            # All public methods carry the same bounded JSON identity envelope,
            # including GET file/dir. The query path must agree with the envelope.
            op = Operation.model_validate_json(await request.read())
        except ValidationError:
            raise web.HTTPBadRequest(text="invalid operation") from None
        if op.identity.sandbox_id != self.sandbox_id or op.pod_uid != self.pod_uid:
            raise web.HTTPConflict(
                text=json.dumps(error_result("workspace_lost", "identity mismatch")),
                content_type="application/json",
            )
        if op.generation != self.generation or self.journal.closed:
            raise web.HTTPConflict(
                text=json.dumps(error_result("workspace_lost", "generation mismatch")),
                content_type="application/json",
            )
        if op.operation == "exec" and op.timeout_s > self.command_timeout_s:
            raise web.HTTPBadRequest(text="command timeout exceeds policy")
        return op

    @staticmethod
    async def _bounded_json(response: Any) -> dict[str, Any]:
        if response.status != 200:
            raise JournalError("outcome_unknown")
        data = bytearray()
        async for chunk in response.content.iter_chunked(8192):
            data.extend(chunk)
            if len(data) > MAX_RESULT_BYTES:
                raise JournalError("outcome_unknown")
        value = json.loads(data)
        if not isinstance(value, dict):
            raise JournalError("outcome_unknown")
        return value

    def _forward_op(self, op: Operation) -> Operation:
        assert self.executor_generation is not None
        return op.model_copy(update={"generation": self.executor_generation})

    async def dispatch(self, op: Operation) -> dict[str, Any]:
        if not self.broker:
            assert self.executor is not None
            return await self.executor.execute(op)
        assert self.client is not None
        await self._executor_health()
        forwarded = self._forward_op(op)
        try:
            async with self.client.post(
                "http://executor/operate",
                json=forwarded.model_dump(),
                timeout=ClientTimeout(total=op.response_timeout_s(self.cleanup_s)),
            ) as resp:
                return await self._bounded_json(resp)
        except BaseException:
            # A dropped upstream connection is not proof of process termination.
            # Explicitly cancel the exact identity at the executor before marking
            # this attempt terminal; failures quarantine this broker generation.
            try:
                async with asyncio.timeout(self.cleanup_s):
                    async with self.client.post(
                        "http://executor/cancel",
                        json=forwarded.model_dump(),
                        timeout=ClientTimeout(total=self.cleanup_s),
                    ) as resp:
                        await self._bounded_json(resp)
            except BaseException:
                self.journal.closed = True
            raise

    async def operate(self, request: web.Request) -> web.Response:
        op = await self.parse(request)
        routes = {
            ("POST", "/v1/exec"): "exec",
            ("GET", "/v1/files"): "read_text",
            ("PUT", "/v1/files"): "write_text",
            ("GET", "/v1/dirs"): "list_dir",
        }
        if self.broker:
            if routes.get((request.method, request.path)) != op.operation:
                raise web.HTTPBadRequest(text="operation does not match endpoint")
            if op.operation != "exec" and request.query.get("path") != op.path:
                raise web.HTTPBadRequest(text="path does not match envelope")
        try:
            payload = await self.journal.execute(op, lambda: self.dispatch(op))
        except JournalError as exc:
            return web.json_response(error_result(exc.code, exc.code), status=409)
        return web.Response(
            body=payload, content_type="application/json", headers={"Cache-Control": "no-store"}
        )

    async def cancel(self, request: web.Request) -> web.Response:
        op = await self.parse(request)
        try:
            # Cancellation is a tombstone, even when it wins against a late POST.
            await self.journal.cancel(op)
        except JournalError as exc:
            return web.json_response(error_result(exc.code, exc.code), status=409)
        if self.journal.closed:
            raise web.HTTPGone(text="workspace_lost")
        return web.json_response({"cancelled": True})

    def app(self) -> web.Application:
        app = web.Application(client_max_size=MAX_REQUEST_BYTES)
        app.on_startup.append(self.start)
        app.on_cleanup.append(self.close)
        app.router.add_get("/healthz", self.health)
        if self.broker:
            app.router.add_post("/v1/exec", self.operate)
            app.router.add_get("/v1/files", self.operate)
            app.router.add_put("/v1/files", self.operate)
            app.router.add_get("/v1/dirs", self.operate)
            app.router.add_post("/v1/cancel", self.cancel)
        else:
            app.router.add_post("/operate", self.operate)
            app.router.add_post("/cancel", self.cancel)
        return app
