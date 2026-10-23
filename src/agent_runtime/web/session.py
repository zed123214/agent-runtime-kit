"""One owned Core connection, independent of any browser subscription."""

from __future__ import annotations

import asyncio
from contextlib import suppress
from typing import Any

from agent_runtime.core.transport.socket_client import SocketClient
from agent_runtime.web.projection import Projection, now


class WebSession:
    def __init__(self, sid: str, instance: str, client: SocketClient, core: dict[str, Any]) -> None:
        self.id = sid
        self.client = client
        self.core_id = ""
        self.projection = Projection(sid, instance, core)
        self.reader: asyncio.Task[None] | None = None
        self.run_task: asyncio.Task[None] | None = None
        self.close_task: asyncio.Task[None] | None = None
        self.sync_task: asyncio.Task[None] | None = None
        self.approval_tasks: set[asyncio.Task[bool]] = set()
        self.subscribers: set[asyncio.Queue[dict[str, Any]]] = set()
        self._flush_handle: asyncio.TimerHandle | None = None
        self.last_seen = asyncio.get_running_loop().time()
        self.disconnected = False
        self.permission_lock = asyncio.Lock()

    def _permissions(self, value: dict[str, Any]) -> None:
        if value["revision"] >= self.projection.data["permissions"]["revision"]:
            self.projection.meta(permissions=value)
            self.flush()

    async def configure_permissions(self, params: dict[str, Any]) -> dict[str, Any]:
        async with self.permission_lock:
            try:
                reply = await self.rpc("session.set_permissions", session_id=self.core_id, **params)
            except Exception:
                # Re-read after a lost response or revision conflict, never assume a change failed.
                reply = await self.rpc("session.get_permissions", session_id=self.core_id)
                self._permissions(reply)
                raise
            self._permissions(reply)
            return reply

    @property
    def state(self) -> str:
        return str(self.projection.data["state"])

    async def read(self) -> None:
        try:
            await self.client.run_event_loop()
        finally:
            self.disconnected = True
            if self.state not in {"closed", "closing"}:
                self.projection.meta(state="lost", error="Core 连接已丢失，旧会话不可继续。")
                self.flush()

    async def event(self, event: dict[str, Any]) -> None:
        # Broadcaster owns authorization; this filter also excludes unrelated global events.
        if not self.core_id or event.get("session_id") != self.core_id:
            return
        try:
            self.projection.event(event)
        except OverflowError as exc:
            self.projection.meta(error=str(exc), state="closing")
            self.start_close()
        if event.get("type") == "llm.token":
            if self._flush_handle is None:
                self._flush_handle = asyncio.get_running_loop().call_later(0.05, self.flush)
        else:
            self.flush()

    def flush(self) -> None:
        if self._flush_handle is not None:
            self._flush_handle.cancel()
            self._flush_handle = None
        patch = self.projection.drain()
        if patch is None:
            return
        for queue in list(self.subscribers):
            try:
                queue.put_nowait(patch)
            except asyncio.QueueFull:
                self.subscribers.discard(queue)
                while not queue.empty():
                    queue.get_nowait()
                queue.put_nowait({"type": "resync"})

    def subscribe(self) -> tuple[dict[str, Any], asyncio.Queue[dict[str, Any]]]:
        # No await between flushing, subscription registration and high-water capture.
        self.flush()
        queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=64)
        self.subscribers.add(queue)
        self.last_seen = asyncio.get_running_loop().time()
        return self.projection.snapshot(), queue

    def unsubscribe(self, queue: asyncio.Queue[dict[str, Any]]) -> None:
        self.subscribers.discard(queue)
        self.last_seen = asyncio.get_running_loop().time()

    async def rpc(self, method: str, **params: Any) -> dict[str, Any]:
        if self.disconnected:
            raise ConnectionError("Core 连接已丢失")
        return await asyncio.wait_for(self.client.send_command(method, params), 15)

    async def send(self, content: str, record: dict[str, Any]) -> None:
        try:
            reply = await self.client.send_command(
                "session.send_message",
                {
                    "session_id": self.core_id,
                    "content": content,
                },
            )
            record.update(state="executed", run_id=reply["run_id"])
            run_id = str(reply["run_id"])
            run = self.projection.data["runs"].get(run_id, {"id": run_id})
            self.projection.put(
                "runs",
                run_id,
                {
                    **run,
                    "request_id": record["request_id"],
                    "request_content": content,
                },
            )
            if self.state not in {"closing", "closed", "lost"}:
                await self.synchronize(run_id)
            record["state"] = "completed"
        except asyncio.CancelledError:
            record["state"] = "unknown" if self.disconnected else "cancelled"
            if self.state not in {"closing", "closed"}:
                self.projection.meta(state="lost", error="执行响应丢失；不会自动重发任务。")
        except Exception:
            record.update(state="failed", error="Core 未能完成请求，请检查运行详情。")
            if self.state not in {"closing", "closed", "lost"}:
                self.projection.meta(
                    state="sync_error", error="结果同步失败，可重新同步或结束会话。"
                )
        finally:
            self.last_seen = asyncio.get_running_loop().time()
            self.flush()

    async def synchronize(self, run_id: str) -> None:
        if self.sync_task is not None and not self.sync_task.done():
            await asyncio.shield(self.sync_task)
            return
        if run_id != self.projection.data["current_run_id"] or self.state in {
            "closing",
            "closed",
            "lost",
        }:
            raise RuntimeError("当前运行已变化，无法应用旧结果")
        self.projection.meta(state="syncing")
        self.flush()
        self.sync_task = asyncio.create_task(self._synchronize_run(run_id))
        self.sync_task.add_done_callback(
            lambda task: task.exception() if not task.cancelled() else None
        )
        await asyncio.shield(self.sync_task)

    async def _synchronize_run(self, run_id: str) -> None:
        try:
            result = await self.rpc("run.get_result", session_id=self.core_id, run_id=run_id)
            if result.get("commit_state") != "committed":
                raise RuntimeError("本轮结果未提交或已不再保留")
            history = await self.rpc("session.get_history", session_id=self.core_id)
            if self.state != "syncing" or self.projection.data["current_run_id"] != run_id:
                return
            self.projection.synchronize(history["messages"], result)
        except OverflowError:
            self.projection.meta(error="结果超出展示容量，无法导出完整报告；会话将结束。")
            self.start_close()
        except Exception:
            if self.state not in {"closing", "closed", "lost"}:
                self.projection.meta(state="sync_error", error="结果同步失败，可重新读取。")
            raise
        finally:
            self.flush()

    async def approve(self, aid: str, decision: str) -> bool:
        approval = self.projection.data["approvals"].get(aid)
        if approval is None or approval["status"] != "pending":
            return False
        if decision not in {"allow_once", "allow_session", "deny_once"}:
            return False
        if decision != "deny_once" and not approval["can_allow"]:
            return False
        self.projection.put("approvals", aid, {**approval, "status": "submitting"})
        self.flush()
        task = asyncio.create_task(self._decide(aid, decision, approval))
        self.approval_tasks.add(task)
        task.add_done_callback(self.approval_tasks.discard)
        return await asyncio.shield(task)

    async def _decide(self, aid: str, decision: str, approval: dict[str, Any]) -> bool:
        try:
            reply = await self.rpc(
                "permission.respond",
                session_id=self.core_id,
                run_id=approval["run_id"],
                tool_use_id=approval["tool_use_id"],
                decision=decision,
            )
            ok = reply.get("ok") is True
            if ok and isinstance(reply.get("permissions"), dict):
                self._permissions(reply["permissions"])
            current = self.projection.data["approvals"][aid]
            self.projection.put(
                "approvals",
                aid,
                {
                    **current,
                    "status": "resolved" if ok else "expired",
                    "decision": decision if ok else None,
                },
            )
            return ok
        except (Exception, asyncio.CancelledError):
            current = self.projection.data["approvals"].get(aid, approval)
            if current["status"] != "resolved":
                self.projection.put("approvals", aid, {**current, "status": "unknown"})
            if decision == "allow_session" and self.state not in {"closing", "closed", "lost"}:
                with suppress(Exception):
                    self._permissions(
                        await self.rpc("session.get_permissions", session_id=self.core_id)
                    )
            return False
        finally:
            self.flush()

    def start_close(self) -> asyncio.Task[None]:
        if self.close_task is None:
            self.projection.meta(state="closing")
            self.flush()
            self.close_task = asyncio.create_task(self._close())
        return self.close_task

    async def _close(self) -> None:
        try:
            if self.disconnected and self.core_id:
                raise ConnectionError("Core ownership lost")
            if self.core_id and not self.disconnected:
                await self.rpc("session.close", session_id=self.core_id)
            self.projection.meta(state="closed", closed_at=now())
        except (Exception, asyncio.CancelledError):
            self.projection.meta(state="lost", error="无法确认 Core 已完成会话清理。")
        finally:
            with suppress(Exception):
                await self.client.close()
            pending: list[asyncio.Task[Any]] = list(self.approval_tasks)
            if self.sync_task is not None and self.sync_task is not asyncio.current_task():
                pending.append(self.sync_task)
            for task in pending:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
            if self.reader is not None and self.reader is not asyncio.current_task():
                self.reader.cancel()
                await asyncio.gather(self.reader, return_exceptions=True)
            if self.run_task is not None and not self.run_task.done():
                self.run_task.cancel()
                await asyncio.gather(self.run_task, return_exceptions=True)
            self.flush()
