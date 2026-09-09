from __future__ import annotations

import asyncio
import uuid
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from agent_runtime.core.bus.envelope import HandlerError
from agent_runtime.core.bus.events import (
    SessionClosedEvent,
    SessionCreatedEvent,
    SessionMessageReceivedEvent,
    SessionResumedEvent,
    SessionWaitingForInputEvent,
    SkillInvokedEvent,
)
from agent_runtime.core.events.bus import EventBus
from agent_runtime.core.runs import new_run_id
from agent_runtime.core.session.model import Session, SessionMode
from agent_runtime.core.session.store import SessionStore, TranscriptStoreError
from agent_runtime.core.skills.loader import SkillLoader

if TYPE_CHECKING:
    from agent_runtime.core.engine.base import EngineRunResult
    from agent_runtime.core.graph.recovery import RecoveryRun, RecoveryStore
    from agent_runtime.core.llm.base import LLMProvider
    from agent_runtime.core.runner import AgentRunner

SESSION_NOT_FOUND = -32010
SESSION_CLOSED = -32011
SESSION_BUSY = -32012
SESSION_SUSPENDED = -32013
SESSION_STATE_ERROR = -32014
RECOVERY_CONFLICT = -32031


# 返回当前 UTC 时间的 ISO 8601 字符串
def _now() -> str:
    return datetime.now(UTC).isoformat()


class SessionManager:
    # 初始化会话管理器，接入文件存储、runner 工厂、事件总线和可选的压缩 provider
    def __init__(
        self,
        store: SessionStore,
        runner_factory: Callable[[], AgentRunner],
        bus: EventBus,
        provider: LLMProvider | None = None,
        provider_factory: Callable[[], LLMProvider] | None = None,
        on_session_closed: Callable[[str], Awaitable[None]] | None = None,
        recovery_store: RecoveryStore | None = None,
        durable_supported: bool = True,
    ) -> None:
        self._store = store
        self._runner_factory = runner_factory
        self._bus = bus
        self._provider = provider
        self._provider_factory = provider_factory
        self._on_session_closed = on_session_closed
        self._recovery_store = recovery_store
        self._durable_supported = durable_supported
        self._sessions: dict[str, Session] = {}
        self._locks: dict[str, asyncio.Lock] = {}
        self._closing: set[str] = set()
        self._active_tasks: dict[str, set[asyncio.Task[Any]]] = {}
        self._close_tasks: dict[str, asyncio.Task[None]] = {}
        self._close_pending_events: set[str] = set()
        self._shutting_down = False
        self._skill_loader = SkillLoader()

    def begin_close(self, sid: str) -> None:
        """Stop admitting turns before cancellation yields to another request."""

        self._closing.add(sid)

    def begin_shutdown(self) -> None:
        """Reject both new sessions and new work before transport shutdown."""

        self._shutting_down = True
        self._closing.update(self._sessions)

    async def _cancel_active(self, sid: str) -> None:
        current = asyncio.current_task()
        tasks = [
            task
            for task in self._active_tasks.get(sid, set())
            if task is not current and not task.done()
        ]
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    def _untrack_active(self, sid: str, task: asyncio.Task[Any] | None) -> None:
        if task is None:
            return
        active = self._active_tasks.get(sid)
        if active is not None:
            active.discard(task)
            if not active:
                self._active_tasks.pop(sid, None)

    # 创建新 session；transport 可在发布 session.created 前原子登记 owner
    async def create(
        self,
        mode: SessionMode,
        title: str = "",
        *,
        before_publish: Callable[[Session], None] | None = None,
        durable: bool = False,
    ) -> Session:
        if durable and not self._durable_supported:
            raise HandlerError(SESSION_STATE_ERROR, "Kubernetes durable sessions require M3")
        if self._shutting_down:
            raise HandlerError(SESSION_CLOSED, "session manager is closing")
        sid = f"sess-{uuid.uuid4().hex[:12]}"
        ts = _now()
        session = Session(
            id=sid,
            mode=mode,
            status="active",
            title=title,
            created_at=ts,
            updated_at=ts,
            run_ids=[],
            durable=durable,
        )
        if before_publish is not None:
            before_publish(session)
        self._sessions[sid] = session
        self._locks[sid] = asyncio.Lock()
        self._store.write_meta(session)
        await self._bus.publish(SessionCreatedEvent(session_id=sid, mode=mode, ts=ts))
        return session

    # 处理用户消息，追加 thread 并启动一次 agent run
    async def send_message(self, sid: str, content: str, *, run_id: str | None = None) -> str:
        session = self._get_session(sid)
        if self._shutting_down or sid in self._closing:
            raise HandlerError(SESSION_CLOSED, "session already closed")
        if self._locks[sid].locked():
            raise HandlerError(SESSION_BUSY, "session busy")
        current = asyncio.current_task()
        if current is not None:
            self._active_tasks.setdefault(sid, set()).add(current)
        try:
            return await self._send_message(sid, content, run_id=run_id)
        finally:
            self._untrack_active(sid, current)
            # The runner has already joined its children, including on early
            # failure/cancellation. A one-shot cannot leave an open ownership.
            if session.mode == "one_shot" and session.status != "suspended":
                if sid not in self._close_tasks:
                    await self.close(sid)

    async def _send_message(self, sid: str, content: str, *, run_id: str | None = None) -> str:
        from agent_runtime.core.engine.base import RunSuspension

        session = self._get_session(sid)
        lock = self._locks[sid]
        if lock.locked():
            raise HandlerError(SESSION_BUSY, "session busy")

        async with lock:
            if session.status == "closed":
                raise HandlerError(SESSION_CLOSED, "session already closed")
            if session.status == "suspended":
                raise HandlerError(SESSION_SUSPENDED, "session suspended")

            run_id = run_id or new_run_id()
            if session.durable:
                if self._recovery_store is None:
                    raise RuntimeError("durable session requires a RecoveryStore")
                if await self._recovery_store.latest_unfinished_run(sid) is not None:
                    raise HandlerError(SESSION_SUSPENDED, "session has an unfinished run")

            if session.status == "waiting_for_input":
                await self._bus.publish(SessionResumedEvent(session_id=sid, ts=_now()))

            try:
                self._store.append_message(sid, "user", content)
                if session.durable:
                    self._store.read_messages_strict(sid)
            except TranscriptStoreError as exc:
                raise HandlerError(
                    SESSION_STATE_ERROR,
                    "session transcript is not recoverable",
                    {"code": exc.code},
                ) from exc
            await self._bus.publish(
                SessionMessageReceivedEvent(session_id=sid, content=content, ts=_now())
            )

            if not session.title:
                session.title = content[:40]

            if session.durable:
                assert self._recovery_store is not None
                await self._recovery_store.create_run(
                    session_id=sid,
                    run_id=run_id,
                    thread_id=sid,
                    engine="graph",
                    status="running",
                )
            session.run_ids.append(run_id)
            session.active_run_id = run_id
            session.updated_at = _now()
            self._store.write_meta(session)

            # Skill 解析：检测 "/" 前缀，展开为系统提示覆盖和工具白名单
            goal = content
            system_prompt_override: str | None = None
            tool_whitelist: list[str] | None = None
            if content.startswith("/"):
                parts = content[1:].split(None, 1)
                skill_name = parts[0]
                arguments = parts[1] if len(parts) > 1 else ""
                skill = self._skill_loader.resolve(skill_name)
                if skill is not None:
                    goal = self._skill_loader.render_prompt(skill, arguments)
                    system_prompt_override = skill.system_prompt_template
                    tool_whitelist = skill.allowed_tools or None
                    await self._bus.publish(
                        SkillInvokedEvent(
                            skill_name=skill_name,
                            arguments=arguments,
                            run_id=run_id,
                            correlation_id=run_id,
                            session_id=sid,
                            ts=_now(),
                        )
                    )

            runner = self._runner_factory()
            result = await runner.run_and_capture(
                goal,
                run_id=run_id,
                session=session,
                store=self._store,
                system_prompt_override=system_prompt_override,
                tool_whitelist=tool_whitelist,
            )

            session.updated_at = _now()
            if isinstance(result, RunSuspension):
                session.status = "suspended"
                session.active_run_id = run_id
                self._store.write_meta(session)
                evict = True
            else:
                evict = False
                session.active_run_id = None
            if not evict and session.mode == "one_shot":
                session.status = "closed"
                await self._bus.publish(SessionClosedEvent(session_id=sid, ts=session.updated_at))
            elif not evict:
                session.status = "waiting_for_input"
                await self._bus.publish(
                    SessionWaitingForInputEvent(
                        session_id=sid,
                        last_run_id=run_id,
                        ts=session.updated_at,
                    )
                )
            self._store.write_meta(session)
        if evict:
            self.evict(sid)
        return run_id

    def hydrate(self, sid: str) -> Session:
        """Load one explicitly authorized session into the process-local index."""

        existing = self._sessions.get(sid)
        if existing is not None:
            return existing
        try:
            session = self._store.read_meta(sid)
        except (FileNotFoundError, OSError, ValueError, KeyError):
            raise HandlerError(SESSION_NOT_FOUND, "session not found") from None
        self._sessions[sid] = session
        self._locks[sid] = asyncio.Lock()
        return session

    def hydrate_recovery(self, sid: str, run: RecoveryRun | None) -> Session:
        """Hydrate metadata and reconcile it with the authoritative recovery index."""

        session = self.hydrate(sid)
        if not session.durable or session.mode != "chat":
            raise HandlerError(SESSION_NOT_FOUND, "session not found")
        if session.status == "closed":
            raise HandlerError(SESSION_CLOSED, "session already closed")
        if run is None:
            session.status = "waiting_for_input"
            session.active_run_id = None
        else:
            session.status = "suspended"
            session.active_run_id = run.run_id
            if run.run_id not in session.run_ids:
                session.run_ids.append(run.run_id)
        session.updated_at = _now()
        self._store.write_meta(session)
        return session

    def is_recovery_closed(self, sid: str) -> bool:
        """Check persisted closed state without hydrating or mutating the session."""

        try:
            session = self._store.read_meta(sid)
        except (FileNotFoundError, OSError, ValueError, KeyError):
            return False
        return session.durable and session.mode == "chat" and session.status == "closed"

    def evict(self, sid: str) -> None:
        self._sessions.pop(sid, None)
        self._locks.pop(sid, None)

    async def resume_run(
        self,
        sid: str,
        run: RecoveryRun,
        *,
        resume_value: object | None = None,
    ) -> EngineRunResult:
        if self._shutting_down or sid in self._closing:
            raise HandlerError(SESSION_CLOSED, "session already closed")
        current = asyncio.current_task()
        if current is not None:
            self._active_tasks.setdefault(sid, set()).add(current)
        try:
            return await self._resume_run(sid, run, resume_value=resume_value)
        finally:
            self._untrack_active(sid, current)

    async def _resume_run(
        self,
        sid: str,
        run: RecoveryRun,
        *,
        resume_value: object | None = None,
    ) -> EngineRunResult:
        """Resume the leased durable run without creating a second logical run."""

        from agent_runtime.core.engine.base import RunSuspension

        session = self._get_session(sid)
        if run.session_id != sid or run.status != "resuming":
            raise HandlerError(SESSION_SUSPENDED, "run is not resumable")
        if run.checkpoint_revision is None:
            raise HandlerError(SESSION_SUSPENDED, "run has no checkpoint revision")
        lock = self._locks[sid]
        if lock.locked():
            raise HandlerError(SESSION_BUSY, "session busy")

        evict = False
        async with lock:
            session.status = "active"
            session.active_run_id = run.run_id
            if run.run_id not in session.run_ids:
                session.run_ids.append(run.run_id)
            session.updated_at = _now()
            self._store.write_meta(session)
            try:
                messages = self._store.read_messages_strict(sid)
            except TranscriptStoreError as exc:
                raise HandlerError(
                    SESSION_STATE_ERROR,
                    "session transcript is not recoverable",
                    {"code": exc.code},
                ) from exc
            goal = next(
                (
                    str(message.get("content", ""))
                    for message in reversed(messages)
                    if message.get("role") == "user"
                ),
                "",
            )
            result = await self._runner_factory().run_and_capture(
                goal,
                run_id=run.run_id,
                session=session,
                store=self._store,
                resume=True,
                resume_value=resume_value,
                expected_checkpoint_revision=run.checkpoint_revision,
                resume_epoch=run.resume_epoch,
            )
            session.updated_at = _now()
            if isinstance(result, RunSuspension):
                session.status = "suspended"
                session.active_run_id = run.run_id
                evict = True
            else:
                session.status = "waiting_for_input"
                session.active_run_id = None
                await self._bus.publish(
                    SessionWaitingForInputEvent(
                        session_id=sid,
                        last_run_id=run.run_id,
                        ts=session.updated_at,
                    )
                )
            self._store.write_meta(session)
        if evict:
            self.evict(sid)
        return result

    # 关闭指定 session 并更新 meta.json
    async def close(self, sid: str) -> None:
        self._get_session(sid)
        self.begin_close(sid)
        task = self._close_tasks.get(sid)
        if task is None:
            task = asyncio.create_task(self._close_attempt(sid))
            self._close_tasks[sid] = task
        cancelled = False
        while not task.done():
            try:
                await asyncio.shield(task)
            except asyncio.CancelledError:
                cancelled = True
        task.result()
        if cancelled:
            raise asyncio.CancelledError()

    async def _close_attempt(self, sid: str) -> None:
        try:
            await self._close_session(sid)
        except BaseException:
            # Only a failed, finished attempt is replaceable. Concurrent callers
            # retain the same task and observe the same failure. A caller being
            # cancelled cannot cancel this shielded ownership transition.
            self._close_tasks.pop(sid, None)
            session = self._sessions.get(sid)
            if session is not None and session.status != "closed" and not self._shutting_down:
                self._closing.discard(sid)
            raise

    async def _close_session(self, sid: str) -> None:
        await self._cancel_active(sid)
        session = self._get_session(sid)
        lock = self._locks[sid]
        async with lock:
            if session.status == "closed":
                await self._persist_and_release_closed(session)
                return
            if session.durable and self._recovery_store is not None:
                unfinished = await self._recovery_store.latest_unfinished_run(sid)
                if unfinished is not None:
                    if unfinished.status != "suspended" or unfinished.checkpoint_revision is None:
                        raise HandlerError(SESSION_BUSY, "session has an active run")
                    from agent_runtime.core.engine.base import ExecutionEngineError
                    from agent_runtime.core.graph.event_log import EventLogError

                    try:
                        await self._runner_factory().close_suspended(
                            session=session,
                            store=self._store,
                            run_id=unfinished.run_id,
                            checkpoint_revision=unfinished.checkpoint_revision,
                            resume_epoch=unfinished.resume_epoch,
                        )
                    except (TranscriptStoreError, EventLogError) as exc:
                        raise HandlerError(
                            SESSION_STATE_ERROR,
                            "durable session state is not recoverable",
                            {"code": exc.code},
                        ) from exc
                    except ExecutionEngineError as exc:
                        raise HandlerError(
                            RECOVERY_CONFLICT,
                            "durable session checkpoint conflicts with recovery state",
                            {"code": exc.detail.code},
                        ) from exc
            session.status = "closed"
            session.active_run_id = None
            session.updated_at = _now()
            self._close_pending_events.add(sid)
            await self._persist_and_release_closed(session)

    async def _persist_and_release_closed(self, session: Session) -> None:
        # A failed metadata write or cleanup is retryable, but closed ownership
        # never reopens. Retry persistence too, so disk cannot retain active state.
        sid = session.id
        try:
            self._store.write_meta(session)
            if sid in self._close_pending_events:
                await self._bus.publish(SessionClosedEvent(session_id=sid, ts=session.updated_at))
                self._close_pending_events.discard(sid)
        finally:
            if self._on_session_closed is not None:
                await self._on_session_closed(sid)

    # 连接已失去 ownership 且运行任务已收尾时，持久化 closed 并回收进程内会话
    async def close_disconnected(self, sid: str) -> bool:
        try:
            return await self._close_disconnected(sid)
        except BaseException:
            session = self._sessions.get(sid)
            if session is not None and session.status != "closed" and not self._shutting_down:
                self._closing.discard(sid)
            raise

    async def _close_disconnected(self, sid: str) -> bool:
        self.begin_close(sid)
        await self._cancel_active(sid)
        session = self._sessions.get(sid)
        if session is None:
            try:
                session = self.hydrate(sid)
            except HandlerError:
                return False
        lock = self._locks[sid]
        async with lock:
            if session.durable and self._recovery_store is not None:
                unfinished = await self._recovery_store.latest_unfinished_run(sid)
                if unfinished is not None and unfinished.status == "suspended":
                    session.status = "suspended"
                    session.active_run_id = unfinished.run_id
                    session.updated_at = _now()
                    self._store.write_meta(session)
                    preserve = True
                else:
                    preserve = False
            else:
                preserve = False
            if not preserve:
                if session.status != "closed":
                    session.status = "closed"
                    session.active_run_id = None
                    session.updated_at = _now()
                    self._close_pending_events.add(sid)
                await self._persist_and_release_closed(session)
        if preserve:
            # A durable suspension is detached, not closed; its lazy facade
            # remains valid for the explicitly authorized resume path.
            self._closing.discard(sid)
        self.evict(sid)
        return preserve

    # 手动压缩指定 session 的 thread，将摘要持久化写入 thread.jsonl
    async def compact(self, sid: str, focus: str = "") -> Any:
        session = self._get_session(sid)
        lock = self._locks[sid]
        if lock.locked():
            raise HandlerError(SESSION_BUSY, "session busy")
        if session.status == "suspended":
            raise HandlerError(SESSION_SUSPENDED, "session suspended")
        if self._provider is None:
            if self._provider_factory is None:
                raise HandlerError(-32020, "provider not available for compaction")
            self._provider = self._provider_factory()
        async with lock:
            from agent_runtime.core.bus.commands import SessionCompactResult
            from agent_runtime.core.compact.compactor import Compactor

            messages = self._store.read_messages(sid)
            session_dir = self._store.session_dir(sid)
            compactor = Compactor(self._bus, session_dir, sid)
            result = await compactor.compact_messages(messages, self._provider, focus=focus)
            if result is None:
                raise HandlerError(-32021, "compaction failed or not beneficial")
            self._store.write_compacted(
                sid,
                [
                    {"role": "user", "content": result.summary_text},
                    {
                        "role": "assistant",
                        "content": "Understood, I'll continue from this summary.",
                    },
                ],
            )
            return SessionCompactResult(
                summary_tokens=result.summary_tokens,
                saved_tokens=max(0, result.original_token_estimate - result.summary_tokens),
            )

    # 读取指定 session 的完整 thread 历史
    async def get_history(self, sid: str) -> list[dict[str, Any]]:
        self._get_session(sid)
        return self._store.read_messages(sid)

    # 从内存索引取 session，不存在时抛 JSON-RPC 结构化错误
    def _get_session(self, sid: str) -> Session:
        session = self._sessions.get(sid)
        if session is None:
            raise HandlerError(SESSION_NOT_FOUND, "session not found")
        return session
