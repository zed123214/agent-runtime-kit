from __future__ import annotations

import asyncio
import datetime
import fnmatch
import hashlib
import hmac
import json
import logging
import re
import signal
import time
from collections.abc import Callable
from datetime import UTC
from pathlib import Path
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel

import agent_runtime
from agent_runtime.core.bus.commands import (
    AgentRunCommand,
    AgentRunResult,
    EventSubscribeCommand,
    EventSubscribeResult,
    PermissionRespondCommand,
    PermissionRespondResult,
    PongResult,
    RunGetStateCommand,
    RunGetStateResult,
    SessionCloseCommand,
    SessionCloseResult,
    SessionCompactCommand,
    SessionCompactResult,
    SessionCreateCommand,
    SessionCreateResult,
    SessionGetHistoryCommand,
    SessionGetHistoryResult,
    SessionResumeCommand,
    SessionResumeResult,
    SessionSendMessageCommand,
    SessionSendMessageResult,
)
from agent_runtime.core.bus.envelope import EventPushEnvelope, HandlerError
from agent_runtime.core.bus.events import RunFinishedEvent, RunStartedEvent
from agent_runtime.core.config import (
    RuntimeConfig,
    get_config,
    resolve_data_root,
    resolve_graph_checkpoint_path,
    validate_runtime_sandbox,
)
from agent_runtime.core.engine.router import EngineRouter
from agent_runtime.core.events.bus import EventBus
from agent_runtime.core.events.writer import EventWriter
from agent_runtime.core.graph.event_log import EventLogError, read_event_log
from agent_runtime.core.graph.recovery import (
    CapabilityRejectedError,
    RecoveryConflictError,
    RecoveryNotFoundError,
    RecoveryStore,
    RecoveryStoreError,
)
from agent_runtime.core.llm.provider import AnthropicProvider
from agent_runtime.core.logging_setup import setup_logging
from agent_runtime.core.mcp.server import McpServerManager
from agent_runtime.core.permissions.manager import PermissionManager
from agent_runtime.core.permissions.storage import load_policy_file
from agent_runtime.core.runner import AgentRunner
from agent_runtime.core.runs import RUNS_DIR, new_run_id
from agent_runtime.core.sandbox import SandboxKey, SandboxManager
from agent_runtime.core.sandbox.factory import create_sandbox_manager
from agent_runtime.core.session import SessionManager, SessionStore
from agent_runtime.core.session.manager import SESSION_NOT_FOUND
from agent_runtime.core.session.store import TranscriptStoreError
from agent_runtime.core.trace.record import TraceRecord
from agent_runtime.core.trace.writer import TraceWriter
from agent_runtime.core.transport.ipc_broadcaster import IpcEventBroadcaster
from agent_runtime.core.transport.socket_server import (
    SocketServer,
    get_connection_writer,
    redact_sensitive_fields,
)

if TYPE_CHECKING:
    from agent_runtime.core.llm.base import LLMProvider
    from agent_runtime.core.tools.base import BaseTool

logger = logging.getLogger(__name__)

_SAFE_RUN_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}\Z")
_STRONG_REPLAY_RUN_ID = re.compile(r"\d{8}-\d{6}-[0-9a-f]{32}\Z")
_MAX_REPLAY_LINE_BYTES = 1024 * 1024
_MAX_REPLAY_TOTAL_BYTES = 8 * 1024 * 1024
_MAX_REPLAY_LINES = 10_000
_MAX_REPLAY_EVENTS = 1_000
_MAX_REPLAY_SESSION_ROOTS = 10_000
RECOVERY_REJECTED = -32030
RECOVERY_CONFLICT = -32031
RECOVERY_STATE_ERROR = -32032
EVENT_LOG_ERROR = -32033


def _now() -> str:
    return datetime.datetime.now(UTC).isoformat()


def _install_shutdown_handlers(loop: asyncio.AbstractEventLoop, shutdown: asyncio.Event) -> None:
    def request_shutdown() -> None:
        shutdown.set()

    def fallback_handler(_signum: int, _frame: object) -> None:
        loop.call_soon_threadsafe(request_shutdown)

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, request_shutdown)
        except NotImplementedError:
            signal.signal(sig, fallback_handler)


class CoreApp:
    def __init__(
        self,
        engine_router: EngineRouter | None = None,
        *,
        provider_factory: Callable[[RuntimeConfig], LLMProvider] | None = None,
        extra_tools_factory: Callable[[], list[BaseTool]] | None = None,
        sandbox_manager: SandboxManager | None = None,
    ) -> None:
        self._start_time = time.monotonic()
        self._bus = EventBus()
        self._broadcaster: IpcEventBroadcaster | None = None
        self._trace: TraceWriter | None = None
        self._config: RuntimeConfig | None = None
        self._running_runs: set[asyncio.Task[Any]] = set()
        self._run_tasks_by_session: dict[str, set[asyncio.Task[Any]]] = {}
        self._cleanup_tasks: set[asyncio.Task[None]] = set()
        self._session_resource_tasks: dict[str, asyncio.Task[None]] = {}
        self._engine_router = engine_router if engine_router is not None else EngineRouter()
        self._provider_factory = provider_factory
        self._extra_tools_factory = extra_tools_factory or (lambda: [])
        self._sandbox_manager = sandbox_manager if sandbox_manager is not None else SandboxManager()
        self._sandbox_injected = sandbox_manager is not None
        self._server: SocketServer | None = None
        self._sessions: SessionManager | None = None
        self._permission_manager: PermissionManager | None = None
        self._mcp_manager: McpServerManager | None = None
        self._sessions_root = Path("~/.agentrt/sessions").expanduser()
        self._runs_root = RUNS_DIR
        self._data_root = Path("~/.agentrt").expanduser()
        self._recovery_store: RecoveryStore | None = None
        self._durable_enabled = False
        self._resume_attach_inflight: dict[str, bytes] = {}

    def _provider_for_runner(self, config: RuntimeConfig) -> LLMProvider | None:
        if self._provider_factory is None:
            return None
        return self._provider_factory(config)

    def _provider_for_compaction(self, config: RuntimeConfig) -> LLMProvider:
        if self._provider_factory is not None:
            return self._provider_factory(config)
        return AnthropicProvider(config.llm.default_model)

    # 处理 core.ping 请求，返回服务版本、运行时长和接收时间
    async def _ping_handler(self, params: dict[str, Any]) -> PongResult:
        client = params.get("client", "unknown")
        logger.debug("ping from %s", client)
        return PongResult(
            server_version=agent_runtime.__version__,
            uptime_ms=int((time.monotonic() - self._start_time) * 1000),
            received_at=datetime.datetime.now(datetime.UTC).isoformat(),
        )

    # 将 EventBus 事件写入 trace（作为 EventBus 订阅者）
    async def _trace_event_handler(self, event: BaseModel) -> None:
        assert self._trace is not None
        event_dict = redact_sensitive_fields(event.model_dump())
        self._trace.emit(
            TraceRecord(
                ts=_now(),
                direction="CORE",
                layer="event",
                kind="event",
                run_id=event_dict.get("run_id"),
                data=event_dict,
            )
        )

    def _track_run_task(self, session_id: str, task: asyncio.Task[Any]) -> None:
        self._running_runs.add(task)
        self._run_tasks_by_session.setdefault(session_id, set()).add(task)

    def _untrack_run_task(self, session_id: str, task: asyncio.Task[Any]) -> None:
        self._running_runs.discard(task)
        tasks = self._run_tasks_by_session.get(session_id)
        if tasks is None:
            return
        tasks.discard(task)
        if not tasks:
            self._run_tasks_by_session.pop(session_id, None)

    def _track_cleanup_task(self, task: asyncio.Task[None]) -> None:
        self._cleanup_tasks.add(task)

        def _discard(completed: asyncio.Task[None]) -> None:
            self._cleanup_tasks.discard(completed)
            if completed.cancelled():
                return
            error = completed.exception()
            if error is not None:
                logger.error("session disconnect cleanup failed: %s", error)

        task.add_done_callback(_discard)

    async def _cleanup_disconnected_sessions(self, session_ids: frozenset[str]) -> None:
        run_tasks: set[asyncio.Task[Any]] = set()
        for session_id in session_ids:
            if self._sessions is not None:
                self._sessions.begin_close(session_id)
            run_tasks.update(self._run_tasks_by_session.pop(session_id, set()))
        for task in run_tasks:
            if not task.done():
                task.cancel()
        if run_tasks:
            await asyncio.gather(*run_tasks, return_exceptions=True)
            self._running_runs.difference_update(run_tasks)
        errors: list[Exception] = []
        for session_id in session_ids:
            try:
                if self._sessions is not None:
                    preserve = await self._sessions.close_disconnected(session_id)
                    if not preserve:
                        await self._delete_session_resources(session_id)
                else:
                    await self._delete_session_resources(session_id)
            except Exception as exc:
                # A failed close must not strand another session on this socket.
                # A durable recovery lookup failure does not authorize release;
                # each session's close path decides its ownership independently.
                errors.append(exc)
        if errors:
            raise ExceptionGroup("session disconnect cleanup failed", errors)

    async def _delete_session_resources(self, session_id: str) -> None:
        task = self._session_resource_tasks.get(session_id)
        if task is None or (task.done() and (task.cancelled() or task.exception() is not None)):
            task = asyncio.create_task(self._release_session_resources(session_id))
            self._session_resource_tasks[session_id] = task
        cancelled = False
        while not task.done():
            try:
                await asyncio.shield(task)
            except asyncio.CancelledError:
                cancelled = True
        task.result()
        if cancelled:
            raise asyncio.CancelledError()

    async def _release_session_resources(self, session_id: str) -> None:
        try:
            if self._recovery_store is not None:
                await self._recovery_store.delete_capability(session_id)
            await self._engine_router.delete_thread(session_id)
        finally:
            await self._sandbox_manager.release(
                SandboxKey("session", session_id), reason="session_closed"
            )

    async def _cancel_session_run_tasks(self, session_id: str) -> None:
        current = asyncio.current_task()
        tasks = {
            task
            for task in self._run_tasks_by_session.pop(session_id, set())
            if task is not current
        }
        for task in tasks:
            if not task.done():
                task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
            self._running_runs.difference_update(tasks)

    async def _wait_for_cleanup_tasks(self) -> None:
        while self._cleanup_tasks:
            pending = list(self._cleanup_tasks)
            await asyncio.gather(*pending, return_exceptions=True)

    # 启动一次 agent run：异步创建 AgentRunner 并立即返回 run_id
    async def _agent_run_handler(self, params: dict[str, Any]) -> AgentRunResult:
        assert self._sessions is not None
        cmd = AgentRunCommand.model_validate(params)
        writer = get_connection_writer()
        session = await self._sessions.create(
            mode="one_shot",
            title=cmd.goal[:40],
            before_publish=lambda created: self._bind_connection_to_session(writer, created.id),
        )
        # session.created 的写入可能发现连接已断开并同步清理 ownership；此处在创建
        # 后台任务前再次确认，避免断连回调早于 task 登记而留下孤儿 run。
        if self._broadcaster is None or not self._broadcaster.owns_session(writer, session.id):
            raise HandlerError(SESSION_NOT_FOUND, "session not found")
        run_id = new_run_id()
        run_task = asyncio.create_task(
            self._sessions.send_message(session.id, cmd.goal, run_id=run_id)
        )
        self._track_run_task(session.id, run_task)

        def _discard(completed: asyncio.Task[Any]) -> None:
            self._untrack_run_task(session.id, completed)

        run_task.add_done_callback(_discard)
        return AgentRunResult(run_id=run_id)

    # 创建 chat 或 one_shot session，并返回 session_id
    async def _session_create_handler(self, params: dict[str, Any]) -> SessionCreateResult:
        assert self._sessions is not None
        cmd = SessionCreateCommand.model_validate(params)
        writer = get_connection_writer()
        durable = self._durable_enabled and cmd.mode == "chat"
        create_args: dict[str, Any] = {
            "mode": cmd.mode,
            "title": cmd.title,
            "before_publish": lambda created: self._bind_connection_to_session(writer, created.id),
        }
        if durable:
            create_args["durable"] = True
        session = await self._sessions.create(**create_args)
        resume_token: str | None = None
        if durable:
            assert self._recovery_store is not None
            try:
                grant = await self._recovery_store.issue_capability(session.id)
            except RecoveryStoreError as exc:
                raise HandlerError(
                    RECOVERY_STATE_ERROR,
                    "durable session could not be initialized",
                    {"code": exc.code},
                ) from exc
            resume_token = grant.token
        return SessionCreateResult(
            session_id=session.id,
            status=session.status,
            resume_token=resume_token,
        )

    # 向 session 发送一条用户消息并同步等待对应 run 完成
    async def _session_send_handler(self, params: dict[str, Any]) -> SessionSendMessageResult:
        assert self._sessions is not None
        cmd = SessionSendMessageCommand.model_validate(params)
        self._require_current_connection_owns(cmd.session_id)
        self._sessions.hydrate(cmd.session_id)
        current_task = asyncio.current_task()
        if current_task is not None:
            self._track_run_task(cmd.session_id, current_task)
        try:
            try:
                run_id = await self._sessions.send_message(cmd.session_id, cmd.content)
            except RecoveryStoreError as exc:
                raise HandlerError(
                    RECOVERY_STATE_ERROR,
                    "durable run state could not be updated",
                    {"code": exc.code},
                ) from exc
        finally:
            if current_task is not None:
                self._untrack_run_task(cmd.session_id, current_task)
        return SessionSendMessageResult(run_id=run_id)

    # 返回 session 的完整 Anthropic messages 历史
    async def _session_history_handler(self, params: dict[str, Any]) -> SessionGetHistoryResult:
        assert self._sessions is not None
        cmd = SessionGetHistoryCommand.model_validate(params)
        self._require_current_connection_owns(cmd.session_id)
        self._sessions.hydrate(cmd.session_id)
        messages = await self._sessions.get_history(cmd.session_id)
        return SessionGetHistoryResult(messages=messages)

    async def _latest_graph_state(self, session_id: str, run_id: str) -> Any:
        try:
            return await self._engine_router.latest_graph_state(
                session_id,
                session_id=session_id,
                run_id=run_id,
            )
        except RuntimeError as exc:
            if getattr(exc, "code", None) != "checkpoint_conflict":
                raise
            raise HandlerError(
                RECOVERY_CONFLICT,
                "checkpoint state conflicts with recovery metadata",
                {"code": "checkpoint_conflict"},
            ) from exc

    async def _coordinate_recovery_checkpoint(self, run: Any, state: Any) -> Any:
        """Persist a validated checkpoint revision discovered after process loss."""

        assert self._recovery_store is not None
        if (
            run.checkpoint_revision is not None
            and run.checkpoint_revision != state.checkpoint_revision
            and run.suspension_reason != "process_recovery"
        ):
            raise HandlerError(
                RECOVERY_CONFLICT,
                "checkpoint revision conflict",
                {"code": "checkpoint_conflict"},
            )
        event_path = self._sessions_root / run.session_id / "runs" / run.run_id / "events.jsonl"
        try:
            durable_event_seq = read_event_log(event_path, repair_tail=True).last_event_seq
        except EventLogError as exc:
            raise HandlerError(
                EVENT_LOG_ERROR,
                "event log is corrupt",
                {"code": exc.code},
            ) from exc
        event_seq = max(run.event_seq, state.event_seq, durable_event_seq)
        reason = state.suspension_reason or run.suspension_reason or "process_recovery"
        if (
            run.checkpoint_revision == state.checkpoint_revision
            and run.event_seq == event_seq
            and run.suspension_reason == reason
        ):
            return run
        try:
            return await self._recovery_store.mark_run_suspended(
                run.session_id,
                run.run_id,
                reason=reason,
                checkpoint_revision=state.checkpoint_revision,
                event_seq=event_seq,
                expected_resume_epoch=run.resume_epoch,
            )
        except RecoveryStoreError as exc:
            raise HandlerError(
                RECOVERY_CONFLICT,
                "checkpoint coordination conflict",
                {"code": exc.code},
            ) from exc

    def _recovery_event_path(self, session_id: str, run_id: str) -> Path:
        return self._sessions_root / session_id / "runs" / run_id / "events.jsonl"

    def _read_recovery_events(self, session_id: str, run_id: str) -> Any:
        try:
            return read_event_log(
                self._recovery_event_path(session_id, run_id),
                repair_tail=True,
            )
        except EventLogError as exc:
            raise HandlerError(
                EVENT_LOG_ERROR,
                "event log is corrupt",
                {"code": exc.code},
            ) from exc

    @staticmethod
    def _persisted_terminal_event(snapshot: Any, run_id: str) -> dict[str, Any] | None:
        finished = [
            event
            for event in snapshot.events
            if event.get("run_id") == run_id and event.get("type") == "run.finished"
        ]
        if not finished:
            return None
        if len(finished) != 1 or snapshot.events[-1] is not finished[0]:
            raise HandlerError(
                RECOVERY_CONFLICT,
                "run lifecycle conflicts with recovery metadata",
                {"code": "lifecycle_conflict"},
            )
        status = finished[0].get("status")
        if status not in {"success", "failed"}:
            raise HandlerError(
                RECOVERY_CONFLICT,
                "run lifecycle conflicts with recovery metadata",
                {"code": "lifecycle_conflict"},
            )
        return dict(finished[0])

    async def _repair_terminal_manifest(
        self,
        run: Any,
        state: Any,
        snapshot: Any,
        terminal_event: dict[str, Any],
    ) -> Any:
        """Make an already-durable terminal event authoritative after a crash."""

        assert self._recovery_store is not None
        checkpoint_revision = (
            state.checkpoint_revision if state is not None else run.checkpoint_revision
        )
        try:
            terminal = await self._recovery_store.mark_run_terminal(
                run.session_id,
                run.run_id,
                status=terminal_event["status"],
                checkpoint_revision=checkpoint_revision,
                event_seq=max(run.event_seq, snapshot.last_event_seq),
                transcript_commit_count=run.transcript_commit_count,
                transcript_commit_hash=run.transcript_commit_hash,
                expected_resume_epoch=run.resume_epoch,
            )
        except RecoveryStoreError as exc:
            raise HandlerError(
                RECOVERY_CONFLICT,
                "terminal recovery coordination conflict",
                {"code": exc.code},
            ) from exc

        if terminal_event.get("reason") == "session_closed":
            store = SessionStore(self._sessions_root)
            try:
                session = store.read_meta(run.session_id)
            except (FileNotFoundError, OSError, ValueError, KeyError) as exc:
                raise HandlerError(
                    RECOVERY_STATE_ERROR,
                    "durable session metadata is unavailable",
                    {"code": "session_state_error"},
                ) from exc
            session.status = "closed"
            session.active_run_id = None
            session.updated_at = _now()
            store.write_meta(session)
            await self._delete_session_resources(run.session_id)
        return terminal

    async def _fail_run_without_checkpoint(self, run: Any, snapshot: Any) -> Any:
        """Close the create_run-before-first-checkpoint crash window durably."""

        assert self._recovery_store is not None
        event_path = self._recovery_event_path(run.session_id, run.run_id)
        bus = EventBus(correlation_id=run.run_id, session_id=run.session_id)
        async with EventWriter(event_path, run_id=run.run_id) as writer:
            writer.subscribe(bus)
            has_started = any(
                event.get("run_id") == run.run_id and event.get("type") == "run.started"
                for event in snapshot.events
            )
            if not has_started:
                goal = ""
                try:
                    messages = SessionStore(self._sessions_root).read_messages_strict(
                        run.session_id
                    )
                except (FileNotFoundError, OSError, ValueError, KeyError, TranscriptStoreError):
                    messages = []
                for message in reversed(messages):
                    if message.get("role") == "user":
                        goal = str(message.get("content", ""))
                        break
                await bus.publish(RunStartedEvent(run_id=run.run_id, goal=goal, ts=_now()))
            await bus.publish(
                RunFinishedEvent(
                    run_id=run.run_id,
                    status="failed",
                    reason="process_lost_before_checkpoint",
                    steps=0,
                    ts=_now(),
                )
            )
        refreshed = self._read_recovery_events(run.session_id, run.run_id)
        terminal_event = self._persisted_terminal_event(refreshed, run.run_id)
        assert terminal_event is not None
        return await self._repair_terminal_manifest(run, None, refreshed, terminal_event)

    async def _reconcile_interrupted_run(self, run: Any) -> Any:
        """Reconcile one process-loss run without executing Graph or tools."""

        snapshot = self._read_recovery_events(run.session_id, run.run_id)
        terminal_event = self._persisted_terminal_event(snapshot, run.run_id)
        state = await self._latest_graph_state(run.session_id, run.run_id)
        if terminal_event is not None:
            return await self._repair_terminal_manifest(run, state, snapshot, terminal_event)
        if state is None:
            if run.checkpoint_revision is None:
                return await self._fail_run_without_checkpoint(run, snapshot)
            raise HandlerError(
                RECOVERY_CONFLICT,
                "checkpoint state is unavailable",
                {"code": "checkpoint_not_found"},
            )
        return await self._coordinate_recovery_checkpoint(run, state)

    async def _resume_leased_run(self, session_id: str, run: Any, resume_value: object) -> None:
        assert self._sessions is not None
        current_task = asyncio.current_task()
        if current_task is not None:
            self._track_run_task(session_id, current_task)
        try:
            result = await self._sessions.resume_run(
                session_id,
                run,
                resume_value=resume_value,
            )
        finally:
            if current_task is not None:
                self._untrack_run_task(session_id, current_task)
        error = getattr(result, "error", None)
        if error is not None and error.code == "checkpoint_conflict":
            raise HandlerError(
                RECOVERY_CONFLICT,
                "checkpoint revision conflict",
                {"code": error.code},
            )

    async def _session_resume_handler(self, params: dict[str, Any]) -> SessionResumeResult:
        assert self._sessions is not None
        if self._recovery_store is None:
            raise HandlerError(RECOVERY_REJECTED, "resume capability rejected")
        cmd = SessionResumeCommand.model_validate(params)

        token_fingerprint = hashlib.sha256(cmd.resume_token.encode("utf-8")).digest()
        existing_fingerprint = self._resume_attach_inflight.get(cmd.session_id)
        if existing_fingerprint is not None and hmac.compare_digest(
            existing_fingerprint,
            token_fingerprint,
        ):
            raise HandlerError(
                RECOVERY_CONFLICT,
                "durable resume conflict",
                {"code": "resume_in_progress"},
            )
        owns_attach_claim = existing_fingerprint is None
        if owns_attach_claim:
            self._resume_attach_inflight[cmd.session_id] = token_fingerprint
        broadcaster: IpcEventBroadcaster | None = None
        writer: asyncio.StreamWriter | None = None
        reserved_by_request = False
        attach_committed = False
        try:
            validation = await self._recovery_store.validate_capability(
                cmd.session_id,
                cmd.resume_token,
            )
            if not owns_attach_claim:
                current_fingerprint = self._resume_attach_inflight.get(cmd.session_id)
                if current_fingerprint is not None and hmac.compare_digest(
                    current_fingerprint,
                    token_fingerprint,
                ):
                    raise HandlerError(
                        RECOVERY_CONFLICT,
                        "durable resume conflict",
                        {"code": "resume_in_progress"},
                    )
                self._resume_attach_inflight[cmd.session_id] = token_fingerprint
                owns_attach_claim = True
            if self._sessions.is_recovery_closed(cmd.session_id):
                await self._delete_session_resources(cmd.session_id)
                raise HandlerError(RECOVERY_REJECTED, "resume capability rejected")

            broadcaster = self._broadcaster
            assert broadcaster is not None
            writer = get_connection_writer()
            already_owned = broadcaster.owns_session(writer, cmd.session_id)
            if not broadcaster.reserve_session(writer, cmd.session_id):
                raise HandlerError(SESSION_NOT_FOUND, "session not found")
            reserved_by_request = not already_owned
            run = await self._recovery_store.latest_unfinished_run(cmd.session_id)
            state = (
                await self._latest_graph_state(cmd.session_id, run.run_id)
                if run is not None
                else None
            )
            if run is not None and state is not None:
                run = await self._coordinate_recovery_checkpoint(run, state)
            current_revision = (
                state.checkpoint_revision
                if state is not None
                else run.checkpoint_revision
                if run is not None
                else None
            )
            if cmd.expected_revision is not None and cmd.expected_revision != current_revision:
                raise HandlerError(
                    RECOVERY_CONFLICT,
                    "checkpoint revision conflict",
                    {"code": "checkpoint_conflict"},
                )
            if run is not None and (run.status != "suspended" or state is None):
                raise HandlerError(
                    RECOVERY_CONFLICT,
                    "durable run is not attachable",
                    {"code": "recovery_conflict"},
                )
            session = self._sessions.hydrate_recovery(cmd.session_id, run)

            resume_value: object | None = None
            auto_resume = False
            if run is not None and state is not None and state.resumable:
                if state.suspension_reason == "process_recovery":
                    auto_resume = True
                elif state.suspension_reason == "permission" and self._permission_expired(state):
                    auto_resume = True
                    resume_value = "timeout"
            if auto_resume:
                assert run is not None
                assert state is not None
                try:
                    lease = await self._recovery_store.acquire_resume_lease(
                        cmd.session_id,
                        run.run_id,
                        expected_checkpoint_revision=state.checkpoint_revision,
                        expected_resume_epoch=run.resume_epoch,
                    )
                except RecoveryStoreError as exc:
                    raise HandlerError(
                        RECOVERY_CONFLICT,
                        "durable resume lease conflict",
                        {"code": exc.code},
                    ) from exc
                await self._resume_leased_run(cmd.session_id, lease, resume_value)
                run = await self._recovery_store.latest_run(cmd.session_id)
                state = (
                    await self._latest_graph_state(cmd.session_id, run.run_id)
                    if run is not None
                    else None
                )

            result_run_id = run.run_id if run is not None else None
            result_status = state.status if state is not None else session.status
            result_reason = state.suspension_reason if state is not None else None
            result_revision = state.checkpoint_revision if state is not None else None
            result_event_seq = max(
                run.event_seq if run is not None else 0,
                state.event_seq if state is not None else 0,
            )
            grant = await self._recovery_store.rotate_capability(
                cmd.session_id,
                cmd.resume_token,
                expected_token_version=validation.token_version,
            )
            if not broadcaster.commit_reserved_session(writer, cmd.session_id):
                raise HandlerError(SESSION_NOT_FOUND, "session not found")
            attach_committed = True
        except CapabilityRejectedError as exc:
            raise HandlerError(
                RECOVERY_REJECTED,
                "resume capability rejected",
                {"code": exc.code},
            ) from exc
        except (RecoveryConflictError, RecoveryNotFoundError) as exc:
            raise HandlerError(
                RECOVERY_CONFLICT,
                "durable resume conflict",
                {"code": exc.code},
            ) from exc
        except RecoveryStoreError as exc:
            raise HandlerError(
                RECOVERY_STATE_ERROR,
                "durable state is unavailable",
                {"code": exc.code},
            ) from exc
        finally:
            if reserved_by_request and not attach_committed:
                assert broadcaster is not None
                assert writer is not None
                broadcaster.release_reserved_session(writer, cmd.session_id)
            if owns_attach_claim:
                current_fingerprint = self._resume_attach_inflight.get(cmd.session_id)
                if current_fingerprint is not None and hmac.compare_digest(
                    current_fingerprint,
                    token_fingerprint,
                ):
                    self._resume_attach_inflight.pop(cmd.session_id, None)

        return SessionResumeResult(
            session_id=cmd.session_id,
            run_id=result_run_id,
            status=result_status,
            suspension_reason=result_reason,
            checkpoint_revision=result_revision,
            event_seq=result_event_seq,
            next_resume_token=grant.token,
        )

    @staticmethod
    def _permission_expired(state: Any) -> bool:
        if state.pending_expires_at is None:
            return False
        try:
            deadline = datetime.datetime.fromisoformat(
                state.pending_expires_at.replace("Z", "+00:00")
            )
            if deadline.tzinfo is None:
                raise ValueError("durable permission deadline must include a timezone")
        except ValueError as exc:
            raise HandlerError(
                RECOVERY_CONFLICT,
                "checkpoint permission deadline is invalid",
                {"code": "checkpoint_conflict"},
            ) from exc
        return datetime.datetime.now(UTC) >= deadline.astimezone(UTC)

    async def _run_get_state_handler(self, params: dict[str, Any]) -> RunGetStateResult:
        if self._recovery_store is None:
            raise HandlerError(RECOVERY_STATE_ERROR, "durable state is unavailable")
        cmd = RunGetStateCommand.model_validate(params)
        self._require_current_connection_owns(cmd.session_id)
        assert self._sessions is not None
        self._sessions.hydrate(cmd.session_id)
        try:
            run = (
                await self._recovery_store.get_run(cmd.session_id, cmd.run_id)
                if cmd.run_id is not None
                else await self._recovery_store.latest_run(cmd.session_id)
            )
        except RecoveryStoreError as exc:
            raise HandlerError(
                RECOVERY_STATE_ERROR,
                "durable run state is unavailable",
                {"code": exc.code},
            ) from exc
        if run is None:
            raise HandlerError(RECOVERY_STATE_ERROR, "durable run state is unavailable")
        state = await self._latest_graph_state(cmd.session_id, run.run_id)
        if state is None:
            raise HandlerError(
                RECOVERY_STATE_ERROR,
                "checkpoint state is unavailable",
                {"code": "checkpoint_not_found"},
            )
        if run.status == "suspended":
            run = await self._coordinate_recovery_checkpoint(run, state)
        pending_summary: dict[str, str] | None = None
        if state.pending_tool_use_id is not None:
            pending_summary = {"tool_use_id": state.pending_tool_use_id}
            if state.interrupt_id is not None:
                pending_summary["interrupt_id"] = state.interrupt_id
            if state.pending_tool_name is not None:
                pending_summary["tool_name"] = state.pending_tool_name
            if state.pending_param_preview is not None:
                pending_summary["param_preview"] = state.pending_param_preview
            if state.pending_expires_at is not None:
                pending_summary["expires_at"] = state.pending_expires_at
        return RunGetStateResult(
            session_id=state.session_id,
            run_id=state.run_id,
            status=state.status,
            current_node=state.current_node,
            next_node=state.next_node,
            suspension_reason=state.suspension_reason,
            checkpoint_revision=state.checkpoint_revision,
            event_seq=max(run.event_seq, state.event_seq),
            pending_approval_summary=pending_summary,
            resumable=state.resumable,
        )

    # 接收客户端权限审批响应，resolve 对应挂起的 Future
    async def _permission_respond_handler(self, params: dict[str, Any]) -> PermissionRespondResult:
        cmd = PermissionRespondCommand.model_validate(params)
        logger.info(
            "permission.respond received tool_use_id=%s decision=%s",
            cmd.tool_use_id,
            cmd.decision,
        )
        if self._permission_manager is None:
            logger.error("permission.respond: PermissionManager not initialized")
            return PermissionRespondResult(ok=False)
        writer = get_connection_writer()
        authorized_session_ids = (
            self._broadcaster.session_ids_for(writer)
            if self._broadcaster is not None
            else frozenset()
        )
        if (
            cmd.session_id is not None
            and cmd.run_id is not None
            and cmd.session_id in authorized_session_ids
            and self._sessions is not None
        ):
            session = self._sessions.hydrate(cmd.session_id)
            if session.durable:
                return await self._respond_durable_permission(cmd)
        ok = self._permission_manager.respond(
            cmd.tool_use_id,
            cmd.decision,
            authorized_session_ids=authorized_session_ids,
            session_id=cmd.session_id,
            run_id=cmd.run_id,
        )
        return PermissionRespondResult(ok=ok)

    async def _respond_durable_permission(
        self,
        cmd: PermissionRespondCommand,
    ) -> PermissionRespondResult:
        assert self._sessions is not None
        assert self._recovery_store is not None
        if (
            cmd.session_id is None
            or cmd.run_id is None
            or cmd.interrupt_id is None
            or cmd.expected_revision is None
            or cmd.decision not in {"allow_once", "always_allow", "deny_once", "always_deny"}
        ):
            return PermissionRespondResult(ok=False)
        try:
            run = await self._recovery_store.get_run(cmd.session_id, cmd.run_id)
        except RecoveryStoreError as exc:
            raise HandlerError(
                RECOVERY_STATE_ERROR,
                "durable run state is unavailable",
                {"code": exc.code},
            ) from exc
        if run is None or run.status != "suspended":
            return PermissionRespondResult(ok=False)
        state = await self._latest_graph_state(cmd.session_id, cmd.run_id)
        if (
            state is None
            or state.suspension_reason != "permission"
            or not state.resumable
            or state.session_id != cmd.session_id
            or state.run_id != cmd.run_id
            or state.pending_tool_use_id != cmd.tool_use_id
            or state.interrupt_id != cmd.interrupt_id
            or state.checkpoint_revision != cmd.expected_revision
        ):
            return PermissionRespondResult(ok=False)
        run = await self._coordinate_recovery_checkpoint(run, state)

        expired = self._permission_expired(state)

        try:
            lease = await self._recovery_store.acquire_resume_lease(
                cmd.session_id,
                cmd.run_id,
                expected_checkpoint_revision=cmd.expected_revision,
                expected_resume_epoch=run.resume_epoch,
            )
        except RecoveryStoreError as exc:
            raise HandlerError(
                RECOVERY_CONFLICT,
                "durable approval lease conflict",
                {"code": exc.code},
            ) from exc
        await self._resume_leased_run(
            cmd.session_id,
            lease,
            "timeout" if expired else cmd.decision,
        )
        return PermissionRespondResult(ok=not expired)

    # 在 session.created 发布前独占登记连接 owner；断连竞态统一伪装为 not found
    def _bind_connection_to_session(
        self,
        writer: asyncio.StreamWriter,
        session_id: str,
    ) -> None:
        if self._broadcaster is None or not self._broadcaster.bind_session(writer, session_id):
            raise HandlerError(SESSION_NOT_FOUND, "session not found")

    def _require_current_connection_owns(self, session_id: str) -> None:
        writer = get_connection_writer()
        if self._broadcaster is None or not self._broadcaster.owns_session(writer, session_id):
            raise HandlerError(SESSION_NOT_FOUND, "session not found")

    # 断连后先取消并等待所属 run，再清理该 app 的 Graph thread。
    def _on_client_disconnect(self, session_ids: frozenset[str]) -> None:
        for session_id in session_ids:
            if self._permission_manager is not None:
                self._permission_manager.cancel_session(session_id)
        cleanup_task = asyncio.create_task(self._cleanup_disconnected_sessions(session_ids))
        self._track_cleanup_task(cleanup_task)

    # 手动压缩 session thread，将摘要持久化写入 thread.jsonl
    async def _session_compact_handler(self, params: dict[str, Any]) -> SessionCompactResult:
        assert self._sessions is not None
        cmd = SessionCompactCommand.model_validate(params)
        self._require_current_connection_owns(cmd.session_id)
        self._sessions.hydrate(cmd.session_id)
        result = await self._sessions.compact(cmd.session_id, cmd.focus)
        return result  # type: ignore[no-any-return]

    # 关闭 session 并返回 closed 状态
    async def _session_close_handler(self, params: dict[str, Any]) -> SessionCloseResult:
        assert self._sessions is not None
        cmd = SessionCloseCommand.model_validate(params)
        self._require_current_connection_owns(cmd.session_id)
        self._sessions.begin_close(cmd.session_id)
        await self._cancel_session_run_tasks(cmd.session_id)
        self._sessions.hydrate(cmd.session_id)
        try:
            await self._sessions.close(cmd.session_id)
        except RecoveryStoreError as exc:
            raise HandlerError(
                RECOVERY_STATE_ERROR,
                "durable session could not be closed",
                {"code": exc.code},
            ) from exc
        return SessionCloseResult(status="closed")

    # 注册客户端事件订阅，可选先回放 events.jsonl 历史再接收实时流
    async def _subscribe_handler(self, params: dict[str, Any]) -> EventSubscribeResult:
        cmd = EventSubscribeCommand.model_validate(params)
        writer = get_connection_writer()
        assert self._broadcaster is not None

        replayed_count = 0
        sub_id = self._broadcaster.subscribe(
            writer,
            cmd.topics,
            cmd.scope,
            paused=cmd.replay_from_run is not None,
        )
        if cmd.replay_from_run is not None:
            try:
                replayed_count, snapshot_event_seq = await self._replay_events_snapshot(
                    cmd.replay_from_run,
                    writer,
                    cmd.topics,
                    cmd.scope,
                    after_event_seq=cmd.after_event_seq,
                )
                await self._broadcaster.activate_buffered(
                    sub_id,
                    replay_run_id=cmd.replay_from_run,
                    after_event_seq=snapshot_event_seq,
                )
            except (Exception, asyncio.CancelledError):
                self._broadcaster.unsubscribe_id(sub_id)
                raise
        return EventSubscribeResult(subscription_id=sub_id, replayed_count=replayed_count)

    # 从 events.jsonl 向 writer 回放匹配 topic 的历史事件，返回已回放条数
    async def _replay_events(
        self,
        run_id: str,
        writer: asyncio.StreamWriter,
        topics: list[str],
        scope: str,
        *,
        after_event_seq: int = 0,
    ) -> int:
        count, _ = await self._replay_events_snapshot(
            run_id,
            writer,
            topics,
            scope,
            after_event_seq=after_event_seq,
        )
        return count

    async def _replay_events_snapshot(
        self,
        run_id: str,
        writer: asyncio.StreamWriter,
        topics: list[str],
        scope: str,
        *,
        after_event_seq: int = 0,
    ) -> tuple[int, int]:
        if _SAFE_RUN_ID.fullmatch(run_id) is None or run_id in {".", ".."}:
            return 0, after_event_seq

        path = self._find_replay_file(run_id, writer)
        if path is None:
            return 0, after_event_seq

        try:
            snapshot = read_event_log(path)
        except EventLogError as exc:
            raise HandlerError(
                EVENT_LOG_ERROR,
                "event log is corrupt",
                {"code": exc.code},
            ) from exc
        except OSError:
            logger.debug("replay file became unavailable: %s", path, exc_info=True)
            return 0, after_event_seq

        events: list[dict[str, Any]] = []
        for event in snapshot.events:
            event_seq = event.get("event_seq")
            if event_seq is None:
                if after_event_seq == 0:
                    events.append(dict(event))
            elif isinstance(event_seq, int) and event_seq > after_event_seq:
                events.append(dict(event))

        if len(events) > _MAX_REPLAY_LINES:
            raise HandlerError(
                EVENT_LOG_ERROR,
                "event replay exceeds limit",
                {"code": "replay_limit_exceeded"},
            )

        broadcaster = self._broadcaster
        assert broadcaster is not None
        replay_rows: list[tuple[dict[str, Any], bytes]] = []
        total_bytes = 0
        for event in events:
            raw = json.dumps(event, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
            total_bytes += len(raw) + 1
            if total_bytes > _MAX_REPLAY_TOTAL_BYTES:
                raise HandlerError(
                    EVENT_LOG_ERROR,
                    "event replay exceeds limit",
                    {"code": "replay_limit_exceeded"},
                )
            if event.get("run_id") != run_id:
                continue
            event_type = event.get("type")
            if not isinstance(event_type, str):
                continue
            if not any(fnmatch.fnmatch(event_type, pattern) for pattern in topics):
                continue
            if not broadcaster.can_replay(writer, event, scope):
                continue
            if len(raw) > _MAX_REPLAY_LINE_BYTES or len(replay_rows) >= _MAX_REPLAY_EVENTS:
                raise HandlerError(
                    EVENT_LOG_ERROR,
                    "event replay exceeds limit",
                    {"code": "replay_limit_exceeded"},
                )
            replay_rows.append((event, raw))

        count = 0
        replayed_event_seq = after_event_seq
        for event, _raw in replay_rows:
            envelope = EventPushEnvelope(event=event)
            try:
                writer.write(envelope.model_dump_json().encode() + b"\n")
            except (ConnectionResetError, BrokenPipeError, OSError):
                broadcaster.disconnect(writer)
                break
            count += 1
            event_seq = event.get("event_seq")
            if isinstance(event_seq, int):
                replayed_event_seq = event_seq

        if count:
            await broadcaster.drain_replay(writer)
        return count, replayed_event_seq

    def _find_replay_file(
        self,
        run_id: str,
        writer: asyncio.StreamWriter,
    ) -> Path | None:
        assert self._broadcaster is not None
        try:
            sessions_base = self._sessions_root.expanduser().resolve()
        except (OSError, RuntimeError):
            return None
        roots: list[Path] = []
        for session_id in sorted(self._broadcaster.session_ids_for(writer)):
            if len(roots) >= _MAX_REPLAY_SESSION_ROOTS:
                break
            try:
                owned_root = (sessions_base / session_id / "runs").resolve()
            except (OSError, RuntimeError):
                continue
            if owned_root.is_relative_to(sessions_base):
                roots.append(owned_root)

        # New run IDs are unguessable local read-only replay capabilities. Older
        # short/custom IDs remain replayable only from a session owned by this
        # connection; scanning unrelated roots would turn their low entropy into
        # a cross-client history oracle.
        if _STRONG_REPLAY_RUN_ID.fullmatch(run_id) is None:
            return self._find_run_in_roots(run_id, roots)

        try:
            global_root = self._runs_root.expanduser().resolve()
            if len(roots) < _MAX_REPLAY_SESSION_ROOTS and global_root not in roots:
                roots.append(global_root)
        except (OSError, RuntimeError):
            pass
        try:
            if sessions_base.is_dir():
                for session_dir in sessions_base.iterdir():
                    if len(roots) >= _MAX_REPLAY_SESSION_ROOTS:
                        break
                    try:
                        candidate_root = (session_dir / "runs").resolve()
                    except (OSError, RuntimeError):
                        continue
                    if (
                        len(roots) < _MAX_REPLAY_SESSION_ROOTS
                        and candidate_root.is_relative_to(sessions_base)
                        and candidate_root not in roots
                    ):
                        roots.append(candidate_root)
        except OSError:
            pass

        return self._find_run_in_roots(run_id, roots)

    @staticmethod
    def _find_run_in_roots(run_id: str, roots: list[Path]) -> Path | None:
        for root in roots:
            try:
                candidate = (root / run_id / "events.jsonl").resolve()
                if candidate.is_relative_to(root) and candidate.is_file():
                    return candidate
            except (OSError, RuntimeError):
                continue
        return None

    # 启动守护进程：加载配置、初始化日志、启动 trace、启动 TCP 服务器，并等待退出信号
    async def run(self) -> None:
        try:
            await self._serve()
        finally:
            cleanup = asyncio.create_task(self._shutdown())
            cancelled = False
            while not cleanup.done():
                try:
                    await asyncio.shield(cleanup)
                except asyncio.CancelledError:
                    cancelled = True
            cleanup.result()
            if cancelled:
                raise asyncio.CancelledError()

    async def _serve(self) -> None:
        self._start_time = time.monotonic()
        self._config = get_config()
        validate_runtime_sandbox(self._config)
        self._data_root = resolve_data_root(self._config)
        self._data_root.mkdir(parents=True, exist_ok=True)
        self._sessions_root = self._data_root / "sessions"
        self._runs_root = self._data_root / "runs"
        setup_logging(self._config)

        if self._config.trace.enabled:
            trace_path = Path(self._config.trace.file).expanduser()
            self._trace = TraceWriter(trace_path)
            await self._trace.start()
            self._bus.subscribe(self._trace_event_handler)

        config = self._config
        if not self._sandbox_injected:
            self._sandbox_manager = create_sandbox_manager(
                config.sandbox,
                self._data_root,
                trace_sink=self._sandbox_trace,
            )
        # Includes CNI attestation/policy checks and scope-limited orphan scan.
        # No listener, Worker, or command can bypass startup failure.
        await self._sandbox_manager.start()
        if config.agent.engine == "graph":
            checkpoint_path = resolve_graph_checkpoint_path(config, self._data_root)
            if config.graph.checkpoint_backend == "sqlite":
                self._recovery_store = RecoveryStore(self._data_root / "recovery.sqlite3")
            self._engine_router.configure_graph(
                backend=config.graph.checkpoint_backend,
                sqlite_path=checkpoint_path,
                recovery_store=self._recovery_store,
            )
            await self._engine_router.ensure_graph_available()
            if self._recovery_store is not None:
                interrupted = await self._recovery_store.suspend_interrupted_runs()
                for run in interrupted:
                    await self._reconcile_interrupted_run(run)
                logger.info("recovery: indexed %d interrupted run(s)", len(interrupted))
                self._durable_enabled = True

        policy_file = self._data_root / "policy.toml"
        self._permission_manager = PermissionManager(
            policy_file=policy_file,
            timeout_s=self._config.permission.timeout_s,
        )
        logger.info(
            "permission manager: timeout_s=%.1f  persistent=%d entries",
            self._config.permission.timeout_s,
            len(load_policy_file(policy_file)),
        )

        self._broadcaster = IpcEventBroadcaster(
            trace=self._trace,
            on_disconnect=self._on_client_disconnect,
        )
        self._bus.subscribe(self._broadcaster.handle)
        store = SessionStore(self._sessions_root)

        self._mcp_manager = McpServerManager()
        if self._config.mcp.servers:
            logger.info("mcp: starting %d server(s)", len(self._config.mcp.servers))
            await self._mcp_manager.start_all(self._config.mcp.servers)

        self._sessions = SessionManager(
            store,
            runner_factory=lambda: AgentRunner(
                config,
                bus=self._bus,
                provider=self._provider_for_runner(config),
                runs_dir=self._runs_root,
                trace=self._trace,
                permission_manager=self._permission_manager,
                mcp_manager=self._mcp_manager,
                engine_resolver=self._engine_router,
                recovery_store=self._recovery_store,
                extra_tools=self._extra_tools_factory(),
                sandbox_manager=self._sandbox_manager,
            ),
            bus=self._bus,
            provider_factory=lambda: self._provider_for_compaction(config),
            on_session_closed=self._delete_session_resources,
            recovery_store=self._recovery_store,
            durable_supported=config.sandbox.backend != "kubernetes",
        )

        server = SocketServer(
            self._config.host,
            self._config.port,
            self._broadcaster,
            trace=self._trace,
        )
        self._server = server
        server.register("core.ping", self._ping_handler)
        server.register("agent.run", self._agent_run_handler)
        server.register("event.subscribe", self._subscribe_handler)
        server.register("session.create", self._session_create_handler)
        server.register("session.send_message", self._session_send_handler)
        server.register("session.get_history", self._session_history_handler)
        server.register("session.close", self._session_close_handler)
        server.register("session.resume", self._session_resume_handler)
        server.register("run.get_state", self._run_get_state_handler)
        server.register("permission.respond", self._permission_respond_handler)
        server.register("session.compact", self._session_compact_handler)

        addr = await server.start()
        logger.info("agentrt-core %s listening addr=%s", agent_runtime.__version__, addr)
        logger.info(
            "config: engine=%s checkpoint_backend=%s host=%s port=%d "
            "trace_enabled=%s mcp_server_count=%d",
            config.agent.engine,
            config.graph.checkpoint_backend,
            config.host,
            config.port,
            config.trace.enabled,
            len(config.mcp.servers),
        )

        loop = asyncio.get_running_loop()
        shutdown = asyncio.Event()
        _install_shutdown_handlers(loop, shutdown)

        await shutdown.wait()

    def _sandbox_trace(self, record: dict[str, object]) -> None:
        if self._trace is not None:
            run_id = record.get("run_id")
            self._trace.emit(
                TraceRecord(
                    ts=str(record["ts"]),
                    direction="CORE",
                    layer="event",
                    kind="sandbox_lifecycle",
                    run_id=run_id if isinstance(run_id, str) else None,
                    data=record,
                )
            )

    async def _shutdown(self) -> None:
        logger.info("shutting down")
        if self._sessions is not None:
            self._sessions.begin_shutdown()
        try:
            if self._server is not None:
                await self._server.stop()
        finally:
            run_tasks = list(self._running_runs)
            for run_task in run_tasks:
                run_task.cancel()
            if run_tasks:
                await asyncio.gather(*run_tasks, return_exceptions=True)
            await self._wait_for_cleanup_tasks()
            pending_resources = [
                task for task in self._session_resource_tasks.values() if not task.done()
            ]
            if pending_resources:
                await asyncio.gather(*pending_resources, return_exceptions=True)
            try:
                await self._sandbox_manager.close()
            finally:
                try:
                    await self._engine_router.close()
                finally:
                    try:
                        if self._mcp_manager is not None:
                            await self._mcp_manager.stop_all()
                    finally:
                        if self._trace is not None:
                            await self._trace.stop()


# 同步入口：启动 CoreApp 事件循环
def run() -> None:
    asyncio.run(CoreApp().run())
