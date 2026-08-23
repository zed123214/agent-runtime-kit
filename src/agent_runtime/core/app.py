from __future__ import annotations

import asyncio
import datetime
import fnmatch
import json
import logging
import re
import signal
import time
from datetime import UTC
from pathlib import Path
from typing import Any

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
    SessionCloseCommand,
    SessionCloseResult,
    SessionCompactCommand,
    SessionCompactResult,
    SessionCreateCommand,
    SessionCreateResult,
    SessionGetHistoryCommand,
    SessionGetHistoryResult,
    SessionSendMessageCommand,
    SessionSendMessageResult,
)
from agent_runtime.core.bus.envelope import EventPushEnvelope, HandlerError
from agent_runtime.core.config import RuntimeConfig, get_config
from agent_runtime.core.engine.router import EngineRouter
from agent_runtime.core.events.bus import EventBus
from agent_runtime.core.llm.provider import AnthropicProvider
from agent_runtime.core.logging_setup import setup_logging
from agent_runtime.core.mcp.server import McpServerManager
from agent_runtime.core.permissions.manager import PermissionManager
from agent_runtime.core.permissions.storage import load_policy_file
from agent_runtime.core.runner import AgentRunner
from agent_runtime.core.runs import RUNS_DIR, new_run_id
from agent_runtime.core.session import SessionManager, SessionStore
from agent_runtime.core.session.manager import SESSION_NOT_FOUND
from agent_runtime.core.trace.record import TraceRecord
from agent_runtime.core.trace.writer import TraceWriter
from agent_runtime.core.transport.ipc_broadcaster import IpcEventBroadcaster
from agent_runtime.core.transport.socket_server import SocketServer, get_connection_writer

logger = logging.getLogger(__name__)

_SAFE_RUN_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}\Z")
_STRONG_REPLAY_RUN_ID = re.compile(r"\d{8}-\d{6}-[0-9a-f]{32}\Z")
_MAX_REPLAY_LINE_BYTES = 1024 * 1024
_MAX_REPLAY_TOTAL_BYTES = 8 * 1024 * 1024
_MAX_REPLAY_LINES = 10_000
_MAX_REPLAY_EVENTS = 1_000
_MAX_REPLAY_SESSION_ROOTS = 10_000


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
    def __init__(self, engine_router: EngineRouter | None = None) -> None:
        self._start_time = time.monotonic()
        self._bus = EventBus()
        self._broadcaster: IpcEventBroadcaster | None = None
        self._trace: TraceWriter | None = None
        self._config: RuntimeConfig | None = None
        self._running_runs: set[asyncio.Task[Any]] = set()
        self._run_tasks_by_session: dict[str, set[asyncio.Task[Any]]] = {}
        self._cleanup_tasks: set[asyncio.Task[None]] = set()
        self._engine_router = engine_router if engine_router is not None else EngineRouter()
        self._sessions: SessionManager | None = None
        self._permission_manager: PermissionManager | None = None
        self._mcp_manager: McpServerManager | None = None
        self._sessions_root = Path("~/.agentrt/sessions").expanduser()
        self._runs_root = RUNS_DIR

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
        event_dict = event.model_dump()
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

    async def _cancel_runs_and_delete_threads(self, session_ids: frozenset[str]) -> None:
        run_tasks: set[asyncio.Task[Any]] = set()
        for session_id in session_ids:
            run_tasks.update(self._run_tasks_by_session.pop(session_id, set()))
        for task in run_tasks:
            if not task.done():
                task.cancel()
        if run_tasks:
            await asyncio.gather(*run_tasks, return_exceptions=True)
            self._running_runs.difference_update(run_tasks)
        for session_id in session_ids:
            await self._engine_router.delete_thread(session_id)

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
        session = await self._sessions.create(
            mode=cmd.mode,
            title=cmd.title,
            before_publish=lambda created: self._bind_connection_to_session(writer, created.id),
        )
        return SessionCreateResult(session_id=session.id, status=session.status)

    # 向 session 发送一条用户消息并同步等待对应 run 完成
    async def _session_send_handler(self, params: dict[str, Any]) -> SessionSendMessageResult:
        assert self._sessions is not None
        cmd = SessionSendMessageCommand.model_validate(params)
        self._require_current_connection_owns(cmd.session_id)
        current_task = asyncio.current_task()
        if current_task is not None:
            self._track_run_task(cmd.session_id, current_task)
        try:
            run_id = await self._sessions.send_message(cmd.session_id, cmd.content)
        finally:
            if current_task is not None:
                self._untrack_run_task(cmd.session_id, current_task)
        return SessionSendMessageResult(run_id=run_id)

    # 返回 session 的完整 Anthropic messages 历史
    async def _session_history_handler(self, params: dict[str, Any]) -> SessionGetHistoryResult:
        assert self._sessions is not None
        cmd = SessionGetHistoryCommand.model_validate(params)
        self._require_current_connection_owns(cmd.session_id)
        messages = await self._sessions.get_history(cmd.session_id)
        return SessionGetHistoryResult(messages=messages)

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
        ok = self._permission_manager.respond(
            cmd.tool_use_id,
            cmd.decision,
            authorized_session_ids=authorized_session_ids,
        )
        return PermissionRespondResult(ok=ok)

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
        cleanup_task = asyncio.create_task(self._cancel_runs_and_delete_threads(session_ids))
        self._track_cleanup_task(cleanup_task)

    # 手动压缩 session thread，将摘要持久化写入 thread.jsonl
    async def _session_compact_handler(self, params: dict[str, Any]) -> SessionCompactResult:
        assert self._sessions is not None
        cmd = SessionCompactCommand.model_validate(params)
        self._require_current_connection_owns(cmd.session_id)
        result = await self._sessions.compact(cmd.session_id, cmd.focus)
        return result  # type: ignore[no-any-return]

    # 关闭 session 并返回 closed 状态
    async def _session_close_handler(self, params: dict[str, Any]) -> SessionCloseResult:
        assert self._sessions is not None
        cmd = SessionCloseCommand.model_validate(params)
        self._require_current_connection_owns(cmd.session_id)
        await self._sessions.close(cmd.session_id)
        return SessionCloseResult(status="closed")

    # 注册客户端事件订阅，可选先回放 events.jsonl 历史再接收实时流
    async def _subscribe_handler(self, params: dict[str, Any]) -> EventSubscribeResult:
        cmd = EventSubscribeCommand.model_validate(params)
        writer = get_connection_writer()
        assert self._broadcaster is not None

        replayed_count = 0
        if cmd.replay_from_run is not None:
            replayed_count = await self._replay_events(
                cmd.replay_from_run,
                writer,
                cmd.topics,
                cmd.scope,
            )

        sub_id = self._broadcaster.subscribe(writer, cmd.topics, cmd.scope)
        return EventSubscribeResult(subscription_id=sub_id, replayed_count=replayed_count)

    # 从 events.jsonl 向 writer 回放匹配 topic 的历史事件，返回已回放条数
    async def _replay_events(
        self,
        run_id: str,
        writer: asyncio.StreamWriter,
        topics: list[str],
        scope: str,
    ) -> int:
        if _SAFE_RUN_ID.fullmatch(run_id) is None or run_id in {".", ".."}:
            return 0

        path = self._find_replay_file(run_id, writer)
        if path is None:
            return 0

        count = 0
        total_bytes = 0
        line_count = 0
        try:
            with path.open("rb") as stream:
                while line_count < _MAX_REPLAY_LINES and total_bytes < _MAX_REPLAY_TOTAL_BYTES:
                    raw = stream.readline(_MAX_REPLAY_LINE_BYTES + 1)
                    if not raw:
                        break
                    line_count += 1
                    total_bytes += len(raw)

                    if len(raw) > _MAX_REPLAY_LINE_BYTES:
                        while raw and not raw.endswith(b"\n"):
                            raw = stream.readline(_MAX_REPLAY_LINE_BYTES + 1)
                            total_bytes += len(raw)
                            if total_bytes >= _MAX_REPLAY_TOTAL_BYTES:
                                break
                        continue
                    if total_bytes > _MAX_REPLAY_TOTAL_BYTES:
                        break
                    try:
                        event = json.loads(raw)
                    except (UnicodeDecodeError, json.JSONDecodeError):
                        continue
                    if not isinstance(event, dict) or event.get("run_id") != run_id:
                        continue
                    event_type = event.get("type")
                    if not isinstance(event_type, str):
                        continue
                    if not any(fnmatch.fnmatch(event_type, pattern) for pattern in topics):
                        continue
                    assert self._broadcaster is not None
                    if not self._broadcaster.can_replay(writer, event, scope):
                        continue
                    envelope = EventPushEnvelope(event=event)
                    writer.write(envelope.model_dump_json().encode() + b"\n")
                    count += 1
                    if count >= _MAX_REPLAY_EVENTS:
                        break
        except OSError:
            logger.debug("replay file became unavailable: %s", path, exc_info=True)

        if count:
            await writer.drain()
        return count

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
        self._start_time = time.monotonic()
        self._config = get_config()
        setup_logging(self._config)

        if self._config.trace.enabled:
            trace_path = Path(self._config.trace.file).expanduser()
            self._trace = TraceWriter(trace_path)
            await self._trace.start()
            self._bus.subscribe(self._trace_event_handler)

        policy_file = Path("~/.agentrt/policy.toml").expanduser()
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
        assert self._config is not None
        config = self._config

        self._mcp_manager = McpServerManager()
        if self._config.mcp.servers:
            logger.info("mcp: starting %d server(s)", len(self._config.mcp.servers))
            await self._mcp_manager.start_all(self._config.mcp.servers)

        self._sessions = SessionManager(
            store,
            runner_factory=lambda: AgentRunner(
                config,
                bus=self._bus,
                trace=self._trace,
                permission_manager=self._permission_manager,
                mcp_manager=self._mcp_manager,
                engine_resolver=self._engine_router,
            ),
            bus=self._bus,
            provider_factory=lambda: AnthropicProvider(config.llm.default_model),
            on_session_closed=self._engine_router.delete_thread,
        )

        server = SocketServer(
            self._config.host,
            self._config.port,
            self._broadcaster,
            trace=self._trace,
        )
        server.register("core.ping", self._ping_handler)
        server.register("agent.run", self._agent_run_handler)
        server.register("event.subscribe", self._subscribe_handler)
        server.register("session.create", self._session_create_handler)
        server.register("session.send_message", self._session_send_handler)
        server.register("session.get_history", self._session_history_handler)
        server.register("session.close", self._session_close_handler)
        server.register("permission.respond", self._permission_respond_handler)
        server.register("session.compact", self._session_compact_handler)

        addr = await server.start()
        logger.info("agentrt-core %s listening addr=%s", agent_runtime.__version__, addr)
        logger.info("config: %s", self._config)

        loop = asyncio.get_running_loop()
        shutdown = asyncio.Event()
        _install_shutdown_handlers(loop, shutdown)

        await shutdown.wait()

        logger.info("shutting down")
        await server.stop()
        run_tasks = list(self._running_runs)
        for run_task in run_tasks:
            run_task.cancel()
        if run_tasks:
            await asyncio.gather(*run_tasks, return_exceptions=True)
        await self._wait_for_cleanup_tasks()
        await self._engine_router.close()
        if self._mcp_manager is not None:
            await self._mcp_manager.stop_all()
        if self._trace is not None:
            await self._trace.stop()


# 同步入口：启动 CoreApp 事件循环
def run() -> None:
    asyncio.run(CoreApp().run())
