from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime
from pathlib import Path

from agent_runtime.core.bus.events import RunFinishedEvent, RunStartedEvent
from agent_runtime.core.compact.compactor import Compactor
from agent_runtime.core.config import RuntimeConfig
from agent_runtime.core.context import ExecutionContext, TerminalStatus
from agent_runtime.core.engine.base import (
    EngineErrorDetail,
    EngineRunConfig,
    ExecutionEngineConfigurationError,
    ExecutionEngineContractError,
    ExecutionEngineError,
    RunOutcome,
)
from agent_runtime.core.engine.router import EngineResolver, EngineRouter
from agent_runtime.core.events.bus import EventBus, EventHandler
from agent_runtime.core.events.writer import EventWriter
from agent_runtime.core.llm.base import LLMProvider
from agent_runtime.core.llm.provider import AnthropicProvider
from agent_runtime.core.mcp.server import McpServerManager
from agent_runtime.core.memory.loader import load_context_file
from agent_runtime.core.permissions.manager import PermissionManager
from agent_runtime.core.runs import RUNS_DIR, new_run_id
from agent_runtime.core.session.model import Session
from agent_runtime.core.session.store import SessionStore
from agent_runtime.core.subagent.registry import BackgroundTaskRegistry
from agent_runtime.core.subagent.tool import AgentResultTool, SpawnAgentTool
from agent_runtime.core.task.manager import TaskManager
from agent_runtime.core.tools.builtin import (
    BashTool,
    ListDirTool,
    NoteSaveTool,
    ReadFileTool,
    TaskCreateTool,
    TaskGetTool,
    TaskListTool,
    TaskUpdateTool,
    WriteFileTool,
)
from agent_runtime.core.tools.registry import ToolRegistry
from agent_runtime.core.trace.provider import TracingProvider
from agent_runtime.core.trace.writer import TraceWriter

__all__ = ["AgentRunner", "RunOutcome"]


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _terminal_status(status: str) -> TerminalStatus | None:
    if status == "success":
        return "success"
    if status == "failed":
        return "failed"
    return None


class AgentRunner:
    # 组装所有运行时依赖，准备执行一次完整的 agent run
    def __init__(
        self,
        config: RuntimeConfig,
        *,
        bus: EventBus | None = None,
        provider: LLMProvider | None = None,
        extra_handlers: list[EventHandler] | None = None,
        runs_dir: Path | None = None,
        trace: TraceWriter | None = None,
        permission_manager: PermissionManager | None = None,
        mcp_manager: McpServerManager | None = None,
        engine_resolver: EngineResolver | None = None,
    ) -> None:
        self._config = config
        self._bus = bus
        self._provider = provider
        self._extra_handlers: list[EventHandler] = extra_handlers or []
        self._runs_dir = runs_dir or RUNS_DIR
        self._trace = trace
        self._permission_manager = permission_manager
        self._mcp_manager = mcp_manager
        self._engine_resolver = engine_resolver if engine_resolver is not None else EngineRouter()
        # 跨 run 共享的后台 subagent 任务注册表
        self._task_registry = BackgroundTaskRegistry()

    # 构建工具注册表，注入 TaskManager（任务工具共享同一实例）；可选注入 SpawnAgentTool
    def _build_registry(
        self,
        task_manager: TaskManager,
        *,
        session: Session | None = None,
        store: SessionStore | None = None,
        run_id: str | None = None,
        provider: LLMProvider | None = None,
        bus: EventBus | None = None,
        child_runs_dir: Path | None = None,
        session_id: str = "",
        tool_whitelist: list[str] | None = None,
    ) -> ToolRegistry:
        allowed: set[str] | None = set(tool_whitelist) if tool_whitelist else None

        def _ok(name: str) -> bool:
            return allowed is None or name in allowed

        registry = ToolRegistry()
        for t in [ReadFileTool(), BashTool(), WriteFileTool(), ListDirTool()]:
            if _ok(t.name):
                registry.register(t)
        for t in [
            TaskCreateTool(task_manager),
            TaskUpdateTool(task_manager),
            TaskListTool(task_manager),
            TaskGetTool(task_manager),
        ]:
            if _ok(t.name):
                registry.register(t)
        if session is not None and store is not None and run_id is not None:
            note_tool = NoteSaveTool(store, session.id, run_id)
            if _ok(note_tool.name):
                registry.register(note_tool)
        if provider is not None and bus is not None and run_id is not None:
            runs_dir = child_runs_dir or self._runs_dir
            if _ok("spawn_agent"):
                registry.register(
                    SpawnAgentTool(
                        provider=provider,
                        parent_bus=bus,
                        parent_run_id=run_id,
                        permission_manager=self._permission_manager,
                        max_steps=self._config.agent.max_steps,
                        task_registry=self._task_registry,
                        runs_dir=runs_dir,
                        session_id=session_id,
                        depth=0,
                    )
                )
            if _ok("agent_result"):
                registry.register(AgentResultTool(self._task_registry))
        if self._mcp_manager is not None:
            for mcp_tool in self._mcp_manager.get_tools():
                if _ok(mcp_tool.name):
                    registry.register(mcp_tool)
        return registry

    # 执行一次完整的 agent run（委托给 run_and_capture，忽略返回值）
    async def run(self, goal: str, *, run_id: str | None = None) -> None:
        await self.run_and_capture(goal, run_id=run_id)

    # 执行 agent run 并返回 RunOutcome（含最终文字结果）
    async def run_and_capture(
        self,
        goal: str,
        *,
        run_id: str | None = None,
        session: Session | None = None,
        store: SessionStore | None = None,
        system_prompt_override: str | None = None,
        tool_whitelist: list[str] | None = None,
    ) -> RunOutcome:
        run_id = run_id or new_run_id()
        if session is not None and store is not None:
            run_path = store.runs_dir(session.id) / run_id
            history = store.read_messages(session.id)
            notes = store.read_notes(session.id)
        else:
            run_path = self._runs_dir / run_id
            history = [{"role": "user", "content": goal}]
            notes = ""
        run_path.mkdir(parents=True, exist_ok=True)

        global_ctx = load_context_file(Path("~/.agentrt/context.md").expanduser())
        project_ctx = load_context_file(Path(".agentrt/context.md"))

        task_manager = TaskManager(run_path / ".tasks")

        session_id_str = session.id if session is not None else ""
        # Each run gets immutable correlation metadata and its own subscriber
        # list. Forwarding preserves the existing daemon-wide EventBus API while
        # keeping concurrent run event files and metadata isolated.
        bus = EventBus(
            correlation_id=run_id,
            session_id=session_id_str or None,
        )

        context = ExecutionContext(
            run_id=run_id,
            goal=goal,
            max_steps=self._config.agent.max_steps,
            prefill_messages=history,
            session_notes=notes,
            global_context=global_ctx,
            project_context=project_ctx,
            system_prompt_override=system_prompt_override,
        )
        prefill_len = len(history)

        async with EventWriter(run_path / "events.jsonl", run_id=run_id) as writer:
            # Persist the run-scoped event before forwarding it to external/global
            # subscribers. A slow client must not prevent the authoritative JSONL
            # log from observing a matched Graph node lifecycle.
            writer.subscribe(bus)
            if self._bus is not None:
                bus.subscribe(self._bus.publish)
            for h in self._extra_handlers:
                bus.subscribe(h)
            await bus.publish(RunStartedEvent(run_id=run_id, goal=goal, ts=_now()))

            cancelled = False
            engine_outcome: RunOutcome | None = None
            engine_error: EngineErrorDetail | None = None
            engine_name: str = self._config.agent.engine
            engine_invocation_started = False
            try:
                if (
                    self._config.agent.engine == "graph"
                    and self._config.compaction.auto_threshold > 0
                ):
                    raise ExecutionEngineConfigurationError(
                        engine="graph",
                        message=(
                            "The graph engine does not support automatic context "
                            "compaction; set compaction.auto_threshold to 0 and use "
                            "manual session compaction."
                        ),
                    )
                # Resolve capability before provider construction. Selecting the
                # optional graph engine therefore yields a typed availability
                # error even when no Anthropic API key is configured.
                engine_builder = self._engine_resolver(self._config.agent.engine)
                provider: LLMProvider = self._provider or AnthropicProvider(
                    self._config.llm.default_model
                )
                if self._trace is not None:
                    provider = TracingProvider(
                        provider,
                        self._trace,
                        include_payload=self._config.trace.include_llm_payload,
                    )
                child_runs_dir = (
                    store.runs_dir(session.id)
                    if session is not None and store is not None
                    else self._runs_dir
                )
                registry = self._build_registry(
                    task_manager,
                    session=session,
                    store=store,
                    run_id=run_id,
                    provider=provider,
                    bus=bus,
                    child_runs_dir=child_runs_dir,
                    session_id=session_id_str,
                    tool_whitelist=tool_whitelist,
                )
                session_dir = (
                    store.session_dir(session.id)
                    if session is not None and store is not None
                    else run_path
                )
                compactor = Compactor(bus, session_dir, session_id_str)
                engine = engine_builder(
                    provider,
                    permission_manager=self._permission_manager,
                    compactor=compactor,
                )
                engine_name = engine.name
                engine_invocation_started = True
                engine_outcome = await engine.run(
                    context,
                    tools=registry,
                    events=bus,
                    config=EngineRunConfig(
                        session_id=session_id_str,
                        thread_id=(
                            session.id
                            if session is not None and session.mode == "chat"
                            else context.run_id
                        ),
                        retain_thread=session is not None and session.mode == "chat",
                        compact_threshold=self._config.compaction.auto_threshold,
                        recursion_limit=self._config.graph.recursion_limit,
                        tool_call_budget=self._config.graph.tool_call_budget,
                        wall_time_s=self._config.graph.wall_time_s,
                        trace_event_limit=self._config.graph.trace_event_limit,
                    ),
                )
                outcome_status = _terminal_status(engine_outcome.status)
                context_status = _terminal_status(context.status)
                if outcome_status is None or context_status is None:
                    raise ExecutionEngineContractError(
                        engine=engine.name,
                        message=(
                            "Execution engine must finish with status 'success' or "
                            f"'failed'; got outcome.status={engine_outcome.status!r} "
                            f"and context.status={context.status!r}."
                        ),
                    )
                if engine_outcome.status == "success" and engine_outcome.error is not None:
                    raise ExecutionEngineContractError(
                        engine=engine.name,
                        message="A successful execution-engine outcome cannot carry an error.",
                    )
                canonical_outcome = RunOutcome.from_context(
                    context,
                    error=engine_outcome.error,
                )
                if engine_outcome != canonical_outcome:
                    raise ExecutionEngineContractError(
                        engine=engine.name,
                        message=(
                            "Execution engine returned an outcome that does not match "
                            "the canonical ExecutionContext."
                        ),
                    )
                engine_error = engine_outcome.error
            except asyncio.CancelledError:
                cancelled = True
                engine_outcome = None
                engine_error = None
                context.mark_failed("cancelled")
            except ExecutionEngineError as exc:
                engine_outcome = None
                engine_error = exc.detail
                logging.getLogger(__name__).error(
                    "execution engine error run_id=%s engine=%s code=%s: %s",
                    run_id,
                    exc.detail.engine,
                    exc.detail.code,
                    exc.detail.message,
                )
                context.mark_failed(exc.detail.code)
            except SystemExit as exc:
                logging.getLogger(__name__).exception(
                    "agent run failed due to SystemExit run_id=%s step=%d",
                    run_id,
                    context.step,
                )
                engine_outcome = None
                if engine_invocation_started:
                    engine_error = EngineErrorDetail(
                        code="engine_execution_error",
                        engine=engine_name,
                        message=(
                            f"Execution engine raised SystemExit: {exc}"
                            if str(exc)
                            else "Execution engine raised SystemExit."
                        ),
                    )
                    context.mark_failed(engine_error.code)
                else:
                    engine_error = None
                    context.mark_failed("llm_error")
            except Exception as exc:
                logging.getLogger(__name__).exception(
                    "agent run failed run_id=%s step=%d", run_id, context.step
                )
                engine_outcome = None
                if engine_invocation_started:
                    engine_error = EngineErrorDetail(
                        code="engine_execution_error",
                        engine=engine_name,
                        message=f"Execution engine raised {type(exc).__name__}: {exc}",
                    )
                    context.mark_failed(engine_error.code)
                else:
                    engine_error = None
                    context.mark_failed("llm_error")

            # Background subagents are scoped to this Runner. Once the root run
            # reaches a terminal boundary there is no reachable agent_result
            # consumer, so pending children must not outlive cancellation or
            # continue tool execution after the owning connection disappears.
            await self._task_registry.cancel_all()

            terminal_status = _terminal_status(context.status)
            if terminal_status is None:
                engine_outcome = None
                engine_error = EngineErrorDetail(
                    code="engine_contract_error",
                    engine=engine_name,
                    message=(
                        "Execution engine left the canonical ExecutionContext in "
                        f"non-terminal status {context.status!r}."
                    ),
                )
                logging.getLogger(__name__).error(
                    "execution engine error run_id=%s engine=%s code=%s: %s",
                    run_id,
                    engine_error.engine,
                    engine_error.code,
                    engine_error.message,
                )
                context.mark_failed(engine_error.code)
                terminal_status = "failed"

            await bus.publish(
                RunFinishedEvent(
                    run_id=run_id,
                    status=terminal_status,
                    reason=context.reason,
                    steps=context.step,
                    error=engine_error.as_dict() if engine_error is not None else None,
                    ts=_now(),
                )
            )

        if session is not None and store is not None:
            store.append_messages(session.id, context.messages[prefill_len:], run_id=run_id)

        if cancelled:
            raise asyncio.CancelledError()

        if engine_outcome is not None:
            return engine_outcome
        return RunOutcome.from_context(context, error=engine_error)
