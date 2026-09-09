from __future__ import annotations

import asyncio
import logging
from collections.abc import Coroutine
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from agent_runtime.core.bus.events import (
    RunFinishedEvent,
    RunResumedEvent,
    RunStartedEvent,
    RunSuspendedEvent,
)
from agent_runtime.core.compact.compactor import Compactor
from agent_runtime.core.config import RuntimeConfig, resolve_data_root, validate_runtime_sandbox
from agent_runtime.core.context import ExecutionContext, TerminalStatus
from agent_runtime.core.engine.base import (
    EngineErrorDetail,
    EngineRunConfig,
    EngineRunResult,
    ExecutionEngineConfigurationError,
    ExecutionEngineContractError,
    ExecutionEngineError,
    RunOutcome,
    RunSuspension,
)
from agent_runtime.core.engine.router import EngineResolver, EngineRouter
from agent_runtime.core.events.bus import EventBus, EventHandler
from agent_runtime.core.events.writer import EventWriter
from agent_runtime.core.graph.event_log import read_event_log
from agent_runtime.core.graph.recovery import RecoveryStore
from agent_runtime.core.llm.base import LLMProvider
from agent_runtime.core.llm.provider import AnthropicProvider
from agent_runtime.core.mcp.server import McpServerManager
from agent_runtime.core.memory.loader import load_context_file
from agent_runtime.core.permissions.manager import PermissionManager
from agent_runtime.core.runs import RUNS_DIR, new_run_id
from agent_runtime.core.sandbox import SandboxKey, SandboxManager, SandboxRuntime
from agent_runtime.core.sandbox.factory import create_sandbox_manager
from agent_runtime.core.sandbox.models import SandboxError
from agent_runtime.core.session.model import Session
from agent_runtime.core.session.store import SessionStore, TranscriptConflictError
from agent_runtime.core.subagent.registry import BackgroundTaskRegistry
from agent_runtime.core.subagent.tool import AgentResultTool, SpawnAgentTool
from agent_runtime.core.task.manager import TaskManager
from agent_runtime.core.tools.base import BaseTool
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


async def _finish_despite_cancellation(operation: Coroutine[Any, Any, None]) -> bool:
    """Finish terminal cleanup even if the owning task is cancelled again."""

    task = asyncio.create_task(operation)
    cancelled_while_waiting = False
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            cancelled_while_waiting = True
    task.result()
    return cancelled_while_waiting


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
        recovery_store: RecoveryStore | None = None,
        extra_tools: list[BaseTool] | None = None,
        sandbox_manager: SandboxManager | None = None,
    ) -> None:
        validate_runtime_sandbox(config, durable=recovery_store is not None)
        self._config = config
        self._bus = bus
        self._provider = provider
        # Internal extensions are trusted to finish promptly and cooperate with
        # cancellation; the runtime does not attempt to kill arbitrary coroutines.
        self._extra_handlers: list[EventHandler] = extra_handlers or []
        self._runs_dir = runs_dir or RUNS_DIR
        self._trace = trace
        self._permission_manager = permission_manager
        self._mcp_manager = mcp_manager
        self._engine_resolver = engine_resolver if engine_resolver is not None else EngineRouter()
        self._recovery_store = recovery_store
        self._extra_tools = list(extra_tools or [])
        self._sandbox_manager = (
            sandbox_manager
            if sandbox_manager is not None
            else create_sandbox_manager(config.sandbox, resolve_data_root(config))
        )
        self._owns_sandbox_manager = sandbox_manager is None
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
        sandbox_runtime: SandboxRuntime | None = None,
        sandbox_key: SandboxKey | None = None,
        task_registry: BackgroundTaskRegistry | None = None,
    ) -> ToolRegistry:
        allowed: set[str] | None = set(tool_whitelist) if tool_whitelist else None

        def _ok(name: str) -> bool:
            return allowed is None or name in allowed

        registry = ToolRegistry()
        for sandbox_tool in [
            ReadFileTool(sandbox_runtime, sandbox_key=sandbox_key),
            BashTool(sandbox_runtime, sandbox_key=sandbox_key),
            WriteFileTool(sandbox_runtime, sandbox_key=sandbox_key),
            ListDirTool(sandbox_runtime, sandbox_key=sandbox_key),
        ]:
            if _ok(sandbox_tool.name):
                registry.register(sandbox_tool)
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
                        task_registry=task_registry or self._task_registry,
                        runs_dir=runs_dir,
                        session_id=session_id,
                        depth=0,
                        sandbox_runtime=sandbox_runtime,
                        sandbox_key=sandbox_key,
                    )
                )
            if _ok("agent_result"):
                registry.register(AgentResultTool(task_registry or self._task_registry))
        if self._mcp_manager is not None:
            for mcp_tool in self._mcp_manager.get_tools():
                if _ok(mcp_tool.name):
                    registry.register(mcp_tool)
        for extra_tool in self._extra_tools:
            if _ok(extra_tool.name):
                registry.register(extra_tool)
        return registry

    # 执行一次完整的 agent run（委托给 run_and_capture，忽略返回值）
    async def run(self, goal: str, *, run_id: str | None = None) -> None:
        await self.run_and_capture(goal, run_id=run_id)

    async def close_suspended(
        self,
        *,
        session: Session,
        store: SessionStore,
        run_id: str,
        checkpoint_revision: str,
        resume_epoch: int,
    ) -> RunOutcome:
        """Close one durable suspended run without re-entering a Graph node."""

        if self._recovery_store is None:
            raise RuntimeError("close_suspended requires a RecoveryStore")
        recovery_store = self._recovery_store
        event_path = store.runs_dir(session.id) / run_id / "events.jsonl"
        snapshot = read_event_log(event_path, repair_tail=True)
        terminal_rows = [
            event
            for event in snapshot.events
            if event.get("run_id") == run_id and event.get("type") == "run.finished"
        ]
        if len(terminal_rows) > 1:
            raise ExecutionEngineContractError(
                engine="graph",
                message=f"Run {run_id!r} has more than one persisted terminal event.",
            )
        terminal_status: TerminalStatus = "failed"
        terminal_reason: str | None = "session_closed"
        terminal_steps = 0
        if terminal_rows:
            row = terminal_rows[0]
            raw_status = row.get("status")
            if raw_status not in ("success", "failed"):
                raise ExecutionEngineContractError(
                    engine="graph",
                    message=f"Run {run_id!r} has an invalid persisted terminal status.",
                )
            terminal_status = raw_status
            raw_reason = row.get("reason")
            terminal_reason = raw_reason if isinstance(raw_reason, str) else None
            raw_steps = row.get("steps")
            terminal_steps = raw_steps if isinstance(raw_steps, int) else 0

        bus = EventBus(correlation_id=run_id, session_id=session.id)
        async with EventWriter(event_path, run_id=run_id) as writer:
            writer.subscribe(bus)
            if self._bus is not None:
                bus.subscribe(self._bus.publish)
            for handler in self._extra_handlers:
                bus.subscribe(handler)

            async def finish_close() -> None:
                await recovery_store.mark_started_tools_outcome_unknown(
                    session.id,
                    run_id,
                )
                if not terminal_rows:
                    await bus.publish(
                        RunFinishedEvent(
                            run_id=run_id,
                            status="failed",
                            reason="session_closed",
                            steps=terminal_steps,
                            ts=_now(),
                        )
                    )
                await writer.__aexit__(None, None, None)
                transcript = store.transcript_commit(session.id, run_id)
                await recovery_store.mark_run_terminal(
                    session.id,
                    run_id,
                    status=terminal_status,
                    checkpoint_revision=checkpoint_revision,
                    event_seq=bus.last_event_seq,
                    transcript_commit_count=transcript.transcript_commit_count,
                    transcript_commit_hash=transcript.transcript_commit_hash,
                    expected_resume_epoch=resume_epoch,
                )

            cancelled = await _finish_despite_cancellation(finish_close())
        if cancelled:
            raise asyncio.CancelledError()
        return RunOutcome(
            status=terminal_status,
            result="",
            reason=terminal_reason,
            steps=terminal_steps,
        )

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
        resume: bool = False,
        resume_value: object | None = None,
        expected_checkpoint_revision: str | None = None,
        resume_epoch: int = 0,
    ) -> EngineRunResult:
        run_id = run_id or new_run_id()
        validate_runtime_sandbox(self._config, durable=session is not None and session.durable)
        key = SandboxKey("session", session.id) if session else SandboxKey("direct_run", run_id)
        task_registry = BackgroundTaskRegistry()
        try:
            return await self._run_and_capture(
                goal,
                run_id=run_id,
                session=session,
                store=store,
                system_prompt_override=system_prompt_override,
                tool_whitelist=tool_whitelist,
                resume=resume,
                resume_value=resume_value,
                expected_checkpoint_revision=expected_checkpoint_revision,
                resume_epoch=resume_epoch,
                sandbox_key=key,
                task_registry=task_registry,
            )
        finally:

            async def cleanup() -> None:
                try:
                    await task_registry.cancel_all()
                finally:
                    if session is None:
                        # Never close an injected manager: its other root/session
                        # resources belong to their own callers.
                        await self._sandbox_manager.release(key, reason="run_finished")
                        if self._owns_sandbox_manager:
                            await self._sandbox_manager.stop_if_idle()

            if await _finish_despite_cancellation(cleanup()):
                raise asyncio.CancelledError()

    async def _run_and_capture(
        self,
        goal: str,
        *,
        sandbox_key: SandboxKey,
        task_registry: BackgroundTaskRegistry,
        run_id: str | None = None,
        session: Session | None = None,
        store: SessionStore | None = None,
        system_prompt_override: str | None = None,
        tool_whitelist: list[str] | None = None,
        resume: bool = False,
        resume_value: object | None = None,
        expected_checkpoint_revision: str | None = None,
        resume_epoch: int = 0,
    ) -> EngineRunResult:
        if resume and resume_epoch <= 0:
            raise ValueError("resume_epoch must be positive for a resumed attempt")
        if resume and not expected_checkpoint_revision:
            raise ValueError("expected_checkpoint_revision is required for resume")
        if not resume and resume_epoch != 0:
            raise ValueError("a new attempt must start at resume_epoch 0")
        run_id = run_id or new_run_id()
        durable_recovery = (
            self._recovery_store is not None and session is not None and session.durable
        )
        recovery_store = self._recovery_store if durable_recovery else None
        committed_run_messages: list[dict[str, Any]] = []
        if session is not None and store is not None:
            run_path = store.runs_dir(session.id) / run_id
            history = (
                store.read_messages_strict(session.id)
                if durable_recovery
                else store.read_messages(session.id)
            )
            if durable_recovery:
                committed_run_messages = store.read_run_messages_strict(session.id, run_id)
            notes = store.read_notes(session.id)
        else:
            run_path = self._runs_dir / run_id
            history = [{"role": "user", "content": goal}]
            notes = ""
        run_path.mkdir(parents=True, exist_ok=True)
        event_path = run_path / "events.jsonl"
        event_snapshot = read_event_log(event_path, repair_tail=True)
        lifecycle_types = [
            str(event.get("type", ""))
            for event in event_snapshot.events
            if event.get("run_id") == run_id
        ]
        has_started = "run.started" in lifecycle_types
        has_finished = "run.finished" in lifecycle_types
        if not resume and has_started:
            raise ExecutionEngineContractError(
                engine=self._config.agent.engine,
                message=f"Run {run_id!r} already has a persisted start event.",
            )

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

        async with EventWriter(event_path, run_id=run_id) as writer:
            # Persist the run-scoped event before forwarding it to external/global
            # subscribers. A slow client must not prevent the authoritative JSONL
            # log from observing a matched Graph node lifecycle.
            writer.subscribe(bus)
            if self._bus is not None:
                bus.subscribe(self._bus.publish)
            for h in self._extra_handlers:
                bus.subscribe(h)
            if not has_started:
                await bus.publish(RunStartedEvent(run_id=run_id, goal=goal, ts=_now()))
            elif resume and not has_finished:
                assert expected_checkpoint_revision is not None
                await bus.publish(
                    RunResumedEvent(
                        run_id=run_id,
                        session_id=session_id_str,
                        checkpoint_revision=expected_checkpoint_revision,
                        resume_epoch=resume_epoch,
                        ts=_now(),
                    )
                )

            cancelled = False
            engine_outcome: RunOutcome | None = None
            engine_suspension: RunSuspension | None = None
            engine_error: EngineErrorDetail | None = None
            engine_name: str = self._config.agent.engine
            engine_invocation_started = False
            try:
                # Resolve the lazy facade inside the recorded Run boundary. A
                # TTL tombstone must produce a typed terminal event, not escape
                # as an IPC internal error with active Session metadata left over.
                sandbox_runtime = self._sandbox_manager.runtime_for(sandbox_key)
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
                    sandbox_runtime=sandbox_runtime,
                    sandbox_key=sandbox_key,
                    task_registry=task_registry,
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
                engine_config = EngineRunConfig(
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
                    expected_checkpoint_revision=expected_checkpoint_revision,
                    resume_value=resume_value,
                    event_seq=event_snapshot.last_event_seq,
                    durable_recovery=durable_recovery,
                )
                engine_result = await (engine.resume if resume else engine.run)(
                    context,
                    tools=registry,
                    events=bus,
                    config=engine_config,
                )
                if isinstance(engine_result, RunSuspension):
                    engine_suspension = engine_result
                    if context.status != "running":
                        raise ExecutionEngineContractError(
                            engine=engine.name,
                            message="A suspended execution must leave its context running.",
                        )
                    if engine_result.run_id != run_id or engine_result.session_id != session_id_str:
                        raise ExecutionEngineContractError(
                            engine=engine.name,
                            message="Execution suspension identity does not match the active run.",
                        )
                else:
                    engine_outcome = engine_result
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
                    if (
                        engine_outcome.status != canonical_outcome.status
                        or engine_outcome.result != canonical_outcome.result
                        or engine_outcome.reason != canonical_outcome.reason
                        or engine_outcome.steps != canonical_outcome.steps
                        or engine_outcome.error != canonical_outcome.error
                    ):
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
                engine_suspension = None
                engine_error = None
                context.mark_failed("cancelled")
            except SandboxError as exc:
                engine_outcome = None
                engine_suspension = None
                engine_error = EngineErrorDetail(
                    code=exc.code,
                    engine=engine_name,
                    message=str(exc),
                    operation="sandbox",
                )
                context.mark_failed(exc.code)
            except ExecutionEngineError as exc:
                engine_outcome = None
                engine_suspension = None
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
                engine_suspension = None
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
                engine_suspension = None
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

            if engine_suspension is not None:
                if has_finished:
                    raise ExecutionEngineContractError(
                        engine=engine_name,
                        message="A run with a persisted terminal event cannot suspend again.",
                    )

                async def suspend_run() -> RunSuspension:
                    await task_registry.cancel_all()
                    await bus.publish(
                        RunSuspendedEvent(
                            run_id=run_id,
                            session_id=session_id_str,
                            reason=engine_suspension.reason,
                            checkpoint_revision=engine_suspension.checkpoint_revision,
                            interrupt_id=engine_suspension.interrupt_id,
                            ts=_now(),
                        )
                    )
                    persisted = replace(engine_suspension, event_seq=bus.last_event_seq)
                    if recovery_store is not None:
                        await recovery_store.mark_run_suspended(
                            session_id_str,
                            run_id,
                            reason=persisted.reason,
                            checkpoint_revision=persisted.checkpoint_revision,
                            event_seq=persisted.event_seq,
                            expected_resume_epoch=resume_epoch,
                        )
                    return persisted

                suspension_task = asyncio.create_task(suspend_run())
                cancelled_while_suspending = False
                while not suspension_task.done():
                    try:
                        await asyncio.shield(suspension_task)
                    except asyncio.CancelledError:
                        cancelled_while_suspending = True
                suspension = suspension_task.result()
                if cancelled_while_suspending:
                    raise asyncio.CancelledError()
                return suspension

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

            async def finish_run() -> None:
                # Background subagents are scoped to this root run. Complete the
                # entire terminal boundary in a separate task so repeated
                # cancellation of the caller cannot strand a child, omit the
                # root terminal event, leave the writer open, or skip the
                # session increment.
                await task_registry.cancel_all()
                if recovery_store is not None:
                    # A durable tool is journaled as ``started`` immediately before
                    # its external invocation.  Once this attempt is terminating,
                    # any such row may already represent an applied side effect but
                    # has no safely reusable result.  Fail closed in one transaction
                    # before publishing the authoritative terminal event.
                    await recovery_store.mark_started_tools_outcome_unknown(
                        session_id_str,
                        run_id,
                    )
                if not has_finished:
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
                await writer.__aexit__(None, None, None)
                transcript_count = 0
                transcript_hash: str | None = None
                if session is not None and store is not None:
                    run_messages = context.messages[prefill_len:]
                    if committed_run_messages:
                        if (
                            len(context.messages) < len(committed_run_messages)
                            or context.messages[-len(committed_run_messages) :]
                            != committed_run_messages
                        ):
                            raise TranscriptConflictError(
                                "The terminal checkpoint conflicts with the committed run "
                                "transcript."
                            )
                        run_messages = committed_run_messages
                    transcript_commit = store.append_messages(
                        session.id,
                        run_messages,
                        run_id=run_id,
                    )
                    if recovery_store is not None:
                        transcript_count = transcript_commit.transcript_commit_count
                        transcript_hash = transcript_commit.transcript_commit_hash
                if recovery_store is not None:
                    checkpoint_revision = (
                        getattr(engine_outcome, "checkpoint_revision", None)
                        or expected_checkpoint_revision
                        if engine_outcome is not None
                        else expected_checkpoint_revision
                    )
                    await recovery_store.mark_run_terminal(
                        session_id_str,
                        run_id,
                        status=terminal_status,
                        checkpoint_revision=checkpoint_revision,
                        event_seq=bus.last_event_seq,
                        transcript_commit_count=transcript_count,
                        transcript_commit_hash=transcript_hash,
                        expected_resume_epoch=resume_epoch,
                    )

            cleanup_cancelled = await _finish_despite_cancellation(finish_run())
            cancelled = cancelled or cleanup_cancelled

        if cancelled:
            raise asyncio.CancelledError()

        if engine_outcome is not None:
            return engine_outcome
        return RunOutcome.from_context(context, error=engine_error)
