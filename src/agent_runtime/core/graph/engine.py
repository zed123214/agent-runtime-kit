from __future__ import annotations

import asyncio
import copy
from collections.abc import Coroutine
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, cast

from langgraph.errors import GraphRecursionError
from langgraph.graph import END, START, StateGraph
from langgraph.graph.state import CompiledStateGraph
from langgraph.types import Command

from agent_runtime.core.bus.events import PermissionRequestedEvent
from agent_runtime.core.context import ExecutionContext
from agent_runtime.core.engine.base import (
    EngineRunConfig,
    EngineRunResult,
    ExecutionEngineConfigurationError,
    ExecutionEngineOperationUnsupportedError,
    RunOutcome,
    RunSuspension,
    SuspensionReason,
)
from agent_runtime.core.events.bus import EventBus
from agent_runtime.core.graph.event_bridge import NodeEventBridge
from agent_runtime.core.graph.nodes import ModelNode, route_after_model
from agent_runtime.core.graph.runtime import (
    GraphRuntime,
    GraphStateConflictError,
    GraphStateSummary,
    thread_config,
)
from agent_runtime.core.graph.state import RuntimeState, new_run_input
from agent_runtime.core.graph.tool_node import KitToolNode, route_after_tools
from agent_runtime.core.llm.base import LLMProvider
from agent_runtime.core.tools.invocation import ToolOutcomeUnknownError
from agent_runtime.core.tools.registry import ToolRegistry

if TYPE_CHECKING:
    from agent_runtime.core.compact.compactor import Compactor
    from agent_runtime.core.permissions.manager import PermissionManager


RuntimeGraph = CompiledStateGraph[RuntimeState, None, RuntimeState, RuntimeState]

_BASE_SYSTEM_PROMPT = (
    "You are a helpful AI assistant. "
    "Use the available tools to complete the user's goal. "
    "When the goal is fully achieved, respond with a final answer "
    "and do not call any more tools."
)


def _history_is_prefix(
    checkpoint_messages: list[dict[str, Any]],
    store_messages: list[dict[str, Any]],
) -> bool:
    return len(checkpoint_messages) <= len(store_messages) and (
        checkpoint_messages == store_messages[: len(checkpoint_messages)]
    )


async def _finish_despite_cancellation[T](
    operation: Coroutine[Any, Any, T],
) -> tuple[T, bool]:
    """Finish one cleanup operation and report cancellation of its caller."""

    task = asyncio.create_task(operation)
    cancelled_while_waiting = False
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            cancelled_while_waiting = True
    return task.result(), cancelled_while_waiting


class GraphExecutionEngine:
    """LangGraph orchestration adapter over KitAgent's existing runtime services."""

    name = "graph"

    def __init__(
        self,
        provider: LLMProvider,
        *,
        runtime: GraphRuntime,
        permission_manager: PermissionManager | None = None,
        compactor: Compactor | None = None,
    ) -> None:
        self._provider = provider
        self._runtime = runtime
        self._permission_manager = permission_manager
        self._compactor = compactor
        self._active_task: asyncio.Task[object] | None = None

    def _build_graph(
        self,
        context: ExecutionContext,
        *,
        tools: ToolRegistry,
        events: EventBus,
        config: EngineRunConfig,
        forced_permission_tool_use_id: str | None = None,
    ) -> tuple[RuntimeGraph, NodeEventBridge]:
        bridge = NodeEventBridge(
            events,
            run_id=context.run_id,
            trace_limit=config.trace_event_limit,
        )
        model = ModelNode(
            self._provider,
            tool_schemas=tools.tool_schemas(),
            system_prompt=context.system_prompt(_BASE_SYSTEM_PROMPT),
            max_steps=context.max_steps,
            on_step_started=lambda step: setattr(context, "step", step),
        )
        kit_tools = KitToolNode(
            tools,
            permission_manager=self._permission_manager,
            recovery_store=(self._runtime.recovery_store if config.durable_recovery else None),
            durable=config.durable_recovery,
            session_id=config.session_id,
            forced_permission_tool_use_id=forced_permission_tool_use_id,
            tool_call_budget=config.tool_call_budget,
            max_steps=context.max_steps,
        )

        builder = StateGraph(RuntimeState)
        builder.add_node(  # type: ignore[call-overload]
            "model", bridge.wrap("model", model)
        )
        builder.add_node(  # type: ignore[call-overload]
            "kit_tools", bridge.wrap("kit_tools", kit_tools)
        )
        builder.add_edge(START, "model")
        builder.add_conditional_edges(
            "model",
            route_after_model,
            {"model": "model", "kit_tools": "kit_tools", "end": END},
        )
        builder.add_conditional_edges(
            "kit_tools",
            route_after_tools,
            {"model": "model", "end": END},
        )
        return builder.compile(checkpointer=self._runtime.saver), bridge

    async def _incoming_messages(
        self,
        graph: RuntimeGraph,
        context: ExecutionContext,
        thread_id: str,
    ) -> list[dict[str, Any]]:
        base_config = thread_config(thread_id)
        checkpoint = await self._runtime.saver.aget_tuple(base_config)  # type: ignore[arg-type]
        history = copy.deepcopy(context.messages)
        if checkpoint is None:
            return history

        snapshot = await graph.aget_state(base_config)  # type: ignore[arg-type]
        raw_messages = snapshot.values.get("messages", [])
        checkpoint_messages = cast(list[dict[str, Any]], raw_messages)
        if _history_is_prefix(checkpoint_messages, history):
            return history[len(checkpoint_messages) :]

        await self._runtime._reset_thread_unlocked(thread_id)
        return history

    async def _sync_context(
        self,
        graph: RuntimeGraph | None,
        context: ExecutionContext,
        thread_id: str,
    ) -> dict[str, Any] | None:
        if graph is None:
            return None
        base_config = thread_config(thread_id)
        checkpoint = await self._runtime.saver.aget_tuple(base_config)  # type: ignore[arg-type]
        if checkpoint is None:
            return None
        snapshot = await graph.aget_state(base_config)  # type: ignore[arg-type]
        values = dict(snapshot.values)
        if not values:
            return values

        raw_messages = values.get("messages")
        if isinstance(raw_messages, list):
            context.messages = copy.deepcopy(cast(list[dict[str, Any]], raw_messages))

        raw_step = values.get("step")
        if isinstance(raw_step, int) and not isinstance(raw_step, bool):
            context.step = max(context.step, raw_step)

        raw_status = values.get("status")
        if raw_status in ("running", "success", "failed"):
            context.status = raw_status

        raw_reason = values.get("reason")
        context.reason = raw_reason if isinstance(raw_reason, str) else None

        raw_answer = values.get("final_answer")
        context.result = raw_answer if isinstance(raw_answer, str) else ""
        return values

    async def _finalize_abnormal_locked(
        self,
        graph: RuntimeGraph | None,
        context: ExecutionContext,
        thread_id: str,
        *,
        reason: str,
        retain_thread: bool,
    ) -> None:
        """Sync an abnormal checkpoint and make pending tool history well formed.

        A cancelled tool may already have produced an external side effect even
        though its graph node never committed a result. The synthetic result is
        deliberately explicit about that uncertainty; it repairs the Anthropic
        transcript without claiming exactly-once execution.
        """

        values = await self._sync_context(graph, context, thread_id)
        if values is None:
            return

        raw_pending = values.get("pending_tool_calls")
        if not isinstance(raw_pending, list) or not raw_pending:
            return

        blocks: list[dict[str, object]] = []
        for raw_call in raw_pending:
            if not isinstance(raw_call, dict):
                continue
            tool_use_id = raw_call.get("id")
            if not isinstance(tool_use_id, str) or not tool_use_id:
                continue
            blocks.append(
                {
                    "type": "tool_result",
                    "tool_use_id": tool_use_id,
                    "content": (
                        f"Error: graph run ended with {reason} before a result for this "
                        "tool call was durably recorded. Its execution outcome is unknown; "
                        "do not assume the call is safe to repeat."
                    ),
                    "is_error": True,
                }
            )

        if blocks:
            context.messages.append({"role": "user", "content": blocks})

        # SessionStore receives the repaired canonical transcript from Runner.
        # Dropping the orchestration cache forces the next turn to cold-bootstrap
        # from that authoritative, paired history.
        if retain_thread:
            await self._runtime._reset_thread_unlocked(thread_id)

    async def _consume_updates(
        self,
        graph: RuntimeGraph,
        bridge: NodeEventBridge,
        initial_state: RuntimeState | Command[Any] | None,
        runnable_config: dict[str, object],
    ) -> bool:
        interrupted = False
        async for raw_update in graph.astream(  # type: ignore[call-overload]
            initial_state,
            config=runnable_config,
            stream_mode="updates",
            version="v1",
            durability="sync" if self._runtime.is_durable else None,
        ):
            if not isinstance(raw_update, dict):
                raise RuntimeError("LangGraph v1 updates stream returned a non-object update")
            for node_id, node_update in raw_update.items():
                if node_id == "__interrupt__":
                    interrupted = True
                    continue
                if node_id not in ("model", "kit_tools") or not isinstance(node_update, dict):
                    raise RuntimeError(f"Unexpected LangGraph node update: {node_id!r}")
                await bridge.publish_state_diff(node_id, cast(dict[str, object], node_update))
        return interrupted

    async def _suspension(
        self,
        *,
        context: ExecutionContext,
        events: EventBus,
        config: EngineRunConfig,
        reason: SuspensionReason,
    ) -> RunSuspension:
        summary = await self._runtime.latest_state(
            config.thread_id,
            session_id=config.session_id,
            run_id=context.run_id,
        )
        if summary is None:
            raise GraphStateConflictError("A suspended run has no checkpoint.")
        interrupt_id: str | None = None
        if reason == "permission":
            if summary.pending_tool_use_id is None or summary.pending_tool_name is None:
                raise GraphStateConflictError("Permission checkpoint has no pending tool identity.")
            interrupt_id = summary.interrupt_id
            await events.publish(
                PermissionRequestedEvent(
                    run_id=context.run_id,
                    tool_use_id=summary.pending_tool_use_id,
                    tool_name=summary.pending_tool_name,
                    params={},
                    param_preview=summary.pending_param_preview or "",
                    session_id=config.session_id,
                    interrupt_id=summary.interrupt_id,
                    checkpoint_revision=summary.checkpoint_revision,
                    expires_at=summary.pending_expires_at,
                    ts=datetime.now(UTC).isoformat(),
                )
            )
        return RunSuspension(
            run_id=context.run_id,
            session_id=config.session_id,
            reason=reason,
            checkpoint_revision=summary.checkpoint_revision,
            interrupt_id=interrupt_id,
            event_seq=events.last_event_seq,
        )

    @staticmethod
    def _permission_expired(summary: GraphStateSummary) -> bool:
        if summary.pending_expires_at is None:
            return False
        try:
            deadline = datetime.fromisoformat(summary.pending_expires_at)
        except ValueError as exc:
            raise GraphStateConflictError("Permission expiry is invalid.") from exc
        if deadline.tzinfo is None:
            raise GraphStateConflictError("Permission expiry must include a timezone.")
        return datetime.now(UTC) >= deadline.astimezone(UTC)

    async def _load_resume_summary(
        self,
        context: ExecutionContext,
        config: EngineRunConfig,
    ) -> GraphStateSummary:
        """Validate the caller's optimistic revision before compiling a resume graph."""

        expected_revision = config.expected_checkpoint_revision
        if not expected_revision:
            raise GraphStateConflictError("Resume requires an expected checkpoint revision.")
        revision = await self._runtime.checkpoint_revision(config.thread_id)
        if revision is None:
            raise GraphStateConflictError("Resume checkpoint was not found.")
        if revision != expected_revision:
            raise GraphStateConflictError("Resume checkpoint revision changed.")

        summary = await self._runtime.latest_state(
            config.thread_id,
            session_id=config.session_id,
            run_id=context.run_id,
        )
        if summary is None:
            raise GraphStateConflictError("Resume checkpoint was not found.")
        return summary

    async def _prepare_resume(
        self,
        graph: RuntimeGraph,
        context: ExecutionContext,
        config: EngineRunConfig,
        summary: GraphStateSummary,
    ) -> tuple[Command[Any] | None, GraphStateSummary, bool]:
        base_config = thread_config(config.thread_id)
        snapshot = await graph.aget_state(base_config)  # type: ignore[arg-type]
        raw_messages = snapshot.values.get("messages")
        if not isinstance(raw_messages, list):
            raise GraphStateConflictError("Checkpoint messages are invalid.")
        checkpoint_messages = cast(list[dict[str, Any]], raw_messages)
        if not _history_is_prefix(context.messages, checkpoint_messages):
            raise GraphStateConflictError(
                "Session transcript does not match the checkpoint history."
            )
        await self._sync_context(graph, context, config.thread_id)
        pending_nodes = tuple(snapshot.next)
        pending_interrupts = tuple(snapshot.interrupts)

        if context.status in ("success", "failed"):
            if pending_nodes or pending_interrupts:
                raise GraphStateConflictError("Terminal checkpoint still has pending work.")
            return None, summary, False
        if not pending_nodes and not pending_interrupts:
            raise GraphStateConflictError("Non-terminal checkpoint has no pending work.")

        if pending_interrupts:
            if summary.interrupt_id is None or summary.pending_tool_use_id is None:
                raise GraphStateConflictError("Permission checkpoint identity is incomplete.")
            if config.resume_value is None:
                raise GraphStateConflictError("Permission resume requires a decision.")
            if self._permission_expired(summary):
                # Consume the original native interrupt through the node's normal
                # deny path. This commits a paired tool_result and removes the
                # pending interrupt instead of leaving terminal metadata beside a
                # resumable checkpoint.
                return Command(resume="timeout"), summary, True
            return Command(resume=config.resume_value), summary, True

        if config.resume_value is not None:
            raise GraphStateConflictError(
                "A process-recovery checkpoint does not accept an approval decision."
            )
        return None, summary, True

    async def run(
        self,
        context: ExecutionContext,
        *,
        tools: ToolRegistry,
        events: EventBus,
        config: EngineRunConfig,
    ) -> EngineRunResult:
        if self._active_task is not None:
            raise RuntimeError("GraphExecutionEngine instances cannot run concurrently")
        thread_id = config.thread_id.strip()
        if not thread_id:
            raise ExecutionEngineConfigurationError(
                engine=self.name,
                message="The graph engine requires a non-empty thread_id.",
            )
        if config.compact_threshold > 0:
            raise ExecutionEngineConfigurationError(
                engine=self.name,
                message=(
                    "Graph P1 does not support automatic compaction; set "
                    "compaction.auto_threshold to 0 and use session.compact manually."
                ),
            )
        if config.durable_recovery and not self._runtime.is_durable:
            raise ExecutionEngineConfigurationError(
                engine=self.name,
                message="Durable Graph recovery requires the SQLite checkpoint backend.",
            )

        active_task = asyncio.current_task()
        if active_task is None:
            raise RuntimeError("GraphExecutionEngine requires an asyncio task")
        self._active_task = active_task

        timeout_scope = asyncio.timeout(config.wall_time_s)
        checkpoint_revision: str | None = None
        try:
            try:
                async with timeout_scope:
                    async with self._runtime.thread_scope(thread_id):
                        graph: RuntimeGraph | None = None
                        cleanup_cancelled = False
                        suspension: RunSuspension | None = None
                        try:
                            graph, bridge = self._build_graph(
                                context,
                                tools=tools,
                                events=events,
                                config=config,
                            )
                            incoming = await self._incoming_messages(graph, context, thread_id)
                            initial_state = new_run_input(context, incoming)
                            recursion_limit = (
                                config.recursion_limit
                                if config.recursion_limit is not None
                                else 2 * context.max_steps + 1
                            )
                            runnable_config: dict[str, object] = {
                                "configurable": {"thread_id": thread_id},
                                "recursion_limit": recursion_limit,
                            }
                            interrupted_stream = await self._consume_updates(
                                graph,
                                bridge,
                                initial_state,
                                runnable_config,
                            )
                        except ToolOutcomeUnknownError:
                            _, interrupted = await _finish_despite_cancellation(
                                self._sync_context(graph, context, thread_id)
                            )
                            if interrupted:
                                reason = (
                                    "exceeded_wall_time" if timeout_scope.expired() else "cancelled"
                                )
                                context.mark_failed(reason)
                                raise asyncio.CancelledError()
                            suspension = await self._suspension(
                                context=context,
                                events=events,
                                config=config,
                                reason="outcome_unknown",
                            )
                        except GraphRecursionError:
                            _, interrupted = await _finish_despite_cancellation(
                                self._finalize_abnormal_locked(
                                    graph,
                                    context,
                                    thread_id,
                                    reason="exceeded_recursion_limit",
                                    retain_thread=config.retain_thread,
                                )
                            )
                            if interrupted:
                                reason = (
                                    "exceeded_wall_time" if timeout_scope.expired() else "cancelled"
                                )
                                context.mark_failed(reason)
                                raise asyncio.CancelledError()
                            context.mark_failed("exceeded_recursion_limit")
                        except asyncio.CancelledError:
                            reason = (
                                "exceeded_wall_time" if timeout_scope.expired() else "cancelled"
                            )
                            await _finish_despite_cancellation(
                                self._finalize_abnormal_locked(
                                    graph,
                                    context,
                                    thread_id,
                                    reason=reason,
                                    retain_thread=config.retain_thread,
                                )
                            )
                            context.mark_failed(reason)
                            raise
                        except GraphStateConflictError:
                            raise
                        except BaseException:
                            await _finish_despite_cancellation(
                                self._finalize_abnormal_locked(
                                    graph,
                                    context,
                                    thread_id,
                                    reason="engine_execution_error",
                                    retain_thread=config.retain_thread,
                                )
                            )
                            raise
                        else:
                            _, interrupted = await _finish_despite_cancellation(
                                self._sync_context(graph, context, thread_id)
                            )
                            if interrupted:
                                reason = (
                                    "exceeded_wall_time" if timeout_scope.expired() else "cancelled"
                                )
                                context.mark_failed(reason)
                                raise asyncio.CancelledError()
                            if interrupted_stream:
                                if not config.durable_recovery:
                                    raise RuntimeError(
                                        "A non-recoverable Graph run produced an "
                                        "unexpected interrupt."
                                    )
                                suspension = await self._suspension(
                                    context=context,
                                    events=events,
                                    config=config,
                                    reason="permission",
                                )
                            elif self._runtime.is_durable:
                                checkpoint_revision = await self._runtime.checkpoint_revision(
                                    thread_id
                                )
                        finally:
                            if not config.retain_thread and suspension is None:
                                _, cleanup_cancelled = await _finish_despite_cancellation(
                                    self._runtime._delete_thread_unlocked(thread_id)
                                )

                        if cleanup_cancelled:
                            reason = (
                                "exceeded_wall_time" if timeout_scope.expired() else "cancelled"
                            )
                            context.mark_failed(reason)
                            raise asyncio.CancelledError()
                        if suspension is not None:
                            return suspension
            except TimeoutError:
                if not timeout_scope.expired():
                    raise
                context.mark_failed("exceeded_wall_time")

            return RunOutcome.from_context(
                context,
                checkpoint_revision=checkpoint_revision,
            )
        except asyncio.CancelledError:
            reason = "exceeded_wall_time" if timeout_scope.expired() else "cancelled"
            context.mark_failed(reason)
            raise
        finally:
            self._active_task = None

    async def resume(
        self,
        context: ExecutionContext,
        *,
        tools: ToolRegistry,
        events: EventBus,
        config: EngineRunConfig,
    ) -> EngineRunResult:
        if not self._runtime.is_durable or not config.durable_recovery:
            raise ExecutionEngineOperationUnsupportedError(
                engine=self.name,
                operation="resume",
                message="Graph resume requires a durable SQLite recovery run.",
            )
        if self._active_task is not None:
            raise RuntimeError("GraphExecutionEngine instances cannot run concurrently")
        thread_id = config.thread_id.strip()
        if not thread_id:
            raise ExecutionEngineConfigurationError(
                engine=self.name,
                message="The graph engine requires a non-empty thread_id.",
            )
        if config.compact_threshold > 0:
            raise ExecutionEngineConfigurationError(
                engine=self.name,
                message=(
                    "The graph engine does not support automatic compaction; set "
                    "compaction.auto_threshold to 0 and use session.compact manually."
                ),
            )

        active_task = asyncio.current_task()
        if active_task is None:
            raise RuntimeError("GraphExecutionEngine requires an asyncio task")
        self._active_task = active_task

        timeout_scope = asyncio.timeout(config.wall_time_s)
        checkpoint_revision: str | None = None
        try:
            try:
                async with timeout_scope:
                    async with self._runtime.thread_scope(thread_id):
                        graph: RuntimeGraph | None = None
                        cleanup_cancelled = False
                        suspension: RunSuspension | None = None
                        should_execute = False
                        try:
                            summary = await self._load_resume_summary(context, config)
                            graph, bridge = self._build_graph(
                                context,
                                tools=tools,
                                events=events,
                                config=config,
                                forced_permission_tool_use_id=(
                                    summary.pending_tool_use_id
                                    if summary.interrupt_id is not None
                                    else None
                                ),
                            )
                            graph_input, _summary, should_execute = await self._prepare_resume(
                                graph,
                                context,
                                config,
                                summary,
                            )
                            interrupted_stream = False
                            if should_execute:
                                recursion_limit = (
                                    config.recursion_limit
                                    if config.recursion_limit is not None
                                    else 2 * context.max_steps + 1
                                )
                                runnable_config: dict[str, object] = {
                                    "configurable": {"thread_id": thread_id},
                                    "recursion_limit": recursion_limit,
                                }
                                interrupted_stream = await self._consume_updates(
                                    graph,
                                    bridge,
                                    graph_input,
                                    runnable_config,
                                )
                        except ToolOutcomeUnknownError:
                            _, interrupted = await _finish_despite_cancellation(
                                self._sync_context(graph, context, thread_id)
                            )
                            if interrupted:
                                reason = (
                                    "exceeded_wall_time" if timeout_scope.expired() else "cancelled"
                                )
                                context.mark_failed(reason)
                                raise asyncio.CancelledError()
                            suspension = await self._suspension(
                                context=context,
                                events=events,
                                config=config,
                                reason="outcome_unknown",
                            )
                        except GraphRecursionError:
                            _, interrupted = await _finish_despite_cancellation(
                                self._finalize_abnormal_locked(
                                    graph,
                                    context,
                                    thread_id,
                                    reason="exceeded_recursion_limit",
                                    retain_thread=config.retain_thread,
                                )
                            )
                            if interrupted:
                                reason = (
                                    "exceeded_wall_time" if timeout_scope.expired() else "cancelled"
                                )
                                context.mark_failed(reason)
                                raise asyncio.CancelledError()
                            context.mark_failed("exceeded_recursion_limit")
                        except asyncio.CancelledError:
                            reason = (
                                "exceeded_wall_time" if timeout_scope.expired() else "cancelled"
                            )
                            await _finish_despite_cancellation(
                                self._finalize_abnormal_locked(
                                    graph,
                                    context,
                                    thread_id,
                                    reason=reason,
                                    retain_thread=config.retain_thread,
                                )
                            )
                            context.mark_failed(reason)
                            raise
                        except GraphStateConflictError:
                            raise
                        except BaseException:
                            await _finish_despite_cancellation(
                                self._finalize_abnormal_locked(
                                    graph,
                                    context,
                                    thread_id,
                                    reason="engine_execution_error",
                                    retain_thread=config.retain_thread,
                                )
                            )
                            raise
                        else:
                            if should_execute:
                                _, interrupted = await _finish_despite_cancellation(
                                    self._sync_context(graph, context, thread_id)
                                )
                                if interrupted:
                                    reason = (
                                        "exceeded_wall_time"
                                        if timeout_scope.expired()
                                        else "cancelled"
                                    )
                                    context.mark_failed(reason)
                                    raise asyncio.CancelledError()
                            if interrupted_stream:
                                suspension = await self._suspension(
                                    context=context,
                                    events=events,
                                    config=config,
                                    reason="permission",
                                )
                            else:
                                checkpoint_revision = await self._runtime.checkpoint_revision(
                                    thread_id
                                )
                        finally:
                            if not config.retain_thread and suspension is None:
                                _, cleanup_cancelled = await _finish_despite_cancellation(
                                    self._runtime._delete_thread_unlocked(thread_id)
                                )

                        if cleanup_cancelled:
                            reason = (
                                "exceeded_wall_time" if timeout_scope.expired() else "cancelled"
                            )
                            context.mark_failed(reason)
                            raise asyncio.CancelledError()
                        if suspension is not None:
                            return suspension
            except TimeoutError:
                if not timeout_scope.expired():
                    raise
                context.mark_failed("exceeded_wall_time")

            return RunOutcome.from_context(
                context,
                checkpoint_revision=checkpoint_revision,
            )
        except asyncio.CancelledError:
            reason = "exceeded_wall_time" if timeout_scope.expired() else "cancelled"
            context.mark_failed(reason)
            raise
        finally:
            self._active_task = None

    async def cancel(self) -> None:
        task = self._active_task
        if task is not None and not task.done():
            task.cancel()
