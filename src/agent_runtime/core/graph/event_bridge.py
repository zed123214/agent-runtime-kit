from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from typing import Any

from agent_runtime.core.bus.events import NodeFinishedEvent, NodeStartedEvent, StateDiffEvent
from agent_runtime.core.events.bus import EventBus
from agent_runtime.core.graph.state import RuntimeState, StateUpdate

type NodeHandler = Callable[[RuntimeState, EventBus], Awaitable[StateUpdate]]
type WrappedNode = Callable[[RuntimeState], Awaitable[StateUpdate]]

_DIFF_FIELD_LIMIT = 32
_REASON_LIMIT = 128
_CANCEL_PUBLISH_GRACE_S = 0.25

log = logging.getLogger(__name__)


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _list_size(value: object) -> int | None:
    return len(value) if isinstance(value, list) else None


class NodeEventBridge:
    """Add node lifecycle metadata while forwarding through the run-scoped bus."""

    def __init__(
        self,
        parent: EventBus,
        run_id: str,
        trace_limit: int,
        cancel_publish_grace_s: float = _CANCEL_PUBLISH_GRACE_S,
    ) -> None:
        if trace_limit <= 0:
            raise ValueError("trace_limit must be positive")
        if cancel_publish_grace_s <= 0:
            raise ValueError("cancel_publish_grace_s must be positive")
        self._parent = parent
        self._run_id = run_id
        self._trace_limit = trace_limit
        self._cancel_publish_grace_s = cancel_publish_grace_s
        self._deferred_cancel_nodes: set[str] = set()

    def _node_bus(self, node_id: str) -> EventBus:
        bus = EventBus(
            correlation_id=self._parent.correlation_id,
            session_id=self._parent.session_id,
            node_id=node_id,
        )
        bus.subscribe(self._parent.publish)
        return bus

    async def _publish_started(
        self,
        bus: EventBus,
        node_id: str,
    ) -> tuple[bool, BaseException | None]:
        """Publish start normally, but bound teardown after caller cancellation."""

        task = asyncio.create_task(
            bus.publish(NodeStartedEvent(run_id=self._run_id, node_id=node_id, ts=_now()))
        )
        loop = asyncio.get_running_loop()
        cancel_deadline: float | None = None
        cancelled_while_waiting = False

        while not task.done():
            timeout = None if cancel_deadline is None else max(0.0, cancel_deadline - loop.time())
            try:
                done, _pending = await asyncio.wait({task}, timeout=timeout)
            except asyncio.CancelledError:
                cancelled_while_waiting = True
                if cancel_deadline is None:
                    cancel_deadline = loop.time() + self._cancel_publish_grace_s
                    task.cancel()
                continue
            if not done:
                # A subscriber may swallow the first cancellation. Send one final
                # cancellation before detaching so ordinary cancellable sinks
                # cannot resume and emit a stale node event after run.finished.
                task.cancel()
                self._detach_publisher(task, node_id=node_id, event_type="node.started")
                return cancelled_while_waiting, None

        try:
            task.result()
        except BaseException as exc:
            if cancelled_while_waiting:
                return True, None
            return False, exc
        return cancelled_while_waiting, None

    async def _publish_finished(
        self,
        bus: EventBus,
        node_id: str,
        status: str,
    ) -> tuple[bool, BaseException | None]:
        """Publish one terminal event, deferring cancellation after node completion."""

        event = NodeFinishedEvent(
            run_id=self._run_id,
            correlation_id=bus.correlation_id,
            session_id=bus.session_id,
            node_id=node_id,
            status=status,
            ts=_now(),
        )
        return await self._publish_after_completion(
            bus,
            event,
            node_id=node_id,
            event_type="node.finished",
            cancellation_already_pending=status == "cancelled",
        )

    async def _publish_after_completion(
        self,
        bus: EventBus,
        event: NodeFinishedEvent | StateDiffEvent,
        *,
        node_id: str,
        event_type: str,
        cancellation_already_pending: bool,
    ) -> tuple[bool, BaseException | None]:
        """Keep post-completion event order while bounding cancellation cleanup."""

        task = asyncio.create_task(bus.publish(event))
        loop = asyncio.get_running_loop()
        cancel_deadline = (
            loop.time() + self._cancel_publish_grace_s if cancellation_already_pending else None
        )
        cancelled_while_waiting = cancellation_already_pending

        while not task.done():
            timeout = None if cancel_deadline is None else max(0.0, cancel_deadline - loop.time())
            try:
                done, _pending = await asyncio.wait({task}, timeout=timeout)
            except asyncio.CancelledError:
                cancelled_while_waiting = True
                if cancel_deadline is None:
                    cancel_deadline = loop.time() + self._cancel_publish_grace_s
                continue
            if not done:
                # EventWriter is the first run-bus subscriber, so the authoritative
                # JSONL has already observed the event before a later sink can
                # block. Cancel the remaining fan-out to prevent a stale event from
                # surfacing after run.finished. Detach only as a last resort for a
                # subscriber that suppresses cancellation itself.
                task.cancel()
                self._detach_publisher(task, node_id=node_id, event_type=event_type)
                return cancelled_while_waiting, None

        try:
            task.result()
        except BaseException as exc:
            return cancelled_while_waiting, exc
        return cancelled_while_waiting, None

    def _detach_publisher(
        self,
        task: asyncio.Task[None],
        *,
        node_id: str,
        event_type: str,
    ) -> None:
        """Detach one cancellation-delayed publisher and consume its eventual result."""

        task.add_done_callback(self._consume_detached_result)
        log.warning(
            "%s publish exceeded cancellation grace run_id=%s node_id=%s",
            event_type,
            self._run_id,
            node_id,
        )

    @staticmethod
    def _consume_detached_result(task: asyncio.Task[None]) -> None:
        """Retrieve a detached publisher result without extending run lifetime."""

        try:
            task.result()
        except BaseException:
            pass

    def _with_trace(
        self,
        state: RuntimeState,
        update: StateUpdate,
        *,
        node_id: str,
        status: str,
        elapsed_ms: int,
    ) -> StateUpdate:
        traced = dict(update)
        raw_trace = traced.get("trace_events", state.get("trace_events", []))
        trace = [dict(item) for item in raw_trace] if isinstance(raw_trace, list) else []
        step = traced.get("step", state.get("step", 0))
        tool_count = traced.get("tool_call_count", state.get("tool_call_count", 0))
        trace.append(
            {
                "node_id": node_id,
                "status": status,
                "elapsed_ms": elapsed_ms,
                "step": step if isinstance(step, int) else 0,
                "tool_call_count": tool_count if isinstance(tool_count, int) else 0,
            }
        )
        traced["trace_events"] = trace[-self._trace_limit :]
        return traced

    def wrap(self, node_id: str, handler: NodeHandler) -> WrappedNode:
        """Wrap a two-argument node as a LangGraph-compatible one-argument callable."""

        async def wrapped(state: RuntimeState) -> StateUpdate:
            node_bus = self._node_bus(node_id)
            try:
                cancelled, start_error = await self._publish_started(node_bus, node_id)
                if cancelled:
                    raise asyncio.CancelledError()
                if start_error is not None:
                    raise start_error
                started = time.monotonic()
                raw_update = await handler(state, node_bus)
                elapsed_ms = int((time.monotonic() - started) * 1000)
                raw_status = raw_update.get("status", state.get("status", "running"))
                node_status = "failed" if raw_status == "failed" else "success"
                update = self._with_trace(
                    state,
                    raw_update,
                    node_id=node_id,
                    status=node_status,
                    elapsed_ms=elapsed_ms,
                )
            except asyncio.CancelledError:
                # Subscriber failures must not replace the cancellation that
                # caused this terminal event.
                await self._publish_finished(node_bus, node_id, "cancelled")
                raise
            except BaseException:
                cancelled, _publish_error = await self._publish_finished(
                    node_bus, node_id, "failed"
                )
                if cancelled:
                    raise asyncio.CancelledError()
                raise

            cancelled, publish_error = await self._publish_finished(node_bus, node_id, node_status)
            if cancelled:
                # The handler completed, so its successful/failed terminal event
                # remains authoritative. Defer cancellation until the matching
                # state.diff has been offered to the run bus.
                self._deferred_cancel_nodes.add(node_id)
                return update
            if publish_error is not None:
                raise publish_error
            return update

        return wrapped

    async def publish_state_diff(self, node_id: str, update: StateUpdate) -> None:
        """Publish a bounded summary of an astream update, never its sensitive values."""

        fields = sorted(str(key) for key in update)[:_DIFF_FIELD_LIMIT]
        diff: dict[str, Any] = {
            "changed_fields": fields,
            "changed_field_count": len(update),
        }
        if len(update) > _DIFF_FIELD_LIMIT:
            diff["changed_fields_truncated"] = True

        for source, target in (
            ("messages", "message_count_delta"),
            ("pending_tool_calls", "pending_tool_call_count"),
            ("tool_results", "tool_result_count"),
            ("errors", "error_count"),
            ("trace_events", "trace_event_count"),
        ):
            size = _list_size(update.get(source))
            if size is not None:
                diff[target] = size

        for field in ("step", "tool_call_count"):
            value = update.get(field)
            if isinstance(value, int):
                diff[field] = value

        status = update.get("status")
        if isinstance(status, str):
            diff["status"] = status
        reason = update.get("reason")
        if isinstance(reason, str):
            diff["reason"] = reason[:_REASON_LIMIT]
        if "final_answer" in update:
            diff["has_final_answer"] = bool(update.get("final_answer"))

        deferred_cancel = node_id in self._deferred_cancel_nodes
        event = StateDiffEvent(
            run_id=self._run_id,
            node_id=node_id,
            diff=diff,
            ts=_now(),
        )
        cancelled, publish_error = await self._publish_after_completion(
            self._parent,
            event,
            node_id=node_id,
            event_type="state.diff",
            cancellation_already_pending=deferred_cancel,
        )
        self._deferred_cancel_nodes.discard(node_id)
        if cancelled:
            raise asyncio.CancelledError()
        if publish_error is not None:
            raise publish_error
