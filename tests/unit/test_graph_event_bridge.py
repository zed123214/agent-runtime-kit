from __future__ import annotations

import asyncio
import json

import pytest
from pydantic import BaseModel

from agent_runtime.core.context import ExecutionContext
from agent_runtime.core.events.bus import EventBus
from agent_runtime.core.graph.event_bridge import NodeEventBridge
from agent_runtime.core.graph.state import RuntimeState, StateUpdate, new_run_input

pytestmark = pytest.mark.graph


async def _collector(bus: EventBus) -> list[BaseModel]:
    events: list[BaseModel] = []

    async def collect(event: BaseModel) -> None:
        events.append(event)

    bus.subscribe(collect)
    return events


def _state() -> RuntimeState:
    context = ExecutionContext(run_id="run-1", goal="goal", max_steps=3)
    return new_run_input(context, context.messages)


async def test_wrapper_forwards_metadata_and_finished_precedes_state_diff() -> None:
    parent = EventBus(correlation_id="corr-1", session_id="session-1")
    events = await _collector(parent)
    bridge = NodeEventBridge(parent, "run-1", trace_limit=2)

    async def handler(state: RuntimeState, node_bus: EventBus) -> StateUpdate:
        assert node_bus.correlation_id == "corr-1"
        assert node_bus.session_id == "session-1"
        return {
            "messages": [{"role": "assistant", "content": "secret answer"}],
            "step": 1,
            "status": "running",
            "tool_call_count": 0,
        }

    update = await bridge.wrap("model", handler)(_state())
    await bridge.publish_state_diff("model", update)

    assert [event.type for event in events] == [  # type: ignore[attr-defined]
        "node.started",
        "node.finished",
        "state.diff",
    ]
    assert all(event.correlation_id == "corr-1" for event in events)  # type: ignore[attr-defined]
    assert all(event.session_id == "session-1" for event in events)  # type: ignore[attr-defined]
    assert all(event.node_id == "model" for event in events)  # type: ignore[attr-defined]
    serialized_diff = json.dumps(events[-1].diff)  # type: ignore[attr-defined]
    assert "secret answer" not in serialized_diff
    assert events[-1].diff["message_count_delta"] == 1  # type: ignore[attr-defined]
    assert len(update["trace_events"]) == 1  # type: ignore[arg-type]


async def test_wrapper_bounds_trace_events() -> None:
    parent = EventBus()
    bridge = NodeEventBridge(parent, "run-1", trace_limit=2)
    state = _state()

    async def handler(_state: RuntimeState, _bus: EventBus) -> StateUpdate:
        return {"status": "running"}

    wrapped = bridge.wrap("model", handler)
    for _ in range(3):
        update = await wrapped(state)
        state["trace_events"] = update["trace_events"]  # type: ignore[assignment]

    assert len(state["trace_events"]) == 2
    assert {event["node_id"] for event in state["trace_events"]} == {"model"}


async def test_cancelled_handler_publishes_one_cancelled_finished_and_reraises() -> None:
    parent = EventBus()
    events = await _collector(parent)
    bridge = NodeEventBridge(parent, "run-1", trace_limit=2)
    entered = asyncio.Event()

    async def handler(_state: RuntimeState, _bus: EventBus) -> StateUpdate:
        entered.set()
        await asyncio.Event().wait()
        raise AssertionError("unreachable")

    task = asyncio.create_task(bridge.wrap("kit_tools", handler)(_state()))
    await asyncio.wait_for(entered.wait(), timeout=1.0)
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, timeout=1.0)
    assert [event.type for event in events] == [  # type: ignore[attr-defined]
        "node.started",
        "node.finished",
    ]
    assert events[-1].status == "cancelled"  # type: ignore[attr-defined]


async def test_cancel_during_started_publish_still_emits_one_cancelled_finished() -> None:
    parent = EventBus()
    events: list[BaseModel] = []
    started_observed = asyncio.Event()
    release_started_subscriber = asyncio.Event()
    started_subscriber_drained = asyncio.Event()
    handler_called = False

    async def collect(event: BaseModel) -> None:
        events.append(event)

    async def block_started(event: BaseModel) -> None:
        if getattr(event, "type", None) == "node.started":
            started_observed.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                await release_started_subscriber.wait()
            finally:
                started_subscriber_drained.set()

    parent.subscribe(collect)
    parent.subscribe(block_started)
    bridge = NodeEventBridge(
        parent,
        "run-1",
        trace_limit=2,
        cancel_publish_grace_s=0.02,
    )

    async def handler(_state: RuntimeState, _bus: EventBus) -> StateUpdate:
        nonlocal handler_called
        handler_called = True
        return {"status": "success"}

    task = asyncio.create_task(bridge.wrap("model", handler)(_state()))
    await asyncio.wait_for(started_observed.wait(), timeout=1.0)
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, timeout=1.0)
    assert not handler_called
    assert [event.type for event in events] == [  # type: ignore[attr-defined]
        "node.started",
        "node.finished",
    ]
    assert events[-1].status == "cancelled"  # type: ignore[attr-defined]
    await asyncio.wait_for(started_subscriber_drained.wait(), timeout=1.0)
    release_started_subscriber.set()


async def test_cancel_after_handler_defers_until_successful_finished_and_state_diff() -> None:
    parent = EventBus(correlation_id="corr-1", session_id="session-1")
    events: list[BaseModel] = []
    finish_observed = asyncio.Event()
    release_finish = asyncio.Event()

    async def slow_collector(event: BaseModel) -> None:
        events.append(event)
        if getattr(event, "type", None) == "node.finished":
            finish_observed.set()
            await release_finish.wait()

    parent.subscribe(slow_collector)
    bridge = NodeEventBridge(parent, "run-1", trace_limit=2)

    async def handler(_state: RuntimeState, _bus: EventBus) -> StateUpdate:
        return {"status": "success"}

    task = asyncio.create_task(bridge.wrap("model", handler)(_state()))
    await asyncio.wait_for(finish_observed.wait(), timeout=1.0)
    task.cancel()
    release_finish.set()
    update = await asyncio.wait_for(task, timeout=1.0)

    assert [event.type for event in events] == [  # type: ignore[attr-defined]
        "node.started",
        "node.finished",
    ]
    assert events[-1].status == "success"  # type: ignore[attr-defined]

    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(bridge.publish_state_diff("model", update), timeout=1.0)
    assert [event.type for event in events] == [  # type: ignore[attr-defined]
        "node.started",
        "node.finished",
        "state.diff",
    ]


async def test_cancel_waits_for_finished_publisher_that_swallows_first_cancellation() -> None:
    parent = EventBus()
    handler_entered = asyncio.Event()
    finish_entered = asyncio.Event()
    cancellation_swallowed = asyncio.Event()
    release_subscriber = asyncio.Event()
    delivery_completed = asyncio.Event()

    async def stubborn_subscriber(event: BaseModel) -> None:
        if getattr(event, "type", None) != "node.finished":
            return
        finish_entered.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancellation_swallowed.set()
            await release_subscriber.wait()
        delivery_completed.set()

    parent.subscribe(stubborn_subscriber)
    bridge = NodeEventBridge(
        parent,
        "run-1",
        trace_limit=2,
        cancel_publish_grace_s=0.02,
    )

    async def handler(_state: RuntimeState, _bus: EventBus) -> StateUpdate:
        handler_entered.set()
        await asyncio.Event().wait()
        raise AssertionError("unreachable")

    task = asyncio.create_task(bridge.wrap("model", handler)(_state()))
    await asyncio.wait_for(handler_entered.wait(), timeout=1.0)
    task.cancel()
    await asyncio.wait_for(finish_entered.wait(), timeout=1.0)
    await asyncio.wait_for(cancellation_swallowed.wait(), timeout=1.0)

    assert not task.done()
    release_subscriber.set()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, timeout=1.0)
    assert delivery_completed.is_set()


async def test_cancel_is_bounded_after_state_diff_reaches_slow_subscriber() -> None:
    parent = EventBus()
    events: list[BaseModel] = []
    diff_entered = asyncio.Event()
    release_subscriber = asyncio.Event()
    subscriber_drained = asyncio.Event()

    async def slow_subscriber(event: BaseModel) -> None:
        events.append(event)
        if getattr(event, "type", None) != "state.diff":
            return
        diff_entered.set()
        try:
            await release_subscriber.wait()
        finally:
            subscriber_drained.set()

    parent.subscribe(slow_subscriber)
    bridge = NodeEventBridge(
        parent,
        "run-1",
        trace_limit=2,
        cancel_publish_grace_s=0.02,
    )

    async def handler(_state: RuntimeState, _bus: EventBus) -> StateUpdate:
        return {"status": "success"}

    update = await bridge.wrap("model", handler)(_state())
    diff_task = asyncio.create_task(bridge.publish_state_diff("model", update))
    await asyncio.wait_for(diff_entered.wait(), timeout=1.0)
    diff_task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(diff_task, timeout=0.5)
    assert [event.type for event in events] == [  # type: ignore[attr-defined]
        "node.started",
        "node.finished",
        "state.diff",
    ]
    await asyncio.wait_for(subscriber_drained.wait(), timeout=1.0)
    release_subscriber.set()


async def test_finished_subscriber_error_does_not_replace_handler_cancellation() -> None:
    parent = EventBus()
    events = await _collector(parent)
    handler_entered = asyncio.Event()

    async def failing_subscriber(event: BaseModel) -> None:
        if getattr(event, "type", None) == "node.finished":
            raise RuntimeError("terminal sink failed")

    parent.subscribe(failing_subscriber)
    bridge = NodeEventBridge(parent, "run-1", trace_limit=2)

    async def handler(_state: RuntimeState, _bus: EventBus) -> StateUpdate:
        handler_entered.set()
        await asyncio.Event().wait()
        raise AssertionError("unreachable")

    task = asyncio.create_task(bridge.wrap("model", handler)(_state()))
    await asyncio.wait_for(handler_entered.wait(), timeout=1.0)
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, timeout=1.0)
    assert [event.type for event in events] == [  # type: ignore[attr-defined]
        "node.started",
        "node.finished",
    ]
    assert events[-1].status == "cancelled"  # type: ignore[attr-defined]


async def test_repeated_cancel_waits_until_finished_publish_reaches_subscribers() -> None:
    parent = EventBus()
    events: list[BaseModel] = []
    node_entered = asyncio.Event()
    finish_entered = asyncio.Event()
    release_finish = asyncio.Event()

    async def slow_collector(event: BaseModel) -> None:
        events.append(event)
        if getattr(event, "type", None) == "node.finished":
            finish_entered.set()
            await release_finish.wait()

    parent.subscribe(slow_collector)
    bridge = NodeEventBridge(parent, "run-1", trace_limit=2)

    async def handler(_state: RuntimeState, _bus: EventBus) -> StateUpdate:
        node_entered.set()
        await asyncio.Event().wait()
        raise AssertionError("unreachable")

    task = asyncio.create_task(bridge.wrap("model", handler)(_state()))
    await asyncio.wait_for(node_entered.wait(), timeout=1.0)
    task.cancel()
    await asyncio.wait_for(finish_entered.wait(), timeout=1.0)
    task.cancel()
    await asyncio.sleep(0)
    assert not task.done()

    release_finish.set()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, timeout=1.0)

    assert [event.type for event in events] == [  # type: ignore[attr-defined]
        "node.started",
        "node.finished",
    ]
    assert events[-1].status == "cancelled"  # type: ignore[attr-defined]


async def test_exception_publishes_failed_finished_without_state_diff() -> None:
    parent = EventBus()
    events = await _collector(parent)
    bridge = NodeEventBridge(parent, "run-1", trace_limit=2)

    async def handler(_state: RuntimeState, _bus: EventBus) -> StateUpdate:
        raise RuntimeError("boom")

    with pytest.raises(RuntimeError, match="boom"):
        await bridge.wrap("model", handler)(_state())

    assert [event.type for event in events] == [  # type: ignore[attr-defined]
        "node.started",
        "node.finished",
    ]
    assert events[-1].status == "failed"  # type: ignore[attr-defined]


def test_trace_limit_must_be_positive() -> None:
    with pytest.raises(ValueError, match="trace_limit"):
        NodeEventBridge(EventBus(), "run-1", trace_limit=0)


def test_cancel_publish_grace_must_be_positive() -> None:
    with pytest.raises(ValueError, match="cancel_publish_grace_s"):
        NodeEventBridge(
            EventBus(),
            "run-1",
            trace_limit=1,
            cancel_publish_grace_s=0,
        )
