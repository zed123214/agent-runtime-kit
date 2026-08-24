from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import cast
from unittest.mock import AsyncMock, MagicMock

from pydantic import BaseModel

import agent_runtime.core.transport.ipc_broadcaster as broadcaster_module
from agent_runtime.core.bus.events import (
    CoreStartedEvent,
    PermissionGrantedEvent,
    PermissionRequestedEvent,
    RunStartedEvent,
    StepStartedEvent,
    ToolCallStartedEvent,
)
from agent_runtime.core.config import RuntimeConfig
from agent_runtime.core.events.bus import EventBus
from agent_runtime.core.llm.types import LlmResponse
from agent_runtime.core.runner import AgentRunner
from agent_runtime.core.session.model import Session
from agent_runtime.core.session.store import SessionStore
from agent_runtime.core.transport.ipc_broadcaster import IpcEventBroadcaster


def _make_writer(*, drain_raises: Exception | None = None) -> asyncio.StreamWriter:
    writer = MagicMock(spec=asyncio.StreamWriter)
    if drain_raises is not None:
        writer.drain = AsyncMock(side_effect=drain_raises)
    else:
        writer.drain = AsyncMock()
    return cast(asyncio.StreamWriter, writer)


def _run_started(run_id: str = "r1", session_id: str | None = "s1") -> RunStartedEvent:
    return RunStartedEvent(
        run_id=run_id,
        session_id=session_id,
        goal="test",
        ts="2026-01-01T00:00:00Z",
    )


def _run_started_seq(event_seq: int, run_id: str = "r1") -> RunStartedEvent:
    return _run_started(run_id=run_id).model_copy(update={"event_seq": event_seq})


class _EndTurnProvider:
    async def chat(
        self,
        messages: list[dict[str, object]],
        tool_schemas: list[dict[str, object]],
        bus: EventBus,
        run_id: str,
        *,
        step: int = 0,
        system: str | None = None,
    ) -> LlmResponse:
        return LlmResponse(stop_reason="end_turn", text="done")


# 功能：验证 subscribe 后 handle 将匹配 topic 的事件写入 writer，且内容是合法的 EventPushEnvelope
# 设计：用 MagicMock writer 捕获写入的字节，反序列化后断言 kind 和 event.type，排除对网络层的依赖
async def test_subscriber_receives_matching_event() -> None:
    broadcaster = IpcEventBroadcaster()
    writer = _make_writer()
    broadcaster.subscribe(writer, topics=["run.*"])
    broadcaster.bind_session(writer, "s1")

    await broadcaster.handle(_run_started())

    writer.write.assert_called_once()  # type: ignore[attr-defined]
    data = json.loads(writer.write.call_args[0][0].rstrip(b"\n"))  # type: ignore[attr-defined]
    assert data["kind"] == "event"
    assert data["event"]["type"] == "run.started"


# 功能：验证无订阅时 handle 不向任何 writer 写入数据
# 设计：创建 broadcaster 但不 subscribe，调用 handle 后断言 write 从未被调用，验证空 fan-out 的边界情况
async def test_no_subscription_no_write() -> None:
    broadcaster = IpcEventBroadcaster()
    writer = _make_writer()

    await broadcaster.handle(_run_started())

    writer.write.assert_not_called()  # type: ignore[attr-defined]


# 功能：验证 topic glob "step.*" 匹配 step.started 但不匹配 run.started
# 设计：向同一 broadcaster 发布两种事件，断言 write 只被调用一次，验证 fnmatch 语义的 glob 边界行为
async def test_topic_glob_matches_step_not_run() -> None:
    broadcaster = IpcEventBroadcaster()
    writer = _make_writer()
    broadcaster.subscribe(writer, topics=["step.*"])
    broadcaster.bind_session(writer, "s1")

    step_event = StepStartedEvent(
        run_id="r1",
        session_id="s1",
        step=1,
        ts="2026-01-01T00:00:00Z",
    )
    run_event = _run_started()

    await broadcaster.handle(step_event)
    await broadcaster.handle(run_event)

    assert writer.write.call_count == 1  # type: ignore[attr-defined]
    data = json.loads(writer.write.call_args[0][0].rstrip(b"\n"))  # type: ignore[attr-defined]
    assert data["event"]["type"] == "step.started"


# 功能：验证 scope="global" 的订阅能收到任意 run_id 的事件
# 设计：发布两个不同 run_id 的事件，断言两次都写入，确认 global scope 不过滤 run_id 字段
async def test_scope_global_receives_all_run_ids() -> None:
    broadcaster = IpcEventBroadcaster()
    writer = _make_writer()
    broadcaster.subscribe(writer, topics=["run.*"], scope="global")
    broadcaster.bind_session(writer, "s1")

    await broadcaster.handle(_run_started("r1"))
    await broadcaster.handle(_run_started("r2"))

    assert writer.write.call_count == 2  # type: ignore[attr-defined]


# 功能：验证 scope="run:<id>" 只接收匹配 run_id 的事件，过滤其他 run_id
# 设计：订阅 scope="run:abc"，发布 run_id="abc" 和 run_id="xyz"，断言只写入一次，验证 run-specific scope 的过滤语义
async def test_scope_run_specific_filters_other_run_ids() -> None:
    broadcaster = IpcEventBroadcaster()
    writer = _make_writer()
    broadcaster.subscribe(writer, topics=["run.*"], scope="run:abc")
    broadcaster.bind_session(writer, "s1")

    await broadcaster.handle(_run_started("abc"))
    await broadcaster.handle(_run_started("xyz"))

    assert writer.write.call_count == 1  # type: ignore[attr-defined]


# 功能：验证 global 订阅只收到属于当前连接 session 的权限请求
# 设计：两个连接分别绑定 s1/s2，发布 s1 的 permission.requested，仅 s1 owner 收到
async def test_permission_event_is_filtered_by_connection_session_owner() -> None:
    broadcaster = IpcEventBroadcaster()
    owner = _make_writer()
    other = _make_writer()
    broadcaster.subscribe(owner, topics=["permission.*"], scope="global")
    broadcaster.subscribe(other, topics=["permission.*"], scope="global")
    broadcaster.bind_session(owner, "s1")
    broadcaster.bind_session(other, "s2")

    await broadcaster.handle(
        PermissionRequestedEvent(
            run_id="r1",
            tool_use_id="tool-1",
            tool_name="bash",
            params={"command": "echo safe"},
            param_preview="echo safe",
            session_id="s1",
            ts="2026-01-01T00:00:00Z",
        )
    )

    owner.write.assert_called_once()  # type: ignore[attr-defined]
    other.write.assert_not_called()  # type: ignore[attr-defined]


# 功能：验证缺失 session_id 的旧权限事件不会通过 global 订阅泄露
# 设计：即使连接绑定了 session，无法证明归属的 permission.granted 也应 fail closed
async def test_permission_event_without_session_id_is_not_delivered() -> None:
    broadcaster = IpcEventBroadcaster()
    writer = _make_writer()
    broadcaster.subscribe(writer, topics=["permission.*"], scope="global")
    broadcaster.bind_session(writer, "s1")

    await broadcaster.handle(
        PermissionGrantedEvent(
            run_id="r1",
            tool_use_id="tool-1",
            decision="allow_once",
            ts="2026-01-01T00:00:00Z",
        )
    )

    writer.write.assert_not_called()  # type: ignore[attr-defined]


async def test_run_and_tool_events_without_session_id_fail_closed() -> None:
    broadcaster = IpcEventBroadcaster()
    writer = _make_writer()
    broadcaster.subscribe(writer, topics=["run.*", "tool.*"], scope="global")
    broadcaster.bind_session(writer, "s1")

    await broadcaster.handle(_run_started(session_id=None))
    await broadcaster.handle(
        ToolCallStartedEvent(
            run_id="r1",
            tool_use_id="tool-1",
            tool_name="bash",
            params={"command": "secret"},
            ts="2026-01-01T00:00:00Z",
        )
    )

    writer.write.assert_not_called()  # type: ignore[attr-defined]


async def test_session_event_is_sent_only_to_exclusive_owner() -> None:
    broadcaster = IpcEventBroadcaster()
    owner = _make_writer()
    other = _make_writer()
    broadcaster.subscribe(owner, topics=["run.*"])
    broadcaster.subscribe(other, topics=["run.*"])
    assert broadcaster.bind_session(owner, "s1") is True
    assert broadcaster.bind_session(other, "s1") is False

    await broadcaster.handle(_run_started(session_id="s1"))

    owner.write.assert_called_once()  # type: ignore[attr-defined]
    other.write.assert_not_called()  # type: ignore[attr-defined]


async def test_tool_call_params_are_visible_only_to_session_owner() -> None:
    broadcaster = IpcEventBroadcaster()
    owner = _make_writer()
    other = _make_writer()
    broadcaster.subscribe(owner, topics=["tool.*"])
    broadcaster.subscribe(other, topics=["tool.*"])
    broadcaster.bind_session(owner, "s1")
    broadcaster.bind_session(other, "s2")

    await broadcaster.handle(
        ToolCallStartedEvent(
            run_id="r1",
            session_id="s1",
            tool_use_id="tool-1",
            tool_name="bash",
            params={"command": "echo private-token"},
            ts="2026-01-01T00:00:00Z",
        )
    )

    owner.write.assert_called_once()  # type: ignore[attr-defined]
    other.write.assert_not_called()  # type: ignore[attr-defined]


async def test_non_sensitive_core_event_remains_globally_visible() -> None:
    broadcaster = IpcEventBroadcaster()
    writer = _make_writer()
    broadcaster.subscribe(writer, topics=["core.*"])

    await broadcaster.handle(CoreStartedEvent(listen_addr="127.0.0.1:7437", version="1"))

    writer.write.assert_called_once()  # type: ignore[attr-defined]


async def test_unknown_ownerless_event_fails_closed() -> None:
    class PluginEvent(BaseModel):
        type: str = "plugin.secret"
        payload: str

    broadcaster = IpcEventBroadcaster()
    writer = _make_writer()
    broadcaster.subscribe(writer, topics=["*"])

    await broadcaster.handle(PluginEvent(payload="private"))

    writer.write.assert_not_called()  # type: ignore[attr-defined]


# 功能：验证 unsubscribe 后 handle 不再向该 writer 发送事件
# 设计：先 subscribe 再 unsubscribe，再调用 handle，断言 write 从未被调用，验证订阅生命周期的正确性
async def test_unsubscribe_stops_delivery() -> None:
    broadcaster = IpcEventBroadcaster()
    writer = _make_writer()
    broadcaster.subscribe(writer, topics=["run.*"])
    broadcaster.bind_session(writer, "s1")
    broadcaster.unsubscribe(writer)

    await broadcaster.handle(_run_started())

    writer.write.assert_not_called()  # type: ignore[attr-defined]
    assert broadcaster.session_ids_for(writer) == frozenset()


def test_unbind_session_preserves_sibling_ownership() -> None:
    broadcaster = IpcEventBroadcaster()
    writer = _make_writer()
    assert broadcaster.bind_session(writer, "s1")
    assert broadcaster.bind_session(writer, "s2")

    assert broadcaster.unbind_session(writer, "s1")

    assert broadcaster.session_ids_for(writer) == frozenset({"s2"})
    assert broadcaster.owns_session(writer, "s1") is False
    assert broadcaster.owns_session(writer, "s2") is True
    assert broadcaster.unbind_session(writer, "s1") is False


def test_reserved_session_is_exclusive_but_hidden_until_commit() -> None:
    broadcaster = IpcEventBroadcaster()
    writer = _make_writer()
    other = _make_writer()

    assert broadcaster.reserve_session(writer, "s1")
    assert broadcaster.reserve_session(writer, "s1") is False
    assert broadcaster.reserve_session(other, "s1") is False
    assert broadcaster.owns_session(writer, "s1") is False
    assert broadcaster.session_ids_for(writer) == frozenset()
    assert broadcaster.can_receive(
        writer,
        {"type": "run.finished", "run_id": "r1", "session_id": "s1"},
    )
    assert not broadcaster.can_receive(
        other,
        {"type": "run.finished", "run_id": "r1", "session_id": "s1"},
    )

    assert broadcaster.commit_reserved_session(writer, "s1")
    assert broadcaster.owns_session(writer, "s1") is True


# 功能：验证写入失败（ConnectionResetError）后订阅自动移除，下次 handle 不再尝试写入
# 设计：drain() 抛出 ConnectionResetError 触发死连接清理；断言第二次 handle 时 write 未被调用；
#       第一次 write 在 drain 前已执行，call_count==1 是预期行为而非被测点
async def test_dead_connection_removed_after_failure() -> None:
    broadcaster = IpcEventBroadcaster()
    writer = _make_writer(drain_raises=ConnectionResetError())
    broadcaster.subscribe(writer, topics=["run.*"])
    broadcaster.bind_session(writer, "s1")

    event = _run_started()
    await broadcaster.handle(event)  # drain fails → subscription removed

    assert writer.write.call_count == 1  # type: ignore[attr-defined]

    writer.write.reset_mock()  # type: ignore[attr-defined]
    await broadcaster.handle(event)  # no subscribers remain
    writer.write.assert_not_called()  # type: ignore[attr-defined]


async def test_blocked_drain_is_bounded_without_blocking_another_session(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(broadcaster_module, "_DRAIN_TIMEOUT_S", 0.5)
    drain_started = asyncio.Event()
    never_release = asyncio.Event()

    async def blocked_drain() -> None:
        drain_started.set()
        await never_release.wait()

    slow_writer = _make_writer()
    slow_writer.drain = AsyncMock(side_effect=blocked_drain)  # type: ignore[method-assign]
    fast_writer = _make_writer()
    disconnected: list[frozenset[str]] = []
    broadcaster = IpcEventBroadcaster(on_disconnect=disconnected.append)
    broadcaster.subscribe(slow_writer, topics=["run.*"])
    broadcaster.subscribe(fast_writer, topics=["run.*"])
    broadcaster.bind_session(slow_writer, "slow-session")
    broadcaster.bind_session(fast_writer, "fast-session")
    global_bus = EventBus()
    global_bus.subscribe(broadcaster.handle)

    store = SessionStore(tmp_path / "sessions")
    slow_session = Session(
        id="slow-session",
        mode="chat",
        status="active",
        title="slow",
        created_at="2026-01-01T00:00:00Z",
        updated_at="2026-01-01T00:00:00Z",
    )
    fast_session = Session(
        id="fast-session",
        mode="chat",
        status="active",
        title="fast",
        created_at="2026-01-01T00:00:00Z",
        updated_at="2026-01-01T00:00:00Z",
    )
    config = RuntimeConfig()
    slow_runner = AgentRunner(
        config,
        bus=global_bus,
        provider=_EndTurnProvider(),  # type: ignore[arg-type]
        runs_dir=tmp_path / "runs",
    )
    fast_runner = AgentRunner(
        config,
        bus=global_bus,
        provider=_EndTurnProvider(),  # type: ignore[arg-type]
        runs_dir=tmp_path / "runs",
    )

    slow_run = asyncio.create_task(
        slow_runner.run_and_capture(
            "slow goal",
            run_id="slow-run",
            session=slow_session,
            store=store,
        )
    )
    await asyncio.wait_for(drain_started.wait(), timeout=1.0)

    fast_outcome = await asyncio.wait_for(
        fast_runner.run_and_capture(
            "fast goal",
            run_id="fast-run",
            session=fast_session,
            store=store,
        ),
        timeout=0.25,
    )
    assert fast_outcome.status == "success"
    assert slow_run.done() is False

    slow_outcome = await asyncio.wait_for(slow_run, timeout=1.0)
    assert slow_outcome.status == "success"
    assert disconnected == [frozenset({"slow-session"})]
    assert broadcaster.session_ids_for(slow_writer) == frozenset()
    assert broadcaster.session_ids_for(fast_writer) == frozenset({"fast-session"})

    event_path = store.runs_dir("slow-session") / "slow-run" / "events.jsonl"
    events = [json.loads(line) for line in event_path.read_text(encoding="utf-8").splitlines()]
    finished = [event for event in events if event["type"] == "run.finished"]
    assert len(finished) == 1
    assert finished[0]["status"] == "success"
    assert events[-1] == finished[0]


async def test_paused_replay_activation_drains_without_gap_or_duplicate() -> None:
    first_drain_started = asyncio.Event()
    release_first_drain = asyncio.Event()
    drain_calls = 0

    async def gated_drain() -> None:
        nonlocal drain_calls
        drain_calls += 1
        if drain_calls == 1:
            first_drain_started.set()
            await release_first_drain.wait()

    broadcaster = IpcEventBroadcaster()
    writer = _make_writer()
    writer.drain = AsyncMock(side_effect=gated_drain)  # type: ignore[method-assign]
    sub_id = broadcaster.subscribe(writer, ["run.*"], paused=True)
    broadcaster.bind_session(writer, "s1")

    # seq=2 is already present in the replay snapshot; seq=3 landed after the
    # snapshot was read and must be the first buffered row sent.
    await broadcaster.handle(_run_started_seq(2))
    await broadcaster.handle(_run_started_seq(3))

    activation = asyncio.create_task(
        broadcaster.activate_buffered(
            sub_id,
            replay_run_id="r1",
            after_event_seq=2,
        )
    )
    await asyncio.wait_for(first_drain_started.wait(), timeout=1.0)

    # Publishers must not wait behind the replay client's drain. The event is
    # appended to the next paused batch while seq=3 is still draining.
    await asyncio.wait_for(broadcaster.handle(_run_started_seq(4)), timeout=0.1)
    assert activation.done() is False

    release_first_drain.set()
    assert await asyncio.wait_for(activation, timeout=1.0) is True

    # Once activation sees an empty buffer it switches directly to live mode.
    await broadcaster.handle(_run_started_seq(5))

    delivered = [
        json.loads(call.args[0].rstrip(b"\n"))["event"]["event_seq"]
        for call in writer.write.call_args_list  # type: ignore[attr-defined]
    ]
    assert delivered == [3, 4, 5]


async def test_replay_cursor_does_not_filter_other_runs_in_global_scope() -> None:
    broadcaster = IpcEventBroadcaster()
    writer = _make_writer()
    sub_id = broadcaster.subscribe(writer, ["run.*"], paused=True)
    broadcaster.bind_session(writer, "s1")

    await broadcaster.handle(_run_started_seq(10, "run-a"))
    await broadcaster.handle(_run_started_seq(1, "run-b"))

    assert await broadcaster.activate_buffered(
        sub_id,
        replay_run_id="run-a",
        after_event_seq=10,
    )
    delivered = [
        json.loads(call.args[0].rstrip(b"\n"))["event"]["run_id"]
        for call in writer.write.call_args_list  # type: ignore[attr-defined]
    ]
    assert delivered == ["run-b"]


async def test_paused_replay_buffer_is_bounded(monkeypatch) -> None:
    monkeypatch.setattr(broadcaster_module, "_MAX_PAUSED_EVENTS", 2)
    disconnected: list[frozenset[str]] = []
    broadcaster = IpcEventBroadcaster(on_disconnect=disconnected.append)
    writer = _make_writer()
    broadcaster.subscribe(writer, ["run.*"], paused=True)
    broadcaster.bind_session(writer, "s1")

    await broadcaster.handle(_run_started_seq(1))
    await broadcaster.handle(_run_started_seq(2))
    await broadcaster.handle(_run_started_seq(3))

    assert disconnected == [frozenset({"s1"})]
    assert broadcaster.session_ids_for(writer) == frozenset()
    writer.write.assert_not_called()  # type: ignore[attr-defined]


async def test_paused_activation_preserves_slow_drain_disconnect_semantics(
    monkeypatch,
) -> None:
    monkeypatch.setattr(broadcaster_module, "_DRAIN_TIMEOUT_S", 0.01)
    never_release = asyncio.Event()

    async def blocked_drain() -> None:
        await never_release.wait()

    disconnected: list[frozenset[str]] = []
    broadcaster = IpcEventBroadcaster(on_disconnect=disconnected.append)
    writer = _make_writer()
    writer.drain = AsyncMock(side_effect=blocked_drain)  # type: ignore[method-assign]
    sub_id = broadcaster.subscribe(writer, ["run.*"], paused=True)
    broadcaster.bind_session(writer, "s1")
    await broadcaster.handle(_run_started_seq(1))

    assert (
        await broadcaster.activate_buffered(
            sub_id,
            replay_run_id="r1",
            after_event_seq=0,
        )
        is False
    )
    assert disconnected == [frozenset({"s1"})]
    assert broadcaster.session_ids_for(writer) == frozenset()


def test_disconnected_writer_cannot_rebind_or_subscribe() -> None:
    disconnected: list[frozenset[str]] = []
    broadcaster = IpcEventBroadcaster(on_disconnect=disconnected.append)
    writer = _make_writer()
    assert broadcaster.bind_session(writer, "s1") is True
    broadcaster.mark_disconnected(writer)

    # Tombstoning closes the authorization window before ownership cleanup.
    assert broadcaster.session_ids_for(writer) == frozenset()

    assert broadcaster.disconnect(writer) == frozenset({"s1"})

    assert disconnected == [frozenset({"s1"})]
    assert broadcaster.bind_session(writer, "s2") is False
    broadcaster.subscribe(writer, ["core.*"])
    assert broadcaster.session_ids_for(writer) == frozenset()
