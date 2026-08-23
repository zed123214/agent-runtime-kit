from __future__ import annotations

import asyncio
import json
from typing import cast
from unittest.mock import AsyncMock, MagicMock

from pydantic import BaseModel

from agent_runtime.core.bus.events import (
    CoreStartedEvent,
    PermissionGrantedEvent,
    PermissionRequestedEvent,
    RunStartedEvent,
    StepStartedEvent,
    ToolCallStartedEvent,
)
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
