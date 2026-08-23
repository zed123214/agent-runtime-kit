from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock

import pytest

from agent_runtime.core.app import CoreApp
from agent_runtime.core.bus.events import (
    RunStartedEvent,
    SubagentFinishedEvent,
    SubagentStartedEvent,
)
from agent_runtime.core.events.bus import EventBus
from agent_runtime.core.events.writer import EventWriter
from agent_runtime.core.llm.types import LlmResponse, UsageStats
from agent_runtime.core.loop import AgentLoop
from agent_runtime.core.subagent.registry import BackgroundTaskRegistry
from agent_runtime.core.subagent.tool import AgentResultTool, SpawnAgentTool
from agent_runtime.core.transport.ipc_broadcaster import IpcEventBroadcaster


def _make_provider(result_text: str = "child done") -> Any:
    provider = AsyncMock()
    provider.chat = AsyncMock(
        return_value=LlmResponse(
            stop_reason="end_turn",
            tool_calls=[],
            text=result_text,
            usage=UsageStats(
                input_tokens=10,
                output_tokens=5,
                cache_read_input_tokens=0,
                cache_creation_input_tokens=0,
                context_pct=0.01,
            ),
        )
    )
    return provider


def _make_tool(
    tmp_path: Path,
    provider: Any = None,
    depth: int = 0,
) -> tuple[SpawnAgentTool, BackgroundTaskRegistry, EventBus]:
    bus = EventBus()
    registry = BackgroundTaskRegistry()
    tool = SpawnAgentTool(
        provider=provider or _make_provider(),
        parent_bus=bus,
        parent_run_id="parent-run-01",
        permission_manager=None,
        max_steps=5,
        task_registry=registry,
        runs_dir=tmp_path,
        session_id="sess-test",
        depth=depth,
    )
    return tool, registry, bus


def _child_run_id(content: str) -> str:
    return content.split("run_id=")[1].split(".")[0]


def _read_events(path: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]


def _make_writer() -> asyncio.StreamWriter:
    writer = MagicMock(spec=asyncio.StreamWriter)
    writer.drain = AsyncMock()
    return cast(asyncio.StreamWriter, writer)


# 功能：前台模式下 spawn_agent 应阻塞直到子 agent 完成并返回其结果
# 设计：使用返回 end_turn 的 mock provider，验证 tool_result.content 包含 provider 返回的文字
@pytest.mark.asyncio
async def test_foreground_returns_result(tmp_path: Path) -> None:
    tool, _, _ = _make_tool(tmp_path, _make_provider("analysis complete"))
    result = await asyncio.wait_for(
        tool.invoke(
            {
                "description": "分析代码",
                "prompt": "分析 src/ 目录",
            }
        ),
        timeout=2.0,
    )
    assert not result.is_error
    assert "analysis complete" in result.content


# 功能：后台模式应立即返回含 run_id 的消息，不阻塞等待子 agent
# 设计：run_in_background=true 后验证返回消息含 "run_id=" 并且任务注册表已有对应条目
@pytest.mark.asyncio
async def test_background_returns_run_id(tmp_path: Path) -> None:
    tool, registry, bus = _make_tool(tmp_path)
    events: list[Any] = []

    async def collect(event: Any) -> None:
        events.append(event)

    bus.subscribe(collect)
    result = await asyncio.wait_for(
        tool.invoke(
            {
                "description": "后台任务",
                "prompt": "做点事",
                "run_in_background": True,
            }
        ),
        timeout=2.0,
    )
    assert not result.is_error
    assert "run_id=" in result.content
    run_id = _child_run_id(result.content)
    entry = registry.get(run_id)
    assert entry is not None

    # Background invoke does not return until started is both durable and bridged.
    started = [event for event in events if isinstance(event, SubagentStartedEvent)]
    assert [event.run_id for event in started] == [run_id]
    persisted_at_return = _read_events(tmp_path / run_id / "events.jsonl")
    assert persisted_at_return[0]["type"] == "subagent.started"

    task, context = entry
    await asyncio.wait_for(task, timeout=2.0)
    assert context.status == "success"

    persisted = _read_events(tmp_path / run_id / "events.jsonl")
    assert persisted[0]["type"] == "subagent.started"
    assert persisted[-1]["type"] == "subagent.finished"
    assert [event["type"] for event in persisted].count("subagent.started") == 1
    assert [event["type"] for event in persisted].count("subagent.finished") == 1
    assert persisted[-1]["status"] == "success"
    assert {event["run_id"] for event in persisted} == {run_id}

    live_child_types = [event.type for event in events if getattr(event, "run_id", None) == run_id]
    assert live_child_types[0] == "subagent.started"
    assert live_child_types[-1] == "subagent.finished"


@pytest.mark.asyncio
async def test_background_lifecycle_events_are_replayable(
    tmp_path: Path,
) -> None:
    tool, registry, _ = _make_tool(tmp_path)
    result = await asyncio.wait_for(
        tool.invoke(
            {
                "description": "replay task",
                "prompt": "produce replayable lifecycle",
                "run_in_background": True,
            }
        ),
        timeout=2.0,
    )
    run_id = _child_run_id(result.content)
    entry = registry.get(run_id)
    assert entry is not None
    await asyncio.wait_for(entry[0], timeout=2.0)

    app = CoreApp()
    app._runs_root = tmp_path
    app._sessions_root = tmp_path / "sessions"
    app._broadcaster = IpcEventBroadcaster()
    replay_writer = _make_writer()
    app._broadcaster.bind_session(replay_writer, "sess-test")

    replayed = await asyncio.wait_for(
        app._replay_events(
            run_id,
            replay_writer,
            ["subagent.*"],
            f"run:{run_id}",
        ),
        timeout=2.0,
    )

    assert replayed == 2
    write_mock = cast(Any, replay_writer.write)
    replayed_events = [json.loads(call.args[0])["event"] for call in write_mock.call_args_list]
    assert [event["type"] for event in replayed_events] == [
        "subagent.started",
        "subagent.finished",
    ]
    assert {event["run_id"] for event in replayed_events} == {run_id}


# 功能：后台任务未完成时 agent_result 应返回 "still running"
# 设计：用 Event 阻塞 provider.chat，在未等待任务完成时查询 agent_result
@pytest.mark.asyncio
async def test_agent_result_pending(tmp_path: Path) -> None:
    entered = asyncio.Event()
    release = asyncio.Event()

    async def slow_chat(*args: Any, **kwargs: Any) -> LlmResponse:
        entered.set()
        await release.wait()
        return LlmResponse(
            stop_reason="end_turn",
            tool_calls=[],
            text="done",
            usage=UsageStats(0, 0, 0, 0, 0.0),
        )

    provider = MagicMock()
    provider.chat = slow_chat

    tool, registry, _ = _make_tool(tmp_path, provider)
    spawn_result = await asyncio.wait_for(
        tool.invoke(
            {
                "description": "slow task",
                "prompt": "do something slow",
                "run_in_background": True,
            }
        ),
        timeout=2.0,
    )
    run_id = _child_run_id(spawn_result.content)
    await asyncio.wait_for(entered.wait(), timeout=2.0)

    result_tool = AgentResultTool(registry)
    result = await result_tool.invoke({"run_id": run_id})
    assert result.content == "still running"
    assert not result.is_error

    release.set()
    entry = registry.get(run_id)
    assert entry is not None
    await asyncio.wait_for(entry[0], timeout=2.0)


# 功能：后台任务完成后 agent_result 应返回子 agent 的最终文本
# 设计：等待后台任务 task 完成后调用 agent_result，断言返回内容与 provider 结果一致
@pytest.mark.asyncio
async def test_agent_result_done(tmp_path: Path) -> None:
    tool, registry, _ = _make_tool(tmp_path, _make_provider("final answer"))
    spawn_result = await asyncio.wait_for(
        tool.invoke(
            {
                "description": "bg task",
                "prompt": "do it",
                "run_in_background": True,
            }
        ),
        timeout=2.0,
    )
    run_id = _child_run_id(spawn_result.content)

    entry = registry.get(run_id)
    assert entry is not None
    task, _ = entry
    await asyncio.wait_for(task, timeout=5.0)

    result_tool = AgentResultTool(registry)
    result = await result_tool.invoke({"run_id": run_id})
    assert not result.is_error
    assert "final answer" in result.content


# 功能：depth=2 时调用 spawn_agent 应返回 is_error=True（嵌套限制）
# 设计：构造 depth=2 的工具，断言 invoke 直接返回错误而不调用 provider
@pytest.mark.asyncio
async def test_nesting_limit(tmp_path: Path) -> None:
    provider = _make_provider()
    tool, _, _ = _make_tool(tmp_path, provider, depth=2)
    result = await asyncio.wait_for(
        tool.invoke(
            {
                "description": "nested",
                "prompt": "do nested work",
            }
        ),
        timeout=2.0,
    )
    assert result.is_error
    assert "nesting limit" in result.content
    provider.chat.assert_not_called()


# 功能：agent_result 查询不存在的 run_id 应返回 is_error=True
# 设计：空 registry 中查询随机 run_id，验证错误消息含 "Unknown"
@pytest.mark.asyncio
async def test_agent_result_unknown_run_id(tmp_path: Path) -> None:
    registry = BackgroundTaskRegistry()
    tool = AgentResultTool(registry)
    result = await asyncio.wait_for(tool.invoke({"run_id": "nonexistent-id"}), timeout=2.0)
    assert result.is_error
    assert "Unknown" in result.content


# 功能：SubagentStartedEvent 应在前台 spawn 时发布到父 bus
# 设计：订阅父 bus 收集所有事件，断言 subagent.started 出现，且 parent_run_id 和 description 正确
@pytest.mark.asyncio
async def test_foreground_publishes_started_event(tmp_path: Path) -> None:
    tool, _, bus = _make_tool(tmp_path)
    events: list[Any] = []

    async def _collect(e: Any) -> None:
        events.append(e)

    bus.subscribe(_collect)

    parent_event_file = tmp_path / "parent-run-01" / "events.jsonl"
    async with EventWriter(parent_event_file, run_id="parent-run-01") as writer:
        writer.subscribe(bus)
        await bus.publish(RunStartedEvent(run_id="parent-run-01", goal="parent", ts="t"))
        result = await asyncio.wait_for(
            tool.invoke(
                {
                    "description": "test task",
                    "prompt": "test prompt",
                }
            ),
            timeout=2.0,
        )
    assert not result.is_error
    started = [e for e in events if isinstance(e, SubagentStartedEvent)]
    finished = [e for e in events if isinstance(e, SubagentFinishedEvent)]
    assert len(started) == 1
    assert len(finished) == 1
    assert started[0].parent_run_id == "parent-run-01"
    assert started[0].correlation_id == "parent-run-01"
    assert started[0].session_id == "sess-test"
    assert started[0].description == "test task"

    child_run_id = started[0].run_id
    child_events = [e for e in events if getattr(e, "run_id", None) == child_run_id]
    assert child_events[0].type == "subagent.started"
    assert child_events[-1].type == "subagent.finished"
    assert {getattr(e, "correlation_id", None) for e in child_events} == {"parent-run-01"}
    assert {getattr(e, "session_id", None) for e in child_events} == {"sess-test"}
    assert finished[0].run_id == child_run_id
    assert finished[0].status == "success"

    event_file = tmp_path / child_run_id / "events.jsonl"
    persisted = _read_events(event_file)
    assert persisted[0]["type"] == "subagent.started"
    assert persisted[-1]["type"] == "subagent.finished"
    assert [event["type"] for event in persisted].count("subagent.started") == 1
    assert [event["type"] for event in persisted].count("subagent.finished") == 1
    assert persisted[-1]["status"] == "success"
    assert {event["run_id"] for event in persisted} == {child_run_id}
    assert {event["correlation_id"] for event in persisted} == {"parent-run-01"}
    assert {event["session_id"] for event in persisted} == {"sess-test"}

    parent_persisted = _read_events(parent_event_file)
    assert {event["run_id"] for event in parent_persisted} == {"parent-run-01"}
    assert not any(event["type"].startswith("subagent.") for event in parent_persisted)


@pytest.mark.asyncio
async def test_foreground_exception_overrides_success_and_persists_failed_finished(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fail_after_success(
        _loop: AgentLoop,
        context: Any,
    ) -> None:
        context.mark_success()
        raise RuntimeError("failure after provisional success")

    monkeypatch.setattr(AgentLoop, "run", fail_after_success)
    tool, _, bus = _make_tool(tmp_path)
    events: list[Any] = []

    async def collect(event: Any) -> None:
        events.append(event)

    bus.subscribe(collect)

    with pytest.raises(RuntimeError, match="provisional success"):
        await asyncio.wait_for(
            tool.invoke(
                {
                    "description": "failing task",
                    "prompt": "fail after success",
                }
            ),
            timeout=2.0,
        )

    started = next(event for event in events if isinstance(event, SubagentStartedEvent))
    finished = next(event for event in events if isinstance(event, SubagentFinishedEvent))
    assert finished.run_id == started.run_id
    assert finished.status == "failed"
    persisted = _read_events(tmp_path / started.run_id / "events.jsonl")
    assert persisted[0]["type"] == "subagent.started"
    assert persisted[-1]["type"] == "subagent.finished"
    assert persisted[-1]["status"] == "failed"


@pytest.mark.asyncio
async def test_background_cancellation_persists_failed_finished(tmp_path: Path) -> None:
    entered = asyncio.Event()

    async def blocking_chat(*args: Any, **kwargs: Any) -> LlmResponse:
        entered.set()
        await asyncio.Event().wait()
        raise AssertionError("unreachable")

    provider = MagicMock()
    provider.chat = blocking_chat
    tool, registry, bus = _make_tool(tmp_path, provider)
    events: list[Any] = []

    async def collect(event: Any) -> None:
        events.append(event)

    bus.subscribe(collect)
    result = await asyncio.wait_for(
        tool.invoke(
            {
                "description": "cancel task",
                "prompt": "wait until cancelled",
                "run_in_background": True,
            }
        ),
        timeout=2.0,
    )
    run_id = _child_run_id(result.content)
    entry = registry.get(run_id)
    assert entry is not None
    task, context = entry
    await asyncio.wait_for(entered.wait(), timeout=2.0)

    await asyncio.wait_for(registry.cancel_all(), timeout=2.0)

    assert task.cancelled()
    assert context.status == "failed"
    assert context.reason == "cancelled"
    persisted = _read_events(tmp_path / run_id / "events.jsonl")
    assert persisted[0]["type"] == "subagent.started"
    assert persisted[-1]["type"] == "subagent.finished"
    assert persisted[-1]["status"] == "failed"
    live_child = [event for event in events if getattr(event, "run_id", None) == run_id]
    assert live_child[0].type == "subagent.started"
    assert live_child[-1].type == "subagent.finished"
