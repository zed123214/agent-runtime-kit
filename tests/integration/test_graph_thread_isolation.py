from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

pytest.importorskip("langgraph")

from pydantic import BaseModel

from agent_runtime.core.config import RuntimeConfig
from agent_runtime.core.engine.router import EngineRouter
from agent_runtime.core.events.bus import EventBus
from agent_runtime.core.graph.fake_provider import ScriptedProvider
from agent_runtime.core.graph.runtime import GraphRuntime, thread_config
from agent_runtime.core.llm.base import LLMProvider
from agent_runtime.core.llm.types import LlmResponse
from agent_runtime.core.runner import AgentRunner
from agent_runtime.core.session.manager import SessionManager
from agent_runtime.core.session.model import Session
from agent_runtime.core.session.store import SessionStore
from agent_runtime.core.task.manager import TaskManager
from agent_runtime.core.tools.base import BaseTool, ToolResult
from agent_runtime.core.tools.registry import ToolRegistry

pytestmark = pytest.mark.graph


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _graph_config() -> RuntimeConfig:
    config = RuntimeConfig()
    config.agent.engine = "graph"
    config.agent.max_steps = 4
    config.compaction.auto_threshold = 0.0
    return config


def _session(sid: str, *, mode: str = "chat") -> Session:
    return Session(
        id=sid,
        mode=mode,  # type: ignore[arg-type]
        status="active",
        title="",
        created_at=_now(),
        updated_at=_now(),
    )


class _StaticTool(BaseTool):
    name = "shared_tool"
    description = "Return one registry-specific offline value."
    input_schema: dict[str, object] = {"type": "object", "properties": {}}

    def __init__(self, output: str) -> None:
        self._output = output

    async def invoke(self, params: dict[str, object]) -> ToolResult:
        return ToolResult(content=self._output)


class _RegistryRunner(AgentRunner):
    def __init__(
        self,
        config: RuntimeConfig,
        *,
        registry: ToolRegistry,
        provider: LLMProvider,
        router: EngineRouter,
        events: list[BaseModel],
        runs_dir: Path,
    ) -> None:
        async def collect(event: BaseModel) -> None:
            events.append(event)

        super().__init__(
            config,
            provider=provider,
            engine_resolver=router,
            extra_handlers=[collect],
            runs_dir=runs_dir,
        )
        self._test_registry = registry

    def _build_registry(
        self,
        task_manager: TaskManager,
        **kwargs: object,
    ) -> ToolRegistry:
        del task_manager, kwargs
        return self._test_registry


def _registry(output: str) -> ToolRegistry:
    registry = ToolRegistry()
    registry.register(_StaticTool(output))
    return registry


def _message_dump(messages: list[dict[str, Any]]) -> str:
    return json.dumps(messages, ensure_ascii=False, sort_keys=True)


def _assert_run_metadata(events: list[BaseModel], *, run_id: str, session_id: str) -> None:
    run_events = [event for event in events if hasattr(event, "run_id")]
    assert run_events
    for event in run_events:
        assert event.run_id == run_id  # type: ignore[attr-defined]
        assert event.correlation_id == run_id  # type: ignore[attr-defined]
        assert event.session_id == session_id  # type: ignore[attr-defined]


async def test_shared_saver_concurrent_threads_isolate_state_tools_events_and_metadata(
    tmp_path: Path,
) -> None:
    runtime = GraphRuntime()
    router = EngineRouter(runtime)
    store = SessionStore(tmp_path / "sessions")
    session_a = _session("session-a")
    session_b = _session("session-b")
    for session, goal in ((session_a, "goal-a"), (session_b, "goal-b")):
        store.write_meta(session)
        store.append_message(session.id, "user", goal)

    provider_a = ScriptedProvider.tool(
        "shared_tool",
        tool_use_id="same-tool-use-id",
        final_text="final-a",
    )
    provider_b = ScriptedProvider.tool(
        "shared_tool",
        tool_use_id="same-tool-use-id",
        final_text="final-b",
    )
    events_a: list[BaseModel] = []
    events_b: list[BaseModel] = []
    runner_a = _RegistryRunner(
        _graph_config(),
        registry=_registry("registry-a-result"),
        provider=provider_a,
        router=router,
        events=events_a,
        runs_dir=tmp_path / "runs-a",
    )
    runner_b = _RegistryRunner(
        _graph_config(),
        registry=_registry("registry-b-result"),
        provider=provider_b,
        router=router,
        events=events_b,
        runs_dir=tmp_path / "runs-b",
    )

    outcome_a, outcome_b = await asyncio.gather(
        asyncio.wait_for(
            runner_a.run_and_capture(
                "goal-a",
                run_id="run-a",
                session=session_a,
                store=store,
            ),
            timeout=2.0,
        ),
        asyncio.wait_for(
            runner_b.run_and_capture(
                "goal-b",
                run_id="run-b",
                session=session_b,
                store=store,
            ),
            timeout=2.0,
        ),
    )

    assert outcome_a.status == outcome_b.status == "success"
    assert runtime.known_threads == frozenset({session_a.id, session_b.id})
    assert await runtime.saver.aget_tuple(thread_config(session_a.id)) is not None  # type: ignore[arg-type]
    assert await runtime.saver.aget_tuple(thread_config(session_b.id)) is not None  # type: ignore[arg-type]

    messages_a = store.read_messages(session_a.id)
    messages_b = store.read_messages(session_b.id)
    dump_a = _message_dump(messages_a)
    dump_b = _message_dump(messages_b)
    assert "registry-a-result" in dump_a
    assert "registry-b-result" not in dump_a
    assert "registry-b-result" in dump_b
    assert "registry-a-result" not in dump_b
    assert "final-a" in dump_a and "final-b" not in dump_a
    assert "final-b" in dump_b and "final-a" not in dump_b

    second_call_a = _message_dump(provider_a.calls[1]["messages"])
    second_call_b = _message_dump(provider_b.calls[1]["messages"])
    assert "registry-a-result" in second_call_a
    assert "registry-b-result" not in second_call_a
    assert "registry-b-result" in second_call_b
    assert "registry-a-result" not in second_call_b

    _assert_run_metadata(events_a, run_id="run-a", session_id=session_a.id)
    _assert_run_metadata(events_b, run_id="run-b", session_id=session_b.id)
    tool_events_a = [event for event in events_a if event.type == "tool.call_finished"]  # type: ignore[attr-defined]
    tool_events_b = [event for event in events_b if event.type == "tool.call_finished"]  # type: ignore[attr-defined]
    assert [event.tool_use_id for event in tool_events_a] == ["same-tool-use-id"]  # type: ignore[attr-defined]
    assert [event.tool_use_id for event in tool_events_b] == ["same-tool-use-id"]  # type: ignore[attr-defined]
    assert [event.tool_name for event in tool_events_a] == ["shared_tool"]  # type: ignore[attr-defined]
    assert [event.tool_name for event in tool_events_b] == ["shared_tool"]  # type: ignore[attr-defined]
    assert [event.output for event in tool_events_a] == ["registry-a-result"]  # type: ignore[attr-defined]
    assert [event.output for event in tool_events_b] == ["registry-b-result"]  # type: ignore[attr-defined]
    assert all(
        event.node_id in {"model", "kit_tools"}
        for event in events_a + events_b
        if event.type.startswith(("node.", "state.", "tool.", "llm.", "step."))  # type: ignore[attr-defined]
    )

    await router.close()


async def test_two_runtimes_do_not_share_an_identically_named_thread(tmp_path: Path) -> None:
    runtime_a = GraphRuntime()
    runtime_b = GraphRuntime()
    router_a = EngineRouter(runtime_a)
    router_b = EngineRouter(runtime_b)
    store_a = SessionStore(tmp_path / "store-a")
    store_b = SessionStore(tmp_path / "store-b")
    session_a = _session("same-thread")
    session_b = _session("same-thread")
    store_a.write_meta(session_a)
    store_b.write_meta(session_b)
    store_a.append_message(session_a.id, "user", "only-a")
    store_b.append_message(session_b.id, "user", "only-b")
    provider_a = ScriptedProvider.direct("answer-a")
    provider_b = ScriptedProvider.direct("answer-b")

    outcome_a, outcome_b = await asyncio.gather(
        asyncio.wait_for(
            AgentRunner(
                _graph_config(),
                provider=provider_a,
                engine_resolver=router_a,
                runs_dir=tmp_path / "runs-a",
            ).run_and_capture(
                "only-a",
                run_id="same-run-a",
                session=session_a,
                store=store_a,
            ),
            timeout=2.0,
        ),
        asyncio.wait_for(
            AgentRunner(
                _graph_config(),
                provider=provider_b,
                engine_resolver=router_b,
                runs_dir=tmp_path / "runs-b",
            ).run_and_capture(
                "only-b",
                run_id="same-run-b",
                session=session_b,
                store=store_b,
            ),
            timeout=2.0,
        ),
    )

    assert outcome_a.result == "answer-a"
    assert outcome_b.result == "answer-b"
    assert runtime_a.saver is not runtime_b.saver
    assert runtime_a.known_threads == runtime_b.known_threads == frozenset({"same-thread"})
    assert provider_a.calls[0]["messages"] == [{"role": "user", "content": "only-a"}]
    assert provider_b.calls[0]["messages"] == [{"role": "user", "content": "only-b"}]
    assert "answer-b" not in _message_dump(store_a.read_messages("same-thread"))
    assert "answer-a" not in _message_dump(store_b.read_messages("same-thread"))

    await router_a.close()
    await router_b.close()


async def test_one_shot_and_session_close_delete_threads_without_checkpoint_files(
    tmp_path: Path,
) -> None:
    runtime = GraphRuntime()
    router = EngineRouter(runtime)
    store = SessionStore(tmp_path / "sessions")
    provider = ScriptedProvider(
        [
            LlmResponse(stop_reason="end_turn", text="one-shot answer"),
            LlmResponse(stop_reason="end_turn", text="chat answer"),
        ]
    )

    def runner_factory() -> AgentRunner:
        return AgentRunner(
            _graph_config(),
            provider=provider,
            engine_resolver=router,
            runs_dir=tmp_path / "runs",
        )

    manager = SessionManager(
        store,
        runner_factory,
        EventBus(),
        on_session_closed=router.delete_thread,
    )
    one_shot = await manager.create("one_shot")
    one_shot_run = await asyncio.wait_for(
        manager.send_message(one_shot.id, "one shot", run_id="run-one-shot"),
        timeout=2.0,
    )

    assert one_shot_run == "run-one-shot"
    assert "run-one-shot" not in runtime.known_threads
    assert await runtime.saver.aget_tuple(thread_config("run-one-shot")) is None  # type: ignore[arg-type]

    chat = await manager.create("chat")
    await asyncio.wait_for(
        manager.send_message(chat.id, "chat", run_id="run-chat"),
        timeout=2.0,
    )
    assert chat.id in runtime.known_threads
    assert await runtime.saver.aget_tuple(thread_config(chat.id)) is not None  # type: ignore[arg-type]

    await asyncio.wait_for(manager.close(chat.id), timeout=2.0)

    assert runtime.known_threads == frozenset()
    assert await runtime.saver.aget_tuple(thread_config(chat.id)) is None  # type: ignore[arg-type]
    checkpoint_files = [
        path
        for path in tmp_path.rglob("*")
        if path.is_file()
        and (
            "checkpoint" in path.name.lower()
            or path.suffix.lower() in {".db", ".sqlite", ".sqlite3"}
        )
    ]
    assert checkpoint_files == []

    await router.close()
