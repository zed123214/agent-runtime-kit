from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

pytest.importorskip("langgraph")

from agent_runtime.core.config import RuntimeConfig
from agent_runtime.core.engine.router import EngineRouter
from agent_runtime.core.events.bus import EventBus
from agent_runtime.core.graph.fake_provider import ScriptedProvider
from agent_runtime.core.graph.runtime import GraphRuntime, thread_config
from agent_runtime.core.llm.types import LlmResponse
from agent_runtime.core.runner import AgentRunner
from agent_runtime.core.session.manager import SessionManager
from agent_runtime.core.session.store import SessionStore

pytestmark = pytest.mark.graph


def _graph_config() -> RuntimeConfig:
    config = RuntimeConfig()
    config.agent.engine = "graph"
    config.agent.max_steps = 4
    config.compaction.auto_threshold = 0.0
    return config


def _manager(
    root: Path,
    provider: ScriptedProvider,
    router: EngineRouter,
) -> tuple[SessionManager, SessionStore, list[AgentRunner]]:
    store = SessionStore(root / "sessions")
    runners: list[AgentRunner] = []

    def runner_factory() -> AgentRunner:
        runner = AgentRunner(
            _graph_config(),
            provider=provider,
            engine_resolver=router,
            runs_dir=root / "runs",
        )
        runners.append(runner)
        return runner

    manager = SessionManager(
        store,
        runner_factory,
        EventBus(),
        on_session_closed=router.delete_thread,
    )
    return manager, store, runners


async def test_session_manager_two_turns_reuse_checkpoint_without_store_duplicates(
    tmp_path: Path,
) -> None:
    runtime = GraphRuntime()
    router = EngineRouter(runtime)
    provider = ScriptedProvider(
        [
            LlmResponse(stop_reason="end_turn", text="assistant1"),
            LlmResponse(stop_reason="end_turn", text="assistant2"),
        ]
    )
    manager, store, runners = _manager(tmp_path, provider, router)
    session = await manager.create("chat")

    await asyncio.wait_for(
        manager.send_message(session.id, "user1", run_id="run-turn-1"),
        timeout=2.0,
    )
    await asyncio.wait_for(
        manager.send_message(session.id, "user2", run_id="run-turn-2"),
        timeout=2.0,
    )

    first_assistant = {
        "role": "assistant",
        "content": [{"type": "text", "text": "assistant1"}],
    }
    expected_before_second_call = [
        {"role": "user", "content": "user1"},
        first_assistant,
        {"role": "user", "content": "user2"},
    ]
    assert provider.calls[1]["messages"] == expected_before_second_call

    stored = store.read_messages(session.id)
    assert stored == [
        *expected_before_second_call,
        {
            "role": "assistant",
            "content": [{"type": "text", "text": "assistant2"}],
        },
    ]
    assert stored.count({"role": "user", "content": "user1"}) == 1
    assert stored.count(first_assistant) == 1
    assert stored.count({"role": "user", "content": "user2"}) == 1
    assert len(runners) == 2
    assert runners[0] is not runners[1]
    assert runtime.known_threads == frozenset({session.id})

    await router.close()


async def test_cold_saver_bootstraps_the_full_persisted_history(tmp_path: Path) -> None:
    runtime = GraphRuntime()
    router = EngineRouter(runtime)
    provider = ScriptedProvider.direct("fresh answer")
    manager, store, _runners = _manager(tmp_path, provider, router)
    session = await manager.create("chat")
    historical = [
        {"role": "user", "content": "historical user"},
        {
            "role": "assistant",
            "content": [{"type": "text", "text": "historical assistant"}],
        },
    ]
    for message in historical:
        store.append_message(session.id, message["role"], message["content"])

    assert await runtime.saver.aget_tuple(thread_config(session.id)) is None  # type: ignore[arg-type]

    await asyncio.wait_for(
        manager.send_message(session.id, "new user", run_id="run-cold-bootstrap"),
        timeout=2.0,
    )

    assert provider.calls[0]["messages"] == [
        *historical,
        {"role": "user", "content": "new user"},
    ]
    assert await runtime.saver.aget_tuple(thread_config(session.id)) is not None  # type: ignore[arg-type]
    assert store.read_messages(session.id) == [
        *historical,
        {"role": "user", "content": "new user"},
        {
            "role": "assistant",
            "content": [{"type": "text", "text": "fresh answer"}],
        },
    ]

    await router.close()


async def test_manual_compaction_resets_checkpoint_and_reseeds_next_turn(
    tmp_path: Path,
) -> None:
    runtime = GraphRuntime()
    router = EngineRouter(runtime)
    provider = ScriptedProvider(
        [
            LlmResponse(stop_reason="end_turn", text="assistant1"),
            LlmResponse(stop_reason="end_turn", text="assistant2"),
        ]
    )
    manager, store, runners = _manager(tmp_path, provider, router)
    session = await manager.create("chat")

    await asyncio.wait_for(
        manager.send_message(session.id, "user1", run_id="run-before-compact"),
        timeout=2.0,
    )
    compacted = [
        {"role": "user", "content": "summary of turn one"},
        {"role": "assistant", "content": "summary acknowledged"},
    ]
    store.write_compacted(session.id, compacted)

    await asyncio.wait_for(
        manager.send_message(session.id, "user2", run_id="run-after-compact"),
        timeout=2.0,
    )

    reseeded = [*compacted, {"role": "user", "content": "user2"}]
    assert provider.calls[1]["messages"] == reseeded
    assert store.read_messages(session.id) == [
        *reseeded,
        {
            "role": "assistant",
            "content": [{"type": "text", "text": "assistant2"}],
        },
    ]
    assert all(message["content"] != "user1" for message in store.read_messages(session.id))
    assert len(runners) == 2
    assert await runtime.saver.aget_tuple(thread_config(session.id)) is not None  # type: ignore[arg-type]
    assert runtime.known_threads == frozenset({session.id})

    await router.close()


async def test_graph_auto_compaction_returns_typed_error_without_affecting_loop(
    tmp_path: Path,
) -> None:
    graph_config = _graph_config()
    graph_config.compaction.auto_threshold = 0.5
    graph_provider = ScriptedProvider.direct("must not be called")
    graph_outcome = await asyncio.wait_for(
        AgentRunner(
            graph_config,
            provider=graph_provider,
            runs_dir=tmp_path / "graph-runs",
        ).run_and_capture("graph goal", run_id="run-invalid-auto-compact"),
        timeout=2.0,
    )

    assert graph_outcome.status == "failed"
    assert graph_outcome.reason == "engine_configuration_error"
    assert graph_outcome.error is not None
    assert graph_outcome.error.code == "engine_configuration_error"
    assert graph_outcome.error.engine == "graph"
    assert graph_provider.call_count == 0

    loop_config = RuntimeConfig()
    loop_config.agent.engine = "loop"
    loop_config.compaction.auto_threshold = 0.5
    loop_provider = ScriptedProvider.direct("loop answer")
    loop_outcome = await asyncio.wait_for(
        AgentRunner(
            loop_config,
            provider=loop_provider,
            runs_dir=tmp_path / "loop-runs",
        ).run_and_capture("loop goal", run_id="run-loop-auto-compact"),
        timeout=2.0,
    )

    assert loop_outcome.status == "success"
    assert loop_outcome.result == "loop answer"
    assert loop_provider.call_count == 1
