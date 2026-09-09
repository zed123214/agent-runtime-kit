from __future__ import annotations

import asyncio
import json
from collections import Counter
from pathlib import Path
from typing import Any, Literal

import pytest

from agent_runtime.core.config import RuntimeConfig
from agent_runtime.core.context import ExecutionContext
from agent_runtime.core.events.bus import EventBus
from agent_runtime.core.llm.types import LlmResponse
from agent_runtime.core.runner import AgentRunner
from agent_runtime.core.session.model import Session
from agent_runtime.core.session.store import SessionStore
from agent_runtime.core.tools.registry import ToolRegistry


class _BlockingProvider:
    def __init__(self) -> None:
        self.entered = asyncio.Event()

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
        self.entered.set()
        await asyncio.Event().wait()
        raise AssertionError("unreachable")


class _CountingSessionStore(SessionStore):
    def __init__(self, root: Path) -> None:
        super().__init__(root)
        self.append_calls = 0

    def append_messages(
        self,
        sid: str,
        messages: list[dict[str, Any]],
        run_id: str,
    ) -> None:
        self.append_calls += 1
        super().append_messages(sid, messages, run_id)


def _session() -> Session:
    return Session(
        id="session-terminal-repair",
        mode="chat",
        status="active",
        title="terminal repair",
        created_at="2026-08-24T00:00:00+00:00",
        updated_at="2026-08-24T00:00:00+00:00",
    )


def _event_rows(store: SessionStore, session: Session, run_id: str) -> list[dict[str, Any]]:
    path = store.runs_dir(session.id) / run_id / "events.jsonl"
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]


@pytest.mark.parametrize(
    "engine",
    [
        pytest.param("loop", id="loop"),
        pytest.param("graph", id="graph", marks=pytest.mark.graph),
    ],
)
async def test_repeated_cancel_finishes_children_root_event_writer_and_session_increment(
    tmp_path: Path,
    engine: Literal["loop", "graph"],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = RuntimeConfig()
    config.agent.engine = engine
    config.compaction.auto_threshold = 0
    config.graph.wall_time_s = 30.0
    provider = _BlockingProvider()
    store = _CountingSessionStore(tmp_path / "sessions")
    session = _session()
    store.write_meta(session)
    runner = AgentRunner(config, provider=provider, runs_dir=tmp_path / "runs")

    child_started = asyncio.Event()
    child_cleanup_entered = asyncio.Event()
    release_child_cleanup = asyncio.Event()

    async def delayed_background_child() -> None:
        child_started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            child_cleanup_entered.set()
            while not release_child_cleanup.is_set():
                try:
                    await release_child_cleanup.wait()
                except asyncio.CancelledError:
                    continue
            raise

    child_task = asyncio.create_task(delayed_background_child())
    child_context = ExecutionContext(
        run_id="child-terminal-repair",
        goal="child",
        max_steps=1,
    )
    build_registry = runner._build_registry

    def registry_with_child(*args: Any, **kwargs: Any) -> ToolRegistry:
        registry = build_registry(*args, **kwargs)
        kwargs["task_registry"].register("child-terminal-repair", child_task, child_context)
        return registry

    monkeypatch.setattr(runner, "_build_registry", registry_with_child)
    await asyncio.wait_for(child_started.wait(), timeout=1.0)

    run_id = f"run-terminal-repair-{engine}"
    root_task = asyncio.create_task(
        runner.run_and_capture(
            "goal",
            run_id=run_id,
            session=session,
            store=store,
        )
    )
    root_done = asyncio.Event()
    root_task.add_done_callback(lambda _task: root_done.set())
    root_finished_during_child_cleanup = False
    root_cancelled = False
    try:
        await asyncio.wait_for(provider.entered.wait(), timeout=5.0)

        root_task.cancel()
        await asyncio.wait_for(child_cleanup_entered.wait(), timeout=1.0)
        root_task.cancel()

        # Two queued callbacks form a deterministic event-loop turn barrier:
        # the root has processed its second cancellation before this check.
        second_cancel_processed = asyncio.Event()
        loop = asyncio.get_running_loop()
        loop.call_soon(lambda: loop.call_soon(second_cancel_processed.set))
        await asyncio.wait_for(second_cancel_processed.wait(), timeout=1.0)
        root_finished_during_child_cleanup = root_done.is_set()

        release_child_cleanup.set()
        try:
            await asyncio.wait_for(root_task, timeout=2.0)
        except asyncio.CancelledError:
            root_cancelled = True
    finally:
        release_child_cleanup.set()
        if not root_task.done():
            root_task.cancel()
        if not child_task.done():
            child_task.cancel()
        await asyncio.gather(root_task, child_task, return_exceptions=True)

    assert root_cancelled
    assert not root_finished_during_child_cleanup
    assert child_task.done()
    assert store.append_calls == 1

    rows = _event_rows(store, session, run_id)
    finished = [row for row in rows if row["type"] == "run.finished"]
    assert len(finished) == 1
    assert finished[0]["status"] == "failed"
    assert finished[0]["reason"] == "cancelled"
    assert rows[-1]["type"] == "run.finished"

    started_nodes = Counter(row["node_id"] for row in rows if row["type"] == "node.started")
    finished_nodes = Counter(row["node_id"] for row in rows if row["type"] == "node.finished")
    assert started_nodes == finished_nodes
