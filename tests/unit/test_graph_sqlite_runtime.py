from __future__ import annotations

from pathlib import Path
from typing import TypedDict

import pytest

pytest.importorskip("langgraph")

from langgraph.graph import END, START, StateGraph

from agent_runtime.core.graph.checkpoint import CheckpointRuntime
from agent_runtime.core.graph.recovery import RecoveryStore

pytestmark = [pytest.mark.graph, pytest.mark.recovery]


class _State(TypedDict):
    value: int


def _graph(saver: object) -> object:
    builder = StateGraph(_State)
    builder.add_node("increment", lambda state: {"value": state["value"] + 1})
    builder.add_edge(START, "increment")
    builder.add_edge("increment", END)
    return builder.compile(checkpointer=saver)  # type: ignore[arg-type]


async def _save(runtime: CheckpointRuntime, thread_id: str, value: int) -> None:
    graph = _graph(runtime.saver)
    await graph.ainvoke(  # type: ignore[attr-defined]
        {"value": value},
        {"configurable": {"thread_id": thread_id}},
    )


async def _checkpoint_value(runtime: CheckpointRuntime, thread_id: str) -> int | None:
    checkpoint = await runtime.saver.aget_tuple({"configurable": {"thread_id": thread_id}})
    if checkpoint is None:
        return None
    return int(checkpoint.checkpoint["channel_values"]["value"])


async def test_sqlite_saver_close_reopen_delete_and_data_root_isolation(
    tmp_path: Path,
) -> None:
    first_path = tmp_path / "first" / "checkpoints.sqlite3"
    second_path = tmp_path / "second" / "checkpoints.sqlite3"

    first = await CheckpointRuntime.open("sqlite", sqlite_path=first_path)
    await _save(first, "target", 1)
    await _save(first, "survivor", 10)
    assert await _checkpoint_value(first, "target") == 2
    await first.close()
    await first.close()
    with pytest.raises(RuntimeError, match="closed"):
        _ = first.saver

    recovery = RecoveryStore(first_path)
    await recovery.create_run(
        session_id="sess-1",
        run_id="run-1",
        thread_id="survivor",
        engine="graph",
        status="suspended",
    )
    reopened = await CheckpointRuntime.open("sqlite", sqlite_path=first_path)
    assert await _checkpoint_value(reopened, "target") == 2
    assert await _checkpoint_value(reopened, "survivor") == 11
    assert await recovery.get_run("sess-1", "run-1") is not None
    await reopened.delete_thread("target")
    assert await _checkpoint_value(reopened, "target") is None
    assert await _checkpoint_value(reopened, "survivor") == 11

    isolated = await CheckpointRuntime.open("sqlite", sqlite_path=second_path)
    await _save(isolated, "survivor", 100)
    assert await _checkpoint_value(isolated, "survivor") == 101
    assert await _checkpoint_value(reopened, "survivor") == 11

    await isolated.close()
    await reopened.close()
