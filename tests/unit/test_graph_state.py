from __future__ import annotations

import pytest

from agent_runtime.core.context import ExecutionContext
from agent_runtime.core.graph.state import append_messages, new_run_input

pytestmark = pytest.mark.graph


def test_new_run_input_resets_run_scoped_fields_and_copies_history() -> None:
    history = [{"role": "user", "content": [{"type": "text", "text": "hello"}]}]
    context = ExecutionContext(run_id="run-1", goal="goal", max_steps=3)

    state = new_run_input(context, history)
    history[0]["content"][0]["text"] = "mutated"  # type: ignore[index]

    assert state["run_id"] == "run-1"
    assert state["messages"][0]["content"][0]["text"] == "hello"  # type: ignore[index]
    assert state["step"] == 0
    assert state["status"] == "running"
    assert state["pending_tool_calls"] == []
    assert state["tool_call_count"] == 0
    assert state["trace_events"] == []


def test_append_messages_preserves_real_duplicates() -> None:
    repeated = {"role": "user", "content": "same"}

    combined = append_messages([repeated], [repeated])

    assert combined == [repeated, repeated]
    assert len(combined) == 2
