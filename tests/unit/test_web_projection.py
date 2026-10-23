import pytest

from agent_runtime.web.projection import Projection


def test_child_sequence_does_not_replace_parent_and_retry_keeps_failure() -> None:
    view = Projection("web", "instance", {})
    view.event({"type": "run.started", "run_id": "root", "event_seq": 1, "goal": "task"})
    view.event(
        {
            "type": "subagent.started",
            "run_id": "child",
            "parent_run_id": "root",
            "correlation_id": "root",
            "event_seq": 1,
        }
    )
    view.event({"type": "run.started", "run_id": "child", "correlation_id": "root", "event_seq": 2})
    assert view.data["runs"]["child"]["parent_run_id"] == "root"
    view.event(
        {
            "type": "llm.token",
            "run_id": "child",
            "correlation_id": "root",
            "event_seq": 3,
            "token": "private child output",
        }
    )
    assert view.data["messages"] == {}
    assert view.data["runs"]["child"]["output"] == "private child output"
    for sequence, kind in enumerate(
        ["tool.call_started", "tool.call_failed", "tool.call_finished"], 4
    ):
        view.event(
            {
                "type": kind,
                "run_id": "child",
                "correlation_id": "root",
                "event_seq": sequence,
                "tool_use_id": "same",
                "tool_name": "bash",
                "attempt": 1,
                "error_message": "retryable",
                "output": "ok",
            }
        )
    tool = view.data["tools"]["child:same"]
    assert tool["status"] == "success"
    assert tool["attempts"] == [{"attempt": 1, "error": "retryable"}]
    assert view.data["current_run_id"] == "root"


def test_incomplete_approval_params_cannot_be_allowed() -> None:
    view = Projection("web", "instance", {})
    view.event(
        {
            "type": "permission.requested",
            "run_id": "run",
            "tool_use_id": "tool",
            "params": {"command": "x" * 20000},
        }
    )
    approval = next(iter(view.data["approvals"].values()))
    assert approval["can_allow"] is False
    assert len(approval["params"].encode()) <= 16384


def test_control_summary_capacity_is_bounded() -> None:
    view = Projection("web", "instance", {})
    for index in range(1000):
        view.event(
            {"type": "tool.call_finished", "run_id": "r", "tool_use_id": str(index), "output": "x"}
        )
        view.drain()
    assert len(view.data["tools"]) == 200
    with pytest.raises(OverflowError):
        view.event({"type": "tool.call_started", "run_id": "r", "tool_use_id": "over"})


def test_oversized_history_never_leaves_a_frozen_report() -> None:
    view = Projection("web", "instance", {})
    view.meta(state="syncing", current_run_id="run")
    before = view.snapshot()
    history = [{"role": "assistant", "content": "x" * 65536} for _ in range(100)]
    with pytest.raises(OverflowError):
        view.synchronize(history, {"run_id": "run", "commit_state": "committed"})
    assert view.snapshot() == before
    assert "run" not in view.data["reports"]
