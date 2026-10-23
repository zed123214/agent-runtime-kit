from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import pytest

from agent_runtime.core.permissions.manager import PermissionManager
from agent_runtime.core.permissions.policy import PermissionDecision, ToolPolicy


async def pending(manager: PermissionManager, session: str, tid: str, command: str):
    requested = asyncio.Event()

    async def emit(event: dict[str, Any]) -> None:
        requested.set()

    task = asyncio.create_task(
        manager.check_and_wait(tid, "bash", {"command": command}, session, emit, run_id=tid)
    )
    await asyncio.wait_for(requested.wait(), 1)
    return task


async def test_session_grant_covers_next_calls_and_pending_siblings_only_in_owner_session(
    tmp_path: Path,
) -> None:
    policy_file = tmp_path / "policy.toml"
    manager = PermissionManager(policy_file=policy_file)
    first = await pending(manager, "a", "one", "cd /tmp && pwd")
    sibling = await pending(manager, "a", "two", "ls /tmp")
    other = await pending(manager, "b", "three", "ls /tmp")
    assert manager.respond(
        "one", "allow_session", authorized_session_ids={"a"}, session_id="a", run_id="one"
    )
    assert await first == (True, "allow_session")
    assert await sibling == (True, "session_allow")
    assert not other.done()
    assert manager.evaluate_durable("bash", {"command": "cd /tmp && ls"}, "a") == (
        True,
        "session_allow",
    )
    assert manager.evaluate_durable("write_file", {}, "a") == (None, "ask")
    assert manager.session_permissions("a").allowed_tools == ["bash"]
    assert not policy_file.exists()
    manager.cancel_session("a")
    assert manager.session_permissions("a").allowed_tools == []
    manager.cancel_session("b")
    assert await other == (False, "deny_once")


async def test_revoke_and_mode_changes_recheck_pending_approvals() -> None:
    manager = PermissionManager()
    task = await pending(manager, "a", "one", "pwd")
    state = manager.configure_session("a", mode="auto", expected_revision=0)
    assert await task == (True, "session_auto")
    assert manager.evaluate_durable("bash", {"command": "cd /tmp"}, "b") == (None, "ask")
    state = manager.configure_session("a", mode="ask", expected_revision=state.revision)
    task = await pending(manager, "a", "two", "pwd")
    state = manager.configure_session("a", mode="read_only", expected_revision=state.revision)
    assert await task == (False, "read_only")
    assert not manager.respond("two", "allow_session", authorized_session_ids={"a"})
    assert manager.evaluate_durable("read_file", {"path": "README.md"}, "a")[0] is True
    for tool in ["bash", "write_file", "note_save", "spawn_agent", "mcp_custom"]:
        assert manager.evaluate_durable(tool, {}, "a") == (False, "read_only")
    with pytest.raises(ValueError, match="changed"):
        manager.configure_session("a", mode="auto", expected_revision=0)
    assert manager.session_permissions("a").mode == "read_only"


async def test_revoking_tool_grant_restores_approval_and_cannot_resurrect_late() -> None:
    manager = PermissionManager()
    task = await pending(manager, "a", "one", "pwd")
    assert manager.respond("one", "allow_session", authorized_session_ids={"a"})
    revision = manager.session_permissions("a").revision
    manager.configure_session("a", mode="ask", revoke_tool="bash", expected_revision=revision)
    await task
    assert manager.evaluate_durable("bash", {"command": "pwd"}, "a") == (None, "ask")


async def test_unowned_expired_and_unknown_decisions_do_not_install_grants() -> None:
    manager = PermissionManager(timeout_s=0.02)
    task = await pending(manager, "a", "one", "pwd")
    assert not manager.respond("one", "allow_session", authorized_session_ids={"b"})
    assert not manager.respond("one", "session_auto", authorized_session_ids={"a"})
    assert await task == (False, "timeout")
    assert not manager.respond("one", "allow_session", authorized_session_ids={"a"})
    assert manager.session_permissions("a").allowed_tools == []


def test_auto_mode_preserves_explicit_denials() -> None:
    manager = PermissionManager(
        {
            "bash": ToolPolicy(PermissionDecision.ASK, deny_patterns=[r"\brm\b"]),
            "write_file": ToolPolicy(PermissionDecision.DENY),
        }
    )
    manager.apply_durable_response("always_deny", "a", "mcp_remote")
    manager.configure_session("a", mode="auto", expected_revision=0)
    for tool, params in [
        ("bash", {"command": "rm /tmp/file"}),
        ("write_file", {}),
        ("mcp_remote", {}),
    ]:
        assert manager.evaluate_durable(tool, params, "a")[0] is False
    assert manager.evaluate_durable("bash", {"command": "cd /tmp && pwd"}, "a")[0] is True
