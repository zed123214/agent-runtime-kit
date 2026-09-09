"""Linux Worker regressions; written only, not run during M1 delivery."""

from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
from typing import Any

import pytest

from agent_runtime.sandbox_server.executor import Executor, Output
from agent_runtime.sandbox_server.files import Workspace
from agent_runtime.sandbox_server.journal import Journal, JournalError
from agent_runtime.sandbox_server.protocol import Identity, Operation


def operation(**changes: Any) -> Operation:
    fields = {
        "identity": Identity(sandbox_id="a" * 32, run_id="root", tool_call_id="call"),
        "pod_uid": "pod-uid",
        "generation": "generation",
        "operation": "exec",
        "command": "true",
    }
    fields.update(changes)
    return Operation.model_validate(fields)


@pytest.mark.parametrize(
    ("command", "counts", "reason", "signal_number"),
    [
        ("exit 137", (0, 0), "command_failed", None),
        ("kill -TERM $$", (0, 0), "signal", 15),
        ("kill -KILL $$", (0, 1), "oom_killed", 9),
        ("exit 0", (0, 1), "completed", None),
    ],
)
async def test_command_resource_reason_requires_kernel_evidence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    command: str,
    counts: tuple[int, int],
    reason: str,
    signal_number: int | None,
) -> None:
    real_spawn = asyncio.create_subprocess_exec

    async def spawn(*args: Any, **kwargs: Any) -> asyncio.subprocess.Process:
        kwargs["cwd"] = str(tmp_path)
        return await real_spawn(*args, **kwargs)

    monkeypatch.setattr(asyncio, "create_subprocess_exec", spawn)
    values = iter(counts)
    monkeypatch.setattr("agent_runtime.sandbox_server.executor.oom_kills", lambda: next(values))
    workspace = Workspace(str(tmp_path))
    try:
        result = await Executor(workspace).execute(operation(command=command))
        assert result["terminal_reason"] == reason
        assert result["signal"] == signal_number
        assert result["resource_observations"] == {
            "oom_kill_before": counts[0],
            "oom_kill_after": counts[1],
        }
    finally:
        workspace.close()


async def test_pending_and_completed_duplicates_share_one_dispatch() -> None:
    journal = Journal()
    started, release = asyncio.Event(), asyncio.Event()
    count = 0

    async def execute() -> dict[str, Any]:
        nonlocal count
        count += 1
        started.set()
        await release.wait()
        return {"content": "same-result", "is_error": False}

    op = operation()
    first = asyncio.create_task(journal.execute(op, execute))
    await started.wait()
    duplicate = asyncio.create_task(journal.execute(op, execute))
    await asyncio.sleep(0)
    assert count == 1
    release.set()
    result, repeated = await asyncio.gather(first, duplicate)
    assert result == repeated
    attempt = op.model_copy(update={"identity": op.identity.model_copy(update={"attempt": 2})})
    assert await journal.execute(attempt, execute) == result
    assert count == 1


async def test_conflict_and_capacity_never_reopen_side_effect_identity() -> None:
    journal = Journal(max_entries=1)
    count = 0

    async def dispatch() -> dict[str, Any]:
        nonlocal count
        count += 1
        return {"content": "ok"}

    op = operation()
    first = await journal.execute(op, dispatch)
    with pytest.raises(JournalError, match="conflict"):
        await journal.execute(operation(command="different"), dispatch)
    new = op.model_copy(update={"identity": op.identity.model_copy(update={"tool_call_id": "new"})})
    with pytest.raises(JournalError, match="capacity"):
        await journal.execute(new, dispatch)
    assert await journal.execute(op, dispatch) == first
    assert count == 1


async def test_byte_capacity_rejects_before_dispatch() -> None:
    journal = Journal(max_bytes=1)

    async def forbidden() -> dict[str, Any]:
        raise AssertionError("capacity was checked after dispatch")

    with pytest.raises(JournalError, match="capacity"):
        await journal.execute(operation(), forbidden)


async def test_cancel_before_dispatch_is_a_terminal_tombstone() -> None:
    journal = Journal()
    op = operation()
    await journal.cancel(op)

    async def forbidden() -> dict[str, Any]:
        raise AssertionError("cancelled request executed")

    assert json.loads(await journal.execute(op, forbidden))["error_type"] == "cancelled"


async def test_caller_disconnect_does_not_erase_pending_identity() -> None:
    journal = Journal()
    started, finish = asyncio.Event(), asyncio.Event()

    async def dispatch() -> dict[str, Any]:
        started.set()
        await finish.wait()
        return {"content": "committed"}

    op = operation()
    caller = asyncio.create_task(journal.execute(op, dispatch))
    await started.wait()
    caller.cancel()
    with pytest.raises(asyncio.CancelledError):
        await caller
    finish.set()
    assert json.loads(await journal.execute(op, dispatch))["content"] == "committed"


def test_output_is_bounded_while_collecting_and_after_utf8_replacement() -> None:
    output = Output()
    for _ in range(100):
        output.append(output.stdout, b"\xff" * 8192)
        output.append(output.stderr, b"\xe4\xb8\xad" * 3000)
    assert output.truncated
    for stream in (output.stdout, output.stderr, output.merged):
        assert len(stream) <= 65536
        assert len(output.text(stream).encode()) <= 65536


@pytest.mark.parametrize("api", ["read", "write", "list"])
def test_symlink_swap_at_open_cannot_escape_workspace(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    api: str,
) -> None:
    root, outside = tmp_path / "workspace", tmp_path / "outside"
    root.mkdir()
    outside.mkdir()
    (root / "pivot").mkdir()
    (root / "pivot" / "value").write_text("inside")
    (outside / "value").write_text("protected")
    workspace = Workspace(str(root))
    real_open = os.open
    swapped = False

    def racing_open(path: Any, flags: int, *args: Any, **kwargs: Any) -> int:
        nonlocal swapped
        if path == "pivot" and not swapped:
            swapped = True
            (root / "pivot").rename(root / "old")
            (root / "pivot").symlink_to(outside, target_is_directory=True)
        return real_open(path, flags, *args, **kwargs)

    monkeypatch.setattr(os, "open", racing_open)
    try:
        result = (
            workspace.read("pivot/value")
            if api == "read"
            else workspace.write("pivot/value", "attack")
            if api == "write"
            else workspace.list("pivot", 2)
        )
        assert result.is_error
        assert (outside / "value").read_text() == "protected"
        assert "protected" not in result.content
    finally:
        workspace.close()


def test_files_contract_and_no_follow_final_component(tmp_path: Path) -> None:
    root = tmp_path / "workspace"
    root.mkdir()
    workspace = Workspace(str(root))
    try:
        assert not workspace.write("a/b/value", "你好").is_error
        assert workspace.read("a/b/value").content == "你好"
        assert "a/" in workspace.list(".", 4).content
        (root / "link").symlink_to(root / "a/b/value")
        assert workspace.read("link").is_error
        assert workspace.write("link", "changed").is_error
        assert workspace.read("../outside").is_error
        assert workspace.read("/etc/passwd").is_error
        assert workspace.write("huge", "x" * (1024 * 1024 + 1)).is_error
    finally:
        workspace.close()


@pytest.mark.parametrize("cancel", [False, True])
async def test_timeout_or_cancel_kills_process_group_before_delayed_marker(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    cancel: bool,
) -> None:
    real_spawn = asyncio.create_subprocess_exec
    started = asyncio.Event()

    async def spawn(*args: Any, **kwargs: Any) -> asyncio.subprocess.Process:
        assert kwargs["cwd"] == "/workspace"
        assert set(kwargs["env"]) == {"PATH", "HOME", "TMPDIR", "LANG"}
        kwargs["cwd"] = str(tmp_path)  # Test directory, not a production configuration field.
        process = await real_spawn(*args, **kwargs)
        started.set()
        return process

    monkeypatch.setattr(asyncio, "create_subprocess_exec", spawn)
    workspace = Workspace(str(tmp_path))
    executor = Executor(workspace)
    try:
        request = operation(
            command="(sleep 0.4; printf leaked > marker) & wait", timeout_s=2.0 if cancel else 0.05
        )
        task = asyncio.create_task(executor.execute(request))
        await started.wait()
        if cancel:
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        else:
            assert (await task)["terminal_reason"] == "timeout"
        await asyncio.sleep(0.5)
        assert not (tmp_path / "marker").exists()
    finally:
        workspace.close()
