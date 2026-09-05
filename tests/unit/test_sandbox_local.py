"""M0 Local contract cases, including POSIX process cleanup; added but not executed."""

from __future__ import annotations

import asyncio
import os
import shlex
import sys
from pathlib import Path

import pytest

from agent_runtime.core.sandbox import (
    ExecRequest,
    ListRequest,
    LocalSandboxBackend,
    LocalSandboxRuntime,
    ReadRequest,
    SandboxCallContext,
    SandboxClosedError,
    SandboxKey,
    SandboxManager,
    SandboxSpec,
    SandboxStatus,
    WriteRequest,
)


def context(runtime: LocalSandboxRuntime) -> SandboxCallContext:
    return SandboxCallContext(runtime.key, "root", "call")


async def test_local_uses_call_time_cwd_and_preserves_absolute_paths(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    runtime = LocalSandboxRuntime()
    first = tmp_path / "first"
    second = tmp_path / "second"
    first.mkdir()
    second.mkdir()
    monkeypatch.chdir(first)
    await runtime.write_text(WriteRequest(context(runtime), "nested/out.txt", "first"))
    monkeypatch.chdir(second)
    await runtime.write_text(WriteRequest(context(runtime), "out.txt", "second"))
    result = await runtime.read_text(ReadRequest(context(runtime), "out.txt"))
    absolute = await runtime.read_text(ReadRequest(context(runtime), str(first / "nested/out.txt")))
    assert result.content == "second"
    assert absolute.content == "first"
    await runtime.close()
    assert (first / "nested/out.txt").read_text() == "first"
    assert (second / "out.txt").read_text() == "second"


async def test_local_read_replaces_invalid_bytes_and_reports_truncation(tmp_path: Path) -> None:
    runtime = LocalSandboxRuntime()
    target = tmp_path / "data"
    target.write_bytes(b"\xff" + b"x" * (512 * 1024))
    result = await runtime.read_text(ReadRequest(context(runtime), str(target)))
    assert result.content.startswith("\ufffd")
    assert result.content.endswith("\n[truncated]")
    assert result.truncated
    await runtime.close()


async def test_local_write_uses_utf8_byte_limit_and_leaves_existing_file(tmp_path: Path) -> None:
    runtime = LocalSandboxRuntime()
    target = tmp_path / "marker"
    target.write_text("keep")
    result = await runtime.write_text(
        WriteRequest(context(runtime), str(target), "界" * (1024 * 1024 // 3 + 1))
    )
    assert result.is_error
    assert result.error_type == "runtime_error"
    assert "limit 1 MB" in result.content
    assert target.read_text() == "keep"
    await runtime.close()


async def test_local_file_operations_reject_parent_components(tmp_path: Path) -> None:
    runtime = LocalSandboxRuntime()
    with pytest.raises(PermissionError, match="path traversal not allowed"):
        await runtime.read_text(ReadRequest(context(runtime), "../secret"))
    with pytest.raises(PermissionError, match="path traversal not allowed"):
        await runtime.write_text(WriteRequest(context(runtime), "../secret", "x"))
    with pytest.raises(PermissionError, match="path traversal not allowed"):
        await runtime.list_dir(ListRequest(context(runtime), "../"))
    with pytest.raises(FileNotFoundError):
        await runtime.read_text(ReadRequest(context(runtime), str(tmp_path / "missing")))
    await runtime.close()


async def test_local_tree_keeps_hidden_entries_sorting_depth_and_limit(tmp_path: Path) -> None:
    runtime = LocalSandboxRuntime()
    child = tmp_path / "z-dir"
    child.mkdir()
    (child / "grandchild").write_text("x")
    (tmp_path / ".hidden").write_text("x")
    (tmp_path / "a-file").write_text("x")
    result = await runtime.list_dir(ListRequest(context(runtime), str(tmp_path), max_depth=1))
    assert result.content.splitlines()[1:] == [
        "├── z-dir/",
        "├── .hidden",
        "└── a-file",
    ]
    for index in range(205):
        (tmp_path / f"entry-{index:03}").write_text("x")
    truncated = await runtime.list_dir(ListRequest(context(runtime), str(tmp_path), max_depth=1))
    assert truncated.truncated
    assert len(truncated.content.splitlines()) == 202  # root + 200 entries + marker
    await runtime.close()


async def test_local_keys_only_separate_state_and_release_never_deletes_user_files(
    tmp_path: Path,
) -> None:
    manager = SandboxManager()
    first = SandboxKey("session", "first")
    second = SandboxKey("session", "second")
    target = str(tmp_path / "user-marker")
    await manager.runtime_for(first).write_text(
        WriteRequest(SandboxCallContext(first, "root-a", "write"), target, "shared host")
    )
    observed = await manager.runtime_for(second).read_text(
        ReadRequest(SandboxCallContext(second, "root-b", "read"), target)
    )
    assert observed.content == "shared host"  # Local explicitly offers no filesystem isolation.
    await manager.release(first)
    assert manager.status(second) == SandboxStatus.READY
    assert Path(target).read_text() == "shared host"
    await manager.close()
    assert Path(target).read_text() == "shared host"


async def test_local_backend_is_idempotent_and_does_not_invent_pod_metadata() -> None:
    backend = LocalSandboxBackend()
    key = SandboxKey("direct_run", "root")
    handle = await backend.ensure(key, SandboxSpec())
    assert await backend.ensure(key, SandboxSpec()) is handle
    assert handle.namespace is handle.pod_uid is handle.endpoint is None
    assert await backend.status(handle) == SandboxStatus.READY
    await backend.destroy(handle, "done")
    await backend.destroy(handle, "again")
    assert await backend.status(handle) == SandboxStatus.TERMINATED
    assert (await backend.reconcile()).destroyed == 0


@pytest.mark.skipif(os.name != "posix", reason="Local process semantics target Linux/macOS")
async def test_local_write_exec_read_list_and_existing_bash_presentation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    runtime = LocalSandboxRuntime()
    monkeypatch.chdir(tmp_path)
    await runtime.write_text(WriteRequest(context(runtime), "input.txt", "hello"))
    result = await runtime.exec(ExecRequest(context(runtime), "cat input.txt > output.txt"))
    assert result.content == "[no output]"
    assert result.exit_code == 0
    assert result.terminal_reason == "completed"
    assert result.stdout is result.stderr is None
    assert (await runtime.read_text(ReadRequest(context(runtime), "output.txt"))).content == "hello"
    assert "output.txt" in (await runtime.list_dir(ListRequest(context(runtime)))).content
    error = await runtime.exec(ExecRequest(context(runtime), "printf err >&2; exit 2"))
    assert error.content == "[exit 2]\nerr"
    assert error.output == "err"
    assert error.exit_code == 2
    assert error.error_type == "runtime_error"
    await runtime.close()


@pytest.mark.skipif(os.name != "posix", reason="POSIX process group cleanup")
async def test_local_timeout_kills_command_descendants_before_they_write(tmp_path: Path) -> None:
    runtime = LocalSandboxRuntime()
    marker = tmp_path / "late-marker"
    inner = f"sleep 2; printf leaked > {shlex.quote(str(marker))}"
    command = f"sh -c {shlex.quote(inner)} & wait"
    result = await runtime.exec(ExecRequest(context(runtime), command, timeout_s=1))
    assert result.content == "[timeout after 1s]"
    assert result.terminal_reason == "timeout"
    await asyncio.sleep(1.3)
    assert not marker.exists()
    await runtime.close()


@pytest.mark.skipif(os.name != "posix", reason="POSIX process group cleanup")
async def test_local_cancel_cleans_process_group_and_propagates_cancel(tmp_path: Path) -> None:
    runtime = LocalSandboxRuntime()
    started = tmp_path / "started"
    late = tmp_path / "late"
    script = (
        "from pathlib import Path; import time; "
        f"Path({str(started)!r}).write_text('ready'); "
        f"time.sleep(1); Path({str(late)!r}).write_text('leaked')"
    )
    command = f"{shlex.quote(sys.executable)} -c {shlex.quote(script)}"
    task = asyncio.create_task(runtime.exec(ExecRequest(context(runtime), command)))
    async with asyncio.timeout(5):
        while not started.exists():
            await asyncio.sleep(0.01)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    await asyncio.sleep(1.2)
    assert not late.exists()
    await runtime.close()
    with pytest.raises(SandboxClosedError):
        await runtime.read_text(ReadRequest(context(runtime), str(started)))
