"""Untrusted-side executor. It never receives the Core authentication token."""

from __future__ import annotations

import asyncio
import os
import signal
import time
from dataclasses import asdict
from pathlib import Path
from typing import Any

from agent_runtime.sandbox_server.files import Workspace
from agent_runtime.sandbox_server.protocol import Operation

OUTPUT_BYTES = 64 * 1024


def oom_kills() -> int | None:
    """cgroup v2 evidence, not a guess based only on shell exit status 137."""
    try:
        values = dict(
            line.split() for line in Path("/sys/fs/cgroup/memory.events").read_text().splitlines()
        )
        return int(values["oom_kill"])
    except (OSError, ValueError, KeyError):
        return None


class Output:
    def __init__(self) -> None:
        self.stdout = bytearray()
        self.stderr = bytearray()
        self.merged = bytearray()
        self.truncated = False

    def append(self, stream: bytearray, chunk: bytes) -> None:
        for target in (stream, self.merged):
            remaining = OUTPUT_BYTES - len(target)
            target.extend(chunk[:remaining])
            if len(chunk) > remaining:
                self.truncated = True

    async def drain(self, pipe: asyncio.StreamReader, stream: bytearray) -> None:
        while chunk := await pipe.read(8192):
            self.append(stream, chunk)

    def text(self, raw: bytearray) -> str:
        # Replacement can expand UTF-8 even if collection did not hit its bound.
        encoded = raw.decode("utf-8", "replace").encode("utf-8")
        if len(encoded) > OUTPUT_BYTES:
            self.truncated = True
        return encoded[:OUTPUT_BYTES].decode("utf-8", "ignore")


async def finish_cleanup(task: asyncio.Task[Any]) -> None:
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            continue
    task.result()


class Executor:
    def __init__(self, workspace: Workspace, cleanup_s: float = 5) -> None:
        self.workspace = workspace
        self.cleanup_s = cleanup_s
        self.lock = asyncio.Lock()

    async def execute(self, op: Operation) -> dict[str, Any]:
        # Even if a caller bypasses Core queuing, file/command operations cannot
        # interleave inside this executor. No lock is held over an Agent run.
        async with self.lock:
            if op.operation == "exec":
                return await self.command(op)
            if op.operation == "read_text":
                return asdict(self.workspace.read(op.path))
            if op.operation == "write_text":
                return asdict(self.workspace.write(op.path, op.content))
            return asdict(self.workspace.list(op.path, op.max_depth))

    async def command(self, op: Operation) -> dict[str, Any]:
        capture = Output()
        oom_before = oom_kills()
        spawn = asyncio.create_task(
            asyncio.create_subprocess_exec(
                "/bin/sh",
                "-c",
                op.command,
                cwd="/workspace",
                env={
                    "PATH": "/usr/local/bin:/usr/bin:/bin",
                    "HOME": "/workspace",
                    "TMPDIR": "/tmp",
                    "LANG": "C.UTF-8",
                },
                stdin=asyncio.subprocess.DEVNULL,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                start_new_session=True,
                limit=16384,
            )
        )
        proc: asyncio.subprocess.Process | None = None
        readers: list[asyncio.Task[None]] = []
        started = time.monotonic()
        reason = "completed"

        async def cleanup() -> None:
            nonlocal proc
            if proc is None:
                proc = await spawn  # A late subprocess creation is still owned.
            try:
                os.killpg(proc.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            try:
                async with asyncio.timeout(self.cleanup_s):
                    await proc.wait()
                    await asyncio.gather(*readers, return_exceptions=True)
            finally:
                for reader in readers:
                    if not reader.done():
                        reader.cancel()
                await asyncio.gather(*readers, return_exceptions=True)

        try:
            proc = await asyncio.shield(spawn)
            started = time.monotonic()
            assert proc.stdout is not None and proc.stderr is not None
            readers = [
                asyncio.create_task(capture.drain(proc.stdout, capture.stdout)),
                asyncio.create_task(capture.drain(proc.stderr, capture.stderr)),
            ]
            try:
                async with asyncio.timeout(op.timeout_s):
                    # Process.wait() can wait for inherited pipes to close as
                    # well as for shell exit. The public returncode is updated
                    # independently, so observe it before joining pipe readers.
                    while proc.returncode is None:
                        await asyncio.sleep(0.005)
            except TimeoutError:
                reason = "timeout"
        finally:
            # Includes normal exit: a non-interactive command does not grant a
            # background process lifetime. Escaping a POSIX group via setsid is
            # outside this group guarantee; destroying the Pod kills the cgroup.
            await finish_cleanup(asyncio.create_task(cleanup()))
        stdout = capture.text(capture.stdout)
        stderr = capture.text(capture.stderr)
        merged = capture.text(capture.merged)
        assert proc is not None
        oom_after = oom_kills()
        resource_observations = None
        process_signal = (
            -proc.returncode if proc.returncode is not None and proc.returncode < 0 else None
        )
        if oom_before is not None and oom_after is not None:
            resource_observations = {"oom_kill_before": oom_before, "oom_kill_after": oom_after}
        if reason == "completed" and proc.returncode:
            if (
                oom_before is not None
                and oom_after is not None
                and oom_after > oom_before
                and proc.returncode in (-9, 137)
            ):
                reason = "oom_killed"
                process_signal = signal.SIGKILL
            else:
                reason = "signal" if process_signal else "command_failed"
        content = merged or "[no output]"
        if capture.truncated:
            content += "\n[output truncated at 64 KiB]"
        if reason == "timeout":
            content += f"\n[command timed out after {op.timeout_s:g}s]"
        elif proc.returncode:
            content += f"\n[exit {proc.returncode}]"
        return {
            "content": content,
            "is_error": reason != "completed" or proc.returncode != 0,
            "error_type": reason if reason != "completed" else None,
            "stdout": stdout,
            "stderr": stderr,
            "output": merged,
            "merged_output": merged,
            "truncated": capture.truncated,
            "exit_code": proc.returncode,
            "signal": process_signal,
            "resource_observations": resource_observations,
            "terminal_reason": reason,
            "duration_ms": (time.monotonic() - started) * 1000,
        }
