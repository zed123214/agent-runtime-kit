from __future__ import annotations

import asyncio
import os
import socket
import subprocess
import sys
import time
import uuid
from collections.abc import AsyncGenerator
from pathlib import Path

import pytest


@pytest.fixture
def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    return port  # socket released; daemon can bind to this port


@pytest.fixture
async def running_daemon(
    free_port: int,
    tmp_path: Path,
) -> AsyncGenerator[subprocess.Popen[bytes], None]:
    env = os.environ.copy()
    env["AGENTRT_PORT"] = str(free_port)
    env["AGENTRT_DATA_ROOT"] = str(tmp_path / f"agentrt-daemon-{uuid.uuid4().hex}")
    env["AGENTRT_LOG_FILE"] = ""
    env["AGENTRT_LOG_LEVEL"] = "WARNING"
    env["ANTHROPIC_API_KEY"] = ""

    proc = subprocess.Popen([sys.executable, "-m", "agent_runtime.core"], env=env)

    # A cold isolated Windows interpreter can spend more than five seconds in
    # imports before the daemon binds. Readiness is still probed, not slept.
    deadline = time.monotonic() + 20.0
    while time.monotonic() < deadline:
        await asyncio.sleep(0.05)
        try:
            _reader, writer = await asyncio.open_connection("127.0.0.1", free_port)
            writer.close()
            await writer.wait_closed()
            break
        except (ConnectionRefusedError, OSError):
            pass
    else:
        proc.terminate()
        proc.wait()
        pytest.fail("Daemon did not start within 20 seconds")

    yield proc

    proc.terminate()
    try:
        proc.wait(timeout=2)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()
