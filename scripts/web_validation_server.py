"""Launch an isolated real Core + gateway for browser acceptance tests.

One READY JSON frame is written to the parent's pipe. Never save that frame in logs:
it carries the one-use launch capability. A line on stdin requests graceful shutdown.
"""

from __future__ import annotations

import asyncio
import json
import os
import socket
import subprocess
import sys
import tempfile
from pathlib import Path

import uvicorn

from agent_runtime.web.app import create_app
from agent_runtime.web.gateway import Gateway


def free_port() -> int:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


async def main() -> None:
    core_port, web_port = free_port(), free_port()
    environment = os.environ.copy()
    if "--slow" in sys.argv:
        environment.update(AGENTRT_DEMO_INITIAL_DELAY="1.5", AGENTRT_DEMO_TOOL_DELAY="5")
    with tempfile.TemporaryDirectory(prefix="agentrt-web-acceptance-") as root:
        process = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "agent_runtime.web.demo",
                "--port",
                str(core_port),
                "--directory",
                str(Path(root) / "demo"),
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            env=environment,
        )
        try:
            gateway = Gateway(core_port=core_port)
            for _ in range(100):
                try:
                    await gateway.describe()
                    break
                except Exception:
                    await asyncio.sleep(0.05)
            else:
                raise RuntimeError("Core did not become ready")
            origin = f"http://localhost:{web_port}"
            app = create_app(gateway, origins={origin, f"http://127.0.0.1:{web_port}"})
            server = uvicorn.Server(
                uvicorn.Config(
                    app, host="127.0.0.1", port=web_port, log_level="error", access_log=False
                )
            )
            task = asyncio.create_task(server.serve())
            while not server.started:
                if task.done():
                    await task
                    raise RuntimeError("Web server did not start")
                await asyncio.sleep(0.02)
            print(
                "READY " + json.dumps({"origin": origin, "token": gateway.bootstrap_token}),
                flush=True,
            )
            await asyncio.to_thread(sys.stdin.readline)
            server.should_exit = True
            await task
        finally:
            process.terminate()
            try:
                await asyncio.to_thread(process.wait, 10)
            except subprocess.TimeoutExpired:
                process.kill()
                await asyncio.to_thread(process.wait)


if __name__ == "__main__":
    asyncio.run(main())
