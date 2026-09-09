from __future__ import annotations

import argparse
import os
import socket


def main() -> None:
    try:
        from aiohttp import web

        from agent_runtime.sandbox_server.server import WorkerServer
    except ImportError as exc:
        raise SystemExit("Install agent-runtime-kit[sandbox-worker] to run the Worker") from exc
    parser = argparse.ArgumentParser()
    parser.add_argument("role", choices=("broker", "executor"))
    args = parser.parse_args()
    os.umask(0o077)
    worker = WorkerServer(broker=args.role == "broker")
    if worker.broker:
        web.run_app(
            worker.app(),
            host="0.0.0.0",
            port=int(os.environ.get("SERVICE_PORT", "8080")),
            access_log=None,
            print=None,
            handler_cancellation=True,
        )
    else:
        # The shared socket holds no credentials. Executor owns it; broker's
        # filesystem view is read-only. Containers do not share PID namespaces.
        path = "/tmp/control/executor.sock"
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.bind(path)
        os.chown(path, -1, 10000)
        os.chmod(path, 0o660)
        web.run_app(worker.app(), sock=sock, access_log=None, print=None, handler_cancellation=True)


if __name__ == "__main__":
    main()
