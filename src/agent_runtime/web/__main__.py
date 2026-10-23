from __future__ import annotations

import argparse

from agent_runtime.web.gateway import Gateway


def main() -> None:
    parser = argparse.ArgumentParser(description="Local browser gateway for Agent Runtime Kit")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--core-port", type=int, default=7437)
    parser.add_argument("--core-host", default="127.0.0.1")
    parser.add_argument("--dev-origin", action="append", default=[])
    args = parser.parse_args()
    try:
        import uvicorn

        from agent_runtime.web.app import create_app
    except ImportError:
        parser.exit(1, "Install the optional web dependencies: uv sync --extra web\n")
    gateway = Gateway(core_host=args.core_host, core_port=args.core_port)
    origins = {f"http://127.0.0.1:{args.port}", f"http://localhost:{args.port}", *args.dev_origin}
    print(f"Open http://localhost:{args.port}/#bootstrap={gateway.bootstrap_token}", flush=True)
    uvicorn.run(
        create_app(gateway, origins=origins),
        host="127.0.0.1",
        port=args.port,
        access_log=False,
        ws_max_size=1024,
    )


if __name__ == "__main__":
    main()
