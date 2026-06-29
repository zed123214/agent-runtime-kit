# Runbook

This runbook targets Linux/macOS-style environments. Native Windows runtime
execution is not supported. On Windows machines, use WSL2 or Docker for runtime
experiments.

## Daily Operations

### Start the daemon

```bash
uv run agentrt-core
```

By default the daemon listens on `127.0.0.1:7437`. Press `Ctrl+C` to stop it.

### Check connectivity

```bash
uv run agentrt ping
# pong server=0.0.1 uptime=12ms latency=2ms
```

### Stop the daemon

```bash
kill $(pgrep -f agentrt-core)
```

## Configuration

Configuration precedence, from low to high:

```text
built-in defaults -> ~/.agentrt/config.toml -> .env -> process environment
```

### `~/.agentrt/config.toml`

```toml
[core]
host = "127.0.0.1"
port = 7437

[logging]
level  = "INFO"
file   = "~/.agentrt/logs/core.log"
format = "text"    # "text" | "json"
```

### `.env`

Copy `.env.example` and edit local values. Do not commit `.env`.

```bash
cp .env.example .env
```

### Environment variables

| Variable | Default | Description |
| --- | --- | --- |
| `AGENTRT_CONFIG` | `~/.agentrt/config.toml` | Override config file path |
| `AGENTRT_HOST` | `127.0.0.1` | TCP bind address |
| `AGENTRT_PORT` | `7437` | TCP bind port |
| `AGENTRT_LOG_LEVEL` | `INFO` | `DEBUG`, `INFO`, `WARNING`, or `ERROR` |
| `AGENTRT_LOG_FILE` | `~/.agentrt/logs/core.log` | Log file path; empty means stderr only |
| `AGENTRT_LOG_FORMAT` | `text` | `text` or `json` |
| `ANTHROPIC_API_KEY` | none | Required only for real LLM runs |
| `AGENTRT_LLM_DEFAULT_MODEL` | `claude-sonnet-4-6` | Default LLM model name |
| `AGENTRT_MAX_STEPS` | `20` | Maximum loop steps for a run |

## Development Commands

```bash
uv run ruff check src tests scripts
uv run ruff format --check src tests scripts
uv run mypy src
uv run pytest tests/ -v
uv run pytest tests/unit/ -v

make docs
make verify-s0
```

## Logs

```bash
tail -f ~/.agentrt/logs/core.log
```

## Common Errors

| Error | Cause | Fix |
| --- | --- | --- |
| `core already running at 127.0.0.1:7437` | A daemon is already running | `kill $(pgrep -f agentrt-core)` |
| `core not running` | The daemon has not been started | `uv run agentrt-core` |
| `Address already in use` | Another process is using the port | `AGENTRT_PORT=8000 uv run agentrt-core` |
| `Config error: AGENTRT_PORT must be an integer` | Invalid port value in `.env` or environment | Check `AGENTRT_PORT` |
| `ANTHROPIC_API_KEY not set` | No key is available for real LLM runs | Set the key in `.env` or shell environment |
