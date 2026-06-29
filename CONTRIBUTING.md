# Contributing

This repository is a portfolio-oriented local AI Agent runtime. Keep changes
grounded in runtime infrastructure: daemon execution, typed IPC, events, tool
permissions, sessions, context governance, skills, subagents, and MCP.

## Commands

```bash
uv sync
uv run ruff check src tests scripts
uv run ruff format --check src tests scripts
uv run mypy src
uv run pytest tests/ -v
uv run python scripts/check_wire_protocol.py --check
```

Runtime commands assume Linux/macOS-style process and shell semantics. Native
Windows execution is not supported; use WSL2 or Docker on Windows machines.

## Development Notes

- Keep public naming provider-neutral: use Agent Runtime Kit, `agent_runtime`,
  `agentrt`, `agentrt-core`, and `agentrt-tui`.
- Keep Anthropic-specific behavior inside the provider layer.
- Update `WIRE_PROTOCOL.md` with `uv run python scripts/generate_wire_protocol.py`
  whenever protocol models under `src/agent_runtime/core/bus/` change.
- Avoid committing `.env`, logs, local sessions, caches, virtual environments,
  or runtime workspaces.

## Code Style

The codebase uses `ruff`, strict `mypy`, and `pytest`. Unit tests should avoid
real LLM calls. Integration tests that require `ANTHROPIC_API_KEY` must skip
cleanly when the key is absent.
