# Architecture

Agent Runtime Kit separates agent execution from user-facing clients.
`agentrt-core` is the long-running daemon. `agentrt` and `agentrt-tui` are thin
clients that send JSON-RPC commands and subscribe to event streams.

## Runtime Path

1. A client sends a command through JSON-RPC 2.0 over NDJSON TCP.
2. The daemon validates the request and routes it to a handler.
3. `SessionManager` records user input and creates a run.
4. `AgentRunner` assembles provider, tools, permissions, events, and context.
5. `AgentLoop` runs the plan-act-observe cycle until success or failure.
6. `EventBus` publishes facts to JSONL files and subscribed clients.

## Why Daemon First

The daemon owns execution state, so a CLI or TUI disconnect does not need to
cancel an in-flight task. This also makes multi-client observability possible:
one client can trigger work while another subscribes to the same run or session.

## Key Modules

- `core/app.py`: daemon lifecycle and command registration.
- `core/runner.py`: run assembly and dependency wiring.
- `core/loop.py`: LLM/tool execution loop.
- `core/bus/`: typed command, event, and JSON-RPC envelope models.
- `core/transport/`: socket server, socket client, and event broadcasting.
- `core/events/`: in-process event bus and JSONL writer.
