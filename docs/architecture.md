# Architecture

Agent Runtime Kit separates agent execution from user-facing clients.
`agentrt-core` is the long-running daemon. `agentrt` and `agentrt-tui` are thin
clients that send JSON-RPC commands and subscribe to event streams.

## Runtime Path

1. A client sends a command through JSON-RPC 2.0 over NDJSON TCP.
2. The daemon validates the request and routes it to a handler.
3. `SessionManager` records user input and creates a run.
4. `AgentRunner` assembles provider, tools, permissions, events, and the canonical
   `ExecutionContext`.
5. The configured `ExecutionEngine` runs that context. The default
   `LoopExecutionEngine` delegates to `AgentLoop`; the optional
   `GraphExecutionEngine` schedules explicit `model` and `kit_tools` nodes.
6. A run-scoped `EventBus` adds stable correlation metadata, writes only that
   run's JSONL file, and forwards events to daemon subscribers.

## Execution Engine Boundary

`agent.engine` (or `AGENTRT_ENGINE`) selects `loop` or `graph`; the default is
`loop`, so existing CLI, TUI, daemon, and Quick Start commands need no new
argument or dependency. Selecting `graph` lazily loads the optional Graph engine;
when the Graph extra is absent, the runner produces a typed
`engine_unavailable` outcome and terminal event before provider startup. The
Graph checkpoint backend independently defaults to `memory`; `sqlite` requires
the `graph-sqlite` extra and is also validated before provider construction.

An engine receives the existing `ExecutionContext`, `ToolRegistry`, `EventBus`,
and run-scoped options. It updates that exact context in place and returns the
equivalent `RunOutcome` or, for a durable non-terminal boundary, a
`RunSuspension`. Both keep events and session persistence coordinated through
one canonical state object.

| Operation | Loop engine | Graph engine |
| --- | --- | --- |
| `run` | Delegates to `AgentLoop` | Runs `START -> model <-> kit_tools -> END` |
| `resume` | Typed `resume_unsupported` | Memory: typed unsupported; SQLite: latest-checkpoint continuation with the original run ID |
| `cancel` | Cancels the active loop task | Cancels model, tool, or permission waits and propagates to the runner |

This remains an internal engine contract: there is no user-callable `run.cancel`
IPC command. Durable ownership transfer is exposed only through capability-bound
`session.resume`, and redacted inspection through `run.get_state`.

## Graph State and Lifecycle

LangGraph owns orchestration only. Nodes keep using KitAgent's provider, native
message dictionaries, tool registry, permission manager, event bus, and
`invoke_tool()` governance path. RuntimeState contains only JSON-compatible
messages, pending calls, bounded results/errors/trace metadata, counts, and
terminal fields; runtime handles never enter checkpoints.

`SessionStore.thread.jsonl` remains the transcript authority. With the memory
backend, `InMemorySaver` is a process-local cache: a first run seeds the full
transcript, an exact checkpoint prefix receives only the new suffix, and a
mismatch may delete and reseed the thread when no unfinished run exists. With
SQLite, an unfinished checkpoint is authoritative for orchestration; a transcript
prefix mismatch is a typed conflict so recovery cannot silently repeat work.

One lazily initialized `GraphRuntime` belongs to each `CoreApp` and is shared by
the per-message runners created by that app. Chat sessions map
`thread_id=session_id` and retain state until close; one-shot and direct runs map
`thread_id=run_id` and delete it at the terminal boundary. Disconnect and
shutdown cancel and await work before clearing owned memory threads. SQLite
runtime close retains suspended checkpoints and closes the official saver
connection. Separate data roots have separate checkpoint, recovery, session,
event, permission, and journal state. See [Optional LangGraph Engine](graph-engine.md)
and [Durable Graph Recovery](durable-recovery.md) for the reconciliation, budget,
capability, and cleanup contracts.

## Run Event Contract

Existing wire event type names and field meanings remain intact. Run-scoped
events now share optional `correlation_id`, `session_id`, `node_id`, and
`event_seq` fields.
For root runs, `correlation_id` is the root `run_id`; child-agent events inherit
it, session-backed runs propagate their session ID, and the loop engine leaves
`node_id` unset. Separate runs use scoped buses, and every `EventWriter` filters
on its owning `run_id`, so a child event may still be forwarded to the parent
bus for live CLI/TUI observation without being written into the parent's JSONL.
Each direct subagent JSONL is self-describing: it starts with
`subagent.started`, contains that child's loop events, and ends with
`subagent.finished`. The same lifecycle and loop events are bridged to the
parent stream for live observation without contaminating the parent JSONL.

`llm.token` remains the content stream, existing `tool.call_*` events remain the
tool lifecycle, and `run.finished` is the terminal event for runs managed by
`AgentRunner`; its status is restricted to `success` or `failed`. Direct
subagents use `subagent.finished` as their persisted terminal lifecycle event.
The Graph engine publishes matched `node.started`/`node.finished` events and a
bounded, content-free `state.diff` after a node returns normally. The runner
still owns the single run terminal event. The loop engine does not synthesize
Graph node or state-diff events.

## Why Daemon First

The daemon owns execution state and binds each live session to one connection.
Live run, model, tool, permission, and session events are delivered to the
committed owner. During a capability-validated `session.resume`, they may also be
buffered for that session's exclusive provisional connection so recovery cannot
lose its terminal event; the reservation does not authorize history, send,
compact, close, state, or approval commands. Disconnecting an active run cancels
its pending approvals, root work, and background subagents. A suspended durable
chat is evicted from memory but keeps its checkpoint and hashed capability. A
later client can consume and rotate the capability through `session.resume`;
possession of a session ID alone never transfers command or approval authority.

## Key Modules

- `core/app.py`: daemon lifecycle and command registration.
- `core/runner.py`: run assembly and dependency wiring.
- `core/engine/`: execution protocol, loop adapter, and lazy engine router.
- `core/graph/`: state graph, nodes, event bridge, memory/SQLite runtime,
  RecoveryStore, native interrupts, strict event log, and tool journal.
- `core/loop.py`: LLM/tool execution loop.
- `core/bus/`: typed command, event, and JSON-RPC envelope models.
- `core/transport/`: socket server, socket client, and event broadcasting.
- `core/events/`: in-process event bus and JSONL writer.
