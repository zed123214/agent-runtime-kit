# Durable Graph Recovery and Human Approval (P2)

P2 adds an opt-in SQLite backend for resumable chat runs. The default runtime is
unchanged: `agent.engine=loop` and the Graph checkpoint backend is `memory`.
Durability is enabled only when the Graph engine and SQLite checkpoint backend
are both selected.

## Install and Configure

The lock file pins the direct optional dependencies to:

| Package | Version | Role |
|---|---:|---|
| `langgraph` | `1.2.8` | State graph, `interrupt()`, and `Command(resume=...)` |
| `langgraph-checkpoint-sqlite` | `3.1.1` | Official async SQLite checkpointer |

Install the durable extra and select a daemon-controlled data root:

```powershell
uv sync --extra graph-sqlite
$env:AGENTRT_ENGINE = 'graph'
$env:AGENTRT_GRAPH_CHECKPOINT_BACKEND = 'sqlite'
$env:AGENTRT_DATA_ROOT = 'D:\agentrt-data'
uv run agentrt-core
```

`AGENTRT_GRAPH_CHECKPOINT_PATH` may override the checkpoint file, but the
resolved path must remain inside `AGENTRT_DATA_ROOT`. IPC clients cannot supply
a checkpoint path. Selecting SQLite without its optional package produces the
typed `engine_unavailable` error before provider construction.

## Persistence Responsibilities

P2 deliberately uses two persistence layers instead of wrapping them in a
generic checkpoint abstraction:

| Store | Authority |
|---|---|
| `thread.jsonl` (`SessionStore`) | Committed conversation transcript |
| official `AsyncSqliteSaver` | Graph state, latest task/node, pending native interrupt, and checkpoint revision |
| `events.jsonl` | Ordered run audit stream and replay cursor |
| `RecoveryStore` SQLite tables | Run recovery index, resume capability hash, transcript commit progress, resume lease, and tool invocation journal |

The saver is accessed only through public LangGraph methods. The runtime does
not read or write LangGraph's private SQLite tables. `RecoveryStore` has its own
versioned schema and rejects an unknown or incomplete schema.

## Suspend and Resume

A terminal engine result remains a `RunOutcome`. A durable non-terminal attempt
returns `RunSuspension` with the original session/run identity, suspension
reason, latest checkpoint revision, optional interrupt identity, and event
cursor.

The lifecycle of one logical run is:

```text
run.started
  -> node and tool events
  -> run.suspended                 # no run.finished
  -> explicit session.resume
  -> run.resumed                   # same run_id, no second run.started
  -> node and tool events
  -> run.finished                  # exactly one terminal root event
```

Daemon startup marks attempts left in `running` or `resuming` as suspended and
does not execute them. A client must explicitly attach with `session.resume`.
The command first validates the capability without consuming it, takes an
exclusive provisional reservation, and verifies the durable run, checkpoint,
revision, and resume lease. Process-recovery or expired-approval coordination can
then finish while session-scoped events are routed to that provisional
connection. Owner-only commands remain unavailable until the final capability
compare-and-swap rotates the token and synchronously commits ownership. Competing
resume requests with the same token receive a stable recovery conflict and cannot
share the reservation or acquire the same revision/epoch lease.

`run.get_state` is available only to the owning connection. It returns a
redacted summary: run status, current/next node, suspension reason, revision,
cursor, resumability, and a bounded approval summary. It does not expose the
raw capability, complete checkpoint, provider payload, or unredacted tool
arguments.

## Resume Capability

A durable chat session receives a high-entropy resume token once at creation.
Only its SHA-256 digest and version are stored. Successful attachment uses a
constant-time comparison and atomically rotates the token; the response returns
the next token. Old, incorrect, or repeated tokens after a completed attach are
rejected with the same capability error; a genuinely overlapping same-token
attach receives a recovery conflict. If a live handler is cancelled while the
SQLite rotation worker is committing, KitAgent waits for that worker and restores
the previous token with a compare-and-swap before propagating cancellation.
Session identifiers are locators, not write capabilities.

Token rotation and delivery of the response frame are not a distributed atomic
transaction. The complete `session.resume` response is the capability handoff
boundary; P2 does not add a confirmation protocol for a hard process kill or
response loss inside that token exchange.

The socket trace redacts resume-token fields in both requests and responses.
Raw token values are not included in run events, state diffs, logs, or protocol
examples; the Wire document uses explicit opaque placeholders only.

## Native Permission Interrupts

The durable Graph ASK path uses LangGraph's native `interrupt()` and resumes the
latest checkpoint with `Command(resume=decision)`. KitAgent still owns policy
evaluation, schema validation, retry/timeout behavior, tool execution, and tool
events. Allow and deny policies do not create an interrupt.

The interrupt payload contains JSON-safe identity and a bounded parameter
preview. The persisted approval event adds the native interrupt ID and current
checkpoint revision. A durable approval response must provide the full
session/run/tool identity and match the pending interrupt and revision. Legacy
`permission.respond` remains available only for a unique in-process request
owned by the current connection.

Because LangGraph restarts an interrupted node from its beginning, the ASK gate
precedes every tool side effect. Completed earlier calls in the same batch are
recovered from the journal instead of being invoked again.

## Tool Invocation Journal

Durable calls use `(session_id, run_id, tool_use_id)` as the stable key and a
canonical input hash as the identity check.

```text
permission/schema accepted
  -> journal started
  -> invoke the existing KitAgent tool path once
  -> known ToolResult: journal completed(serialized ToolResult)
  -> raised exception/timeout: journal outcome_unknown (no durable retry)
  -> known result only: commit Graph node update
```

On recovery:

- `completed` reuses the serialized `ToolResult` and does not invoke the tool;
- `started` after process loss becomes `outcome_unknown` and is not replayed;
- the same key with a different name or input hash is a typed conflict;
- non-durable calls retain the existing retry policy; a durable raised
  exception or timeout is not replayed without an explicit idempotency protocol.

This is a fail-closed recovery protocol for the tested local tools. It is not a
general exactly-once guarantee for arbitrary shell commands, MCP servers,
external services, background subagents, or other side effects.

## Event and Transcript Reconciliation

Every root run event has an optional, monotonically increasing `event_seq`.
New logs persist the event before live fan-out. Resume seeds the counter from the
last complete row, and replay can request events strictly after an acknowledged
cursor. Child runs maintain their own sequence.

Replay establishes a bounded paused subscription before reading the snapshot,
deduplicates only the requested run's cursor, then switches atomically to live
delivery. A slow replay drain is disconnected within the same fixed deadline as
live fan-out. If the requested history exceeds the replay line, byte, or event
limit, the command returns a typed `replay_limit_exceeded` error before emitting
partial history or activating the live subscription.

An incomplete final JSONL row is treated as a torn tail and can be truncated.
Invalid UTF-8/JSON or a cursor gap in the middle of a log is a typed corruption
error. Recovery never skips a corrupt middle row.

`SessionStore` commits one run delta as an idempotent batch and records a
canonical count/hash in `RecoveryStore`. Recovery compares the committed prefix
and appends only a missing suffix. This coordinates the three terminal crash
windows: terminal checkpoint before `run.finished`, terminal event before the
manifest update, and final checkpoint before transcript commit.

## Budgets and Lifecycle

- `max_steps` and `tool_call_budget` live in Graph state and accumulate across
  resume attempts.
- `wall_time_s` applies to each active attempt; approval wait and daemon downtime
  do not consume it.
- `recursion_limit` applies to each LangGraph invocation.
- Memory runtime close preserves the P1 cleanup behavior.
- SQLite runtime close closes its connection and retains suspended checkpoints.
- Explicit `session.close` terminalizes the run and removes its checkpoint and
  capability while retaining transcript, events, and terminal recovery metadata.
- A suspended durable disconnect removes ownership and in-memory session state
  while retaining checkpoint and capability for a later attach.

One data root supports sequential daemon takeover after the previous daemon has
exited. P2 does not provide concurrent multi-daemon HA.

## Offline Recovery Demo

Run the deterministic two-process demonstration with no provider API key:

```powershell
uv run --frozen --isolated --extra graph-sqlite python examples/durable_recovery_demo.py
```

The parent starts daemon A on a random loopback port, waits until the tool result
is journaled but the Graph node has not committed, kills it, starts daemon B on
the same data root, attaches with the capability, and verifies completed-result
reuse, the original run ID, cursor continuity, one effective tool call, and one
terminal event. The recovery test suite separately covers the native permission
interrupt and exact approval identity path.

## Scope Boundary

P2 does not add time travel, Postgres, multi-daemon HA, general exactly-once
execution, a P3 workflow DSL, durable subagents, or a real-provider end-to-end
test. Provider-independent recovery is covered by deterministic offline
subprocess tests and the two-process demonstration.
