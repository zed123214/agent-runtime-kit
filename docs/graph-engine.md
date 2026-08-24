# Optional LangGraph Engine (P1/P2)

KitAgent has two execution engines behind the same `ExecutionEngine` boundary:

- `loop` remains the default and delegates to the existing `AgentLoop`.
- `graph` uses LangGraph `StateGraph` for explicit orchestration while reusing
  KitAgent's provider, Anthropic-style message dictionaries, tool registry,
  permission manager, event bus, session store, and runner terminal boundary.

LangGraph does not replace the host runtime or its governance. `AgentRunner`
still owns `run.started`, the single `run.finished`, JSONL persistence, and
session transcript updates. The Graph engine owns only node scheduling and its
checkpointed orchestration state.

## Install and select the engine

The normal Quick Start installs no Graph packages and continues to use `loop`.
To enable Graph, use PowerShell:

```powershell
uv sync --extra graph
$env:AGENTRT_ENGINE = 'graph'
uv run agentrt-core
```

Then open a second PowerShell window and connect a client:

```powershell
$env:AGENTRT_ENGINE = 'graph'
uv run agentrt chat
```

The equivalent TOML selection is:

```toml
[agent]
engine = "graph"
```

If `graph` is selected without its optional dependencies, the daemon returns a
typed `engine_unavailable` failure with the installation command before provider
or API-key validation. Ordinary imports, the default loop, CLI, and TUI do not
eagerly import LangGraph.

## Configuration

Graph-specific settings can be supplied through `[graph]` or environment
variables. Numeric budget values must be greater than zero.

| TOML key | Environment variable | Default | Meaning |
| --- | --- | --- | --- |
| `checkpoint_backend` | `AGENTRT_GRAPH_CHECKPOINT_BACKEND` | `memory` | `memory` for P1 compatibility or opt-in `sqlite` durability |
| `checkpoint_path` | `AGENTRT_GRAPH_CHECKPOINT_PATH` | `graph-checkpoints.sqlite3` inside the data root | Daemon-owned SQLite saver path |
| `recursion_limit` | `AGENTRT_GRAPH_RECURSION_LIMIT` | `2 * max_steps + 1` | LangGraph superstep limit |
| `tool_call_budget` | `AGENTRT_GRAPH_TOOL_CALL_BUDGET` | `64` | Maximum root tool calls submitted during one run |
| `wall_time_s` | `AGENTRT_GRAPH_WALL_TIME_S` | `300.0` | End-to-end Graph run deadline in seconds |
| `trace_event_limit` | `AGENTRT_GRAPH_TRACE_EVENT_LIMIT` | `64` | Maximum lightweight node summaries kept in checkpoint state |

Example:

```toml
[graph]
checkpoint_backend = "memory"
recursion_limit = 41
tool_call_budget = 64
wall_time_s = 300.0
trace_event_limit = 64
```

`trace_event_limit` bounds observation metadata; it is not an execution budget
and does not terminate a run. The daemon data root defaults to `~/.agentrt` and
can be changed with `AGENTRT_DATA_ROOT`. A configured checkpoint path is accepted
only when its resolved location remains inside that root.

The memory extra remains:

```powershell
uv sync --extra graph
```

The durable backend has its own extra:

```powershell
uv sync --extra graph-sqlite
$env:AGENTRT_GRAPH_CHECKPOINT_BACKEND = 'sqlite'
```

## Explicit graph

```text
START -> model
model ----- running with pending calls -----> kit_tools
model ----- running without pending calls --> model
model ----- success or failed -------------> END
kit_tools - running and able to continue ---> model
kit_tools - budget or max-step failure -----> END
```

The `model` node calls the existing `LLMProvider.chat()` with KitAgent's system
prompt and tool schemas. It creates pending calls but never executes them. The
ordinary asynchronous `KitToolNode` executes calls in model order through the
existing `invoke_tool()` path, preserving schema validation, permission ASK or
deny, timeout, and tool lifecycle events. Non-durable invocations retain the
existing retry/backoff policy. After a durable journal claim, a raised exception
or timeout becomes `outcome_unknown` without an automatic retry. It does not use
LangGraph's prebuilt `ToolNode`, `create_agent`, or `create_react_agent`.

## RuntimeState

Checkpoint state contains only JSON-compatible business data:

- `run_id` and `goal`;
- native KitAgent message dictionaries;
- serialized pending tool calls and their action;
- tool results and bounded error codes;
- model step and root tool-call counts;
- status, reason, and final answer;
- a bounded list of lightweight node trace summaries.

Provider objects, registries, event buses, permission managers, savers,
compiled graphs, exceptions, coroutines, and other runtime handles never enter
checkpoint state. `messages` uses an append reducer so a cold thread can receive
the complete transcript while an existing thread receives only its delta.
Other per-run fields use overwrite semantics so a new run cannot inherit a
previous terminal status, pending call, error, or result.

## Transcript and checkpoint consistency

`SessionStore` and its `thread.jsonl` remain the authoritative transcript.
`InMemorySaver` is only a process-local orchestration cache. Before every Graph
run, the engine reconciles them under the thread lock:

1. With no checkpoint, seed the graph with the full current transcript.
2. If checkpoint messages are an exact structural prefix of the transcript,
   submit only the new suffix. Equality is a valid empty suffix.
3. If they are not a prefix, delete that thread through
   `InMemorySaver.adelete_thread()` and reseed from the complete transcript.

Those reset/reseed rules apply to the memory backend and to manual compaction
when no unfinished run exists. For an unfinished SQLite run, the checkpoint is
the orchestration authority: a non-prefix transcript mismatch is a typed
`checkpoint_conflict`, and recovery does not reset the thread or repeat work.

The comparison does not deduplicate by text, so legitimate repeated user
messages are preserved. Normal turns use a base config containing only the
current `thread_id`; they do not reuse an old `checkpoint_id` and therefore do
not fork from a historical checkpoint. After the graph stops, its complete
state is synchronized back to the same canonical `ExecutionContext`, allowing
`AgentRunner` to append only the messages created during the current run.

## Identifier mapping and saver lifecycle

| Identifier | Scope |
| --- | --- |
| `run_id` | One execution and its `events.jsonl` |
| `session_id` | One KitAgent session exposed through IPC |
| `thread_id` | LangGraph checkpoint namespace used for conversation continuation |

For chat sessions, `thread_id == session_id` and `retain_thread=True`. Each
message may create a new `AgentRunner`, but all runners created by one
`CoreApp` share its lazily initialized `GraphRuntime` and `InMemorySaver`. The
compiled graph remains run-scoped because its node closures capture that run's
provider, tools, permissions, and event bus.

For one-shot sessions and direct runners, `thread_id == run_id` and
`retain_thread=False`; their checkpoint is deleted at the terminal boundary.
Memory chat checkpoints are deleted on explicit or automatic close, and memory
runtime shutdown clears its owned threads. A suspended durable chat disconnect
instead removes ownership and in-memory session state while retaining the
SQLite checkpoint and capability. SQLite runtime shutdown closes the saver
connection without deleting recoverable checkpoints; explicit `session.close`
terminalizes the unfinished run and then deletes that session's checkpoint and
capability. Loop-only use and an uninitialized Graph runtime make these cleanup
hooks no-ops.

## Events and client display

`AgentRunner` remains the only publisher of run lifecycle events. Graph nodes
add `node.started`, `node.finished`, and a bounded `state.diff` after a normal
v1 `updates` item:

```text
# direct end_turn
run.started
node.started(model) -> step.started -> provider events -> step.finished
node.finished(model) -> state.diff(model)
run.finished

# tool path
run.started
node.started(model) -> step.started -> provider events
node.finished(model) -> state.diff(model)
node.started(kit_tools) -> tool/permission events -> step.finished
node.finished(kit_tools) -> state.diff(kit_tools)
[repeat model or finish]
run.finished
```

The engine explicitly consumes
`graph.astream(..., stream_mode="updates", version="v1")`, whose items have the
shape `{node_name: update_dict}`. A node publishes `node.finished` before its
update reaches the consumer, preserving `node.finished -> state.diff`. A thrown
exception or cancellation has a matching failed/cancelled `node.finished` but
no fabricated state diff.

`state.diff` contains only field names, counts, step, tool count, status, reason,
and final-answer presence. It never contains full messages, thinking blocks,
tool arguments or output, tokens, event history, or secrets. CLI and TUI clients
display only this bounded metadata. The loop engine does not synthesize Graph
node or state events.

## Four independent execution budgets

1. **`max_steps`** counts only model-node calls. It does not count tool nodes or
   LangGraph supersteps. A final `end_turn` on the last allowed model call wins
   as success. If that call requests tools, the complete valid batch is handled
   first; a required next model call then fails with `exceeded_max_steps`.
2. **`recursion_limit`** counts LangGraph supersteps. It is placed at the top
   level of `RunnableConfig`, not under `configurable`. When unset, the effective
   value is `2 * context.max_steps + 1`, which accommodates the alternating
   `model <-> kit_tools` topology. Only `GraphRecursionError` maps to
   `exceeded_recursion_limit`.
3. **`tool_call_budget`** counts root calls actually submitted to `invoke_tool`;
   retries do not add counts. A batch that would exceed the budget is rejected
   before any tool in that batch runs. Synthetic error `tool_result` blocks pair
   every rejected call, and the run ends with `exceeded_tool_call_budget`.
4. **`wall_time_s`** covers each active attempt, including model calls, tools,
   and any applicable retry/backoff. An in-process memory permission wait remains
   inside that attempt; a durable approval wait and daemon downtime occur between
   attempts and do not consume it. Timeout cancels in-flight work and ends with
   `exceeded_wall_time`.

The deterministic priority is:

```text
end_turn success
> whole-batch tool budget preflight
> execute the complete accepted batch
> exceeded_max_steps if another model call is required
```

External cancellation interrupts model, tool, or permission waits, records a
matching cancelled node lifecycle, and propagates to the runner so the run still
has one terminal event. Stable Graph failure reasons are
`exceeded_max_steps`, `exceeded_recursion_limit`,
`exceeded_tool_call_budget`, `exceeded_wall_time`, `cancelled`, and `llm_error`.

If cancellation arrives after a node handler has already completed, that node's
real `node.finished -> state.diff` pair is delivered before cancellation reaches
the run boundary. If a timeout, cancellation, recursion boundary, or unexpected
tool-node exception interrupts a retained tool batch before its update commits,
the engine pairs every pending `tool_use` with a synthetic error result that
states the execution outcome is unknown, then resets the in-memory checkpoint.
The next turn is reseeded from the authoritative SessionStore transcript. This
keeps the Anthropic message history well formed; it is not an exactly-once tool
execution guarantee.

## Compaction behavior

Graph P1 rejects a non-zero `compaction.auto_threshold` with typed
`engine_configuration_error`; it never silently ignores automatic compaction.
Manual `session.compact` remains supported because `SessionStore` is
authoritative. On the next turn its rewritten transcript no longer matches the
checkpoint prefix, so the engine deletes and reseeds that thread.

## Locked dependency versions

The current project lock for the Graph extra resolves:

| Package | Version | Relationship to P1 |
| --- | --- | --- |
| `langgraph` | `1.2.8` | Direct optional dependency |
| `langgraph-checkpoint-sqlite` | `3.1.1` | Direct dependency of the `graph-sqlite` extra |
| `langgraph-checkpoint` | `4.2.0` | Transitive saver API |
| `langgraph-prebuilt` | `1.1.0` | Transitive only; its ToolNode is not used |
| `langgraph-sdk` | `0.4.3` | Transitive only |
| `langchain-core` | `1.6.0` | Transitive only; P1 does not use LangChain messages |

These resolved packages do not imply that KitAgent directly uses their prebuilt
agents, remote SDK, or message adapters.

## Offline demo

After installing the extra, run the deterministic demo without an API key:

```powershell
uv run --frozen --isolated --extra graph python examples/graph_offline_demo.py
```

The demo uses a scripted KitAgent provider and local tools. It is offline
validation, not a real-provider or production deployment test.

## P1 memory and P2 durable boundaries

The memory backend preserves P1 process-local continuation, thread isolation,
compact reset/reseed behavior, and typed external `resume_unsupported`.

The opt-in P2 SQLite backend adds daemon-restart continuation, capability-bound
session attachment, native `interrupt`/HITL approval, tool result journaling,
and monotonic event cursors. It uses only the latest checkpoint and does not add
time travel, historical branching, Postgres, concurrent-daemon HA, a general
exactly-once guarantee, a plan/review/report business graph, durable subagents,
subgraphs, fan-out, or LangChain message/StructuredTool adapters. See
[Durable Graph Recovery and Human Approval](durable-recovery.md).
