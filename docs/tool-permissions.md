# Tool Permissions

Local agent tools can read files, write files, run shell commands, call MCP
servers, and spawn subagents. Agent Runtime Kit routes those operations through
a common tool and permission path.

## Flow

1. The LLM emits a `tool_use` block.
2. `ToolRegistry` resolves the tool and validates arguments.
3. `PermissionManager` evaluates tool policy.
4. If approval is required, the daemon publishes a permission event and waits
   for an allow or deny decision.
5. The tool runs only after validation and authorization.
6. The result is returned to the agent as a structured `ToolResult`.

## Decisions

The permission model supports one-shot and persistent decisions:

- `allow_once`
- `allow_session` (Loop + Local, this session and tool only; in memory)
- `always_allow`
- `deny_once`
- `always_deny`

This makes risky local actions visible at runtime instead of relying only on
prompt-level instructions.

## Session modes (Loop + Local)

`core.describe.permission_modes` advertises `ask`, `read_only`, and `auto`.
The owner connection reads `session.get_permissions` and changes
`session.set_permissions` with the last `expected_revision`. Changes can revoke
one tool or reset all session grants. Revision conflicts fail without mutation;
closed, closing, foreign, and durable sessions cannot change modes.

`read_only` permits only `read_file` and `list_dir`, including when a legacy
allow policy exists. It denies Shell and extension tools rather than guessing
which commands are read-only. `auto` authorizes tools for that session, including
pending requests and outside-directory commands. Explicit deny patterns,
tool defaults of DENY, and persistent deny decisions remain effective.

`allow_session` is bound to an actual pending request and grants that tool for
all arguments and paths in the owning session; it also resolves already waiting
calls of that tool. It never writes global policy and is removed on close or
disconnect. Legacy `always_allow` behavior is unchanged, including its forced
ASK heuristic for outside-cwd commands. Session grants intentionally cover those
commands, as their UI explicitly describes host-wide access. Neither session
mode nor the heuristic is an OS filesystem sandbox.

Automatic invocations retain permission events with `session_allow` or
`session_auto`; the initial human decision records `allow_session`. Read-only
denials record `read_only`. Changes affect calls not yet authorized, not effects
already in flight. Mode mutations are logged by Core and mirrored into gateway
snapshots only from Core responses.

## Client Isolation

The daemon binds each socket connection to the sessions that connection creates
through `session.create` or `agent.run`. The binding is installed before
`session.created` is published. Live events carrying a session ID are delivered
only to that owner; run, session, and permission events without a session ID are
rejected rather than broadcast. This also protects tool-call parameters and
results from unrelated `scope=global` subscribers.

`session.send_message`, `session.get_history`, `session.close`, and
`session.compact` verify ownership before looking up the session. An unknown ID
and another connection's ID both return `SESSION_NOT_FOUND` so the command does
not become a session-existence oracle. `permission.respond` performs the same
ownership check without consuming an unauthorized pending request. Disconnects
remove ownership, cancel pending approvals, and cancel work attached to that
connection before a late handler can bind another session.

`replay_from_run` accepts only a single safe run ID, searches controlled run
roots, requires each persisted event's `run_id` to match exactly, and applies
line, byte, root, and event limits. New full-UUID run IDs act as read-only local
history capabilities across reconnects. Legacy short or custom IDs are
replayable only while the connection owns their session, so low-entropy IDs do
not become a cross-client history oracle. A successful replay does not attach
the connection to the session and does not authorize live events, session
commands, or permission responses.

P0 does not provide cross-connection session attachment. A future reconnect or
resume flow must use an authenticated capability instead of granting ownership
from a client-supplied `session_id` alone.

## Trust Boundary

The daemon defaults to loopback and is intended for processes running as the
same local OS user. A new full-UUID run ID locates read-only history within that
local trust domain; it is not a remote authentication credential. The persistent
`always_allow` and `always_deny` policy remains daemon-admin state shared by that
same local user. P0 does not add TLS, remote-user authentication, or multi-tenant
policy isolation.
