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
- `always_allow`
- `deny_once`
- `always_deny`

This makes risky local actions visible at runtime instead of relying only on
prompt-level instructions.
