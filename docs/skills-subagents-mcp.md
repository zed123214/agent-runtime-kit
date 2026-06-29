# Skills, Subagents, and MCP

The extension model intentionally reuses runtime primitives instead of creating
separate execution paths.

## Skills

Skills are Markdown files with front matter. `SkillLoader` resolves a slash
command, renders the prompt, and can restrict available tools through a
whitelist.

## Subagents

`SpawnAgentTool` starts an isolated child agent with its own context and run
events. Parent and child runs are connected through event bridging and a
background task registry.

## MCP

`McpServerManager` discovers external MCP tools and wraps them as ordinary
runtime tools. MCP tools therefore reuse the same `ToolRegistry`,
`PermissionManager`, event stream, and failure handling as built-in tools.
