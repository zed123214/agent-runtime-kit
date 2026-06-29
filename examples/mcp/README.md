# MCP Example

MCP servers can be configured in the runtime config and exposed through the same
`ToolRegistry` path as built-in tools.

Example config shape:

```toml
[[mcp.servers]]
name = "local-tools"
transport = "stdio"
command = "python"
args = ["path/to/server.py"]
```
