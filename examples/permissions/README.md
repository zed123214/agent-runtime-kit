# Permission Flow

The permission path is easiest to inspect with a risky tool such as `bash` or
`write_file`.

Expected flow:

1. The agent requests a tool.
2. The daemon validates arguments.
3. A permission request event is emitted.
4. The user chooses allow or deny in the TUI.
5. The tool result is injected back into the agent conversation.
