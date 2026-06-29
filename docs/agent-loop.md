# Agent Loop

`AgentLoop` implements a ReAct-style plan-act-observe loop.

1. Send current context and tool schemas to the LLM provider.
2. Stream token and usage events through `EventBus`.
3. Append assistant text and `tool_use` blocks to the context.
4. Validate and invoke requested tools through `ToolRegistry`.
5. Inject each `tool_result` back into the conversation.
6. Continue until the model ends the turn or the runtime hits a max-step limit.

Tool failures are returned as structured tool results when possible. That keeps
the conversation balanced and lets the model recover instead of crashing the
whole run for ordinary tool-level errors.

The loop can also trigger context compaction when usage crosses the configured
threshold and a `Compactor` is available.
