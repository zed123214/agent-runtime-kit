# Project Highlights

This document maps the repository to portfolio and resume claims. It is meant
to make the project easy to evaluate from a GitHub link.

## Positioning

Agent Runtime Kit is a local AI Agent runtime framework for developer
automation. It is not positioned as a chat client or a thin LLM API wrapper.

The project focuses on the infrastructure needed to turn an LLM into a
long-running, observable, permission-controlled, and extensible local agent
system.

## Design Highlights

1. **Daemon-first runtime**

   `agentrt-core` owns execution state, while `agentrt` and `agentrt-tui` are
   thin clients. This separates frontend lifecycle from agent execution and
   allows clients to reconnect to existing runs.

2. **Typed protocol boundary**

   IPC uses JSON-RPC 2.0 over NDJSON TCP. Requests, responses, errors, and
   events are modeled with Pydantic, and `WIRE_PROTOCOL.md` is generated from
   source models to reduce protocol drift.

3. **Recoverable tool execution loop**

   `AgentLoop` handles streaming model output, `tool_use`, schema validation,
   permission checks, tool execution, `tool_result` injection, max-step limits,
   cancellation, and failure states.

4. **Runtime-level permission system**

   Tool calls are authorized before execution. Decisions can be scoped to one
   call or persisted for future calls, and permission events are visible to
   clients through the event stream.

5. **Replayable observability**

   Agent execution is represented as append-only events. Events are written to
   JSONL and pushed to subscribed clients, so disconnected clients can replay
   historical events before receiving live updates.

6. **Session memory and context governance**

   Session state is split between full message history and curated notes.
   Context governance includes tool-result truncation, context watermarks, and
   compact summaries.

7. **Unified extension model**

   Built-in tools, skills, subagents, and MCP tools reuse the same registry,
   permission, event, and runner primitives instead of living as separate
   one-off integrations.

## Resume Bullets

- Built a local AI Agent runtime in Python with daemon execution, CLI/TUI
  clients, JSON-RPC 2.0 over NDJSON TCP, and Pydantic-typed protocol models.
- Implemented a ReAct-style LLM/tool execution loop with schema validation,
  tool-result injection, cancellation, max-step limits, and failure recovery.
- Designed runtime-level tool permissions with event-driven approval flow and
  replayable JSONL event logs for multi-client observability.
- Implemented persistent session memory, curated notes, context compaction, and
  a unified extension model for skills, subagents, and MCP tools.

## What Not To Oversell

- Do not frame the project as a general production agent platform.
- Do not present the current provider implementation as provider-agnostic
  completion; the runtime has provider boundaries, while the implemented
  provider is Anthropic.
- Do not commit real API keys, local sessions, logs, or runtime workspaces.
