# Session Memory

Agent Runtime Kit treats runtime memory as a session concern.

## Files

- `thread.jsonl`: append-only message stream, including tool use and tool
  result pairs needed for provider-compatible replay.
- `notes.md`: curated long-term facts recorded by the agent through a note tool.
- `summary_*.md`: compacted summaries created when a session needs context
  reduction.

## Context Governance

The runtime keeps long conversations usable through several mechanisms:

- tool result truncation for oversized outputs;
- context usage watermarks from provider usage events;
- compact summaries that replace bulky history with a concise continuation
  state;
- explicit session locks so one session cannot run overlapping tasks.
