#!/usr/bin/env python3
"""Generate WIRE_PROTOCOL.md from pydantic models in agent_runtime.core.bus."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from agent_runtime.core.bus.commands import (
    AgentRunCommand,
    AgentRunResult,
    EventSubscribeCommand,
    EventSubscribeResult,
    PermissionRespondCommand,
    PermissionRespondResult,
    PingCommand,
    PongResult,
    RunGetStateCommand,
    RunGetStateResult,
    SessionCloseCommand,
    SessionCloseResult,
    SessionCompactCommand,
    SessionCompactResult,
    SessionCreateCommand,
    SessionCreateResult,
    SessionGetHistoryCommand,
    SessionGetHistoryResult,
    SessionResumeCommand,
    SessionResumeResult,
    SessionSendMessageCommand,
    SessionSendMessageResult,
)
from agent_runtime.core.bus.envelope import EventPushEnvelope
from agent_runtime.core.bus.events import (
    ContextCompactedEvent,
    CoreStartedEvent,
    LlmModelSelectedEvent,
    LlmReasoningEvent,
    LlmTokenEvent,
    LlmUsageEvent,
    LogLineEvent,
    NodeFinishedEvent,
    NodeStartedEvent,
    PermissionDeniedEvent,
    PermissionGrantedEvent,
    PermissionRequestedEvent,
    RunFinishedEvent,
    RunResumedEvent,
    RunStartedEvent,
    RunSuspendedEvent,
    SessionClosedEvent,
    SessionCreatedEvent,
    SessionMessageReceivedEvent,
    SessionResumedEvent,
    SessionWaitingForInputEvent,
    SkillInvokedEvent,
    StateDiffEvent,
    StepFinishedEvent,
    StepStartedEvent,
    SubagentFinishedEvent,
    SubagentStartedEvent,
    ToolCallFailedEvent,
    ToolCallFinishedEvent,
    ToolCallStartedEvent,
)

_OUTPUT_PATH = Path(__file__).parent.parent / "WIRE_PROTOCOL.md"


# 从 pydantic 模型生成一个带字段表、JSON Schema 和可选示例的 Markdown 小节
def _model_section(name: str, model: type, example: dict | None = None) -> str:  # type: ignore[type-arg]
    schema = model.model_json_schema()  # type: ignore[attr-defined]
    props = schema.get("properties", {})
    required: set[str] = set(schema.get("required", []))

    table = ""
    if props:
        table = "\n| Field | Type | Required |\n|---|---|---|\n"
        for field_name, field_info in props.items():
            ftype = field_info.get("type", "object")
            if "anyOf" in field_info:
                ftype = " | ".join(t.get("type", "?") for t in field_info["anyOf"])
            req = "yes" if field_name in required else "no"
            table += f"| `{field_name}` | `{ftype}` | {req} |\n"

    schema_block = f"\n```json\n{json.dumps(schema, indent=2)}\n```\n"

    example_block = ""
    if example:
        example_block = f"\n**Example:**\n\n```json\n{json.dumps(example, indent=2)}\n```\n"

    return f"### {name}\n{table}{schema_block}{example_block}"


def _run_event_example(
    event_type: str,
    run_id: str,
    ts: str,
    *,
    correlation_id: str | None = None,
    session_id: str | None = None,
    node_id: str | None = None,
    **fields: object,
) -> dict[str, object]:
    """Build an example with the additive run-correlation metadata."""

    return {
        "type": event_type,
        "run_id": run_id,
        "correlation_id": correlation_id or run_id,
        "session_id": session_id,
        "node_id": node_id,
        **fields,
        "ts": ts,
    }


# 生成完整的 WIRE_PROTOCOL.md 文档字符串
def generate() -> str:
    run_id = "20260516-100000-abc123def4567890abc123def4567890"
    ts = "2026-05-16T10:00:00.001Z"

    ping_req_example = {
        "jsonrpc": "2.0",
        "id": "u-1",
        "method": "core.ping",
        "params": {"client": "cli/0.0.1"},
    }
    pong_resp_example = {
        "jsonrpc": "2.0",
        "id": "u-1",
        "result": {
            "server_version": "0.2.0",
            "uptime_ms": 12,
            "received_at": ts,
        },
    }
    agent_run_req_example = {
        "jsonrpc": "2.0",
        "id": "u-2",
        "method": "agent.run",
        "params": {"goal": "总结 README.md 的主要章节"},
    }
    agent_run_resp_example = {
        "jsonrpc": "2.0",
        "id": "u-2",
        "result": {"run_id": run_id},
    }
    subscribe_req_example = {
        "jsonrpc": "2.0",
        "id": "u-3",
        "method": "event.subscribe",
        "params": {
            "topics": ["run.*", "step.*", "tool.*", "llm.token"],
            "scope": "global",
            "replay_from_run": None,
            "after_event_seq": 0,
        },
    }
    subscribe_resp_example = {
        "jsonrpc": "2.0",
        "id": "u-3",
        "result": {"subscription_id": "sub-abc123", "replayed_count": 0},
    }
    session_id = "sess-abc123def456"
    session_create_req_example = {
        "jsonrpc": "2.0",
        "id": "u-4",
        "method": "session.create",
        "params": {"mode": "chat", "title": ""},
    }
    session_create_resp_example = {
        "jsonrpc": "2.0",
        "id": "u-4",
        "result": {
            "session_id": session_id,
            "status": "active",
            "resume_token": "<opaque-resume-capability>",
        },
    }
    session_send_req_example = {
        "jsonrpc": "2.0",
        "id": "u-5",
        "method": "session.send_message",
        "params": {"session_id": session_id, "content": "总结 README.md"},
    }
    session_send_resp_example = {
        "jsonrpc": "2.0",
        "id": "u-5",
        "result": {"run_id": run_id},
    }
    session_resume_req_example = {
        "jsonrpc": "2.0",
        "id": "u-6",
        "method": "session.resume",
        "params": {
            "session_id": session_id,
            "resume_token": "<opaque-resume-capability>",
            "expected_revision": "checkpoint-revision-7",
        },
    }
    session_resume_resp_example = {
        "jsonrpc": "2.0",
        "id": "u-6",
        "result": {
            "session_id": session_id,
            "run_id": run_id,
            "status": "suspended",
            "suspension_reason": "permission",
            "checkpoint_revision": "checkpoint-revision-7",
            "event_seq": 8,
            "next_resume_token": "<rotated-resume-capability>",
        },
    }
    run_get_state_req_example = {
        "jsonrpc": "2.0",
        "id": "u-7",
        "method": "run.get_state",
        "params": {"session_id": session_id, "run_id": run_id},
    }
    run_get_state_resp_example = {
        "jsonrpc": "2.0",
        "id": "u-7",
        "result": {
            "session_id": session_id,
            "run_id": run_id,
            "status": "suspended",
            "current_node": "kit_tools",
            "next_node": "kit_tools",
            "suspension_reason": "permission",
            "checkpoint_revision": "checkpoint-revision-7",
            "event_seq": 8,
            "pending_approval_summary": {
                "tool_use_id": "toolu_03",
                "tool_name": "bash",
                "param_preview": "command='git status --short'",
                "interrupt_id": "interrupt-7",
            },
            "resumable": True,
        },
    }
    permission_respond_req_example = {
        "jsonrpc": "2.0",
        "id": "u-8",
        "method": "permission.respond",
        "params": {
            "session_id": session_id,
            "run_id": run_id,
            "tool_use_id": "toolu_03",
            "interrupt_id": "interrupt-7",
            "expected_revision": "checkpoint-revision-7",
            "decision": "allow_once",
        },
    }
    permission_respond_resp_example = {
        "jsonrpc": "2.0",
        "id": "u-8",
        "result": {"ok": True},
    }
    session_compact_req_example = {
        "jsonrpc": "2.0",
        "id": "u-9",
        "method": "session.compact",
        "params": {"session_id": session_id, "focus": "保留当前任务和工具结果"},
    }
    session_compact_resp_example = {
        "jsonrpc": "2.0",
        "id": "u-9",
        "result": {"summary_tokens": 1800, "saved_tokens": 10200},
    }
    event_push_example = {
        "kind": "event",
        "event": _run_event_example(
            "step.started",
            run_id,
            ts,
            session_id=session_id,
            step=1,
        ),
    }

    sections = [
        "# Wire Protocol\n\n",
        "> Generated by `scripts/generate_wire_protocol.py`. **Do not edit manually.**\n\n",
        "## Transport\n\n",
        "- TCP loopback `127.0.0.1:7437` (override via `AGENTRT_HOST` / `AGENTRT_PORT`)\n",
        "- Each message is one `\\n`-terminated JSON line (NDJSON)\n",
        "- Commands use JSON-RPC 2.0 (client → server); Events use `kind=event` envelope (server → client)\n\n",
        "## Commands\n\n",
        "All commands are sent as JSON-RPC 2.0 requests. The `type` field inside `params` is used for routing.\n\n",
        _model_section("PingCommand", PingCommand, ping_req_example),
        "\n",
        _model_section("PongResult", PongResult, pong_resp_example),
        "\n",
        _model_section("AgentRunCommand", AgentRunCommand, agent_run_req_example),
        "\n",
        _model_section("AgentRunResult", AgentRunResult, agent_run_resp_example),
        "\n",
        _model_section("EventSubscribeCommand", EventSubscribeCommand, subscribe_req_example),
        "\n",
        _model_section("EventSubscribeResult", EventSubscribeResult, subscribe_resp_example),
        "\n",
        _model_section("SessionCreateCommand", SessionCreateCommand, session_create_req_example),
        "\n",
        _model_section("SessionCreateResult", SessionCreateResult, session_create_resp_example),
        "\n",
        _model_section(
            "SessionSendMessageCommand", SessionSendMessageCommand, session_send_req_example
        ),
        "\n",
        _model_section(
            "SessionSendMessageResult", SessionSendMessageResult, session_send_resp_example
        ),
        "\n",
        _model_section("SessionGetHistoryCommand", SessionGetHistoryCommand),
        "\n",
        _model_section("SessionGetHistoryResult", SessionGetHistoryResult),
        "\n",
        _model_section("SessionCloseCommand", SessionCloseCommand),
        "\n",
        _model_section("SessionCloseResult", SessionCloseResult),
        "\n",
        _model_section(
            "SessionResumeCommand",
            SessionResumeCommand,
            session_resume_req_example,
        ),
        "\n",
        _model_section(
            "SessionResumeResult",
            SessionResumeResult,
            session_resume_resp_example,
        ),
        "\n",
        _model_section(
            "RunGetStateCommand",
            RunGetStateCommand,
            run_get_state_req_example,
        ),
        "\n",
        _model_section(
            "RunGetStateResult",
            RunGetStateResult,
            run_get_state_resp_example,
        ),
        "\n",
        _model_section(
            "PermissionRespondCommand",
            PermissionRespondCommand,
            permission_respond_req_example,
        ),
        "\n",
        _model_section(
            "PermissionRespondResult",
            PermissionRespondResult,
            permission_respond_resp_example,
        ),
        "\n",
        _model_section(
            "SessionCompactCommand",
            SessionCompactCommand,
            session_compact_req_example,
        ),
        "\n",
        _model_section(
            "SessionCompactResult",
            SessionCompactResult,
            session_compact_resp_example,
        ),
        "\n## Server Push\n\n",
        "Events pushed from daemon to subscribed clients over the same TCP connection.\n\n",
        _model_section("EventPushEnvelope", EventPushEnvelope, event_push_example),
        "\n## IPC Events\n\n",
        "Events sent over the IPC socket (daemon → client).\n\n",
        _model_section("CoreStartedEvent", CoreStartedEvent),
        "\n## Run Events\n\n",
        "Events written to `runs/<run_id>/events.jsonl` and forwarded over IPC to subscribed clients. "
        "Run-scoped payloads preserve their existing `type` and fields while adding optional "
        "`correlation_id`, `session_id`, `node_id`, and `event_seq` metadata. `correlation_id` "
        "identifies the root run across child runs, `session_id` is populated only when a "
        "session exists, `node_id` only for a real engine node, and `event_seq` is the durable "
        "per-run cursor. Older payloads without these fields remain valid.\n\n",
        "`llm.reasoning`, `node.*`, and `state.diff` are typed boundaries reserved for engines "
        "that produce those facts. The default loop engine does not synthesize them.\n\n",
        _model_section(
            "RunStartedEvent",
            RunStartedEvent,
            _run_event_example(
                "run.started",
                run_id,
                ts,
                session_id=session_id,
                goal="总结 README.md",
            ),
        ),
        "\n",
        _model_section(
            "RunFinishedEvent",
            RunFinishedEvent,
            _run_event_example(
                "run.finished",
                run_id,
                ts,
                session_id=session_id,
                status="success",
                reason=None,
                steps=2,
            ),
        ),
        "\n",
        _model_section(
            "RunSuspendedEvent",
            RunSuspendedEvent,
            _run_event_example(
                "run.suspended",
                run_id,
                ts,
                session_id=session_id,
                reason="permission",
                checkpoint_revision="checkpoint-revision-7",
                interrupt_id="interrupt-7",
                event_seq=8,
            ),
        ),
        "\n",
        _model_section(
            "RunResumedEvent",
            RunResumedEvent,
            _run_event_example(
                "run.resumed",
                run_id,
                ts,
                session_id=session_id,
                checkpoint_revision="checkpoint-revision-7",
                resume_epoch=1,
                interrupt_id="interrupt-7",
                event_seq=9,
            ),
        ),
        "\n",
        _model_section(
            "StepStartedEvent",
            StepStartedEvent,
            _run_event_example(
                "step.started",
                run_id,
                ts,
                session_id=session_id,
                step=1,
            ),
        ),
        "\n",
        _model_section(
            "StepFinishedEvent",
            StepFinishedEvent,
            _run_event_example(
                "step.finished",
                run_id,
                ts,
                session_id=session_id,
                step=1,
            ),
        ),
        "\n",
        _model_section(
            "NodeStartedEvent",
            NodeStartedEvent,
            _run_event_example(
                "node.started",
                run_id,
                ts,
                session_id=session_id,
                node_id="model",
            ),
        ),
        "\n",
        _model_section(
            "NodeFinishedEvent",
            NodeFinishedEvent,
            _run_event_example(
                "node.finished",
                run_id,
                ts,
                session_id=session_id,
                node_id="model",
                status="success",
            ),
        ),
        "\n",
        _model_section(
            "StateDiffEvent",
            StateDiffEvent,
            _run_event_example(
                "state.diff",
                run_id,
                ts,
                session_id=session_id,
                node_id="model",
                diff={"step": 1, "status": "running"},
            ),
        ),
        "\n",
        _model_section(
            "ToolCallStartedEvent",
            ToolCallStartedEvent,
            _run_event_example(
                "tool.call_started",
                run_id,
                ts,
                session_id=session_id,
                tool_use_id="toolu_01",
                tool_name="read_file",
                params={"path": "README.md"},
            ),
        ),
        "\n",
        _model_section(
            "ToolCallFinishedEvent",
            ToolCallFinishedEvent,
            _run_event_example(
                "tool.call_finished",
                run_id,
                ts,
                session_id=session_id,
                tool_use_id="toolu_01",
                tool_name="read_file",
                elapsed_ms=3,
            ),
        ),
        "\n",
        _model_section(
            "ToolCallFailedEvent",
            ToolCallFailedEvent,
            _run_event_example(
                "tool.call_failed",
                run_id,
                ts,
                session_id=session_id,
                tool_use_id="toolu_02",
                tool_name="read_file",
                error_class="runtime_error",
                error_message="file not found",
                elapsed_ms=1,
                attempt=1,
            ),
        ),
        "\n",
        _model_section(
            "LlmModelSelectedEvent",
            LlmModelSelectedEvent,
            _run_event_example(
                "llm.model_selected",
                run_id,
                ts,
                session_id=session_id,
                model="claude-sonnet-4-6",
                strategy="static",
            ),
        ),
        "\n",
        _model_section(
            "LlmTokenEvent",
            LlmTokenEvent,
            _run_event_example(
                "llm.token",
                run_id,
                ts,
                session_id=session_id,
                token="The ",
            ),
        ),
        "\n",
        _model_section(
            "LlmReasoningEvent",
            LlmReasoningEvent,
            _run_event_example(
                "llm.reasoning",
                run_id,
                ts,
                session_id=session_id,
                reasoning="Inspect the repository structure first.",
            ),
        ),
        "\n",
        _model_section(
            "LlmUsageEvent",
            LlmUsageEvent,
            _run_event_example(
                "llm.usage",
                run_id,
                ts,
                session_id=session_id,
                input_tokens=512,
                output_tokens=48,
                cache_read_input_tokens=490,
                cache_creation_input_tokens=0,
            ),
        ),
        "\n",
        _model_section(
            "LogLineEvent",
            LogLineEvent,
            _run_event_example(
                "log.line",
                run_id,
                ts,
                session_id=session_id,
                level="INFO",
                source="agent_runtime.core.loop",
                message="step 1 started",
            ),
        ),
        "\n",
        _model_section(
            "ContextCompactedEvent",
            ContextCompactedEvent,
            _run_event_example(
                "context.compacted",
                run_id,
                ts,
                session_id=session_id,
                original_tokens=12000,
                summary_tokens=1800,
            ),
        ),
        "\n## Permission Events\n\n",
        _model_section(
            "PermissionRequestedEvent",
            PermissionRequestedEvent,
            _run_event_example(
                "permission.requested",
                run_id,
                ts,
                session_id=session_id,
                tool_use_id="toolu_03",
                tool_name="bash",
                params={"command": "git status --short"},
                param_preview="git status --short",
            ),
        ),
        "\n",
        _model_section(
            "PermissionGrantedEvent",
            PermissionGrantedEvent,
            _run_event_example(
                "permission.granted",
                run_id,
                ts,
                session_id=session_id,
                tool_use_id="toolu_03",
                decision="allow_once",
            ),
        ),
        "\n",
        _model_section(
            "PermissionDeniedEvent",
            PermissionDeniedEvent,
            _run_event_example(
                "permission.denied",
                run_id,
                ts,
                session_id=session_id,
                tool_use_id="toolu_04",
                decision="deny_once",
            ),
        ),
        "\n## Subagent and Skill Events\n\n",
        _model_section(
            "SubagentStartedEvent",
            SubagentStartedEvent,
            _run_event_example(
                "subagent.started",
                "20260516-100001-def4567890abc123def4567890abc123",
                ts,
                correlation_id=run_id,
                session_id=session_id,
                parent_run_id=run_id,
                description="Inspect tests",
            ),
        ),
        "\n",
        _model_section(
            "SubagentFinishedEvent",
            SubagentFinishedEvent,
            _run_event_example(
                "subagent.finished",
                "20260516-100001-def4567890abc123def4567890abc123",
                ts,
                correlation_id=run_id,
                session_id=session_id,
                parent_run_id=run_id,
                status="success",
            ),
        ),
        "\n",
        _model_section(
            "SkillInvokedEvent",
            SkillInvokedEvent,
            _run_event_example(
                "skill.invoked",
                run_id,
                ts,
                session_id=session_id,
                skill_name="review",
                arguments="README.md",
            ),
        ),
        "\n## Session Events\n\n",
        _model_section(
            "SessionCreatedEvent",
            SessionCreatedEvent,
            {"type": "session.created", "session_id": session_id, "mode": "chat", "ts": ts},
        ),
        "\n",
        _model_section(
            "SessionMessageReceivedEvent",
            SessionMessageReceivedEvent,
            {
                "type": "session.message_received",
                "session_id": session_id,
                "content": "总结 README.md",
                "ts": ts,
            },
        ),
        "\n",
        _model_section(
            "SessionWaitingForInputEvent",
            SessionWaitingForInputEvent,
            {
                "type": "session.waiting_for_input",
                "session_id": session_id,
                "last_run_id": run_id,
                "ts": ts,
            },
        ),
        "\n",
        _model_section(
            "SessionResumedEvent",
            SessionResumedEvent,
            {"type": "session.resumed", "session_id": session_id, "ts": ts},
        ),
        "\n",
        _model_section(
            "SessionClosedEvent",
            SessionClosedEvent,
            {"type": "session.closed", "session_id": session_id, "ts": ts},
        ),
        "\n## Error Codes\n\n",
        "| Code | Name | Meaning |\n",
        "|------|------|---------|\n",
        "| -32700 | Parse Error | Invalid JSON received |\n",
        "| -32600 | Invalid Request | Missing required JSON-RPC fields |\n",
        "| -32601 | Method Not Found | Unknown method |\n",
        "| -32602 | Invalid Params | Parameter validation failed |\n",
        "| -32603 | Internal Error | Handler raised an unhandled exception |\n",
        "| -32000 | Application Error | e.g. another run already in progress |\n",
    ]
    return "".join(sections)


# 解析命令行参数，写出或校验 WIRE_PROTOCOL.md
def main() -> None:
    parser = argparse.ArgumentParser(description="Generate WIRE_PROTOCOL.md")
    parser.add_argument("--check", action="store_true", help="Verify file matches generated output")
    parser.add_argument("--output", default=str(_OUTPUT_PATH))
    args = parser.parse_args()

    content = generate()

    if args.check:
        output_path = Path(args.output)
        if not output_path.exists():
            print(f"ERROR: {output_path} not found — run: make docs", file=sys.stderr)
            sys.exit(1)
        if output_path.read_text(encoding="utf-8") != content:
            print(f"ERROR: {output_path} out of sync with code — run: make docs", file=sys.stderr)
            sys.exit(1)
        print(f"OK: {output_path} is up to date.")
    else:
        output_path = Path(args.output)
        output_path.write_text(content, encoding="utf-8")
        print(f"Generated {output_path}")


if __name__ == "__main__":
    main()
