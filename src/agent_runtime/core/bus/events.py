from __future__ import annotations

from typing import Annotated, Any, Literal

from pydantic import BaseModel, Discriminator, Field

from agent_runtime.core.context import TerminalStatus


class RunEvent(BaseModel):
    """Common metadata for run-scoped events.

    Existing event type names and payload fields remain unchanged. The optional
    metadata is filled by a run-scoped EventBus when the lifecycle provides it.
    """

    run_id: str
    correlation_id: str | None = None
    session_id: str | None = None
    node_id: str | None = None
    event_seq: int | None = Field(default=None, ge=1)


class CoreStartedEvent(BaseModel):
    type: Literal["core.started"] = "core.started"
    listen_addr: str  # e.g. "127.0.0.1:7437"
    version: str


class RunStartedEvent(RunEvent):
    type: Literal["run.started"] = "run.started"
    goal: str
    ts: str  # ISO 8601


class RunFinishedEvent(RunEvent):
    type: Literal["run.finished"] = "run.finished"
    status: TerminalStatus
    reason: str | None = None  # "exceeded_max_steps" | "cancelled" | "llm_error" | ...
    steps: int
    error: dict[str, Any] | None = None
    ts: str


class RunSuspendedEvent(RunEvent):
    type: Literal["run.suspended"] = "run.suspended"
    session_id: str
    reason: Literal["permission", "process_recovery", "outcome_unknown"]
    checkpoint_revision: str
    interrupt_id: str | None = None
    ts: str


class RunResumedEvent(RunEvent):
    type: Literal["run.resumed"] = "run.resumed"
    session_id: str
    checkpoint_revision: str
    resume_epoch: int = Field(ge=1)
    interrupt_id: str | None = None
    ts: str


class StepStartedEvent(RunEvent):
    type: Literal["step.started"] = "step.started"
    step: int
    ts: str


class StepFinishedEvent(RunEvent):
    type: Literal["step.finished"] = "step.finished"
    step: int
    ts: str


class NodeStartedEvent(RunEvent):
    type: Literal["node.started"] = "node.started"
    node_id: str
    ts: str


class NodeFinishedEvent(RunEvent):
    type: Literal["node.finished"] = "node.finished"
    node_id: str
    status: str
    ts: str


class StateDiffEvent(RunEvent):
    type: Literal["state.diff"] = "state.diff"
    node_id: str
    diff: dict[str, Any]
    ts: str


class ToolCallStartedEvent(RunEvent):
    type: Literal["tool.call_started"] = "tool.call_started"
    tool_use_id: str
    tool_name: str
    params: dict[str, Any]
    ts: str


class ToolCallFinishedEvent(RunEvent):
    type: Literal["tool.call_finished"] = "tool.call_finished"
    tool_use_id: str
    tool_name: str
    elapsed_ms: int
    output: str = ""  # tool result content, for TUI display
    ts: str


class ToolCallFailedEvent(RunEvent):
    type: Literal["tool.call_failed"] = "tool.call_failed"
    tool_use_id: str
    tool_name: str
    # "runtime_error" | "timeout" | "schema_error" | "permission_denied" | "rate_limited"
    error_class: str
    error_message: str
    elapsed_ms: int
    attempt: int = 1  # 1=first attempt, 2=first retry, 3=second retry
    ts: str


class LlmTokenEvent(RunEvent):
    type: Literal["llm.token"] = "llm.token"
    token: str
    ts: str


class LlmReasoningEvent(RunEvent):
    type: Literal["llm.reasoning"] = "llm.reasoning"
    reasoning: str
    ts: str


class LlmUsageEvent(RunEvent):
    type: Literal["llm.usage"] = "llm.usage"
    input_tokens: int
    output_tokens: int
    cache_read_input_tokens: int
    cache_creation_input_tokens: int
    context_pct: float = 0.0
    ts: str


class LlmModelSelectedEvent(RunEvent):
    type: Literal["llm.model_selected"] = "llm.model_selected"
    model: str
    strategy: str  # "static" | "rule_based" | "cost_budget"
    ts: str


class LogLineEvent(RunEvent):
    type: Literal["log.line"] = "log.line"
    level: str  # "DEBUG" | "INFO" | "WARNING" | "ERROR"
    source: str
    message: str
    ts: str


class SessionCreatedEvent(BaseModel):
    type: Literal["session.created"] = "session.created"
    session_id: str
    mode: str
    ts: str


class SessionMessageReceivedEvent(BaseModel):
    type: Literal["session.message_received"] = "session.message_received"
    session_id: str
    content: str
    ts: str


class SessionWaitingForInputEvent(BaseModel):
    type: Literal["session.waiting_for_input"] = "session.waiting_for_input"
    session_id: str
    last_run_id: str
    ts: str


class SessionResumedEvent(BaseModel):
    type: Literal["session.resumed"] = "session.resumed"
    session_id: str
    ts: str


class SessionClosedEvent(BaseModel):
    type: Literal["session.closed"] = "session.closed"
    session_id: str
    ts: str


class ContextCompactedEvent(RunEvent):
    type: Literal["context.compacted"] = "context.compacted"
    session_id: str
    original_tokens: int
    summary_tokens: int
    ts: str


class PermissionRequestedEvent(RunEvent):
    type: Literal["permission.requested"] = "permission.requested"
    tool_use_id: str
    tool_name: str
    params: dict[str, Any]
    param_preview: str
    session_id: str
    interrupt_id: str | None = None
    checkpoint_revision: str | None = None
    expires_at: str | None = None
    ts: str


class PermissionGrantedEvent(RunEvent):
    type: Literal["permission.granted"] = "permission.granted"
    tool_use_id: str
    # "allow_once" | "always_allow" | "auto_allow"
    decision: str
    ts: str


class PermissionDeniedEvent(RunEvent):
    type: Literal["permission.denied"] = "permission.denied"
    tool_use_id: str
    # "deny_once" | "always_deny" | "auto_deny"
    decision: str
    ts: str


class SubagentStartedEvent(RunEvent):
    type: Literal["subagent.started"] = "subagent.started"
    parent_run_id: str
    description: str
    ts: str


class SubagentFinishedEvent(RunEvent):
    type: Literal["subagent.finished"] = "subagent.finished"
    parent_run_id: str
    status: str  # "success" | "failed"
    ts: str


class SkillInvokedEvent(RunEvent):
    type: Literal["skill.invoked"] = "skill.invoked"
    skill_name: str
    arguments: str
    ts: str


class SandboxLifecycleEvent(RunEvent):
    """Only lifecycle transitions with a currently bound run enter the wire bus."""

    type: Literal[
        "sandbox.creating",
        "sandbox.ready",
        "sandbox.failed",
        "sandbox.terminating",
        "sandbox.terminated",
    ]
    sandbox_id: str
    backend: Literal["kubernetes"] = "kubernetes"
    key_kind: Literal["session", "direct_run"]
    key_id: str
    namespace: str | None = None
    pod_uid: str | None = None
    image_digest: str | None = None
    policy_version: str
    resource_profile: str | None = None
    terminal_reason: str | None = None
    ts: str


# 根据 type 字段决定事件类型的判别联合
Event = Annotated[
    CoreStartedEvent
    | RunStartedEvent
    | RunFinishedEvent
    | RunSuspendedEvent
    | RunResumedEvent
    | StepStartedEvent
    | StepFinishedEvent
    | NodeStartedEvent
    | NodeFinishedEvent
    | StateDiffEvent
    | ToolCallStartedEvent
    | ToolCallFinishedEvent
    | ToolCallFailedEvent
    | LlmTokenEvent
    | LlmReasoningEvent
    | LlmUsageEvent
    | LlmModelSelectedEvent
    | LogLineEvent
    | SessionCreatedEvent
    | SessionMessageReceivedEvent
    | SessionWaitingForInputEvent
    | SessionResumedEvent
    | SessionClosedEvent
    | ContextCompactedEvent
    | PermissionRequestedEvent
    | PermissionGrantedEvent
    | PermissionDeniedEvent
    | SubagentStartedEvent
    | SubagentFinishedEvent
    | SkillInvokedEvent
    | SandboxLifecycleEvent,
    Discriminator("type"),
]
