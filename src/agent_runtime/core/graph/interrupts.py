from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Literal

from langgraph.types import interrupt

from agent_runtime.core.graph.recovery import hash_tool_input
from agent_runtime.core.llm.types import ToolCallBlock
from agent_runtime.core.permissions.manager import PermissionManager
from agent_runtime.core.permissions.policy import param_preview

type DurablePermissionDecision = Literal[
    "allow_once", "always_allow", "deny_once", "always_deny", "timeout"
]

_DECISIONS = frozenset({"allow_once", "always_allow", "deny_once", "always_deny", "timeout"})


@dataclass(frozen=True, slots=True)
class PermissionInterruptPayload:
    kind: Literal["permission"]
    session_id: str
    run_id: str
    tool_use_id: str
    tool_name: str
    input_hash: str
    param_preview: str
    interrupt_id: str | None = None
    checkpoint_revision: str | None = None
    expires_at: str | None = None

    def as_dict(self) -> dict[str, str | None]:
        return asdict(self)

    @classmethod
    def from_value(cls, value: object) -> PermissionInterruptPayload | None:
        if not isinstance(value, dict) or value.get("kind") != "permission":
            return None
        fields = (
            "session_id",
            "run_id",
            "tool_use_id",
            "tool_name",
            "input_hash",
            "param_preview",
        )
        if not all(isinstance(value.get(field), str) for field in fields):
            return None
        optional_fields = ("interrupt_id", "checkpoint_revision", "expires_at")
        if any(
            value.get(field) is not None and not isinstance(value.get(field), str)
            for field in optional_fields
        ):
            return None
        return cls(
            kind="permission",
            session_id=str(value["session_id"]),
            run_id=str(value["run_id"]),
            tool_use_id=str(value["tool_use_id"]),
            tool_name=str(value["tool_name"]),
            input_hash=str(value["input_hash"]),
            param_preview=str(value["param_preview"]),
            interrupt_id=(
                str(value["interrupt_id"]) if value.get("interrupt_id") is not None else None
            ),
            checkpoint_revision=(
                str(value["checkpoint_revision"])
                if value.get("checkpoint_revision") is not None
                else None
            ),
            expires_at=(str(value["expires_at"]) if value.get("expires_at") is not None else None),
        )


def check_durable_permission(
    manager: PermissionManager | None,
    call: ToolCallBlock,
    *,
    session_id: str,
    run_id: str,
    force_interrupt: bool = False,
) -> tuple[bool, str]:
    """Evaluate a tool before dispatch, interrupting only the durable ASK path."""

    if manager is None:
        return True, "auto_allow"
    if not force_interrupt:
        allowed, decision = manager.evaluate_durable(
            call.name,
            dict(call.input),
            session_id,
        )
        if allowed is not None:
            return allowed, decision

    payload = PermissionInterruptPayload(
        kind="permission",
        session_id=session_id,
        run_id=run_id,
        tool_use_id=call.id,
        tool_name=call.name,
        input_hash=hash_tool_input(call.input),
        param_preview=param_preview(call.name, dict(call.input)),
        expires_at=manager.durable_expires_at(),
    )
    raw_decision: Any = interrupt(payload.as_dict())
    if not isinstance(raw_decision, str) or raw_decision not in _DECISIONS:
        return False, "invalid_decision"
    return (
        manager.apply_durable_response(raw_decision, session_id, call.name),
        raw_decision,
    )
