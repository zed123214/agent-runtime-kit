"""Backend-neutral execution identities and results; no model-visible tool fields."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Literal


@dataclass(frozen=True, slots=True)
class SandboxKey:
    kind: Literal["session", "direct_run"]
    id: str

    def __post_init__(self) -> None:
        if self.kind not in ("session", "direct_run") or not self.id:
            raise ValueError("sandbox key requires a valid kind and a non-empty id")


class SandboxStatus(StrEnum):
    ABSENT = "absent"
    CREATING = "creating"
    READY = "ready"
    BUSY = "busy"
    TERMINATING = "terminating"
    TERMINATED = "terminated"
    FAILED = "failed"


@dataclass(frozen=True, slots=True)
class SandboxSpec:
    backend: str = "local"
    workspace_policy: str = "host_cwd"
    policy_version: str = "local-v1"


@dataclass(slots=True)
class SandboxHandle:
    key: SandboxKey
    sandbox_id: str
    backend: str = "local"
    status: SandboxStatus = SandboxStatus.READY
    namespace: str | None = None
    pod_uid: str | None = None
    endpoint: str | None = None


@dataclass(frozen=True, slots=True)
class SandboxCallContext:
    key: SandboxKey
    run_id: str
    tool_call_id: str
    attempt: int = 1
    session_id: str = ""


@dataclass(frozen=True, slots=True)
class ExecRequest:
    context: SandboxCallContext
    command: str
    timeout_s: int = 60


@dataclass(frozen=True, slots=True)
class ReadRequest:
    context: SandboxCallContext
    path: str


@dataclass(frozen=True, slots=True)
class WriteRequest:
    context: SandboxCallContext
    path: str
    content: str


@dataclass(frozen=True, slots=True)
class ListRequest:
    context: SandboxCallContext
    path: str = "."
    max_depth: int = 2


@dataclass(frozen=True, slots=True)
class ExecResult:
    content: str
    is_error: bool = False
    error_type: str | None = None
    truncated: bool = False
    output: str = ""
    exit_code: int | None = None
    terminal_reason: Literal["completed", "timeout", "runtime_error"] = "completed"
    duration_ms: float = 0.0
    # Local receives one merged pipe; it cannot reconstruct separate stream order.
    stdout: str | None = None
    stderr: str | None = None


@dataclass(frozen=True, slots=True)
class FileResult:
    content: str
    is_error: bool = False
    error_type: str | None = None
    truncated: bool = False


@dataclass(frozen=True, slots=True)
class ListResult:
    content: str
    is_error: bool = False
    error_type: str | None = None
    truncated: bool = False


@dataclass(frozen=True, slots=True)
class ReconcileReport:
    examined: int = 0
    destroyed: int = 0
    errors: tuple[str, ...] = ()


class SandboxClosedError(RuntimeError):
    """The owner has released this key, or closed its manager."""
