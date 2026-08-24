from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, Protocol, cast, runtime_checkable

from agent_runtime.core.context import ExecutionContext, TerminalStatus
from agent_runtime.core.events.bus import EventBus
from agent_runtime.core.tools.registry import ToolRegistry


@dataclass(frozen=True, slots=True)
class EngineRunConfig:
    """Run-scoped options shared by execution-engine implementations."""

    session_id: str = ""
    thread_id: str = ""
    retain_thread: bool = False
    compact_threshold: float = 0.0
    recursion_limit: int | None = None
    tool_call_budget: int = 64
    wall_time_s: float = 300.0
    trace_event_limit: int = 64
    expected_checkpoint_revision: str | None = None
    resume_value: object | None = None
    event_seq: int = 0
    durable_recovery: bool = False


@dataclass(frozen=True, slots=True)
class EngineErrorDetail:
    """Stable, machine-readable detail for engine selection and operation errors."""

    code: str
    engine: str
    message: str
    operation: str | None = None

    def as_dict(self) -> dict[str, str | None]:
        return {
            "code": self.code,
            "engine": self.engine,
            "message": self.message,
            "operation": self.operation,
        }


@dataclass(frozen=True, slots=True)
class RunOutcome:
    """Terminal execution result returned across the engine boundary.

    ``error`` may accompany a failed outcome when its status, result, reason,
    and step count match the canonical ``ExecutionContext``. Successful
    outcomes cannot carry an error. ``AgentRunner`` enforces these invariants
    before publishing the terminal event or persisting session state.
    """

    status: TerminalStatus
    result: str
    reason: str | None
    steps: int = 0
    error: EngineErrorDetail | None = None
    checkpoint_revision: str | None = None

    @classmethod
    def from_context(
        cls,
        context: ExecutionContext,
        *,
        error: EngineErrorDetail | None = None,
        checkpoint_revision: str | None = None,
    ) -> RunOutcome:
        return cls(
            # Engine adapters call this after orchestration finishes. AgentRunner
            # validates both this value and the canonical context at the
            # production boundary before it persists or publishes the outcome.
            status=cast(TerminalStatus, context.status),
            result=context.result,
            reason=context.reason,
            steps=context.step,
            error=error,
            checkpoint_revision=checkpoint_revision,
        )


type SuspensionReason = Literal["permission", "process_recovery", "outcome_unknown"]


@dataclass(frozen=True, slots=True)
class RunSuspension:
    """Non-terminal durable Graph result returned to the runner."""

    run_id: str
    session_id: str
    reason: SuspensionReason
    checkpoint_revision: str
    interrupt_id: str | None
    event_seq: int


type EngineRunResult = RunOutcome | RunSuspension


class ExecutionEngineError(RuntimeError):
    """Base exception for typed execution-engine failures."""

    def __init__(self, detail: EngineErrorDetail) -> None:
        super().__init__(detail.message)
        self.detail = detail


class ExecutionEngineUnavailableError(ExecutionEngineError):
    def __init__(self, engine: str, message: str) -> None:
        super().__init__(
            EngineErrorDetail(code="engine_unavailable", engine=engine, message=message)
        )


class ExecutionEngineConfigurationError(ExecutionEngineError):
    def __init__(self, engine: str, message: str) -> None:
        super().__init__(
            EngineErrorDetail(code="engine_configuration_error", engine=engine, message=message)
        )


class ExecutionEngineContractError(ExecutionEngineError):
    def __init__(self, engine: str, message: str) -> None:
        super().__init__(
            EngineErrorDetail(code="engine_contract_error", engine=engine, message=message)
        )


class ExecutionEngineOperationUnsupportedError(ExecutionEngineError):
    def __init__(self, engine: str, operation: str, message: str) -> None:
        super().__init__(
            EngineErrorDetail(
                code=f"{operation}_unsupported",
                engine=engine,
                operation=operation,
                message=message,
            )
        )


@runtime_checkable
class ExecutionEngine(Protocol):
    """Per-run execution boundary used by AgentRunner.

    The runner owns dependency assembly and event persistence. Engines own only
    orchestration and reuse the existing ExecutionContext, ToolRegistry and
    EventBus supplied for that run. ``run`` must update that exact context in
    place and return the equivalent RunOutcome; the runner treats the context as
    canonical for terminal events and session persistence.
    """

    name: str

    async def run(
        self,
        context: ExecutionContext,
        *,
        tools: ToolRegistry,
        events: EventBus,
        config: EngineRunConfig,
    ) -> EngineRunResult: ...

    async def resume(
        self,
        context: ExecutionContext,
        *,
        tools: ToolRegistry,
        events: EventBus,
        config: EngineRunConfig,
    ) -> EngineRunResult: ...

    async def cancel(self) -> None: ...
