from __future__ import annotations

from abc import abstractmethod
from uuid import uuid4

from agent_runtime.core.sandbox import (
    ExecResult,
    FileResult,
    ListResult,
    LocalSandboxRuntime,
    SandboxCallContext,
    SandboxKey,
    SandboxRuntime,
)
from agent_runtime.core.tools.base import BaseTool, ToolInvocationContext, ToolResult


def tool_result(result: ExecResult | FileResult | ListResult) -> ToolResult:
    """Adapt the runtime's stable presentation without reformatting it."""
    return ToolResult(
        content=result.content,
        is_error=result.is_error,
        error_type=result.error_type,
    )


class SandboxTool(BaseTool):
    """Bind execution ownership once; keep each invocation's identity local."""

    def __init__(
        self,
        runtime: SandboxRuntime | None = None,
        *,
        sandbox_key: SandboxKey | None = None,
    ) -> None:
        runtime_key = getattr(runtime, "key", None)
        if isinstance(runtime_key, SandboxKey):
            if sandbox_key is not None and sandbox_key != runtime_key:
                raise ValueError("sandbox_key does not match the injected runtime")
            sandbox_key = runtime_key
        self.sandbox_key = sandbox_key or SandboxKey("direct_run", uuid4().hex)
        self.runtime: SandboxRuntime = (
            runtime if runtime is not None else LocalSandboxRuntime(self.sandbox_key)
        )

    def _call_context(self, context: ToolInvocationContext) -> SandboxCallContext:
        return SandboxCallContext(
            key=self.sandbox_key,
            run_id=context.run_id,
            tool_call_id=context.tool_call_id,
            attempt=context.attempt,
            session_id=context.session_id,
        )

    async def invoke(self, params: dict[str, object]) -> ToolResult:
        """Keep standalone calls working without pretending to be journaled."""
        return await self.invoke_with_context(
            params,
            ToolInvocationContext(
                run_id=(
                    self.sandbox_key.id
                    if self.sandbox_key.kind == "direct_run"
                    else uuid4().hex
                ),
                tool_call_id=uuid4().hex,
                session_id=self.sandbox_key.id if self.sandbox_key.kind == "session" else "",
            ),
        )

    @abstractmethod
    async def invoke_with_context(
        self, params: dict[str, object], context: ToolInvocationContext
    ) -> ToolResult: ...
