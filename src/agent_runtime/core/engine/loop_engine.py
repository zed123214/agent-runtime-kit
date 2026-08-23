from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

from agent_runtime.core.context import ExecutionContext
from agent_runtime.core.engine.base import (
    EngineRunConfig,
    ExecutionEngineOperationUnsupportedError,
    RunOutcome,
)
from agent_runtime.core.events.bus import EventBus
from agent_runtime.core.llm.base import LLMProvider
from agent_runtime.core.loop import AgentLoop
from agent_runtime.core.tools.registry import ToolRegistry

if TYPE_CHECKING:
    from agent_runtime.core.compact.compactor import Compactor
    from agent_runtime.core.permissions.manager import PermissionManager


class LoopExecutionEngine:
    """ExecutionEngine adapter that delegates to the existing AgentLoop."""

    name = "loop"

    def __init__(
        self,
        provider: LLMProvider,
        *,
        permission_manager: PermissionManager | None = None,
        compactor: Compactor | None = None,
    ) -> None:
        self._provider = provider
        self._permission_manager = permission_manager
        self._compactor = compactor
        self._active_task: asyncio.Task[object] | None = None

    async def run(
        self,
        context: ExecutionContext,
        *,
        tools: ToolRegistry,
        events: EventBus,
        config: EngineRunConfig,
    ) -> RunOutcome:
        if self._active_task is not None:
            raise RuntimeError("LoopExecutionEngine instances cannot run concurrently")

        active_task = asyncio.current_task()
        if active_task is None:
            raise RuntimeError("LoopExecutionEngine requires an asyncio task")
        self._active_task = active_task

        loop = AgentLoop(
            self._provider,
            tools,
            events,
            permission_manager=self._permission_manager,
            compactor=self._compactor,
            compact_threshold=config.compact_threshold,
            session_id=config.session_id,
        )
        try:
            await loop.run(context)
        except asyncio.CancelledError:
            if not context.is_done():
                context.mark_failed("cancelled")
            raise
        finally:
            self._active_task = None

        return RunOutcome.from_context(context)

    async def resume(
        self,
        context: ExecutionContext,
        *,
        tools: ToolRegistry,
        events: EventBus,
        config: EngineRunConfig,
    ) -> RunOutcome:
        raise ExecutionEngineOperationUnsupportedError(
            engine=self.name,
            operation="resume",
            message=(
                "The loop engine does not persist execution checkpoints; resume is unavailable."
            ),
        )

    async def cancel(self) -> None:
        """Reuse the runtime's existing asyncio task-cancellation semantics."""

        task = self._active_task
        if task is not None and not task.done():
            task.cancel()
