from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, cast

from agent_runtime.core.engine.base import (
    ExecutionEngine,
    ExecutionEngineConfigurationError,
    ExecutionEngineUnavailableError,
)
from agent_runtime.core.engine.loop_engine import LoopExecutionEngine

if TYPE_CHECKING:
    from agent_runtime.core.compact.compactor import Compactor
    from agent_runtime.core.graph.runtime import GraphRuntime
    from agent_runtime.core.llm.base import LLMProvider
    from agent_runtime.core.permissions.manager import PermissionManager


class EngineBuilder(Protocol):
    def __call__(
        self,
        provider: LLMProvider,
        *,
        permission_manager: PermissionManager | None = None,
        compactor: Compactor | None = None,
    ) -> ExecutionEngine: ...


class EngineResolver(Protocol):
    def __call__(self, name: str) -> EngineBuilder: ...


class GraphRuntimeService(Protocol):
    async def delete_thread(self, thread_id: str) -> None: ...

    async def close(self) -> None: ...


class EngineRouter:
    """Resolve engines and own one lazily-created Graph runtime.

    The router itself has no optional imports. A CoreApp shares one router across
    every per-message AgentRunner, while direct AgentRunner instances receive an
    isolated router of their own.
    """

    def __init__(self, graph_runtime: GraphRuntimeService | None = None) -> None:
        self._graph_runtime = graph_runtime

    def __call__(self, name: str) -> EngineBuilder:
        if name == "loop":
            return LoopExecutionEngine
        if name == "graph":
            return self._resolve_graph_builder()
        raise ExecutionEngineConfigurationError(
            engine=name,
            message=f"Unknown execution engine {name!r}; expected 'loop' or 'graph'.",
        )

    def _resolve_graph_builder(self) -> EngineBuilder:
        try:
            from agent_runtime.core.graph.engine import GraphExecutionEngine
            from agent_runtime.core.graph.runtime import GraphRuntime
        except ModuleNotFoundError as exc:
            missing = exc.name or ""
            if missing == "langgraph" or missing.startswith("langgraph."):
                raise ExecutionEngineUnavailableError(
                    engine="graph",
                    message=(
                        "Execution engine 'graph' requires the optional LangGraph "
                        "dependencies. Install them with `uv sync --extra graph`."
                    ),
                ) from exc
            raise

        if self._graph_runtime is None:
            self._graph_runtime = GraphRuntime()
        runtime = cast("GraphRuntime", self._graph_runtime)

        def build(
            provider: LLMProvider,
            *,
            permission_manager: PermissionManager | None = None,
            compactor: Compactor | None = None,
        ) -> ExecutionEngine:
            return GraphExecutionEngine(
                provider,
                runtime=runtime,
                permission_manager=permission_manager,
                compactor=compactor,
            )

        return build

    async def delete_thread(self, thread_id: str) -> None:
        runtime = self._graph_runtime
        if runtime is not None:
            await runtime.delete_thread(thread_id)

    async def close(self) -> None:
        runtime = self._graph_runtime
        self._graph_runtime = None
        if runtime is not None:
            await runtime.close()


def resolve_engine_builder(name: str) -> EngineBuilder:
    """Compatibility resolver backed by an isolated lazy EngineRouter."""

    return EngineRouter()(name)
