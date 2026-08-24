from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Literal, Protocol, cast

from agent_runtime.core.engine.base import (
    ExecutionEngine,
    ExecutionEngineConfigurationError,
    ExecutionEngineUnavailableError,
)
from agent_runtime.core.engine.loop_engine import LoopExecutionEngine

if TYPE_CHECKING:
    from agent_runtime.core.compact.compactor import Compactor
    from agent_runtime.core.graph.recovery import RecoveryStore
    from agent_runtime.core.graph.runtime import GraphRuntime, GraphStateSummary
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
    async def ensure_open(self) -> None: ...

    async def delete_thread(self, thread_id: str) -> None: ...

    async def latest_state(
        self,
        thread_id: str,
        *,
        session_id: str,
        run_id: str,
    ) -> GraphStateSummary | None: ...

    async def close(self) -> None: ...


class EngineRouter:
    """Resolve engines and own one lazily-created Graph runtime.

    The router itself has no optional imports. A CoreApp shares one router across
    every per-message AgentRunner, while direct AgentRunner instances receive an
    isolated router of their own.
    """

    def __init__(self, graph_runtime: GraphRuntimeService | None = None) -> None:
        self._graph_runtime = graph_runtime
        self._graph_backend: Literal["memory", "sqlite"] = "memory"
        self._graph_sqlite_path: Path | None = None
        self._recovery_store: RecoveryStore | None = None

    def configure_graph(
        self,
        *,
        backend: Literal["memory", "sqlite"],
        sqlite_path: Path | None = None,
        recovery_store: RecoveryStore | None = None,
    ) -> None:
        """Configure the lazy app-scoped Graph runtime before first use."""

        if self._graph_runtime is not None:
            raise ExecutionEngineConfigurationError(
                engine="graph",
                message="The Graph runtime has already been created.",
            )
        self._graph_backend = backend
        self._graph_sqlite_path = sqlite_path
        self._recovery_store = recovery_store

    async def ensure_graph_available(self) -> None:
        """Open the configured saver before provider and MCP construction."""

        runtime = self._resolve_graph_runtime()
        from agent_runtime.core.graph.checkpoint import SqliteCheckpointUnavailableError

        try:
            await runtime.ensure_open()
        except SqliteCheckpointUnavailableError as exc:
            raise ExecutionEngineUnavailableError(
                engine="graph",
                message=(
                    "Execution engine 'graph' with SQLite checkpoints requires the "
                    "optional dependencies. Install them with "
                    "`uv sync --extra graph-sqlite`."
                ),
            ) from exc

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

        # Resolve the runtime only after importing the engine.  Besides preserving
        # the original lazy-loading order, this keeps an internal engine import
        # failure distinguishable from an absent optional LangGraph dependency.
        runtime = self._resolve_graph_runtime()

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

    def _resolve_graph_runtime(self) -> GraphRuntime:
        try:
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
            self._graph_runtime = GraphRuntime(
                checkpoint_backend=self._graph_backend,
                sqlite_path=self._graph_sqlite_path,
                recovery_store=self._recovery_store,
            )
        return cast("GraphRuntime", self._graph_runtime)

    async def latest_graph_state(
        self,
        thread_id: str,
        *,
        session_id: str,
        run_id: str,
    ) -> GraphStateSummary | None:
        runtime = self._resolve_graph_runtime()
        return await runtime.latest_state(
            thread_id,
            session_id=session_id,
            run_id=run_id,
        )

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
