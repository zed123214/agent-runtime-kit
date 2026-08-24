from __future__ import annotations

from contextlib import AsyncExitStack
from pathlib import Path
from typing import Literal, Self, cast

from langgraph.checkpoint.base import BaseCheckpointSaver
from langgraph.checkpoint.memory import InMemorySaver

type CheckpointBackend = Literal["memory", "sqlite"]


class CheckpointConfigurationError(ValueError):
    """Raised when the configured checkpoint backend cannot be opened safely."""


class SqliteCheckpointUnavailableError(RuntimeError):
    """Raised when the optional official SQLite saver is not installed."""


class CheckpointRuntime:
    """Own one LangGraph saver and its process-scoped resource lifecycle."""

    def __init__(
        self,
        backend: CheckpointBackend,
        saver: BaseCheckpointSaver[str],
        exit_stack: AsyncExitStack,
    ) -> None:
        self.backend = backend
        self._saver = saver
        self._exit_stack = exit_stack
        self._closed = False

    @classmethod
    async def open(
        cls,
        backend: CheckpointBackend = "memory",
        *,
        sqlite_path: Path | None = None,
    ) -> Self:
        """Open and set up the selected official LangGraph saver."""

        exit_stack = AsyncExitStack()
        if backend == "memory":
            return cls(backend, InMemorySaver(), exit_stack)
        if backend != "sqlite":
            raise CheckpointConfigurationError(f"Unsupported checkpoint backend: {backend!r}")
        if sqlite_path is None:
            raise CheckpointConfigurationError(
                "The sqlite checkpoint backend requires a configured sqlite_path."
            )

        sqlite_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            from langgraph.checkpoint.sqlite.aio import AsyncSqliteSaver
        except ModuleNotFoundError as exc:
            raise SqliteCheckpointUnavailableError(
                "The sqlite checkpoint backend requires the graph-sqlite extra."
            ) from exc

        try:
            sqlite_saver = await exit_stack.enter_async_context(
                AsyncSqliteSaver.from_conn_string(str(sqlite_path))
            )
            await sqlite_saver.setup()
        except BaseException:
            await exit_stack.aclose()
            raise
        return cls(
            backend,
            cast(BaseCheckpointSaver[str], sqlite_saver),
            exit_stack,
        )

    @property
    def saver(self) -> BaseCheckpointSaver[str]:
        if self._closed:
            raise RuntimeError("CheckpointRuntime is closed")
        return self._saver

    async def delete_thread(self, thread_id: str) -> None:
        cleaned = thread_id.strip()
        if not cleaned:
            raise ValueError("thread_id must not be empty")
        await self.saver.adelete_thread(cleaned)

    async def close(self) -> None:
        if self._closed:
            return
        await self._exit_stack.aclose()
        self._closed = True

    async def __aenter__(self) -> Self:
        if self._closed:
            raise RuntimeError("CheckpointRuntime is closed")
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: object | None,
    ) -> None:
        await self.close()
