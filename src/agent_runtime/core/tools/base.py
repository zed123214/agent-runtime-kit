from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import ClassVar

from pydantic import BaseModel


@dataclass
class ToolResult:
    content: str
    is_error: bool = False
    # "runtime_error" | "timeout" | "schema_error" | "permission_denied"
    error_type: str | None = None


@dataclass(frozen=True)
class ToolInvocationContext:
    """Trusted invocation identity, separate from model-visible parameters."""

    run_id: str
    tool_call_id: str
    attempt: int = 1
    session_id: str = ""
    event_sink: Callable[[BaseModel], Awaitable[None]] | None = None


class BaseTool(ABC):
    name: str
    description: str
    input_schema: dict[str, object]
    params_model: ClassVar[type[BaseModel] | None] = None

    @property
    def remote_execution(self) -> bool:
        return False

    # 执行工具调用，返回结果或错误
    @abstractmethod
    async def invoke(self, params: dict[str, object]) -> ToolResult: ...

    async def invoke_with_context(
        self, params: dict[str, object], context: ToolInvocationContext
    ) -> ToolResult:
        """Internal hook; existing third-party tools only need ``invoke``."""
        return await self.invoke(params)
