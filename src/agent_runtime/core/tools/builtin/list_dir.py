from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field

from agent_runtime.core.sandbox import ListRequest
from agent_runtime.core.tools.base import ToolInvocationContext, ToolResult
from agent_runtime.core.tools.sandbox import SandboxTool, tool_result

_MAX_DEPTH = 4
_MAX_ENTRIES = 200


class ListDirParams(BaseModel):
    model_config = ConfigDict(extra="ignore")
    path: str = "."
    max_depth: int = Field(default=2, ge=1, le=_MAX_DEPTH)


class ListDirTool(SandboxTool):
    params_model = ListDirParams
    name = "list_dir"
    description = (
        "List the contents of a directory as a tree. "
        "Path must be relative to the current working directory. "
        "Hidden entries (starting with .) are included. "
        f"Maximum depth is {_MAX_DEPTH}, maximum total entries is {_MAX_ENTRIES}."
    )
    input_schema: dict[str, object] = {
        "type": "object",
        "properties": {
            "path": {
                "type": "string",
                "description": "Relative path to the directory (default '.').",
            },
            "max_depth": {
                "type": "integer",
                "description": f"How many levels deep to recurse (default 2, max {_MAX_DEPTH}).",
            },
        },
        "required": [],
    }

    async def invoke_with_context(
        self, params: dict[str, object], context: ToolInvocationContext
    ) -> ToolResult:
        p = ListDirParams.model_validate(params)
        return tool_result(
            await self.runtime.list_dir(
                ListRequest(
                    context=self._call_context(context),
                    path=p.path,
                    max_depth=p.max_depth,
                )
            )
        )
