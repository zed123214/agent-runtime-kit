from __future__ import annotations

from pydantic import BaseModel, ConfigDict

from agent_runtime.core.sandbox import ReadRequest
from agent_runtime.core.tools.base import ToolInvocationContext, ToolResult
from agent_runtime.core.tools.sandbox import SandboxTool, tool_result

_MAX_BYTES = 512 * 1024  # 512 KB


class ReadFileParams(BaseModel):
    model_config = ConfigDict(extra="ignore")
    path: str


class ReadFileTool(SandboxTool):
    params_model = ReadFileParams
    name = "read_file"
    description = (
        "Read the text content of a file. "
        "Path must be relative to the current working directory. "
        "Files larger than 512 KB are truncated."
    )
    input_schema: dict[str, object] = {
        "type": "object",
        "properties": {
            "path": {
                "type": "string",
                "description": "Relative path to the file (relative to current working directory).",
            }
        },
        "required": ["path"],
    }

    async def invoke_with_context(
        self, params: dict[str, object], context: ToolInvocationContext
    ) -> ToolResult:
        p = ReadFileParams.model_validate(params)
        return tool_result(
            await self.runtime.read_text(
                ReadRequest(context=self._call_context(context), path=p.path)
            )
        )
