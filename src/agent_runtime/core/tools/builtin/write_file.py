from __future__ import annotations

from pydantic import BaseModel, ConfigDict

from agent_runtime.core.sandbox import WriteRequest
from agent_runtime.core.tools.base import ToolInvocationContext, ToolResult
from agent_runtime.core.tools.sandbox import SandboxTool, tool_result

_MAX_BYTES = 1 * 1024 * 1024  # 1 MB


class WriteFileParams(BaseModel):
    model_config = ConfigDict(extra="ignore")
    path: str
    content: str


class WriteFileTool(SandboxTool):
    params_model = WriteFileParams
    name = "write_file"
    description = (
        "Write text content to a file, creating it (and any parent directories) if it "
        "does not exist, or overwriting it if it does. "
        "Path must be relative to the current working directory. "
        "Content size is limited to 1 MB."
    )
    input_schema: dict[str, object] = {
        "type": "object",
        "properties": {
            "path": {
                "type": "string",
                "description": "Relative path to the file (relative to current working directory).",
            },
            "content": {
                "type": "string",
                "description": "Text content to write.",
            },
        },
        "required": ["path", "content"],
    }

    async def invoke_with_context(
        self, params: dict[str, object], context: ToolInvocationContext
    ) -> ToolResult:
        p = WriteFileParams.model_validate(params)
        return tool_result(
            await self.runtime.write_text(
                WriteRequest(
                    context=self._call_context(context),
                    path=p.path,
                    content=p.content,
                )
            )
        )
