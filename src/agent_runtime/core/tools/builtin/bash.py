from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field

from agent_runtime.core.sandbox import ExecRequest
from agent_runtime.core.tools.base import ToolInvocationContext, ToolResult
from agent_runtime.core.tools.sandbox import SandboxTool, tool_result

_MAX_OUTPUT_BYTES = 64 * 1024  # 64 KB
_DEFAULT_TIMEOUT = 60


class BashParams(BaseModel):
    model_config = ConfigDict(extra="ignore")
    command: str
    timeout: int = Field(default=_DEFAULT_TIMEOUT, ge=1, le=120)


class BashTool(SandboxTool):
    params_model = BashParams
    name = "bash"
    description = (
        "Execute a shell command and return its output (stdout + stderr combined). "
        "Non-interactive only — commands requiring user input will hang and time out. "
        "Prefer short, focused commands. Output is truncated at 64 KB."
    )
    input_schema: dict[str, object] = {
        "type": "object",
        "properties": {
            "command": {
                "type": "string",
                "description": "Shell command to execute.",
            },
            "timeout": {
                "type": "integer",
                "description": f"Maximum seconds to wait (default {_DEFAULT_TIMEOUT}, max 120).",
            },
        },
        "required": ["command"],
    }

    async def invoke_with_context(
        self, params: dict[str, object], context: ToolInvocationContext
    ) -> ToolResult:
        p = BashParams.model_validate(params)
        return tool_result(
            await self.runtime.exec(
                ExecRequest(
                    context=self._call_context(context),
                    command=p.command,
                    timeout_s=p.timeout,
                )
            )
        )
