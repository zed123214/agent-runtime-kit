"""M0 tool contract cases. Added for later validation; not executed in this delivery."""

from __future__ import annotations

from dataclasses import FrozenInstanceError
from pathlib import Path

import pytest
from pydantic import ValidationError

from agent_runtime.core.sandbox import (
    ExecRequest,
    ExecResult,
    FileResult,
    ListRequest,
    ListResult,
    ReadRequest,
    SandboxCallContext,
    SandboxKey,
    WriteRequest,
)
from agent_runtime.core.tools.base import BaseTool, ToolInvocationContext, ToolResult
from agent_runtime.core.tools.builtin.bash import BashTool
from agent_runtime.core.tools.builtin.list_dir import ListDirTool
from agent_runtime.core.tools.builtin.read_file import ReadFileTool
from agent_runtime.core.tools.builtin.write_file import WriteFileTool


class CaptureRuntime:
    """A runtime without a key property, subprocesses or file access."""

    def __init__(self) -> None:
        self.requests: list[ExecRequest | ReadRequest | WriteRequest | ListRequest] = []

    async def exec(self, request: ExecRequest) -> ExecResult:
        self.requests.append(request)
        return ExecResult(
            content="[exit 7]\npartial\n[truncated]",
            is_error=True,
            error_type="runtime_error",
            truncated=True,
            exit_code=7,
        )

    async def read_text(self, request: ReadRequest) -> FileResult:
        self.requests.append(request)
        return FileResult(content="read preview\n[truncated]", truncated=True)

    async def write_text(self, request: WriteRequest) -> FileResult:
        self.requests.append(request)
        return FileResult(content="write refused", is_error=True, error_type="runtime_error")

    async def list_dir(self, request: ListRequest) -> ListResult:
        self.requests.append(request)
        return ListResult(content="./\n└── .hidden", truncated=False)


async def test_all_four_tools_delegate_typed_requests_and_preserve_result_presentation() -> None:
    runtime = CaptureRuntime()
    key = SandboxKey("session", "parent-session")
    context = ToolInvocationContext("child-run", "call-1", attempt=2, session_id="parent-session")
    call_context = SandboxCallContext(
        key, "child-run", "call-1", attempt=2, session_id="parent-session"
    )

    bash = await BashTool(runtime, sandbox_key=key).invoke_with_context(
        {"command": "printf data", "timeout": 19, "run_id": "untrusted-run"}, context
    )
    read = await ReadFileTool(runtime, sandbox_key=key).invoke_with_context(
        {"path": "/absolute/read.txt", "session_id": "untrusted-session"}, context
    )
    write = await WriteFileTool(runtime, sandbox_key=key).invoke_with_context(
        {"path": "nested/./write.txt", "content": "你好", "attempt": 999}, context
    )
    listing = await ListDirTool(runtime, sandbox_key=key).invoke_with_context({}, context)

    assert runtime.requests == [
        ExecRequest(call_context, "printf data", timeout_s=19),
        ReadRequest(call_context, "/absolute/read.txt"),
        WriteRequest(call_context, "nested/./write.txt", "你好"),
        ListRequest(call_context, ".", max_depth=2),
    ]
    assert bash == ToolResult("[exit 7]\npartial\n[truncated]", True, "runtime_error")
    assert read == ToolResult("read preview\n[truncated]")
    assert write == ToolResult("write refused", True, "runtime_error")
    assert listing == ToolResult("./\n└── .hidden")


async def test_direct_invoke_uses_unique_call_identity_and_existing_defaults() -> None:
    runtime = CaptureRuntime()
    key = SandboxKey("direct_run", "parent-run")
    tool = BashTool(runtime, sandbox_key=key)

    await tool.invoke({"command": "first"})
    await tool.invoke({"command": "second"})

    first, second = runtime.requests
    assert isinstance(first, ExecRequest)
    assert isinstance(second, ExecRequest)
    assert first.timeout_s == second.timeout_s == 60
    assert first.context.key == second.context.key == key
    assert first.context.run_id == second.context.run_id == "parent-run"
    assert first.context.tool_call_id != second.context.tool_call_id
    assert first.context.attempt == second.context.attempt == 1


@pytest.mark.parametrize(
    ("tool_type", "properties", "required"),
    [
        (BashTool, {"command", "timeout"}, ["command"]),
        (ReadFileTool, {"path"}, ["path"]),
        (WriteFileTool, {"path", "content"}, ["path", "content"]),
        (ListDirTool, {"path", "max_depth"}, []),
    ],
)
def test_model_schema_contains_only_original_parameters(
    tool_type: type[BaseTool], properties: set[str], required: list[str]
) -> None:
    schema = tool_type.input_schema
    assert set(schema["properties"]) == properties
    assert schema["required"] == required
    assert tool_type.params_model is not None
    assert set(tool_type.params_model.model_fields) == properties


@pytest.mark.parametrize("timeout", [0, 121])
async def test_direct_invoke_validates_before_runtime_dispatch(timeout: int) -> None:
    runtime = CaptureRuntime()
    with pytest.raises(ValidationError):
        await BashTool(runtime).invoke({"command": "unused", "timeout": timeout})
    assert runtime.requests == []


async def test_no_argument_tools_keep_call_time_cwd_and_absolute_paths(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    writer, reader, listing = WriteFileTool(), ReadFileTool(), ListDirTool()
    monkeypatch.chdir(tmp_path)

    written = await writer.invoke({"path": "nested/marker.txt", "content": "你好"})
    read = await reader.invoke({"path": str(tmp_path / "nested" / "marker.txt")})
    tree = await listing.invoke({})

    assert written == ToolResult("wrote 6 bytes to nested/marker.txt")
    assert read == ToolResult("你好")
    assert tree == ToolResult("./\n└── nested/\n    └── marker.txt")


def test_internal_context_is_immutable() -> None:
    context = ToolInvocationContext("root", "call")
    with pytest.raises(FrozenInstanceError):
        context.run_id = "child"  # type: ignore[misc]


async def test_existing_third_party_tool_only_implements_invoke() -> None:
    class ExistingTool(BaseTool):
        name = "existing"
        description = "External tool compatible with the old interface."
        input_schema: dict[str, object] = {"type": "object"}

        async def invoke(self, params: dict[str, object]) -> ToolResult:
            return ToolResult(str(params["value"]))

    assert await ExistingTool().invoke_with_context(
        {"value": "kept"}, ToolInvocationContext("run", "call")
    ) == ToolResult("kept")
