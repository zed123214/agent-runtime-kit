"""Offline Loop/Graph sandbox routing regression; added, not executed in M0."""

from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import BaseModel

from agent_runtime.core.config import EngineName, RuntimeConfig
from agent_runtime.core.engine.base import RunOutcome
from agent_runtime.core.engine.router import EngineRouter
from agent_runtime.core.events.bus import EventBus
from agent_runtime.core.llm.types import LlmResponse, ToolCallBlock
from agent_runtime.core.runner import AgentRunner
from agent_runtime.core.sandbox import (
    ExecRequest,
    ExecResult,
    FileResult,
    ListRequest,
    ListResult,
    LocalSandboxBackend,
    ReadRequest,
    SandboxCallContext,
    SandboxHandle,
    SandboxKey,
    SandboxManager,
    SandboxSpec,
    WriteRequest,
)


class RecordingBackend(LocalSandboxBackend):
    def __init__(self) -> None:
        super().__init__()
        self.ensured: list[SandboxKey] = []
        self.destroyed: list[SandboxKey] = []

    async def ensure(self, key: SandboxKey, spec: SandboxSpec) -> SandboxHandle:
        self.ensured.append(key)
        return await super().ensure(key, spec)

    async def destroy(self, handle: SandboxHandle, reason: str) -> None:
        self.destroyed.append(handle.key)
        await super().destroy(handle, reason)


class RecordingRuntime:
    """Capture requests without executing commands or accessing a workspace."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, SandboxCallContext]] = []

    async def exec(self, request: ExecRequest) -> ExecResult:
        self.calls.append(("bash", request.context))
        assert request.command == "printf sandbox"
        assert request.timeout_s == 60
        return ExecResult(content="sandbox", output="sandbox", exit_code=0)

    async def read_text(self, request: ReadRequest) -> FileResult:
        self.calls.append(("read_file", request.context))
        assert request.path == "example.txt"
        return FileResult(content="example")

    async def write_text(self, request: WriteRequest) -> FileResult:
        self.calls.append(("write_file", request.context))
        assert request.path == "example.txt"
        assert request.content == "example"
        return FileResult(content="wrote 7 bytes to example.txt")

    async def list_dir(self, request: ListRequest) -> ListResult:
        self.calls.append(("list_dir", request.context))
        assert request.path == "."
        assert request.max_depth == 2
        return ListResult(content="./\n└── example.txt")


class FourToolsProvider:
    def __init__(self) -> None:
        self.calls = 0

    async def chat(
        self,
        messages: list[dict[str, object]],
        tool_schemas: list[dict[str, object]],
        bus: EventBus,
        run_id: str,
        *,
        step: int = 0,
        system: str | None = None,
    ) -> LlmResponse:
        self.calls += 1
        if self.calls > 1:
            return LlmResponse(stop_reason="end_turn", text="done")
        return LlmResponse(
            stop_reason="tool_use",
            tool_calls=[
                ToolCallBlock(
                    id="write", name="write_file", input={"path": "example.txt", "content": "example"}
                ),
                ToolCallBlock(id="exec", name="bash", input={"command": "printf sandbox"}),
                ToolCallBlock(id="read", name="read_file", input={"path": "example.txt"}),
                ToolCallBlock(id="list", name="list_dir", input={}),
            ],
        )


@pytest.mark.parametrize("engine_name", ["loop", "graph"])
async def test_real_engine_uses_injected_runtime_and_keeps_tool_event_order(
    engine_name: EngineName, tmp_path: Path
) -> None:
    if engine_name == "graph":
        pytest.importorskip("langgraph")
    backend = RecordingBackend()
    runtime = RecordingRuntime()
    manager = SandboxManager(backend, runtime_factory=lambda _handle: runtime)
    router = EngineRouter()
    events: list[dict[str, object]] = []

    async def collect(event: BaseModel) -> None:
        events.append(event.model_dump())

    config = RuntimeConfig()
    config.agent.engine = engine_name
    config.agent.max_steps = 5
    run_id = f"sandbox-{engine_name}"
    key = SandboxKey("direct_run", run_id)
    runner = AgentRunner(
        config,
        provider=FourToolsProvider(),
        runs_dir=tmp_path,
        engine_resolver=router,
        extra_handlers=[collect],
        sandbox_manager=manager,
    )
    try:
        outcome = await runner.run_and_capture("exercise all four tools", run_id=run_id)
        assert isinstance(outcome, RunOutcome)
        assert outcome.status == "success"
        assert backend.ensured == [key]
        assert backend.destroyed == [key]
        assert [name for name, _ in runtime.calls] == [
            "write_file", "bash", "read_file", "list_dir"
        ]
        assert [context.tool_call_id for _, context in runtime.calls] == [
            "write", "exec", "read", "list"
        ]
        assert all(
            context.key == key
            and context.run_id == run_id
            and context.session_id == ""
            and context.attempt == 1
            for _, context in runtime.calls
        )
        for call_id in ("write", "exec", "read", "list"):
            tool_events = [event for event in events if event.get("tool_use_id") == call_id]
            assert [event["type"] for event in tool_events] == [
                "tool.call_started", "tool.call_finished"
            ]
            params = tool_events[0]["params"]
            assert isinstance(params, dict)
            assert not {"run_id", "session_id", "tool_call_id", "attempt", "sandbox_key"} & params.keys()
        terminal = [event for event in events if event.get("type") == "run.finished"]
        assert len(terminal) == 1
        assert terminal[0]["status"] == "success"
    finally:
        await manager.close()
        await router.close()
