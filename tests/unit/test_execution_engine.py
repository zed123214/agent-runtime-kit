from __future__ import annotations

import asyncio
import builtins
import logging
import sys
from pathlib import Path
from typing import get_type_hints

import pytest
from pydantic import BaseModel, ValidationError

from agent_runtime.core.bus.events import RunFinishedEvent
from agent_runtime.core.config import RuntimeConfig, get_config
from agent_runtime.core.context import ExecutionContext, ExecutionStatus, TerminalStatus
from agent_runtime.core.engine.base import (
    EngineErrorDetail,
    EngineRunConfig,
    ExecutionEngine,
    ExecutionEngineConfigurationError,
    ExecutionEngineOperationUnsupportedError,
    ExecutionEngineUnavailableError,
    RunOutcome,
)
from agent_runtime.core.engine.loop_engine import LoopExecutionEngine
from agent_runtime.core.engine.router import resolve_engine_builder
from agent_runtime.core.events.bus import EventBus
from agent_runtime.core.llm.types import LlmResponse, ToolCallBlock
from agent_runtime.core.loop import AgentLoop
from agent_runtime.core.runner import AgentRunner
from agent_runtime.core.tools.base import BaseTool, ToolResult
from agent_runtime.core.tools.registry import ToolRegistry


class _SequenceProvider:
    def __init__(self, responses: list[LlmResponse]) -> None:
        self._responses = iter(responses)
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
        return next(self._responses)


class _BlockingProvider:
    def __init__(self) -> None:
        self.entered = asyncio.Event()

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
        self.entered.set()
        await asyncio.Event().wait()
        raise AssertionError("unreachable")


class _MustNotBeCalledProvider:
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
        raise AssertionError("the injected engine must own execution")


class _EchoTool(BaseTool):
    name = "echo"
    description = "Return the supplied text"
    input_schema: dict[str, object] = {
        "type": "object",
        "properties": {"text": {"type": "string"}},
        "required": ["text"],
    }

    def __init__(self) -> None:
        self.calls = 0

    async def invoke(self, params: dict[str, object]) -> ToolResult:
        self.calls += 1
        return ToolResult(content=str(params["text"]))


class _BlockingTool(BaseTool):
    name = "block"
    description = "Block until the execution task is cancelled"
    input_schema: dict[str, object] = {"type": "object", "properties": {}}

    def __init__(self) -> None:
        self.entered = asyncio.Event()

    async def invoke(self, params: dict[str, object]) -> ToolResult:
        self.entered.set()
        await asyncio.Event().wait()
        raise AssertionError("unreachable")


def _context(run_id: str = "run-engine", *, max_steps: int = 5) -> ExecutionContext:
    return ExecutionContext(run_id=run_id, goal="test goal", max_steps=max_steps)


def _tool_run_responses() -> list[LlmResponse]:
    return [
        LlmResponse(
            stop_reason="tool_use",
            tool_calls=[
                ToolCallBlock(
                    id="tool-1",
                    name="echo",
                    input={"text": "hello"},
                )
            ],
        ),
        LlmResponse(stop_reason="end_turn", text="finished"),
    ]


async def _collect_events(bus: EventBus) -> list[BaseModel]:
    events: list[BaseModel] = []

    async def collect(event: BaseModel) -> None:
        events.append(event)

    bus.subscribe(collect)
    return events


def test_default_runtime_config_selects_loop_engine() -> None:
    assert RuntimeConfig().agent.engine == "loop"


def test_execution_status_annotations_distinguish_running_from_terminal() -> None:
    assert get_type_hints(ExecutionContext)["status"] == ExecutionStatus
    assert get_type_hints(RunOutcome)["status"] == TerminalStatus


def test_run_finished_event_rejects_non_terminal_status() -> None:
    with pytest.raises(ValidationError):
        RunFinishedEvent(
            run_id="run-invalid-terminal",
            status="running",  # type: ignore[arg-type]
            steps=0,
            ts="2026-08-20T00:00:00+00:00",
        )


@pytest.mark.parametrize("engine", ["loop", "graph"])
def test_toml_can_select_engine(
    engine: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config_path = tmp_path / "agentrt.toml"
    config_path.write_text(f'[agent]\nengine = "{engine}"\n', encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("AGENTRT_CONFIG", str(config_path))
    monkeypatch.delenv("AGENTRT_ENGINE", raising=False)

    assert get_config().agent.engine == engine


def test_environment_engine_overrides_toml(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    config_path = tmp_path / "agentrt.toml"
    config_path.write_text('[agent]\nengine = "graph"\n', encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("AGENTRT_CONFIG", str(config_path))
    monkeypatch.setenv("AGENTRT_ENGINE", "loop")

    assert get_config().agent.engine == "loop"


@pytest.mark.parametrize("engine", ["loop", "graph"])
def test_environment_selects_requested_engine(
    engine: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("AGENTRT_CONFIG", raising=False)
    monkeypatch.setenv("AGENTRT_ENGINE", engine)

    assert get_config().agent.engine == engine


@pytest.mark.parametrize(
    ("source", "value"),
    [("toml", "unknown"), ("environment", "unknown")],
)
def test_unknown_engine_configuration_is_rejected(
    source: str,
    value: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("AGENTRT_CONFIG", raising=False)
    monkeypatch.delenv("AGENTRT_ENGINE", raising=False)
    if source == "toml":
        config_path = tmp_path / "agentrt.toml"
        config_path.write_text(
            f'[agent]\nengine = "{value}"\n',
            encoding="utf-8",
        )
        monkeypatch.setenv("AGENTRT_CONFIG", str(config_path))
    else:
        monkeypatch.setenv("AGENTRT_ENGINE", value)

    with pytest.raises(SystemExit, match="(?i)engine"):
        get_config()


def test_router_resolves_loop_without_optional_graph_import() -> None:
    before = {name for name in sys.modules if name.startswith("langgraph")}

    builder = resolve_engine_builder("loop")

    assert builder is LoopExecutionEngine
    assert {name for name in sys.modules if name.startswith("langgraph")} == before


def test_router_reports_graph_as_typed_unavailable_for_exact_missing_dependency(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    before = {name for name in sys.modules if name.startswith("langgraph")}
    real_import = builtins.__import__

    def import_with_missing_langgraph(name, *args, **kwargs):
        if name == "agent_runtime.core.graph.engine":
            raise ModuleNotFoundError("No module named 'langgraph'", name="langgraph")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", import_with_missing_langgraph)

    with pytest.raises(ExecutionEngineUnavailableError) as exc_info:
        resolve_engine_builder("graph")

    assert exc_info.value.detail.code == "engine_unavailable"
    assert exc_info.value.detail.engine == "graph"
    assert {name for name in sys.modules if name.startswith("langgraph")} == before


def test_router_rejects_unknown_engine_with_typed_configuration_error() -> None:
    with pytest.raises(ExecutionEngineConfigurationError) as exc_info:
        resolve_engine_builder("not-an-engine")

    assert exc_info.value.detail.code == "engine_configuration_error"
    assert exc_info.value.detail.engine == "not-an-engine"


async def test_loop_engine_no_tool_run_returns_observable_outcome() -> None:
    provider = _SequenceProvider([LlmResponse(stop_reason="end_turn", text="done")])
    engine = LoopExecutionEngine(provider)  # type: ignore[arg-type]
    context = _context()
    bus = EventBus()
    events = await _collect_events(bus)

    outcome = await engine.run(
        context,
        tools=ToolRegistry(),
        events=bus,
        config=EngineRunConfig(),
    )

    assert isinstance(engine, ExecutionEngine)
    assert outcome == RunOutcome(
        status="success",
        result="done",
        reason=None,
        steps=1,
    )
    assert provider.calls == 1
    assert [event.type for event in events] == [  # type: ignore[attr-defined]
        "step.started",
        "step.finished",
    ]


async def test_loop_engine_tool_run_preserves_steps_calls_and_event_order() -> None:
    provider = _SequenceProvider(_tool_run_responses())
    tool = _EchoTool()
    tools = ToolRegistry()
    tools.register(tool)
    bus = EventBus()
    events = await _collect_events(bus)
    engine = LoopExecutionEngine(provider)  # type: ignore[arg-type]

    outcome = await engine.run(
        _context(),
        tools=tools,
        events=bus,
        config=EngineRunConfig(),
    )

    assert outcome.status == "success"
    assert outcome.result == "finished"
    assert outcome.steps == 2
    assert provider.calls == 2
    assert tool.calls == 1
    assert [event.type for event in events] == [  # type: ignore[attr-defined]
        "step.started",
        "tool.call_started",
        "tool.call_finished",
        "step.finished",
        "step.started",
        "step.finished",
    ]


async def test_loop_engine_matches_direct_agent_loop_for_same_input() -> None:
    direct_provider = _SequenceProvider(_tool_run_responses())
    direct_tool = _EchoTool()
    direct_tools = ToolRegistry()
    direct_tools.register(direct_tool)
    direct_bus = EventBus()
    direct_events = await _collect_events(direct_bus)
    direct_context = _context(run_id="run-differential")

    engine_provider = _SequenceProvider(_tool_run_responses())
    engine_tool = _EchoTool()
    engine_tools = ToolRegistry()
    engine_tools.register(engine_tool)
    engine_bus = EventBus()
    engine_events = await _collect_events(engine_bus)
    engine_context = _context(run_id="run-differential")

    direct_loop = AgentLoop(  # type: ignore[arg-type]
        direct_provider,
        direct_tools,
        direct_bus,
        compact_threshold=0.0,
        session_id="",
    )
    engine = LoopExecutionEngine(engine_provider)  # type: ignore[arg-type]

    await asyncio.wait_for(direct_loop.run(direct_context), timeout=2.0)
    engine_outcome = await asyncio.wait_for(
        engine.run(
            engine_context,
            tools=engine_tools,
            events=engine_bus,
            config=EngineRunConfig(),
        ),
        timeout=2.0,
    )

    assert engine_outcome == RunOutcome.from_context(direct_context)
    assert engine_context == direct_context
    assert engine_provider.calls == direct_provider.calls == 2
    assert engine_tool.calls == direct_tool.calls == 1
    assert [event.type for event in engine_events] == [  # type: ignore[attr-defined]
        event.type
        for event in direct_events  # type: ignore[attr-defined]
    ]


async def test_loop_engine_resume_is_typed_unsupported() -> None:
    engine = LoopExecutionEngine(_MustNotBeCalledProvider())  # type: ignore[arg-type]

    with pytest.raises(ExecutionEngineOperationUnsupportedError) as exc_info:
        await engine.resume(
            _context(),
            tools=ToolRegistry(),
            events=EventBus(),
            config=EngineRunConfig(),
        )

    assert exc_info.value.detail.code == "resume_unsupported"
    assert exc_info.value.detail.engine == "loop"
    assert exc_info.value.detail.operation == "resume"


async def test_loop_engine_cancel_reuses_cancelled_error_semantics() -> None:
    provider = _BlockingProvider()
    engine = LoopExecutionEngine(provider)  # type: ignore[arg-type]
    context = _context()
    task = asyncio.create_task(
        engine.run(
            context,
            tools=ToolRegistry(),
            events=EventBus(),
            config=EngineRunConfig(),
        )
    )
    await asyncio.wait_for(provider.entered.wait(), timeout=2.0)

    await engine.cancel()

    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, timeout=2.0)
    assert context.status == "failed"
    assert context.reason == "cancelled"


async def test_loop_engine_cancel_during_tool_keeps_context_terminal() -> None:
    provider = _SequenceProvider(
        [
            LlmResponse(
                stop_reason="tool_use",
                tool_calls=[ToolCallBlock(id="tool-1", name="block", input={})],
            )
        ]
    )
    blocking_tool = _BlockingTool()
    tools = ToolRegistry()
    tools.register(blocking_tool)
    engine = LoopExecutionEngine(provider)  # type: ignore[arg-type]
    context = _context()
    task = asyncio.create_task(
        engine.run(
            context,
            tools=tools,
            events=EventBus(),
            config=EngineRunConfig(),
        )
    )
    await asyncio.wait_for(blocking_tool.entered.wait(), timeout=2.0)

    await engine.cancel()

    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, timeout=2.0)
    assert context.status == "failed"
    assert context.reason == "cancelled"
    assert context.step == 1


async def test_runner_executes_injected_engine_resolver(tmp_path: Path) -> None:
    observed: dict[str, object] = {}

    class _InjectedEngine:
        name = "injected"

        async def run(
            self,
            context: ExecutionContext,
            *,
            tools: ToolRegistry,
            events: EventBus,
            config: EngineRunConfig,
        ) -> RunOutcome:
            observed["context"] = context
            observed["tools"] = tools
            observed["events"] = events
            observed["config"] = config
            context.result = "from injected engine"
            context.step = 7
            context.mark_success()
            return RunOutcome.from_context(context)

        async def resume(
            self,
            context: ExecutionContext,
            *,
            tools: ToolRegistry,
            events: EventBus,
            config: EngineRunConfig,
        ) -> RunOutcome:
            raise AssertionError("resume was not requested")

        async def cancel(self) -> None:
            raise AssertionError("cancel was not requested")

    def builder(provider, *, permission_manager=None, compactor=None):
        observed["provider"] = provider
        observed["permission_manager"] = permission_manager
        observed["compactor"] = compactor
        return _InjectedEngine()

    def resolver(name: str):
        observed["engine_name"] = name
        return builder

    runner = AgentRunner(
        RuntimeConfig(),
        provider=_MustNotBeCalledProvider(),  # type: ignore[arg-type]
        engine_resolver=resolver,
        runs_dir=tmp_path,
    )

    outcome = await runner.run_and_capture("goal", run_id="run-injected")

    assert outcome == RunOutcome(
        status="success",
        result="from injected engine",
        reason=None,
        steps=7,
    )
    assert observed["engine_name"] == "loop"
    assert isinstance(observed["context"], ExecutionContext)
    assert isinstance(observed["tools"], ToolRegistry)
    assert isinstance(observed["events"], EventBus)
    assert observed["config"] == EngineRunConfig(
        session_id="",
        thread_id="run-injected",
        retain_thread=False,
        compact_threshold=RuntimeConfig().compaction.auto_threshold,
        recursion_limit=RuntimeConfig().graph.recursion_limit,
        tool_call_budget=RuntimeConfig().graph.tool_call_budget,
        wall_time_s=RuntimeConfig().graph.wall_time_s,
        trace_event_limit=RuntimeConfig().graph.trace_event_limit,
    )


@pytest.mark.parametrize(
    ("mode", "retain_thread", "expected_thread_id"),
    [
        ("chat", True, "sess-engine-config"),
        ("one_shot", False, "run-session-config"),
    ],
)
async def test_runner_maps_session_mode_to_explicit_graph_thread_config(
    mode: str,
    retain_thread: bool,
    expected_thread_id: str,
    tmp_path: Path,
) -> None:
    from agent_runtime.core.session.model import Session
    from agent_runtime.core.session.store import SessionStore

    observed: dict[str, EngineRunConfig] = {}

    class _ConfigEngine:
        name = "config-capture"

        async def run(
            self,
            context: ExecutionContext,
            *,
            tools: ToolRegistry,
            events: EventBus,
            config: EngineRunConfig,
        ) -> RunOutcome:
            observed["config"] = config
            context.result = "done"
            context.mark_success()
            return RunOutcome.from_context(context)

        async def resume(
            self,
            context: ExecutionContext,
            *,
            tools: ToolRegistry,
            events: EventBus,
            config: EngineRunConfig,
        ) -> RunOutcome:
            raise AssertionError("resume was not requested")

        async def cancel(self) -> None:
            raise AssertionError("cancel was not requested")

    def builder(provider, *, permission_manager=None, compactor=None):
        return _ConfigEngine()

    def resolver(name: str):
        return builder

    store = SessionStore(tmp_path / "sessions")
    session = Session(
        id="sess-engine-config",
        mode=mode,  # type: ignore[arg-type]
        status="active",
        title="",
        created_at="now",
        updated_at="now",
    )
    store.write_meta(session)
    store.append_message(session.id, "user", "goal")
    config = RuntimeConfig()
    config.graph.recursion_limit = 17
    config.graph.tool_call_budget = 18
    config.graph.wall_time_s = 19.5
    config.graph.trace_event_limit = 20
    runner = AgentRunner(
        config,
        provider=_MustNotBeCalledProvider(),  # type: ignore[arg-type]
        engine_resolver=resolver,
        runs_dir=tmp_path / "runs",
    )

    await runner.run_and_capture(
        "goal",
        run_id="run-session-config",
        session=session,
        store=store,
    )

    run_config = observed["config"]
    assert run_config.session_id == session.id
    assert run_config.thread_id == expected_thread_id
    assert run_config.retain_thread is retain_thread
    assert run_config.recursion_limit == 17
    assert run_config.tool_call_budget == 18
    assert run_config.wall_time_s == 19.5
    assert run_config.trace_event_limit == 20


async def test_graph_auto_compaction_fails_before_resolver_or_provider(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    config = RuntimeConfig()
    config.agent.engine = "graph"
    config.compaction.auto_threshold = 0.5

    def resolver(_name: str):
        raise AssertionError("invalid graph configuration must fail before resolution")

    outcome = await AgentRunner(
        config,
        engine_resolver=resolver,
        runs_dir=tmp_path,
    ).run_and_capture("goal", run_id="run-graph-auto-compact")

    assert outcome.status == "failed"
    assert outcome.reason == "engine_configuration_error"
    assert outcome.error is not None
    assert outcome.error.code == "engine_configuration_error"
    assert outcome.error.engine == "graph"


async def test_runner_does_not_leave_background_subagent_tasks_alive(tmp_path: Path) -> None:
    runner = AgentRunner(
        RuntimeConfig(),
        provider=_SequenceProvider([LlmResponse(stop_reason="end_turn", text="done")]),
        runs_dir=tmp_path,
    )
    child_started = asyncio.Event()

    async def background_child() -> None:
        child_started.set()
        await asyncio.Event().wait()

    child_task = asyncio.create_task(background_child())
    child_context = _context(run_id="background-child")
    runner._task_registry.register("background-child", child_task, child_context)
    await asyncio.wait_for(child_started.wait(), timeout=1.0)

    outcome = await asyncio.wait_for(
        runner.run_and_capture("goal", run_id="run-cleans-background"),
        timeout=2.0,
    )

    assert outcome.status == "success"
    assert child_task.cancelled()


async def test_runner_rejects_engine_outcome_context_mismatch(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    class _MismatchedEngine:
        name = "mismatched"

        async def run(
            self,
            context: ExecutionContext,
            *,
            tools: ToolRegistry,
            events: EventBus,
            config: EngineRunConfig,
        ) -> RunOutcome:
            return RunOutcome(
                status="success",
                result="not reflected in context",
                reason=None,
                steps=99,
            )

        async def resume(
            self,
            context: ExecutionContext,
            *,
            tools: ToolRegistry,
            events: EventBus,
            config: EngineRunConfig,
        ) -> RunOutcome:
            raise AssertionError("resume was not requested")

        async def cancel(self) -> None:
            raise AssertionError("cancel was not requested")

    def builder(provider, *, permission_manager=None, compactor=None):
        return _MismatchedEngine()

    def resolver(name: str):
        return builder

    runner = AgentRunner(
        RuntimeConfig(),
        provider=_MustNotBeCalledProvider(),  # type: ignore[arg-type]
        engine_resolver=resolver,
        runs_dir=tmp_path,
    )

    with caplog.at_level(logging.ERROR):
        outcome = await runner.run_and_capture("goal", run_id="run-mismatch")

    assert outcome.status == "failed"
    assert outcome.reason == "engine_contract_error"
    assert outcome.steps == 0
    assert outcome.error is not None
    assert outcome.error.code == "engine_contract_error"
    assert outcome.error.engine == "mismatched"
    assert any("execution engine error" in record.message for record in caplog.records)
    assert all("execution engine unavailable" not in record.message for record in caplog.records)


async def test_runner_rejects_non_terminal_engine_completion(tmp_path: Path) -> None:
    collected: list[BaseModel] = []

    async def collect(event: BaseModel) -> None:
        collected.append(event)

    class _NonTerminalEngine:
        name = "non-terminal"

        async def run(
            self,
            context: ExecutionContext,
            *,
            tools: ToolRegistry,
            events: EventBus,
            config: EngineRunConfig,
        ) -> RunOutcome:
            context.result = "partial"
            return RunOutcome(
                status="running",  # type: ignore[arg-type]
                result=context.result,
                reason=context.reason,
                steps=context.step,
            )

        async def resume(
            self,
            context: ExecutionContext,
            *,
            tools: ToolRegistry,
            events: EventBus,
            config: EngineRunConfig,
        ) -> RunOutcome:
            raise AssertionError("resume was not requested")

        async def cancel(self) -> None:
            raise AssertionError("cancel was not requested")

    def builder(provider, *, permission_manager=None, compactor=None):
        return _NonTerminalEngine()

    def resolver(name: str):
        return builder

    runner = AgentRunner(
        RuntimeConfig(),
        provider=_MustNotBeCalledProvider(),  # type: ignore[arg-type]
        engine_resolver=resolver,
        extra_handlers=[collect],
        runs_dir=tmp_path,
    )

    outcome = await asyncio.wait_for(
        runner.run_and_capture("goal", run_id="run-non-terminal"),
        timeout=2.0,
    )

    assert outcome.status == "failed"
    assert outcome.reason == "engine_contract_error"
    assert outcome.error is not None
    assert outcome.error.code == "engine_contract_error"
    assert outcome.error.engine == "non-terminal"
    assert "outcome.status='running'" in outcome.error.message
    assert "context.status='running'" in outcome.error.message
    finished = [event for event in collected if getattr(event, "type", None) == "run.finished"]
    assert len(finished) == 1
    assert finished[0].status == "failed"  # type: ignore[attr-defined]
    assert finished[0].reason == "engine_contract_error"  # type: ignore[attr-defined]


async def test_runner_preserves_canonical_failed_engine_error(tmp_path: Path) -> None:
    collected: list[BaseModel] = []
    detail = EngineErrorDetail(
        code="graph_node_error",
        engine="reported-error",
        message="graph node failed",
        operation="run",
    )

    async def collect(event: BaseModel) -> None:
        collected.append(event)

    class _ReportedErrorEngine:
        name = "reported-error"

        async def run(
            self,
            context: ExecutionContext,
            *,
            tools: ToolRegistry,
            events: EventBus,
            config: EngineRunConfig,
        ) -> RunOutcome:
            context.result = "partial result"
            context.step = 3
            context.mark_failed("graph_node_error")
            return RunOutcome.from_context(context, error=detail)

        async def resume(
            self,
            context: ExecutionContext,
            *,
            tools: ToolRegistry,
            events: EventBus,
            config: EngineRunConfig,
        ) -> RunOutcome:
            raise AssertionError("resume was not requested")

        async def cancel(self) -> None:
            raise AssertionError("cancel was not requested")

    def builder(provider, *, permission_manager=None, compactor=None):
        return _ReportedErrorEngine()

    def resolver(name: str):
        return builder

    runner = AgentRunner(
        RuntimeConfig(),
        provider=_MustNotBeCalledProvider(),  # type: ignore[arg-type]
        engine_resolver=resolver,
        extra_handlers=[collect],
        runs_dir=tmp_path,
    )

    outcome = await runner.run_and_capture("goal", run_id="run-reported-error")

    assert outcome == RunOutcome(
        status="failed",
        result="partial result",
        reason="graph_node_error",
        steps=3,
        error=detail,
    )
    finished = next(event for event in collected if getattr(event, "type", None) == "run.finished")
    assert finished.status == "failed"  # type: ignore[attr-defined]
    assert finished.reason == "graph_node_error"  # type: ignore[attr-defined]
    assert finished.error == detail.as_dict()  # type: ignore[attr-defined]


async def test_runner_rejects_successful_engine_outcome_with_error(tmp_path: Path) -> None:
    class _SuccessfulErrorEngine:
        name = "successful-error"

        async def run(
            self,
            context: ExecutionContext,
            *,
            tools: ToolRegistry,
            events: EventBus,
            config: EngineRunConfig,
        ) -> RunOutcome:
            context.result = "done"
            context.mark_success()
            return RunOutcome.from_context(
                context,
                error=EngineErrorDetail(
                    code="impossible_error",
                    engine=self.name,
                    message="success cannot carry an error",
                ),
            )

        async def resume(
            self,
            context: ExecutionContext,
            *,
            tools: ToolRegistry,
            events: EventBus,
            config: EngineRunConfig,
        ) -> RunOutcome:
            raise AssertionError("resume was not requested")

        async def cancel(self) -> None:
            raise AssertionError("cancel was not requested")

    def builder(provider, *, permission_manager=None, compactor=None):
        return _SuccessfulErrorEngine()

    def resolver(name: str):
        return builder

    outcome = await AgentRunner(
        RuntimeConfig(),
        provider=_MustNotBeCalledProvider(),  # type: ignore[arg-type]
        engine_resolver=resolver,
        runs_dir=tmp_path,
    ).run_and_capture("goal", run_id="run-successful-error")

    assert outcome.status == "failed"
    assert outcome.reason == "engine_contract_error"
    assert outcome.error is not None
    assert outcome.error.code == "engine_contract_error"
    assert outcome.error.engine == "successful-error"


@pytest.mark.parametrize(
    "exception",
    [RuntimeError("boom"), SystemExit("stop")],
    ids=["exception", "system-exit"],
)
async def test_runner_normalizes_engine_exception_after_success_to_failed(
    tmp_path: Path,
    exception: BaseException,
) -> None:
    collected: list[BaseModel] = []

    async def collect(event: BaseModel) -> None:
        collected.append(event)

    class _ExceptionAfterSuccessEngine:
        name = "exception-after-success"

        async def run(
            self,
            context: ExecutionContext,
            *,
            tools: ToolRegistry,
            events: EventBus,
            config: EngineRunConfig,
        ) -> RunOutcome:
            context.result = "premature success"
            context.mark_success()
            raise exception

        async def resume(
            self,
            context: ExecutionContext,
            *,
            tools: ToolRegistry,
            events: EventBus,
            config: EngineRunConfig,
        ) -> RunOutcome:
            raise AssertionError("resume was not requested")

        async def cancel(self) -> None:
            raise AssertionError("cancel was not requested")

    def builder(provider, *, permission_manager=None, compactor=None):
        return _ExceptionAfterSuccessEngine()

    def resolver(name: str):
        return builder

    runner = AgentRunner(
        RuntimeConfig(),
        provider=_MustNotBeCalledProvider(),  # type: ignore[arg-type]
        engine_resolver=resolver,
        extra_handlers=[collect],
        runs_dir=tmp_path,
    )

    outcome = await runner.run_and_capture("goal", run_id="run-engine-exception")

    assert outcome.status == "failed"
    assert outcome.reason == "engine_execution_error"
    assert outcome.error is not None
    assert outcome.error.code == "engine_execution_error"
    assert outcome.error.engine == "exception-after-success"
    finished = next(event for event in collected if getattr(event, "type", None) == "run.finished")
    assert finished.status == "failed"  # type: ignore[attr-defined]
    assert finished.reason == "engine_execution_error"  # type: ignore[attr-defined]
    assert finished.error == outcome.error.as_dict()  # type: ignore[attr-defined]


async def test_runner_normalizes_cancellation_after_success_before_reraising(
    tmp_path: Path,
) -> None:
    collected: list[BaseModel] = []

    async def collect(event: BaseModel) -> None:
        collected.append(event)

    class _CancelledAfterSuccessEngine:
        name = "cancelled-after-success"

        async def run(
            self,
            context: ExecutionContext,
            *,
            tools: ToolRegistry,
            events: EventBus,
            config: EngineRunConfig,
        ) -> RunOutcome:
            context.result = "premature success"
            context.mark_success()
            raise asyncio.CancelledError()

        async def resume(
            self,
            context: ExecutionContext,
            *,
            tools: ToolRegistry,
            events: EventBus,
            config: EngineRunConfig,
        ) -> RunOutcome:
            raise AssertionError("resume was not requested")

        async def cancel(self) -> None:
            raise AssertionError("cancel was not requested")

    def builder(provider, *, permission_manager=None, compactor=None):
        return _CancelledAfterSuccessEngine()

    def resolver(name: str):
        return builder

    runner = AgentRunner(
        RuntimeConfig(),
        provider=_MustNotBeCalledProvider(),  # type: ignore[arg-type]
        engine_resolver=resolver,
        extra_handlers=[collect],
        runs_dir=tmp_path,
    )

    with pytest.raises(asyncio.CancelledError):
        await runner.run_and_capture("goal", run_id="run-engine-cancelled")

    finished = next(event for event in collected if getattr(event, "type", None) == "run.finished")
    assert finished.status == "failed"  # type: ignore[attr-defined]
    assert finished.reason == "cancelled"  # type: ignore[attr-defined]


async def test_runner_graph_failure_is_structured(tmp_path: Path) -> None:
    config = RuntimeConfig()
    config.agent.engine = "graph"
    collected: list[BaseModel] = []

    async def collect(event: BaseModel) -> None:
        collected.append(event)

    def unavailable_resolver(_name: str):
        raise ExecutionEngineUnavailableError(
            "graph",
            "Install optional graph dependencies.",
        )

    runner = AgentRunner(
        config,
        provider=_MustNotBeCalledProvider(),  # type: ignore[arg-type]
        engine_resolver=unavailable_resolver,
        extra_handlers=[collect],
        runs_dir=tmp_path,
    )

    outcome = await runner.run_and_capture("goal", run_id="run-graph")

    assert outcome.status == "failed"
    assert outcome.reason == "engine_unavailable"
    assert outcome.steps == 0
    assert outcome.error is not None
    assert outcome.error.code == "engine_unavailable"
    assert outcome.error.engine == "graph"
    assert outcome.error.message
    finished = next(event for event in collected if getattr(event, "type", None) == "run.finished")
    assert finished.reason == "engine_unavailable"  # type: ignore[attr-defined]
    assert finished.error == outcome.error.as_dict()  # type: ignore[attr-defined]


async def test_runner_resolves_graph_before_anthropic_provider_startup(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    config = RuntimeConfig()
    config.agent.engine = "graph"

    def unavailable_resolver(_name: str):
        raise ExecutionEngineUnavailableError(
            "graph",
            "Install optional graph dependencies.",
        )

    runner = AgentRunner(config, engine_resolver=unavailable_resolver, runs_dir=tmp_path)

    outcome = await runner.run_and_capture("goal", run_id="run-graph-no-key")

    assert outcome.status == "failed"
    assert outcome.reason == "engine_unavailable"
    assert outcome.error is not None
    assert outcome.error.engine == "graph"
