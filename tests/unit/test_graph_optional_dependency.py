from __future__ import annotations

import asyncio
import builtins
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any
from unittest.mock import Mock

import pytest

from agent_runtime.core.config import RuntimeConfig
from agent_runtime.core.engine.base import ExecutionEngineUnavailableError
from agent_runtime.core.engine.router import EngineRouter
from agent_runtime.core.runner import AgentRunner

_WAIT_S = 2.0


def _fresh_graph_engine_import(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delitem(sys.modules, "agent_runtime.core.graph.engine", raising=False)


def _raise_for_import(
    monkeypatch: pytest.MonkeyPatch,
    predicate: Callable[[str], bool],
    *,
    missing_name: str,
) -> None:
    real_import = builtins.__import__

    def guarded_import(
        name: str,
        globals: dict[str, Any] | None = None,
        locals: dict[str, Any] | None = None,
        fromlist: tuple[str, ...] = (),
        level: int = 0,
    ) -> Any:
        if predicate(name):
            raise ModuleNotFoundError(
                f"No module named {missing_name!r}",
                name=missing_name,
            )
        return real_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", guarded_import)


@pytest.mark.parametrize("missing_name", ["langgraph", "langgraph.checkpoint"])
def test_router_maps_only_precise_langgraph_missing_names_to_typed_unavailable(
    monkeypatch: pytest.MonkeyPatch,
    missing_name: str,
) -> None:
    _fresh_graph_engine_import(monkeypatch)
    _raise_for_import(
        monkeypatch,
        lambda name: name.startswith("langgraph"),
        missing_name=missing_name,
    )

    with pytest.raises(ExecutionEngineUnavailableError) as raised:
        EngineRouter()("graph")

    assert raised.value.detail.code == "engine_unavailable"
    assert raised.value.detail.engine == "graph"
    assert "uv sync --extra graph" in raised.value.detail.message


def test_internal_module_not_found_is_not_disguised_as_optional_dependency(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _fresh_graph_engine_import(monkeypatch)
    _raise_for_import(
        monkeypatch,
        lambda name: name == "agent_runtime.core.graph.engine",
        missing_name="internal_graph_bug",
    )

    with pytest.raises(ModuleNotFoundError) as raised:
        EngineRouter()("graph")

    assert raised.value.name == "internal_graph_bug"


async def test_runner_reports_unavailable_before_provider_construction(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _fresh_graph_engine_import(monkeypatch)
    _raise_for_import(
        monkeypatch,
        lambda name: name.startswith("langgraph"),
        missing_name="langgraph",
    )
    provider_constructor = Mock(side_effect=AssertionError("provider must not be constructed"))
    monkeypatch.setattr("agent_runtime.core.runner.AnthropicProvider", provider_constructor)
    config = RuntimeConfig()
    config.agent.engine = "graph"
    runner = AgentRunner(
        config,
        runs_dir=tmp_path,
        engine_resolver=EngineRouter(),
    )

    outcome = await asyncio.wait_for(
        runner.run_and_capture("offline", run_id="missing-graph"),
        timeout=_WAIT_S,
    )

    assert outcome.status == "failed"
    assert outcome.reason == "engine_unavailable"
    assert outcome.error is not None
    assert outcome.error.code == "engine_unavailable"
    provider_constructor.assert_not_called()
