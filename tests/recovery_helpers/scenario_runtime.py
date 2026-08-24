from __future__ import annotations

import asyncio
import json
import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal

from agent_runtime.core.bus.events import LlmModelSelectedEvent, LlmTokenEvent
from agent_runtime.core.events.bus import EventBus
from agent_runtime.core.graph.recovery import RecoveryStore, ToolInvocationRecord
from agent_runtime.core.graph.state import RuntimeState
from agent_runtime.core.graph.tool_node import KitToolNode
from agent_runtime.core.llm.types import LlmResponse, ToolCallBlock
from agent_runtime.core.tools.base import BaseTool, ToolResult

type RecoveryScenario = Literal[
    "model_inflight",
    "tool_pre_dispatch",
    "permission_interrupt",
    "journal_completed",
    "outcome_unknown",
]
type DaemonGeneration = Literal["A", "B"]

TOOL_NAME = "recovery_counter"
TOOL_USE_ID = "recovery-tool-1"


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _write_json_atomic(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    with temporary.open("w", encoding="utf-8", newline="\n") as stream:
        json.dump(payload, stream, ensure_ascii=False, sort_keys=True)
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)


def _append_jsonl(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8", newline="\n") as stream:
        stream.write(json.dumps(payload, ensure_ascii=False, sort_keys=True) + "\n")
        stream.flush()
        os.fsync(stream.fileno())


class ScenarioProvider:
    """A deterministic provider whose first daemon can be killed at a file gate."""

    def __init__(
        self,
        scenario: RecoveryScenario,
        generation: DaemonGeneration,
        control_dir: Path,
    ) -> None:
        self._scenario = scenario
        self._generation = generation
        self._control_dir = control_dir
        self._call_index = 0

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
        del messages, tool_schemas, system
        self._call_index += 1
        _append_jsonl(
            self._control_dir / "provider_calls.jsonl",
            {
                "generation": self._generation,
                "index": self._call_index,
                "run_id": run_id,
                "scenario": self._scenario,
                "step": step,
            },
        )
        await bus.publish(
            LlmModelSelectedEvent(
                run_id=run_id,
                model="scripted-recovery-subprocess",
                strategy="static",
                ts=_now(),
            )
        )

        if self._scenario == "model_inflight" and self._generation == "A":
            _write_json_atomic(
                self._control_dir / "model_inflight.json",
                {"run_id": run_id, "step": step},
            )
            await asyncio.Event().wait()

        if self._generation == "A" and self._scenario != "model_inflight" and self._call_index == 1:
            return LlmResponse(
                stop_reason="tool_use",
                tool_calls=[
                    ToolCallBlock(
                        id=TOOL_USE_ID,
                        name=TOOL_NAME,
                        input={"value": self._scenario},
                    )
                ],
            )

        text = f"recovered:{self._scenario}"
        await bus.publish(LlmTokenEvent(run_id=run_id, token=text, ts=_now()))
        return LlmResponse(stop_reason="end_turn", text=text)


class CountingTool(BaseTool):
    name = TOOL_NAME
    description = "Deterministic offline counter used only by recovery subprocess tests."
    input_schema: dict[str, object] = {
        "type": "object",
        "properties": {"value": {"type": "string"}},
        "required": ["value"],
    }

    def __init__(
        self,
        scenario: RecoveryScenario,
        generation: DaemonGeneration,
        control_dir: Path,
    ) -> None:
        self._scenario = scenario
        self._generation = generation
        self._control_dir = control_dir

    async def invoke(self, params: dict[str, object]) -> ToolResult:
        _append_jsonl(
            self._control_dir / "tool_calls.jsonl",
            {
                "generation": self._generation,
                "params": params,
                "scenario": self._scenario,
            },
        )
        if self._scenario == "outcome_unknown" and self._generation == "A":
            _write_json_atomic(
                self._control_dir / "side_effect.json",
                {"generation": self._generation, "params": params},
            )
            await asyncio.Event().wait()
        return ToolResult(content=f"counted:{params.get('value', '')}")


def install_completed_journal_gate(control_dir: Path) -> None:
    """Block daemon A after the completed result is durable but before node commit."""

    original = RecoveryStore.complete_tool

    async def complete_then_block(
        self: RecoveryStore,
        *,
        session_id: str,
        run_id: str,
        tool_use_id: str,
        tool_name: str,
        input_hash: str,
        serialized_result: str,
    ) -> ToolInvocationRecord:
        record = await original(
            self,
            session_id=session_id,
            run_id=run_id,
            tool_use_id=tool_use_id,
            tool_name=tool_name,
            input_hash=input_hash,
            serialized_result=serialized_result,
        )
        _write_json_atomic(
            control_dir / "journal_completed.json",
            {
                "run_id": run_id,
                "session_id": session_id,
                "tool_use_id": tool_use_id,
            },
        )
        await asyncio.Event().wait()
        return record

    RecoveryStore.complete_tool = complete_then_block  # type: ignore[method-assign]


def install_pre_dispatch_gate(control_dir: Path) -> None:
    """Block daemon A after model checkpointing but before invoke_tool dispatch."""

    async def block_before_dispatch(
        _self: KitToolNode,
        state: RuntimeState,
        node_bus: EventBus,
    ) -> dict[str, object]:
        del node_bus
        _write_json_atomic(
            control_dir / "tool_pre_dispatch.json",
            {
                "run_id": state["run_id"],
                "pending_tool_count": len(state["pending_tool_calls"]),
            },
        )
        await asyncio.Event().wait()
        raise AssertionError("pre-dispatch gate was unexpectedly released")

    KitToolNode.__call__ = block_before_dispatch  # type: ignore[method-assign,assignment]
