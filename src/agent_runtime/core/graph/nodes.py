from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from typing import Literal

from agent_runtime.core.bus.events import StepFinishedEvent, StepStartedEvent
from agent_runtime.core.events.bus import EventBus
from agent_runtime.core.graph.state import RuntimeState, StateUpdate
from agent_runtime.core.llm.base import LLMProvider
from agent_runtime.core.llm.types import LlmResponse

type StepStartedCallback = Callable[[int], Awaitable[None] | None]

log = logging.getLogger(__name__)


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _assistant_blocks(response: LlmResponse) -> list[dict[str, object]]:
    """Mirror AgentLoop's canonical thinking -> text -> tool-use assembly."""

    blocks: list[dict[str, object]] = list(response.thinking_blocks)
    if response.text:
        blocks.append({"type": "text", "text": response.text})
    for call in response.tool_calls:
        blocks.append(
            {
                "type": "tool_use",
                "id": call.id,
                "name": call.name,
                "input": dict(call.input),
            }
        )
    return blocks


def _serialized_tool_calls(response: LlmResponse) -> list[dict[str, object]]:
    return [
        {
            "id": call.id,
            "name": call.name,
            "input": dict(call.input),
        }
        for call in response.tool_calls
    ]


class ModelNode:
    """One KitAgent provider step; tool execution remains a separate graph node."""

    def __init__(
        self,
        provider: LLMProvider,
        tool_schemas: list[dict[str, object]],
        system_prompt: str,
        max_steps: int,
        on_step_started: StepStartedCallback | None = None,
    ) -> None:
        if max_steps <= 0:
            raise ValueError("max_steps must be positive")
        self._provider = provider
        self._tool_schemas = list(tool_schemas)
        self._system_prompt = system_prompt
        self._max_steps = max_steps
        self._on_step_started = on_step_started

    async def __call__(self, state: RuntimeState, node_bus: EventBus) -> StateUpdate:
        step = state["step"] + 1
        if self._on_step_started is not None:
            callback_result = self._on_step_started(step)
            if callback_result is not None:
                await callback_result
        await node_bus.publish(StepStartedEvent(run_id=state["run_id"], step=step, ts=_now()))

        try:
            response = await self._provider.chat(
                messages=state["messages"],
                tool_schemas=self._tool_schemas,
                bus=node_bus,
                run_id=state["run_id"],
                step=step,
                system=self._system_prompt,
            )
        except Exception:
            log.exception("LLM call failed run_id=%s step=%d", state["run_id"], step)
            return {
                "step": step,
                "status": "failed",
                "reason": "llm_error",
                "pending_tool_calls": [],
                "pending_tool_action": None,
                "errors": [*state["errors"], "llm_error"],
            }

        calls = _serialized_tool_calls(response)
        pending: list[dict[str, object]] = []
        pending_action: str | None = None
        status = "running"
        reason: str | None = None
        final_answer = ""

        if response.stop_reason == "end_turn":
            status = "success"
            final_answer = response.text or ""
        elif response.stop_reason == "tool_use" and calls:
            pending = calls
            pending_action = "execute"
        elif response.stop_reason == "max_tokens" and calls:
            pending = calls
            pending_action = "synthetic_error"
        elif step >= self._max_steps:
            status = "failed"
            reason = "exceeded_max_steps"

        update: StateUpdate = {
            "messages": [{"role": "assistant", "content": _assistant_blocks(response)}],
            "pending_tool_calls": pending,
            "pending_tool_action": pending_action,
            "step": step,
            "status": status,
            "reason": reason,
            "final_answer": final_answer,
        }

        # A tool-bearing step remains open until KitToolNode has produced all
        # paired results. Every other normal provider response finishes here.
        if not pending:
            await node_bus.publish(StepFinishedEvent(run_id=state["run_id"], step=step, ts=_now()))
        return update


def route_after_model(state: RuntimeState) -> Literal["model", "kit_tools", "end"]:
    if state["status"] != "running":
        return "end"
    if state["pending_tool_calls"]:
        return "kit_tools"
    return "model"
