from __future__ import annotations

import asyncio
from copy import deepcopy
from datetime import UTC, datetime
from typing import Any, TypedDict

from agent_runtime.core.bus.events import LlmModelSelectedEvent, LlmTokenEvent, LlmUsageEvent
from agent_runtime.core.events.bus import EventBus
from agent_runtime.core.llm.types import LlmResponse, ToolCallBlock

type ScriptItem = LlmResponse | BaseException


class ScriptedCall(TypedDict):
    messages: list[dict[str, object]]
    tool_schemas: list[dict[str, object]]
    run_id: str
    step: int
    system: str | None


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _call(
    name: str,
    params: dict[str, object] | None,
    tool_use_id: str,
) -> ToolCallBlock:
    return ToolCallBlock(id=tool_use_id, name=name, input=dict(params or {}))


class ScriptedProvider:
    """Deterministic offline LLMProvider used by graph tests and demonstrations."""

    def __init__(
        self,
        script: list[ScriptItem],
        *,
        repeat_last: bool = False,
        blocking: bool = False,
        model: str = "scripted-offline",
    ) -> None:
        if not script:
            raise ValueError("script must contain at least one response or exception")
        self._script = list(script)
        self._repeat_last = repeat_last
        self._blocking = blocking
        self._model = model
        self._index = 0
        self._release = asyncio.Event()
        self.entered = asyncio.Event()
        self.calls: list[ScriptedCall] = []

    @classmethod
    def direct(cls, text: str = "done") -> ScriptedProvider:
        return cls([LlmResponse(stop_reason="end_turn", text=text)])

    @classmethod
    def tool(
        cls,
        name: str = "echo",
        params: dict[str, object] | None = None,
        *,
        tool_use_id: str = "tool-1",
        final_text: str = "done",
    ) -> ScriptedProvider:
        return cls(
            [
                LlmResponse(
                    stop_reason="tool_use",
                    tool_calls=[_call(name, params, tool_use_id)],
                ),
                LlmResponse(stop_reason="end_turn", text=final_text),
            ]
        )

    @classmethod
    def multi(
        cls,
        calls: list[ToolCallBlock],
        *,
        final_text: str = "done",
    ) -> ScriptedProvider:
        if not calls:
            raise ValueError("multi-tool script requires at least one call")
        return cls(
            [
                LlmResponse(stop_reason="tool_use", tool_calls=deepcopy(calls)),
                LlmResponse(stop_reason="end_turn", text=final_text),
            ]
        )

    @classmethod
    def failure(cls, exc: BaseException | None = None) -> ScriptedProvider:
        return cls([exc or RuntimeError("scripted provider failure")])

    @classmethod
    def infinite(
        cls,
        name: str = "echo",
        params: dict[str, object] | None = None,
        *,
        tool_use_id: str = "loop-tool",
    ) -> ScriptedProvider:
        return cls(
            [
                LlmResponse(
                    stop_reason="tool_use",
                    tool_calls=[_call(name, params, tool_use_id)],
                )
            ],
            repeat_last=True,
        )

    @classmethod
    def block(
        cls,
        response: LlmResponse | None = None,
    ) -> ScriptedProvider:
        return cls(
            [response or LlmResponse(stop_reason="end_turn", text="released")],
            blocking=True,
        )

    @property
    def call_count(self) -> int:
        return len(self.calls)

    def release(self) -> None:
        self._release.set()

    def _next(self) -> ScriptItem:
        if self._index < len(self._script):
            item = self._script[self._index]
            self._index += 1
            return item
        if self._repeat_last:
            return self._script[-1]
        raise AssertionError("scripted provider response sequence exhausted")

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
        self.calls.append(
            ScriptedCall(
                messages=deepcopy(messages),
                tool_schemas=deepcopy(tool_schemas),
                run_id=run_id,
                step=step,
                system=system,
            )
        )
        await bus.publish(
            LlmModelSelectedEvent(
                run_id=run_id,
                model=self._model,
                strategy="static",
                ts=_now(),
            )
        )
        self.entered.set()
        if self._blocking:
            await self._release.wait()

        item = self._next()
        if isinstance(item, BaseException):
            raise item
        response = deepcopy(item)

        if response.text:
            await bus.publish(LlmTokenEvent(run_id=run_id, token=response.text, ts=_now()))
        if response.usage is not None:
            usage = response.usage
            await bus.publish(
                LlmUsageEvent(
                    run_id=run_id,
                    input_tokens=usage.input_tokens,
                    output_tokens=usage.output_tokens,
                    cache_read_input_tokens=usage.cache_read_input_tokens,
                    cache_creation_input_tokens=usage.cache_creation_input_tokens,
                    context_pct=usage.context_pct,
                    ts=_now(),
                )
            )
        return response


def scripted_tool_call(
    name: str,
    params: dict[str, Any] | None = None,
    *,
    tool_use_id: str = "tool-1",
) -> ToolCallBlock:
    """Build a call for multi-tool scripts without importing provider internals."""

    return _call(name, dict(params or {}), tool_use_id)
