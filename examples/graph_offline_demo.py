from __future__ import annotations

import asyncio
from typing import Any

from pydantic import BaseModel

from agent_runtime.core.context import ExecutionContext
from agent_runtime.core.engine.base import EngineRunConfig
from agent_runtime.core.events.bus import EventBus
from agent_runtime.core.graph.engine import GraphExecutionEngine
from agent_runtime.core.graph.fake_provider import ScriptedProvider
from agent_runtime.core.graph.runtime import GraphRuntime
from agent_runtime.core.tools.base import BaseTool, ToolResult
from agent_runtime.core.tools.registry import ToolRegistry


class EchoTool(BaseTool):
    name = "echo"
    description = "Return the supplied text from an offline in-process tool."
    input_schema: dict[str, object] = {
        "type": "object",
        "properties": {"text": {"type": "string"}},
        "required": ["text"],
    }

    async def invoke(self, params: dict[str, object]) -> ToolResult:
        return ToolResult(content=f"offline echo: {params.get('text', '')}")


async def main() -> None:
    provider = ScriptedProvider.tool(
        "echo",
        {"text": "model -> kit_tools -> model"},
        final_text="Graph offline demo completed.",
    )
    tools = ToolRegistry()
    tools.register(EchoTool())

    events = EventBus(correlation_id="demo-run", session_id="demo-session")

    async def print_graph_event(event: BaseModel) -> None:
        payload: dict[str, Any] = event.model_dump()
        event_type = payload.get("type")
        if event_type in {"node.started", "node.finished", "state.diff"}:
            print(
                f"{event_type:<14} node={payload.get('node_id')} "
                f"status={payload.get('status', '-')} diff={payload.get('diff', '-')}"
            )

    events.subscribe(print_graph_event)
    runtime = GraphRuntime()
    engine = GraphExecutionEngine(provider, runtime=runtime)
    context = ExecutionContext(
        run_id="demo-run",
        goal="Run one deterministic tool path.",
        max_steps=4,
    )

    try:
        outcome = await asyncio.wait_for(
            engine.run(
                context,
                tools=tools,
                events=events,
                config=EngineRunConfig(
                    session_id="demo-session",
                    thread_id="demo-session",
                    retain_thread=True,
                ),
            ),
            timeout=2.0,
        )
    finally:
        await runtime.close()

    print(f"provider_calls={provider.call_count}")
    print(f"outcome={outcome.status} steps={outcome.steps} result={outcome.result!r}")


if __name__ == "__main__":
    asyncio.run(main())
