from __future__ import annotations

import argparse
import asyncio
from pathlib import Path
from typing import cast

from recovery_helpers.scenario_runtime import (
    CountingTool,
    DaemonGeneration,
    RecoveryScenario,
    ScenarioProvider,
    install_completed_journal_gate,
    install_pre_dispatch_gate,
)

from agent_runtime.core.app import CoreApp
from agent_runtime.core.config import RuntimeConfig
from agent_runtime.core.llm.base import LLMProvider
from agent_runtime.core.tools.base import BaseTool


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--scenario",
        required=True,
        choices=(
            "model_inflight",
            "tool_pre_dispatch",
            "permission_interrupt",
            "journal_completed",
            "outcome_unknown",
        ),
    )
    parser.add_argument("--generation", required=True, choices=("A", "B"))
    parser.add_argument("--control-dir", required=True, type=Path)
    return parser.parse_args()


async def _main() -> None:
    args = _parse_args()
    scenario = cast(RecoveryScenario, args.scenario)
    generation = cast(DaemonGeneration, args.generation)
    control_dir = cast(Path, args.control_dir)

    if scenario == "journal_completed" and generation == "A":
        install_completed_journal_gate(control_dir)
    if scenario == "tool_pre_dispatch" and generation == "A":
        install_pre_dispatch_gate(control_dir)

    def provider_factory(_config: RuntimeConfig) -> LLMProvider:
        return ScenarioProvider(scenario, generation, control_dir)

    def extra_tools_factory() -> list[BaseTool]:
        return [CountingTool(scenario, generation, control_dir)]

    app = CoreApp(
        provider_factory=provider_factory,
        extra_tools_factory=extra_tools_factory,
    )
    await app.run()


if __name__ == "__main__":
    asyncio.run(_main())
