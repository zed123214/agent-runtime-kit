"""Deterministic model decisions, real Core, tools and permission gates."""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from datetime import UTC, datetime
from pathlib import Path

from agent_runtime.core.app import CoreApp
from agent_runtime.core.bus.events import LlmModelSelectedEvent, LlmTokenEvent
from agent_runtime.core.events.bus import EventBus
from agent_runtime.core.llm.types import LlmResponse, ToolCallBlock


class DemoProvider:
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
        await asyncio.sleep(0.08)
        now = datetime.now(UTC).isoformat()
        await bus.publish(
            LlmModelSelectedEvent(
                run_id=run_id,
                model="offline-demo",
                strategy="scripted",
                ts=now,
            )
        )
        report = f"reports/{run_id}.md"
        text = ""
        call: ToolCallBlock | None = None
        previous = messages[-1].get("content", []) if messages else []
        goal = next(
            (
                str(message["content"])
                for message in reversed(messages)
                if message.get("role") == "user" and isinstance(message.get("content"), str)
            ),
            "",
        )
        denied = isinstance(previous, list) and any(
            isinstance(item, dict) and item.get("is_error") for item in previous
        )
        if goal.startswith("子任务校验"):
            text = "子任务完成。此回复属于独立的子 run。"
        elif denied:
            text = "本次工具未获准或未完成。已保留执行记录；检查报告未被确认为完成。"
        elif step == 1:
            text = "我会先读取项目说明，再执行检查并整理报告。"
            if "子任务" in goal:
                call = ToolCallBlock(
                    "child",
                    "spawn_agent",
                    {
                        "description": "独立上下文校验",
                        "prompt": "子任务校验：返回完成摘要。",
                    },
                )
            else:
                filename = "not-present.md" if "模拟工具错误" in goal else "README.md"
                call = ToolCallBlock("read-project", "read_file", {"path": filename})
        elif step == 2:
            text = "项目说明已读取。请确认下面的检查命令。"
            call = ToolCallBlock(
                "check",
                "bash",
                {
                    "command": f"{sys.executable} check.py {run_id}",
                },
            )
        elif step == 3:
            text = "检查命令已返回结果。我将保存本轮检查报告。"
            call = ToolCallBlock(
                "write-report",
                "write_file",
                {
                    "path": report,
                    "content": "# 样例仓库检查报告\n\n- 检查脚本完成。\n- 样例数据格式正确。\n\n"
                    f"运行标识：{run_id}\n\n此文件由真实 write_file 工具写入。\n",
                },
            )
        elif step == 4:
            call = ToolCallBlock("verify-report", "read_file", {"path": report})
        else:
            text = "## 检查报告\n\n已完成项目说明读取、检查命令和报告写入，并读回验证。\n\n"
            text += f"报告路径：`{report}`\n\n右侧可查看工具证据，并导出本轮运行报告。"
        for index in range(0, len(text), 8):
            await bus.publish(LlmTokenEvent(run_id=run_id, token=text[index : index + 8], ts=now))
            delay = (
                float(os.environ.get("AGENTRT_DEMO_INITIAL_DELAY", "0.015")) if step == 1 else 0.015
            )
            await asyncio.sleep(delay)
        return LlmResponse(
            stop_reason="tool_use" if call else "end_turn",
            text=text,
            tool_calls=[call] if call else [],
        )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, default=7437)
    parser.add_argument("--directory", type=Path, required=True)
    args = parser.parse_args()
    directory = args.directory.resolve()
    # Exclusive creation prevents overwriting a user's existing workspace.
    directory.mkdir(parents=True, exist_ok=False)
    (directory / "README.md").write_text(
        "# 示例 Python 项目\n\n运行 check.py 检查样例，再生成报告。\n", encoding="utf-8"
    )
    (directory / "check.py").write_text(
        "import os, pathlib, sys, time\n"
        "time.sleep(float(os.environ.get('AGENTRT_DEMO_TOOL_DELAY', '0.5')))\n"
        "directory = pathlib.Path('checks')\n"
        "directory.mkdir(exist_ok=True)\n"
        "with (directory / (sys.argv[1] + '.txt')).open('x') as f: f.write('1')\n"
        "pathlib.Path('check-count.txt').write_text(str(len(list(directory.iterdir()))))\n"
        "print('PASS: sample data is valid; effective execution recorded once')\n",
        encoding="utf-8",
    )
    config_path = directory / "demo.toml"
    config_path.write_text("[agent]\nengine = 'loop'\n", encoding="utf-8")
    os.chdir(directory)
    os.environ.update(
        {
            "AGENTRT_CONFIG": str(config_path),
            "AGENTRT_PORT": str(args.port),
            "AGENTRT_HOST": "127.0.0.1",
            "AGENTRT_DATA_ROOT": str(directory / ".runtime"),
            "AGENTRT_ENGINE": "loop",
            "AGENTRT_SANDBOX_BACKEND": "local",
            "AGENTRT_LOG_LEVEL": "WARNING",
            "AGENTRT_LOG_FILE": "",
            "AGENTRT_LLM_DEFAULT_MODEL": "offline-demo",
            "AGENTRT_PERMISSION_TIMEOUT_S": "300",
        }
    )
    asyncio.run(CoreApp(provider_factory=lambda _config: DemoProvider()).run())


if __name__ == "__main__":
    main()
