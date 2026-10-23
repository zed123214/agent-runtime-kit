from pathlib import Path

import pytest

from agent_runtime.core.config import RuntimeConfig
from agent_runtime.core.events.bus import EventBus
from agent_runtime.core.llm.types import LlmResponse
from agent_runtime.core.runner import AgentRunner
from agent_runtime.core.session.manager import SessionManager
from agent_runtime.core.session.store import SessionStore


class TurnProvider:
    def __init__(self, fail: bool = False) -> None:
        self.fail = fail

    async def chat(self, *args: object, **kwargs: object) -> LlmResponse:
        if self.fail:
            raise RuntimeError("deliberate provider failure")
        return LlmResponse(stop_reason="end_turn", text="本轮已提交")


async def test_result_belongs_to_committed_run_even_when_next_turn_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    provider = TurnProvider()
    store = SessionStore(tmp_path / "sessions")
    manager = SessionManager(
        store,
        lambda: AgentRunner(RuntimeConfig(), provider=provider, runs_dir=tmp_path / "runs"),
        EventBus(),
    )
    session = await manager.create("chat")
    first = await manager.send_message(session.id, "重复的问题")
    result = manager.get_run_result(session.id, first)
    assert result.commit_state == "committed"
    assert result.final_answer == "本轮已提交"
    assert result.transcript_ref

    provider.fail = True
    second = await manager.send_message(session.id, "重复的问题")
    failed = manager.get_run_result(session.id, second)
    assert failed.status == "failed"
    assert failed.no_final_answer is True
    assert failed.final_answer is None
    assert manager.get_run_result(session.id, first) == result
    assert manager.get_run_result(session.id, "missing").commit_state == "unavailable"
    await manager.close(session.id)
