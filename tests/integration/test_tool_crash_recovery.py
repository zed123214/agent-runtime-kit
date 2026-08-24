from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from recovery_helpers.harness import (  # noqa: E402
    assert_event_and_transcript_contract,
    run_recovery_scenario,
)

pytestmark = pytest.mark.recovery


async def test_pending_tool_checkpoint_resumes_before_first_dispatch(
    tmp_path: Path,
) -> None:
    result = await run_recovery_scenario(tmp_path, "tool_pre_dispatch")

    assert result.status == "success"
    assert result.provider_call_count == 2
    assert result.tool_call_count == 1
    assert result.journal_status == "completed"
    assert_event_and_transcript_contract(result, terminal=True)


async def test_completed_journal_result_is_reused_without_second_tool_call(
    tmp_path: Path,
) -> None:
    result = await run_recovery_scenario(tmp_path, "journal_completed")

    assert result.status == "success"
    assert result.provider_call_count == 2
    assert result.tool_call_count == 1
    assert result.journal_status == "completed"
    assert_event_and_transcript_contract(result, terminal=True)


async def test_side_effect_without_result_becomes_outcome_unknown_without_replay(
    tmp_path: Path,
) -> None:
    result = await run_recovery_scenario(tmp_path, "outcome_unknown")

    assert result.status == "suspended"
    assert result.suspension_reason == "outcome_unknown"
    assert result.provider_call_count == 1
    assert result.tool_call_count == 1
    assert result.journal_status == "outcome_unknown"
    assert_event_and_transcript_contract(result, terminal=False)
