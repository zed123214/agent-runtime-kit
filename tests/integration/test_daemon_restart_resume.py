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


async def test_model_inflight_is_retried_in_a_second_real_daemon(tmp_path: Path) -> None:
    result = await run_recovery_scenario(tmp_path, "model_inflight")

    assert result.status == "success"
    assert result.suspension_reason is None
    assert result.provider_call_count == 2
    assert result.tool_call_count == 0
    assert result.journal_status is None
    assert_event_and_transcript_contract(result, terminal=True)
