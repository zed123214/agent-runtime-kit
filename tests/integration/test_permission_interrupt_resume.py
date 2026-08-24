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


async def test_permission_interrupt_survives_restart_and_full_identity_approval(
    tmp_path: Path,
) -> None:
    result = await run_recovery_scenario(tmp_path, "permission_interrupt")

    assert result.status == "success"
    assert result.suspension_reason is None
    assert result.initial_revision is not None
    assert result.provider_call_count == 2
    assert result.tool_call_count == 1
    assert result.journal_status == "completed"
    assert_event_and_transcript_contract(result, terminal=True)


async def test_two_real_clients_compete_for_one_resume_capability(
    tmp_path: Path,
) -> None:
    result = await run_recovery_scenario(
        tmp_path,
        "permission_interrupt",
        concurrent_resume=True,
    )

    assert result.resume_success_count == 1
    assert result.resume_failure_count == 1
    assert result.provider_call_count == 2
    assert result.tool_call_count == 1
    assert result.journal_status == "completed"
    assert_event_and_transcript_contract(result, terminal=True)
