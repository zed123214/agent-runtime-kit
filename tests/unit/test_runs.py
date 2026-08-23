from __future__ import annotations

import re

from agent_runtime.core.runs import new_run_id


def test_new_run_id_keeps_timestamp_prefix_and_full_uuid_entropy() -> None:
    run_ids = {new_run_id() for _ in range(1_000)}

    assert len(run_ids) == 1_000
    assert all(re.fullmatch(r"\d{8}-\d{6}-[0-9a-f]{32}", run_id) for run_id in run_ids)
