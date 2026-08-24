from __future__ import annotations

import asyncio
import json
import sys
import tempfile
import uuid
from pathlib import Path

_TESTS_ROOT = Path(__file__).resolve().parents[1] / "tests"
sys.path.insert(0, str(_TESTS_ROOT))

from recovery_helpers.harness import (  # noqa: E402
    assert_event_and_transcript_contract,
    run_recovery_scenario,
)


async def _main() -> None:
    prefix = f"kitagent-durable-demo-{uuid.uuid4().hex}-"
    with tempfile.TemporaryDirectory(prefix=prefix) as temporary:
        result = await run_recovery_scenario(Path(temporary), "journal_completed")
        assert result.status == "success"
        assert result.tool_call_count == 1
        assert result.journal_status == "completed"
        assert_event_and_transcript_contract(result, terminal=True)
        print(
            json.dumps(
                {
                    "backend": result.backend,
                    "session_id": result.session_id,
                    "run_id": result.run_id,
                    "thread_id": result.thread_id,
                    "checkpoint_revision": result.final_revision or result.initial_revision,
                    "recovered_node": result.recovered_node,
                    "event_cursor": result.event_cursor,
                    "tool_effective_call_count": result.tool_call_count,
                    "final_status": result.status,
                },
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
            )
        )


if __name__ == "__main__":
    asyncio.run(_main())
