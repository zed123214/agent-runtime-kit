from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from agent_runtime.core.graph.recovery import (
    RecoveryStore,
    ToolInvocationConflictError,
    hash_tool_input,
)

pytestmark = pytest.mark.recovery


async def test_concurrent_claim_has_one_executor_and_completed_result_is_reused(
    tmp_path: Path,
) -> None:
    store = RecoveryStore(tmp_path / "recovery.sqlite3")
    second_store = RecoveryStore(store.path)
    input_hash = hash_tool_input({"path": "report.pdf", "page": 1})
    stores = [store, second_store, store, second_store, store, second_store, store, second_store]
    claims = await asyncio.gather(
        *[
            current_store.claim_tool(
                session_id="sess-1",
                run_id="run-1",
                tool_use_id="tool-1",
                tool_name="read_document",
                input_hash=input_hash,
            )
            for current_store in stores
        ]
    )
    assert [claim.action for claim in claims].count("execute") == 1
    assert [claim.action for claim in claims].count("in_progress") == 7

    completed = await store.complete_tool(
        session_id="sess-1",
        run_id="run-1",
        tool_use_id="tool-1",
        tool_name="read_document",
        input_hash=input_hash,
        serialized_result='{"content":"done","is_error":false}',
    )
    assert completed.status == "completed"
    retry = await store.claim_tool(
        session_id="sess-1",
        run_id="run-1",
        tool_use_id="tool-1",
        tool_name="read_document",
        input_hash=input_hash,
    )
    assert retry.action == "reuse"
    assert retry.record.serialized_result == completed.serialized_result


async def test_started_without_result_becomes_outcome_unknown_and_is_not_replayed(
    tmp_path: Path,
) -> None:
    store = RecoveryStore(tmp_path / "recovery.sqlite3")
    input_hash = hash_tool_input({"target": "external-ledger"})
    claim = await store.claim_tool(
        session_id="sess-1",
        run_id="run-1",
        tool_use_id="tool-1",
        tool_name="write_external",
        input_hash=input_hash,
    )
    assert claim.action == "execute"

    unknown = await store.mark_tool_outcome_unknown(
        session_id="sess-1",
        run_id="run-1",
        tool_use_id="tool-1",
        tool_name="write_external",
        input_hash=input_hash,
    )
    assert unknown.status == "outcome_unknown"
    retry = await store.claim_tool(
        session_id="sess-1",
        run_id="run-1",
        tool_use_id="tool-1",
        tool_name="write_external",
        input_hash=input_hash,
    )
    assert retry.action == "outcome_unknown"
    with pytest.raises(ToolInvocationConflictError, match="cannot be completed"):
        await store.complete_tool(
            session_id="sess-1",
            run_id="run-1",
            tool_use_id="tool-1",
            tool_name="write_external",
            input_hash=input_hash,
            serialized_result='{"content":"invented"}',
        )


async def test_tool_identity_conflict_and_session_run_isolation(tmp_path: Path) -> None:
    store = RecoveryStore(tmp_path / "recovery.sqlite3")
    first_hash = hash_tool_input({"value": 1})
    second_hash = hash_tool_input({"value": 2})
    first = await store.claim_tool(
        session_id="sess-1",
        run_id="run-1",
        tool_use_id="shared-id",
        tool_name="side_effect",
        input_hash=first_hash,
    )
    assert first.action == "execute"

    with pytest.raises(ToolInvocationConflictError, match="identity"):
        await store.claim_tool(
            session_id="sess-1",
            run_id="run-1",
            tool_use_id="shared-id",
            tool_name="side_effect",
            input_hash=second_hash,
        )
    with pytest.raises(ToolInvocationConflictError, match="identity"):
        await store.claim_tool(
            session_id="sess-1",
            run_id="run-1",
            tool_use_id="shared-id",
            tool_name="different_tool",
            input_hash=first_hash,
        )

    isolated_claims = await asyncio.gather(
        store.claim_tool(
            session_id="sess-2",
            run_id="run-1",
            tool_use_id="shared-id",
            tool_name="side_effect",
            input_hash=first_hash,
        ),
        store.claim_tool(
            session_id="sess-1",
            run_id="run-2",
            tool_use_id="shared-id",
            tool_name="side_effect",
            input_hash=first_hash,
        ),
    )
    assert [claim.action for claim in isolated_claims] == ["execute", "execute"]
