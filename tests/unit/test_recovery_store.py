from __future__ import annotations

import asyncio
import sqlite3
from pathlib import Path

import pytest

from agent_runtime.core.graph.recovery import (
    SCHEMA_VERSION,
    CapabilityGrant,
    CapabilityRejectedError,
    RecoveryConflictError,
    RecoverySchemaError,
    RecoveryStore,
)

pytestmark = pytest.mark.recovery


async def test_schema_version_parameterized_run_transactions_and_epoch_guard(
    tmp_path: Path,
) -> None:
    path = tmp_path / "recovery.sqlite3"
    store = RecoveryStore(path)
    malicious_session = "sess-'; DROP TABLE runs; --"
    created = await store.create_run(
        session_id=malicious_session,
        run_id="run-1",
        thread_id="thread-1",
        engine="graph",
        status="suspended",
        suspension_reason="permission",
        checkpoint_revision="rev-1",
        event_seq=3,
    )
    assert created.session_id == malicious_session
    assert await store.get_run(malicious_session, "run-1") == created

    updated = await store.update_run(
        malicious_session,
        "run-1",
        status="running",
        suspension_reason=None,
        checkpoint_revision="rev-2",
        event_seq=4,
        resume_epoch=1,
        transcript_commit_count=0,
        transcript_commit_hash=None,
        expected_resume_epoch=0,
    )
    assert updated.resume_epoch == 1
    assert updated.checkpoint_revision == "rev-2"
    with pytest.raises(RecoveryConflictError, match="epoch"):
        await store.update_run(
            malicious_session,
            "run-1",
            status="running",
            suspension_reason=None,
            checkpoint_revision="rev-2",
            event_seq=4,
            resume_epoch=2,
            transcript_commit_count=0,
            transcript_commit_hash=None,
            expected_resume_epoch=0,
        )

    connection = sqlite3.connect(path)
    try:
        version = connection.execute(
            "SELECT version FROM recovery_schema WHERE id = ?", (1,)
        ).fetchone()
        run_count = connection.execute("SELECT COUNT(*) FROM runs").fetchone()
    finally:
        connection.close()
    assert version == (SCHEMA_VERSION,)
    assert run_count == (1,)


def test_unknown_schema_version_fails_closed(tmp_path: Path) -> None:
    path = tmp_path / "recovery.sqlite3"
    RecoveryStore(path)
    connection = sqlite3.connect(path)
    try:
        connection.execute(
            "UPDATE recovery_schema SET version = ? WHERE id = ?",
            (SCHEMA_VERSION + 1, 1),
        )
        connection.commit()
    finally:
        connection.close()

    with pytest.raises(RecoverySchemaError, match="Unsupported"):
        RecoveryStore(path)


async def test_capability_is_hashed_rotated_and_consumed_once_concurrently(
    tmp_path: Path,
) -> None:
    path = tmp_path / "recovery.sqlite3"
    store = RecoveryStore(path)
    issued = await store.issue_capability("sess-1")

    connection = sqlite3.connect(path)
    try:
        persisted = connection.execute(
            "SELECT token_hash, token_version FROM session_capabilities WHERE session_id = ?",
            ("sess-1",),
        ).fetchone()
    finally:
        connection.close()
    assert persisted is not None
    assert isinstance(persisted[0], bytes)
    assert len(persisted[0]) == 32
    assert persisted[1] == 1
    assert all(issued.token.encode() not in file.read_bytes() for file in tmp_path.iterdir())

    second_store = RecoveryStore(path)
    attempts = await asyncio.gather(
        store.consume_capability("sess-1", issued.token),
        second_store.consume_capability("sess-1", issued.token),
        return_exceptions=True,
    )
    grants = [result for result in attempts if isinstance(result, CapabilityGrant)]
    rejected = [result for result in attempts if isinstance(result, CapabilityRejectedError)]
    assert len(grants) == 1
    assert len(rejected) == 1
    rotated = grants[0]
    assert rotated.token != issued.token
    assert rotated.token_version == 2

    with pytest.raises(CapabilityRejectedError):
        await store.consume_capability("sess-1", issued.token)
    next_grant = await store.consume_capability("sess-1", rotated.token)
    assert next_grant.token_version == 3


async def test_restart_coordination_latest_run_and_atomic_resume_lease(tmp_path: Path) -> None:
    store = RecoveryStore(tmp_path / "recovery.sqlite3")
    await store.create_run(
        session_id="sess-1",
        run_id="run-terminal",
        thread_id="thread-terminal",
        engine="graph",
        status="success",
    )
    await store.create_run(
        session_id="sess-1",
        run_id="run-interrupted",
        thread_id="thread-interrupted",
        engine="graph",
        status="running",
        checkpoint_revision="rev-1",
        event_seq=2,
    )
    tool_hash = "input-hash"
    await store.claim_tool(
        session_id="sess-1",
        run_id="run-interrupted",
        tool_use_id="tool-1",
        tool_name="side_effect",
        input_hash=tool_hash,
    )

    interrupted = await store.suspend_interrupted_runs()
    assert [(run.run_id, run.status, run.suspension_reason) for run in interrupted] == [
        ("run-interrupted", "suspended", "process_recovery")
    ]
    latest = await store.latest_unfinished_run("sess-1")
    assert latest is not None
    assert latest.run_id == "run-interrupted"
    journal = await store.get_tool("sess-1", "run-interrupted", "tool-1")
    assert journal is not None
    assert journal.status == "outcome_unknown"

    leases = await asyncio.gather(
        store.acquire_resume_lease(
            "sess-1",
            "run-interrupted",
            expected_checkpoint_revision="rev-1",
            expected_resume_epoch=0,
        ),
        RecoveryStore(store.path).acquire_resume_lease(
            "sess-1",
            "run-interrupted",
            expected_checkpoint_revision="rev-1",
            expected_resume_epoch=0,
        ),
        return_exceptions=True,
    )
    acquired = [result for result in leases if not isinstance(result, BaseException)]
    conflicts = [result for result in leases if isinstance(result, RecoveryConflictError)]
    assert len(acquired) == 1
    assert len(conflicts) == 1
    lease = acquired[0]
    assert lease.status == "resuming"
    assert lease.resume_epoch == 1

    suspended = await store.mark_run_suspended(
        "sess-1",
        "run-interrupted",
        reason="permission",
        checkpoint_revision="rev-2",
        event_seq=3,
        expected_resume_epoch=1,
    )
    assert suspended.status == "suspended"
    second_lease = await store.acquire_resume_lease(
        "sess-1",
        "run-interrupted",
        expected_checkpoint_revision="rev-2",
        expected_resume_epoch=1,
    )
    terminal = await store.mark_run_terminal(
        "sess-1",
        "run-interrupted",
        status="success",
        checkpoint_revision="rev-3",
        event_seq=4,
        transcript_commit_count=1,
        transcript_commit_hash="transcript-hash",
        expected_resume_epoch=second_lease.resume_epoch,
    )
    assert terminal.status == "success"
    assert terminal.transcript_commit_count == 1
    assert (
        await store.mark_run_terminal(
            "sess-1",
            "run-interrupted",
            status="success",
            checkpoint_revision="rev-3",
            event_seq=4,
            transcript_commit_count=1,
            transcript_commit_hash="transcript-hash",
            expected_resume_epoch=second_lease.resume_epoch,
        )
        == terminal
    )
    with pytest.raises(RecoveryConflictError, match="terminal"):
        await store.mark_run_terminal(
            "sess-1",
            "run-interrupted",
            status="failed",
            checkpoint_revision="rev-3",
            event_seq=4,
            transcript_commit_count=1,
            transcript_commit_hash="transcript-hash",
            expected_resume_epoch=second_lease.resume_epoch,
        )
    assert await store.latest_unfinished_run("sess-1") is None
