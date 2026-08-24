from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import secrets
import sqlite3
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal, cast

SCHEMA_VERSION = 1

type ToolInvocationStatus = Literal["started", "completed", "outcome_unknown"]
type ToolClaimAction = Literal["execute", "in_progress", "reuse", "outcome_unknown"]

TERMINAL_RUN_STATUSES = frozenset({"success", "failed"})

_OWNED_TABLES = frozenset({"recovery_schema", "runs", "tool_invocations", "session_capabilities"})
_DUMMY_TOKEN_HASH = bytes(hashlib.sha256(b"kitagent-invalid-capability").digest())
_SCHEMA_STATEMENTS = (
    """
    CREATE TABLE recovery_schema (
        id INTEGER PRIMARY KEY CHECK (id = 1),
        version INTEGER NOT NULL
    )
    """,
    """
    CREATE TABLE runs (
        session_id TEXT NOT NULL,
        run_id TEXT NOT NULL,
        thread_id TEXT NOT NULL,
        engine TEXT NOT NULL,
        status TEXT NOT NULL,
        suspension_reason TEXT,
        checkpoint_revision TEXT,
        event_seq INTEGER NOT NULL CHECK (event_seq >= 0),
        resume_epoch INTEGER NOT NULL CHECK (resume_epoch >= 0),
        transcript_commit_count INTEGER NOT NULL CHECK (transcript_commit_count >= 0),
        transcript_commit_hash TEXT,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        PRIMARY KEY (session_id, run_id)
    )
    """,
    """
    CREATE TABLE tool_invocations (
        session_id TEXT NOT NULL,
        run_id TEXT NOT NULL,
        tool_use_id TEXT NOT NULL,
        tool_name TEXT NOT NULL,
        input_hash TEXT NOT NULL,
        status TEXT NOT NULL CHECK (status IN ('started', 'completed', 'outcome_unknown')),
        serialized_result TEXT,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        PRIMARY KEY (session_id, run_id, tool_use_id),
        CHECK (
            (status = 'completed' AND serialized_result IS NOT NULL)
            OR (status != 'completed' AND serialized_result IS NULL)
        )
    )
    """,
    """
    CREATE TABLE session_capabilities (
        session_id TEXT PRIMARY KEY,
        token_hash BLOB NOT NULL CHECK (length(token_hash) = 32),
        token_version INTEGER NOT NULL CHECK (token_version > 0),
        created_at TEXT NOT NULL,
        rotated_at TEXT
    )
    """,
)


class RecoveryStoreError(RuntimeError):
    code = "recovery_store_error"


class RecoverySchemaError(RecoveryStoreError):
    code = "recovery_schema_error"


class RecoveryNotFoundError(RecoveryStoreError):
    code = "recovery_not_found"


class RecoveryConflictError(RecoveryStoreError):
    code = "recovery_conflict"


class CapabilityRejectedError(RecoveryStoreError):
    code = "capability_rejected"


class ToolInvocationConflictError(RecoveryConflictError):
    code = "tool_invocation_conflict"


@dataclass(frozen=True, slots=True)
class RecoveryRun:
    session_id: str
    run_id: str
    thread_id: str
    engine: str
    status: str
    suspension_reason: str | None
    checkpoint_revision: str | None
    event_seq: int
    resume_epoch: int
    transcript_commit_count: int
    transcript_commit_hash: str | None
    created_at: str
    updated_at: str


@dataclass(frozen=True, slots=True)
class CapabilityGrant:
    session_id: str
    token: str
    token_version: int


@dataclass(frozen=True, slots=True)
class CapabilityValidation:
    session_id: str
    token_version: int


@dataclass(frozen=True, slots=True)
class ToolInvocationRecord:
    session_id: str
    run_id: str
    tool_use_id: str
    tool_name: str
    input_hash: str
    status: ToolInvocationStatus
    serialized_result: str | None
    created_at: str
    updated_at: str


@dataclass(frozen=True, slots=True)
class ToolClaim:
    action: ToolClaimAction
    record: ToolInvocationRecord


def hash_tool_input(tool_input: Mapping[str, object]) -> str:
    """Return a deterministic hash without persisting the raw tool arguments."""

    try:
        canonical = json.dumps(
            tool_input,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        )
    except (TypeError, ValueError) as exc:
        raise ValueError("tool input must be JSON serializable") from exc
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _now() -> str:
    return datetime.now(UTC).isoformat(timespec="microseconds")


async def _finish_task_despite_cancellation[T](task: asyncio.Task[T]) -> tuple[T, bool]:
    cancelled_while_waiting = False
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            cancelled_while_waiting = True
    return task.result(), cancelled_while_waiting


def _token_hash(token: str) -> bytes:
    return hashlib.sha256(token.encode("utf-8")).digest()


def _require_text(name: str, value: str) -> None:
    if not value.strip():
        raise ValueError(f"{name} must not be empty")


class RecoveryStore:
    """KitAgent recovery metadata and tool journal backed by SQLite."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._setup()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=5.0, isolation_level=None)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA busy_timeout = 5000")
        connection.execute("PRAGMA foreign_keys = ON")
        return connection

    @contextmanager
    def _transaction(self) -> Iterator[sqlite3.Connection]:
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            yield connection
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()

    def _setup(self) -> None:
        connection = self._connect()
        try:
            connection.execute("PRAGMA journal_mode = WAL")
            connection.execute("BEGIN IMMEDIATE")
            existing_tables = {
                str(row[0])
                for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type = ? AND name NOT LIKE ?",
                    ("table", "sqlite_%"),
                )
            }
            owned_tables = existing_tables & _OWNED_TABLES
            if "recovery_schema" not in existing_tables:
                if owned_tables:
                    raise RecoverySchemaError("RecoveryStore tables exist without schema metadata.")
                for statement in _SCHEMA_STATEMENTS:
                    connection.execute(statement)
                connection.execute(
                    "INSERT INTO recovery_schema (id, version) VALUES (?, ?)",
                    (1, SCHEMA_VERSION),
                )
            else:
                row = connection.execute(
                    "SELECT version FROM recovery_schema WHERE id = ?",
                    (1,),
                ).fetchone()
                if row is None or int(row["version"]) != SCHEMA_VERSION:
                    found = "missing" if row is None else str(row["version"])
                    raise RecoverySchemaError(f"Unsupported RecoveryStore schema version: {found}.")
                missing_tables = _OWNED_TABLES - existing_tables
                if missing_tables:
                    raise RecoverySchemaError(
                        "RecoveryStore schema is incomplete: " + ", ".join(sorted(missing_tables))
                    )
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()

    async def create_run(
        self,
        *,
        session_id: str,
        run_id: str,
        thread_id: str,
        engine: str,
        status: str,
        suspension_reason: str | None = None,
        checkpoint_revision: str | None = None,
        event_seq: int = 0,
        resume_epoch: int = 0,
        transcript_commit_count: int = 0,
        transcript_commit_hash: str | None = None,
    ) -> RecoveryRun:
        for name, value in (
            ("session_id", session_id),
            ("run_id", run_id),
            ("thread_id", thread_id),
            ("engine", engine),
            ("status", status),
        ):
            _require_text(name, value)
        if min(event_seq, resume_epoch, transcript_commit_count) < 0:
            raise ValueError("run counters must not be negative")
        return await asyncio.to_thread(
            self._create_run_sync,
            session_id,
            run_id,
            thread_id,
            engine,
            status,
            suspension_reason,
            checkpoint_revision,
            event_seq,
            resume_epoch,
            transcript_commit_count,
            transcript_commit_hash,
        )

    def _create_run_sync(
        self,
        session_id: str,
        run_id: str,
        thread_id: str,
        engine: str,
        status: str,
        suspension_reason: str | None,
        checkpoint_revision: str | None,
        event_seq: int,
        resume_epoch: int,
        transcript_commit_count: int,
        transcript_commit_hash: str | None,
    ) -> RecoveryRun:
        timestamp = _now()
        try:
            with self._transaction() as connection:
                connection.execute(
                    """
                    INSERT INTO runs (
                        session_id, run_id, thread_id, engine, status,
                        suspension_reason, checkpoint_revision, event_seq,
                        resume_epoch, transcript_commit_count, transcript_commit_hash,
                        created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        session_id,
                        run_id,
                        thread_id,
                        engine,
                        status,
                        suspension_reason,
                        checkpoint_revision,
                        event_seq,
                        resume_epoch,
                        transcript_commit_count,
                        transcript_commit_hash,
                        timestamp,
                        timestamp,
                    ),
                )
        except sqlite3.IntegrityError as exc:
            raise RecoveryConflictError("Recovery run already exists.") from exc
        return RecoveryRun(
            session_id=session_id,
            run_id=run_id,
            thread_id=thread_id,
            engine=engine,
            status=status,
            suspension_reason=suspension_reason,
            checkpoint_revision=checkpoint_revision,
            event_seq=event_seq,
            resume_epoch=resume_epoch,
            transcript_commit_count=transcript_commit_count,
            transcript_commit_hash=transcript_commit_hash,
            created_at=timestamp,
            updated_at=timestamp,
        )

    async def get_run(self, session_id: str, run_id: str) -> RecoveryRun | None:
        _require_text("session_id", session_id)
        _require_text("run_id", run_id)
        return await asyncio.to_thread(self._get_run_sync, session_id, run_id)

    def _get_run_sync(self, session_id: str, run_id: str) -> RecoveryRun | None:
        connection = self._connect()
        try:
            row = connection.execute(
                "SELECT * FROM runs WHERE session_id = ? AND run_id = ?",
                (session_id, run_id),
            ).fetchone()
        finally:
            connection.close()
        return None if row is None else self._run_from_row(row)

    async def latest_run(
        self,
        session_id: str,
        *,
        unfinished_only: bool = False,
    ) -> RecoveryRun | None:
        _require_text("session_id", session_id)
        return await asyncio.to_thread(self._latest_run_sync, session_id, unfinished_only)

    async def latest_unfinished_run(self, session_id: str) -> RecoveryRun | None:
        return await self.latest_run(session_id, unfinished_only=True)

    def _latest_run_sync(self, session_id: str, unfinished_only: bool) -> RecoveryRun | None:
        connection = self._connect()
        try:
            if unfinished_only:
                row = connection.execute(
                    """
                    SELECT * FROM runs
                    WHERE session_id = ? AND status NOT IN (?, ?)
                    ORDER BY updated_at DESC, rowid DESC
                    LIMIT 1
                    """,
                    (session_id, *sorted(TERMINAL_RUN_STATUSES)),
                ).fetchone()
            else:
                row = connection.execute(
                    """
                    SELECT * FROM runs
                    WHERE session_id = ?
                    ORDER BY updated_at DESC, rowid DESC
                    LIMIT 1
                    """,
                    (session_id,),
                ).fetchone()
        finally:
            connection.close()
        return None if row is None else self._run_from_row(row)

    async def update_run(
        self,
        session_id: str,
        run_id: str,
        *,
        status: str,
        suspension_reason: str | None,
        checkpoint_revision: str | None,
        event_seq: int,
        resume_epoch: int,
        transcript_commit_count: int,
        transcript_commit_hash: str | None,
        expected_resume_epoch: int | None = None,
    ) -> RecoveryRun:
        for name, value in (("session_id", session_id), ("run_id", run_id), ("status", status)):
            _require_text(name, value)
        counters = [event_seq, resume_epoch, transcript_commit_count]
        if expected_resume_epoch is not None:
            counters.append(expected_resume_epoch)
        if min(counters) < 0:
            raise ValueError("run counters must not be negative")
        return await asyncio.to_thread(
            self._update_run_sync,
            session_id,
            run_id,
            status,
            suspension_reason,
            checkpoint_revision,
            event_seq,
            resume_epoch,
            transcript_commit_count,
            transcript_commit_hash,
            expected_resume_epoch,
        )

    def _update_run_sync(
        self,
        session_id: str,
        run_id: str,
        status: str,
        suspension_reason: str | None,
        checkpoint_revision: str | None,
        event_seq: int,
        resume_epoch: int,
        transcript_commit_count: int,
        transcript_commit_hash: str | None,
        expected_resume_epoch: int | None,
    ) -> RecoveryRun:
        timestamp = _now()
        with self._transaction() as connection:
            params: list[object] = [
                status,
                suspension_reason,
                checkpoint_revision,
                event_seq,
                resume_epoch,
                transcript_commit_count,
                transcript_commit_hash,
                timestamp,
                session_id,
                run_id,
            ]
            sql = """
                UPDATE runs
                SET status = ?, suspension_reason = ?, checkpoint_revision = ?,
                    event_seq = ?, resume_epoch = ?, transcript_commit_count = ?,
                    transcript_commit_hash = ?, updated_at = ?
                WHERE session_id = ? AND run_id = ?
            """
            if expected_resume_epoch is not None:
                sql += " AND resume_epoch = ?"
                params.append(expected_resume_epoch)
            cursor = connection.execute(sql, params)
            if cursor.rowcount != 1:
                exists = connection.execute(
                    "SELECT 1 FROM runs WHERE session_id = ? AND run_id = ?",
                    (session_id, run_id),
                ).fetchone()
                if exists is None:
                    raise RecoveryNotFoundError("Recovery run was not found.")
                raise RecoveryConflictError("Recovery run epoch changed.")
            row = connection.execute(
                "SELECT * FROM runs WHERE session_id = ? AND run_id = ?",
                (session_id, run_id),
            ).fetchone()
        if row is None:
            raise RecoveryNotFoundError("Recovery run was not found.")
        return self._run_from_row(row)

    async def suspend_interrupted_runs(self) -> list[RecoveryRun]:
        """Move attempts interrupted by process loss into durable suspension."""

        return await asyncio.to_thread(self._suspend_interrupted_runs_sync)

    def _suspend_interrupted_runs_sync(self) -> list[RecoveryRun]:
        timestamp = _now()
        with self._transaction() as connection:
            interrupted = connection.execute(
                "SELECT session_id, run_id FROM runs WHERE status IN (?, ?)",
                ("running", "resuming"),
            ).fetchall()
            records: list[RecoveryRun] = []
            for identity in interrupted:
                session_id = str(identity["session_id"])
                run_id = str(identity["run_id"])
                connection.execute(
                    """
                    UPDATE runs
                    SET status = ?, suspension_reason = ?, updated_at = ?
                    WHERE session_id = ? AND run_id = ? AND status IN (?, ?)
                    """,
                    (
                        "suspended",
                        "process_recovery",
                        timestamp,
                        session_id,
                        run_id,
                        "running",
                        "resuming",
                    ),
                )
                connection.execute(
                    """
                    UPDATE tool_invocations
                    SET status = ?, updated_at = ?
                    WHERE session_id = ? AND run_id = ? AND status = ?
                    """,
                    ("outcome_unknown", timestamp, session_id, run_id, "started"),
                )
                row = connection.execute(
                    "SELECT * FROM runs WHERE session_id = ? AND run_id = ?",
                    (session_id, run_id),
                ).fetchone()
                if row is not None:
                    records.append(self._run_from_row(row))
        return records

    async def acquire_resume_lease(
        self,
        session_id: str,
        run_id: str,
        *,
        expected_checkpoint_revision: str | None = None,
        expected_resume_epoch: int | None = None,
    ) -> RecoveryRun:
        _require_text("session_id", session_id)
        _require_text("run_id", run_id)
        if expected_resume_epoch is not None and expected_resume_epoch < 0:
            raise ValueError("expected_resume_epoch must not be negative")
        return await asyncio.to_thread(
            self._acquire_resume_lease_sync,
            session_id,
            run_id,
            expected_checkpoint_revision,
            expected_resume_epoch,
        )

    def _acquire_resume_lease_sync(
        self,
        session_id: str,
        run_id: str,
        expected_checkpoint_revision: str | None,
        expected_resume_epoch: int | None,
    ) -> RecoveryRun:
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT * FROM runs WHERE session_id = ? AND run_id = ?",
                (session_id, run_id),
            ).fetchone()
            if row is None:
                raise RecoveryNotFoundError("Recovery run was not found.")
            record = self._run_from_row(row)
            if record.status != "suspended":
                raise RecoveryConflictError("Recovery run is not suspended.")
            if (
                expected_checkpoint_revision is not None
                and record.checkpoint_revision != expected_checkpoint_revision
            ):
                raise RecoveryConflictError("Recovery checkpoint revision changed.")
            if expected_resume_epoch is not None and record.resume_epoch != expected_resume_epoch:
                raise RecoveryConflictError("Recovery run epoch changed.")

            next_epoch = record.resume_epoch + 1
            cursor = connection.execute(
                """
                UPDATE runs
                SET status = ?, suspension_reason = ?, resume_epoch = ?, updated_at = ?
                WHERE session_id = ? AND run_id = ? AND status = ? AND resume_epoch = ?
                """,
                (
                    "resuming",
                    None,
                    next_epoch,
                    _now(),
                    session_id,
                    run_id,
                    "suspended",
                    record.resume_epoch,
                ),
            )
            if cursor.rowcount != 1:
                raise RecoveryConflictError("Recovery run lease was already acquired.")
            updated = connection.execute(
                "SELECT * FROM runs WHERE session_id = ? AND run_id = ?",
                (session_id, run_id),
            ).fetchone()
        if updated is None:
            raise RecoveryNotFoundError("Recovery run was not found.")
        return self._run_from_row(updated)

    async def mark_run_suspended(
        self,
        session_id: str,
        run_id: str,
        *,
        reason: str,
        checkpoint_revision: str | None,
        event_seq: int,
        expected_resume_epoch: int,
    ) -> RecoveryRun:
        _require_text("reason", reason)
        return await asyncio.to_thread(
            self._mark_run_suspended_sync,
            session_id,
            run_id,
            reason,
            checkpoint_revision,
            event_seq,
            expected_resume_epoch,
        )

    def _mark_run_suspended_sync(
        self,
        session_id: str,
        run_id: str,
        reason: str,
        checkpoint_revision: str | None,
        event_seq: int,
        expected_resume_epoch: int,
    ) -> RecoveryRun:
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT * FROM runs WHERE session_id = ? AND run_id = ?",
                (session_id, run_id),
            ).fetchone()
            if row is None:
                raise RecoveryNotFoundError("Recovery run was not found.")
            record = self._run_from_row(row)
            if record.status in TERMINAL_RUN_STATUSES:
                raise RecoveryConflictError("Terminal recovery run cannot be suspended.")
            if record.resume_epoch != expected_resume_epoch:
                raise RecoveryConflictError("Recovery run epoch changed.")
            if event_seq < record.event_seq:
                raise RecoveryConflictError("Recovery event cursor moved backwards.")
            connection.execute(
                """
                UPDATE runs
                SET status = ?, suspension_reason = ?, checkpoint_revision = ?,
                    event_seq = ?, updated_at = ?
                WHERE session_id = ? AND run_id = ? AND resume_epoch = ?
                """,
                (
                    "suspended",
                    reason,
                    checkpoint_revision,
                    event_seq,
                    _now(),
                    session_id,
                    run_id,
                    expected_resume_epoch,
                ),
            )
            updated = connection.execute(
                "SELECT * FROM runs WHERE session_id = ? AND run_id = ?",
                (session_id, run_id),
            ).fetchone()
        if updated is None:
            raise RecoveryNotFoundError("Recovery run was not found.")
        return self._run_from_row(updated)

    async def mark_run_terminal(
        self,
        session_id: str,
        run_id: str,
        *,
        status: Literal["success", "failed"],
        checkpoint_revision: str | None,
        event_seq: int,
        transcript_commit_count: int,
        transcript_commit_hash: str | None,
        expected_resume_epoch: int,
    ) -> RecoveryRun:
        return await asyncio.to_thread(
            self._mark_run_terminal_sync,
            session_id,
            run_id,
            status,
            checkpoint_revision,
            event_seq,
            transcript_commit_count,
            transcript_commit_hash,
            expected_resume_epoch,
        )

    def _mark_run_terminal_sync(
        self,
        session_id: str,
        run_id: str,
        status: Literal["success", "failed"],
        checkpoint_revision: str | None,
        event_seq: int,
        transcript_commit_count: int,
        transcript_commit_hash: str | None,
        expected_resume_epoch: int,
    ) -> RecoveryRun:
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT * FROM runs WHERE session_id = ? AND run_id = ?",
                (session_id, run_id),
            ).fetchone()
            if row is None:
                raise RecoveryNotFoundError("Recovery run was not found.")
            record = self._run_from_row(row)
            if record.resume_epoch != expected_resume_epoch:
                raise RecoveryConflictError("Recovery run epoch changed.")
            if record.status in TERMINAL_RUN_STATUSES:
                if (
                    record.status == status
                    and record.checkpoint_revision == checkpoint_revision
                    and record.event_seq == event_seq
                    and record.transcript_commit_count == transcript_commit_count
                    and record.transcript_commit_hash == transcript_commit_hash
                ):
                    return record
                raise RecoveryConflictError("Recovery run already has a terminal state.")
            if event_seq < record.event_seq:
                raise RecoveryConflictError("Recovery event cursor moved backwards.")
            if transcript_commit_count < record.transcript_commit_count:
                raise RecoveryConflictError("Transcript commit count moved backwards.")
            connection.execute(
                """
                UPDATE runs
                SET status = ?, suspension_reason = ?, checkpoint_revision = ?,
                    event_seq = ?, transcript_commit_count = ?,
                    transcript_commit_hash = ?, updated_at = ?
                WHERE session_id = ? AND run_id = ? AND resume_epoch = ?
                """,
                (
                    status,
                    None,
                    checkpoint_revision,
                    event_seq,
                    transcript_commit_count,
                    transcript_commit_hash,
                    _now(),
                    session_id,
                    run_id,
                    expected_resume_epoch,
                ),
            )
            updated = connection.execute(
                "SELECT * FROM runs WHERE session_id = ? AND run_id = ?",
                (session_id, run_id),
            ).fetchone()
        if updated is None:
            raise RecoveryNotFoundError("Recovery run was not found.")
        return self._run_from_row(updated)

    async def issue_capability(self, session_id: str) -> CapabilityGrant:
        _require_text("session_id", session_id)
        return await asyncio.to_thread(self._issue_capability_sync, session_id)

    def _issue_capability_sync(self, session_id: str) -> CapabilityGrant:
        token = secrets.token_urlsafe(32)
        timestamp = _now()
        try:
            with self._transaction() as connection:
                connection.execute(
                    """
                    INSERT INTO session_capabilities (
                        session_id, token_hash, token_version, created_at, rotated_at
                    ) VALUES (?, ?, ?, ?, ?)
                    """,
                    (session_id, _token_hash(token), 1, timestamp, None),
                )
        except sqlite3.IntegrityError as exc:
            raise RecoveryConflictError("Session capability already exists.") from exc
        return CapabilityGrant(session_id=session_id, token=token, token_version=1)

    async def consume_capability(self, session_id: str, token: str) -> CapabilityGrant:
        _require_text("session_id", session_id)
        _require_text("resume token", token)
        validation = await self.validate_capability(session_id, token)
        return await self.rotate_capability(
            session_id,
            token,
            expected_token_version=validation.token_version,
        )

    async def validate_capability(
        self,
        session_id: str,
        token: str,
    ) -> CapabilityValidation:
        """Validate a capability without rotating or otherwise writing it."""

        _require_text("session_id", session_id)
        _require_text("resume token", token)
        return await asyncio.to_thread(self._validate_capability_sync, session_id, token)

    def _validate_capability_sync(
        self,
        session_id: str,
        token: str,
    ) -> CapabilityValidation:
        presented_hash = _token_hash(token)
        connection = self._connect()
        try:
            row = connection.execute(
                """
                SELECT token_hash, token_version
                FROM session_capabilities
                WHERE session_id = ?
                """,
                (session_id,),
            ).fetchone()
        finally:
            connection.close()
        stored_hash = _DUMMY_TOKEN_HASH if row is None else bytes(row["token_hash"])
        valid = hmac.compare_digest(stored_hash, presented_hash)
        if row is None or not valid:
            raise CapabilityRejectedError("Resume capability was rejected.")
        return CapabilityValidation(
            session_id=session_id,
            token_version=int(row["token_version"]),
        )

    async def rotate_capability(
        self,
        session_id: str,
        token: str,
        *,
        expected_token_version: int,
    ) -> CapabilityGrant:
        """CAS-rotate a previously validated capability as the attach commit."""

        _require_text("session_id", session_id)
        _require_text("resume token", token)
        if expected_token_version <= 0:
            raise ValueError("expected_token_version must be positive")
        rotation_task = asyncio.create_task(
            asyncio.to_thread(
                self._rotate_capability_sync,
                session_id,
                token,
                expected_token_version,
            )
        )
        grant, cancelled = await _finish_task_despite_cancellation(rotation_task)
        if not cancelled:
            return grant

        rollback_task = asyncio.create_task(
            asyncio.to_thread(
                self._rollback_capability_rotation_sync,
                session_id,
                token,
                grant.token,
                grant.token_version,
            )
        )
        await _finish_task_despite_cancellation(rollback_task)
        raise asyncio.CancelledError()

    def _rotate_capability_sync(
        self,
        session_id: str,
        token: str,
        expected_token_version: int,
    ) -> CapabilityGrant:
        presented_hash = _token_hash(token)
        with self._transaction() as connection:
            row = connection.execute(
                """
                SELECT token_hash, token_version
                FROM session_capabilities
                WHERE session_id = ?
                """,
                (session_id,),
            ).fetchone()
            stored_hash = _DUMMY_TOKEN_HASH if row is None else bytes(row["token_hash"])
            valid = hmac.compare_digest(stored_hash, presented_hash)
            if row is None or not valid or int(row["token_version"]) != expected_token_version:
                raise CapabilityRejectedError("Resume capability was rejected.")

            next_token = secrets.token_urlsafe(32)
            next_version = int(row["token_version"]) + 1
            cursor = connection.execute(
                """
                UPDATE session_capabilities
                SET token_hash = ?, token_version = ?, rotated_at = ?
                WHERE session_id = ? AND token_version = ? AND token_hash = ?
                """,
                (
                    _token_hash(next_token),
                    next_version,
                    _now(),
                    session_id,
                    expected_token_version,
                    stored_hash,
                ),
            )
            if cursor.rowcount != 1:
                raise CapabilityRejectedError("Resume capability was rejected.")
        return CapabilityGrant(
            session_id=session_id,
            token=next_token,
            token_version=next_version,
        )

    def _rollback_capability_rotation_sync(
        self,
        session_id: str,
        previous_token: str,
        rotated_token: str,
        expected_rotated_version: int,
    ) -> None:
        if expected_rotated_version <= 1:
            raise ValueError("expected_rotated_version must be greater than one")
        rotated_hash = _token_hash(rotated_token)
        with self._transaction() as connection:
            row = connection.execute(
                """
                SELECT token_hash, token_version
                FROM session_capabilities
                WHERE session_id = ?
                """,
                (session_id,),
            ).fetchone()
            stored_hash = _DUMMY_TOKEN_HASH if row is None else bytes(row["token_hash"])
            if (
                row is None
                or not hmac.compare_digest(stored_hash, rotated_hash)
                or int(row["token_version"]) != expected_rotated_version
            ):
                raise RecoveryConflictError("Capability rotation rollback conflict.")
            cursor = connection.execute(
                """
                UPDATE session_capabilities
                SET token_hash = ?, token_version = ?, rotated_at = ?
                WHERE session_id = ? AND token_version = ? AND token_hash = ?
                """,
                (
                    _token_hash(previous_token),
                    expected_rotated_version - 1,
                    _now(),
                    session_id,
                    expected_rotated_version,
                    rotated_hash,
                ),
            )
            if cursor.rowcount != 1:
                raise RecoveryConflictError("Capability rotation rollback conflict.")

    async def delete_capability(self, session_id: str) -> None:
        _require_text("session_id", session_id)
        await asyncio.to_thread(self._delete_capability_sync, session_id)

    def _delete_capability_sync(self, session_id: str) -> None:
        with self._transaction() as connection:
            connection.execute(
                "DELETE FROM session_capabilities WHERE session_id = ?",
                (session_id,),
            )

    async def get_tool(
        self,
        session_id: str,
        run_id: str,
        tool_use_id: str,
    ) -> ToolInvocationRecord | None:
        for name, value in (
            ("session_id", session_id),
            ("run_id", run_id),
            ("tool_use_id", tool_use_id),
        ):
            _require_text(name, value)
        return await asyncio.to_thread(self._get_tool_sync, session_id, run_id, tool_use_id)

    def _get_tool_sync(
        self,
        session_id: str,
        run_id: str,
        tool_use_id: str,
    ) -> ToolInvocationRecord | None:
        connection = self._connect()
        try:
            row = self._select_tool(connection, session_id, run_id, tool_use_id)
        finally:
            connection.close()
        return None if row is None else self._tool_from_row(row)

    async def claim_tool(
        self,
        *,
        session_id: str,
        run_id: str,
        tool_use_id: str,
        tool_name: str,
        input_hash: str,
    ) -> ToolClaim:
        for name, value in (
            ("session_id", session_id),
            ("run_id", run_id),
            ("tool_use_id", tool_use_id),
            ("tool_name", tool_name),
            ("input_hash", input_hash),
        ):
            _require_text(name, value)
        return await asyncio.to_thread(
            self._claim_tool_sync,
            session_id,
            run_id,
            tool_use_id,
            tool_name,
            input_hash,
        )

    def _claim_tool_sync(
        self,
        session_id: str,
        run_id: str,
        tool_use_id: str,
        tool_name: str,
        input_hash: str,
    ) -> ToolClaim:
        timestamp = _now()
        with self._transaction() as connection:
            row = connection.execute(
                """
                SELECT * FROM tool_invocations
                WHERE session_id = ? AND run_id = ? AND tool_use_id = ?
                """,
                (session_id, run_id, tool_use_id),
            ).fetchone()
            if row is None:
                connection.execute(
                    """
                    INSERT INTO tool_invocations (
                        session_id, run_id, tool_use_id, tool_name, input_hash,
                        status, serialized_result, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        session_id,
                        run_id,
                        tool_use_id,
                        tool_name,
                        input_hash,
                        "started",
                        None,
                        timestamp,
                        timestamp,
                    ),
                )
                record = ToolInvocationRecord(
                    session_id=session_id,
                    run_id=run_id,
                    tool_use_id=tool_use_id,
                    tool_name=tool_name,
                    input_hash=input_hash,
                    status="started",
                    serialized_result=None,
                    created_at=timestamp,
                    updated_at=timestamp,
                )
                return ToolClaim(action="execute", record=record)

            record = self._tool_from_row(row)
            self._check_tool_identity(record, tool_name, input_hash)
            action: ToolClaimAction
            if record.status == "completed":
                action = "reuse"
            elif record.status == "outcome_unknown":
                action = "outcome_unknown"
            else:
                action = "in_progress"
            return ToolClaim(action=action, record=record)

    async def complete_tool(
        self,
        *,
        session_id: str,
        run_id: str,
        tool_use_id: str,
        tool_name: str,
        input_hash: str,
        serialized_result: str,
    ) -> ToolInvocationRecord:
        if not serialized_result:
            raise ValueError("serialized_result must not be empty")
        return await asyncio.to_thread(
            self._complete_tool_sync,
            session_id,
            run_id,
            tool_use_id,
            tool_name,
            input_hash,
            serialized_result,
        )

    def _complete_tool_sync(
        self,
        session_id: str,
        run_id: str,
        tool_use_id: str,
        tool_name: str,
        input_hash: str,
        serialized_result: str,
    ) -> ToolInvocationRecord:
        with self._transaction() as connection:
            row = self._select_tool(connection, session_id, run_id, tool_use_id)
            if row is None:
                raise RecoveryNotFoundError("Tool invocation was not found.")
            record = self._tool_from_row(row)
            self._check_tool_identity(record, tool_name, input_hash)
            if record.status == "completed":
                if record.serialized_result != serialized_result:
                    raise ToolInvocationConflictError(
                        "Completed tool invocation has a different result."
                    )
                return record
            if record.status == "outcome_unknown":
                raise ToolInvocationConflictError(
                    "Outcome-unknown tool invocation cannot be completed automatically."
                )
            connection.execute(
                """
                UPDATE tool_invocations
                SET status = ?, serialized_result = ?, updated_at = ?
                WHERE session_id = ? AND run_id = ? AND tool_use_id = ?
                """,
                (
                    "completed",
                    serialized_result,
                    _now(),
                    session_id,
                    run_id,
                    tool_use_id,
                ),
            )
            updated = self._select_tool(connection, session_id, run_id, tool_use_id)
        if updated is None:
            raise RecoveryNotFoundError("Tool invocation was not found.")
        return self._tool_from_row(updated)

    async def mark_tool_outcome_unknown(
        self,
        *,
        session_id: str,
        run_id: str,
        tool_use_id: str,
        tool_name: str,
        input_hash: str,
    ) -> ToolInvocationRecord:
        return await asyncio.to_thread(
            self._mark_tool_outcome_unknown_sync,
            session_id,
            run_id,
            tool_use_id,
            tool_name,
            input_hash,
        )

    def _mark_tool_outcome_unknown_sync(
        self,
        session_id: str,
        run_id: str,
        tool_use_id: str,
        tool_name: str,
        input_hash: str,
    ) -> ToolInvocationRecord:
        with self._transaction() as connection:
            row = self._select_tool(connection, session_id, run_id, tool_use_id)
            if row is None:
                raise RecoveryNotFoundError("Tool invocation was not found.")
            record = self._tool_from_row(row)
            self._check_tool_identity(record, tool_name, input_hash)
            if record.status == "completed":
                raise ToolInvocationConflictError(
                    "Completed tool invocation cannot become outcome_unknown."
                )
            if record.status == "started":
                connection.execute(
                    """
                    UPDATE tool_invocations
                    SET status = ?, updated_at = ?
                    WHERE session_id = ? AND run_id = ? AND tool_use_id = ?
                    """,
                    ("outcome_unknown", _now(), session_id, run_id, tool_use_id),
                )
                row = self._select_tool(connection, session_id, run_id, tool_use_id)
        if row is None:
            raise RecoveryNotFoundError("Tool invocation was not found.")
        return self._tool_from_row(row)

    async def mark_started_tools_outcome_unknown(self, session_id: str, run_id: str) -> int:
        _require_text("session_id", session_id)
        _require_text("run_id", run_id)
        return await asyncio.to_thread(
            self._mark_started_tools_outcome_unknown_sync,
            session_id,
            run_id,
        )

    def _mark_started_tools_outcome_unknown_sync(self, session_id: str, run_id: str) -> int:
        with self._transaction() as connection:
            cursor = connection.execute(
                """
                UPDATE tool_invocations
                SET status = ?, updated_at = ?
                WHERE session_id = ? AND run_id = ? AND status = ?
                """,
                ("outcome_unknown", _now(), session_id, run_id, "started"),
            )
            return cursor.rowcount

    @staticmethod
    def _select_tool(
        connection: sqlite3.Connection,
        session_id: str,
        run_id: str,
        tool_use_id: str,
    ) -> sqlite3.Row | None:
        return cast(
            sqlite3.Row | None,
            connection.execute(
                """
                SELECT * FROM tool_invocations
                WHERE session_id = ? AND run_id = ? AND tool_use_id = ?
                """,
                (session_id, run_id, tool_use_id),
            ).fetchone(),
        )

    @staticmethod
    def _check_tool_identity(
        record: ToolInvocationRecord,
        tool_name: str,
        input_hash: str,
    ) -> None:
        if record.tool_name != tool_name or record.input_hash != input_hash:
            raise ToolInvocationConflictError(
                "Tool invocation identity conflicts with the persisted journal entry."
            )

    @staticmethod
    def _run_from_row(row: sqlite3.Row) -> RecoveryRun:
        return RecoveryRun(
            session_id=row["session_id"],
            run_id=row["run_id"],
            thread_id=row["thread_id"],
            engine=row["engine"],
            status=row["status"],
            suspension_reason=row["suspension_reason"],
            checkpoint_revision=row["checkpoint_revision"],
            event_seq=int(row["event_seq"]),
            resume_epoch=int(row["resume_epoch"]),
            transcript_commit_count=int(row["transcript_commit_count"]),
            transcript_commit_hash=row["transcript_commit_hash"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    @staticmethod
    def _tool_from_row(row: sqlite3.Row) -> ToolInvocationRecord:
        return ToolInvocationRecord(
            session_id=row["session_id"],
            run_id=row["run_id"],
            tool_use_id=row["tool_use_id"],
            tool_name=row["tool_name"],
            input_hash=row["input_hash"],
            status=row["status"],
            serialized_result=row["serialized_result"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )
