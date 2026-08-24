from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path

from langgraph.checkpoint.base import BaseCheckpointSaver, CheckpointTuple
from langgraph.checkpoint.memory import InMemorySaver

from agent_runtime.core.graph.checkpoint import (
    CheckpointBackend,
    CheckpointRuntime,
)
from agent_runtime.core.graph.interrupts import PermissionInterruptPayload
from agent_runtime.core.graph.recovery import RecoveryStore, hash_tool_input
from agent_runtime.core.permissions.policy import param_preview


def thread_config(thread_id: str) -> dict[str, dict[str, str]]:
    """Return the base config for the latest checkpoint in one thread."""

    cleaned = thread_id.strip()
    if not cleaned:
        raise ValueError("Graph thread_id must not be empty")
    return {"configurable": {"thread_id": cleaned}}


@dataclass(slots=True)
class _ThreadLockEntry:
    lock: asyncio.Lock
    users: int = 0


class GraphStateConflictError(RuntimeError):
    code = "checkpoint_conflict"


@dataclass(frozen=True, slots=True)
class GraphStateSummary:
    session_id: str
    run_id: str
    thread_id: str
    status: str
    current_node: str | None
    next_node: str | None
    suspension_reason: str | None
    checkpoint_revision: str
    event_seq: int
    interrupt_id: str | None
    pending_tool_use_id: str | None
    pending_tool_name: str | None
    pending_param_preview: str | None
    pending_expires_at: str | None
    resumable: bool


class GraphRuntime:
    """App-scoped owner for one LangGraph saver and per-thread execution locks."""

    def __init__(
        self,
        saver: BaseCheckpointSaver[str] | None = None,
        *,
        checkpoint_backend: CheckpointBackend = "memory",
        sqlite_path: Path | None = None,
        recovery_store: RecoveryStore | None = None,
    ) -> None:
        if saver is not None and checkpoint_backend != "memory":
            raise ValueError("A custom saver cannot be combined with a configured backend")
        self._backend = checkpoint_backend
        self._sqlite_path = sqlite_path
        self._recovery_store = recovery_store
        self._saver: BaseCheckpointSaver[str] | None = (
            saver or InMemorySaver() if checkpoint_backend == "memory" else None
        )
        self._checkpoint_owner: CheckpointRuntime | None = None
        self._open_lock = asyncio.Lock()
        self._locks: dict[str, _ThreadLockEntry] = {}
        self._threads: set[str] = set()
        self._close_lock = asyncio.Lock()
        self._closing = False
        self._closed = False

    @property
    def backend(self) -> CheckpointBackend:
        return self._backend

    @property
    def is_durable(self) -> bool:
        return self._backend == "sqlite"

    @property
    def recovery_store(self) -> RecoveryStore | None:
        return self._recovery_store

    @property
    def saver(self) -> BaseCheckpointSaver[str]:
        if self._saver is None:
            raise RuntimeError("GraphRuntime must be opened before accessing its saver")
        if self._closed:
            raise RuntimeError("GraphRuntime is closed")
        return self._saver

    async def ensure_open(self) -> None:
        if self._closed or self._closing:
            raise RuntimeError("GraphRuntime is closing or closed")
        if self._saver is not None:
            return
        async with self._open_lock:
            if self._saver is not None:
                return
            owner = await CheckpointRuntime.open(
                self._backend,
                sqlite_path=self._sqlite_path,
            )
            self._checkpoint_owner = owner
            self._saver = owner.saver

    @property
    def known_threads(self) -> frozenset[str]:
        return frozenset(self._threads)

    def _reserve_entry(self, thread_id: str) -> _ThreadLockEntry:
        entry = self._locks.get(thread_id)
        if entry is None:
            entry = _ThreadLockEntry(lock=asyncio.Lock())
            self._locks[thread_id] = entry
        entry.users += 1
        return entry

    def _release_entry(self, thread_id: str, entry: _ThreadLockEntry) -> None:
        entry.users -= 1
        if entry.users < 0:
            raise RuntimeError(f"GraphRuntime lock reference underflow for {thread_id!r}")
        if (
            entry.users == 0
            and thread_id not in self._threads
            and self._locks.get(thread_id) is entry
        ):
            self._locks.pop(thread_id, None)

    @asynccontextmanager
    async def thread_scope(self, thread_id: str) -> AsyncIterator[None]:
        """Serialize checkpoint reconciliation and graph execution per thread."""

        await self.ensure_open()
        cleaned = thread_id.strip()
        if not cleaned:
            raise ValueError("Graph thread_id must not be empty")
        if self._closed or self._closing:
            raise RuntimeError("GraphRuntime is closing or closed")

        entry = self._reserve_entry(cleaned)
        acquired = False
        try:
            await entry.lock.acquire()
            acquired = True
            # close() may have started while this scope waited for the per-thread
            # lock. Reject it before ownership or checkpoint state can change.
            if self._closed or self._closing:
                raise RuntimeError("GraphRuntime is closing or closed")
            self._threads.add(cleaned)
            yield
        finally:
            if acquired:
                entry.lock.release()
            self._release_entry(cleaned, entry)

    async def delete_thread(self, thread_id: str) -> None:
        """Delete one thread through the saver public API; safe to call repeatedly."""

        cleaned = thread_id.strip()
        if not cleaned:
            return
        if self._closed or self._closing:
            return
        await self.ensure_open()

        entry = self._reserve_entry(cleaned)
        acquired = False
        try:
            await entry.lock.acquire()
            acquired = True
            # A concurrent close owns cleanup for every entry that existed when
            # it raised the closing gate.
            if self._closed or self._closing:
                return
            await self._delete_thread_unlocked(cleaned)
        finally:
            if acquired:
                entry.lock.release()
            self._release_entry(cleaned, entry)

    async def _delete_thread_unlocked(self, thread_id: str) -> None:
        await self._reset_thread_unlocked(thread_id)
        self._threads.discard(thread_id)

    async def _reset_thread_unlocked(self, thread_id: str) -> None:
        """Clear a checkpoint while retaining ownership of an active thread."""

        await self.saver.adelete_thread(thread_id)
        if await self.saver.aget_tuple(thread_config(thread_id)) is not None:  # type: ignore[arg-type]
            raise RuntimeError(f"LangGraph saver did not delete thread {thread_id!r}")

    async def latest_checkpoint(self, thread_id: str) -> CheckpointTuple | None:
        cleaned = thread_id.strip()
        if not cleaned:
            raise ValueError("Graph thread_id must not be empty")
        await self.ensure_open()
        return await self.saver.aget_tuple(thread_config(cleaned))  # type: ignore[arg-type]

    async def checkpoint_revision(self, thread_id: str) -> str | None:
        checkpoint = await self.latest_checkpoint(thread_id)
        if checkpoint is None:
            return None
        return self._revision_from_checkpoint(checkpoint)

    async def latest_state(
        self,
        thread_id: str,
        *,
        session_id: str,
        run_id: str,
    ) -> GraphStateSummary | None:
        checkpoint = await self.latest_checkpoint(thread_id)
        if checkpoint is None:
            return None
        revision = self._revision_from_checkpoint(checkpoint)
        raw_values = checkpoint.checkpoint.get("channel_values", {})
        if not isinstance(raw_values, dict):
            raise GraphStateConflictError("Checkpoint channel values are invalid.")
        values = raw_values
        checkpoint_run_id = values.get("run_id")
        if checkpoint_run_id != run_id:
            raise GraphStateConflictError("Checkpoint run identity does not match the request.")

        pending_interrupt: PermissionInterruptPayload | None = None
        interrupt_id: str | None = None
        for _task_id, channel, raw_write in checkpoint.pending_writes or ():
            if channel != "__interrupt__":
                continue
            raw_interrupts = raw_write if isinstance(raw_write, (list, tuple)) else (raw_write,)
            for raw_interrupt in raw_interrupts:
                raw_value = getattr(raw_interrupt, "value", None)
                parsed = PermissionInterruptPayload.from_value(raw_value)
                if parsed is None:
                    continue
                pending_interrupt = parsed
                raw_id = getattr(raw_interrupt, "id", None)
                interrupt_id = raw_id if isinstance(raw_id, str) else None
                break
            if pending_interrupt is not None:
                break
        if pending_interrupt is not None and (
            pending_interrupt.session_id != session_id or pending_interrupt.run_id != run_id
        ):
            raise GraphStateConflictError(
                "Checkpoint interrupt identity does not match the request."
            )

        raw_status = values.get("status")
        checkpoint_status = raw_status if isinstance(raw_status, str) else "running"
        status = checkpoint_status
        suspension_reason: str | None = "permission" if pending_interrupt is not None else None
        raw_calls = values.get("pending_tool_calls")
        pending_call = (
            raw_calls[0]
            if isinstance(raw_calls, list) and raw_calls and isinstance(raw_calls[0], dict)
            else None
        )
        event_seq = 0
        recovery_run = None
        if self._recovery_store is not None:
            recovery_run = await self._recovery_store.get_run(session_id, run_id)
            if recovery_run is not None:
                status = recovery_run.status
                suspension_reason = recovery_run.suspension_reason
                event_seq = recovery_run.event_seq
            if pending_interrupt is None and pending_call is not None:
                raw_tool_use_id = pending_call.get("id")
                raw_tool_name = pending_call.get("name")
                raw_tool_input = pending_call.get("input")
                if (
                    isinstance(raw_tool_use_id, str)
                    and isinstance(raw_tool_name, str)
                    and isinstance(raw_tool_input, dict)
                ):
                    invocation = await self._recovery_store.get_tool(
                        session_id,
                        run_id,
                        raw_tool_use_id,
                    )
                    if invocation is not None:
                        if (
                            invocation.tool_name != raw_tool_name
                            or invocation.input_hash != hash_tool_input(raw_tool_input)
                        ):
                            raise GraphStateConflictError(
                                "Checkpoint tool identity does not match the journal."
                            )
                        if invocation.status == "outcome_unknown" or (
                            invocation.status == "started" and status == "suspended"
                        ):
                            status = "suspended"
                            suspension_reason = "outcome_unknown"
        if recovery_run is not None and recovery_run.status in ("success", "failed"):
            if (
                checkpoint_status != recovery_run.status
                or pending_interrupt is not None
                or (isinstance(raw_calls, list) and raw_calls)
            ):
                raise GraphStateConflictError(
                    "Terminal recovery metadata conflicts with pending checkpoint work."
                )
        if pending_interrupt is not None and status not in ("success", "failed"):
            status = "suspended"
            suspension_reason = "permission"

        if status in ("success", "failed"):
            next_node = None
        elif pending_interrupt is not None:
            next_node = "kit_tools"
        else:
            next_node = "kit_tools" if isinstance(raw_calls, list) and raw_calls else "model"

        pending_tool_use_id = (
            pending_interrupt.tool_use_id
            if pending_interrupt is not None
            else pending_call.get("id")
            if pending_call is not None and isinstance(pending_call.get("id"), str)
            else None
        )
        pending_tool_name = (
            pending_interrupt.tool_name
            if pending_interrupt is not None
            else pending_call.get("name")
            if pending_call is not None and isinstance(pending_call.get("name"), str)
            else None
        )
        pending_param_preview = (
            pending_interrupt.param_preview if pending_interrupt is not None else None
        )
        if (
            pending_param_preview is None
            and pending_tool_name is not None
            and pending_call is not None
            and isinstance(pending_call.get("input"), dict)
        ):
            pending_param_preview = param_preview(
                pending_tool_name,
                pending_call["input"],
            )

        current_node: str | None = None
        raw_writes = checkpoint.metadata.get("writes")
        if isinstance(raw_writes, dict):
            current_node = next(
                (node for node in raw_writes if isinstance(node, str)),
                None,
            )

        return GraphStateSummary(
            session_id=session_id,
            run_id=run_id,
            thread_id=thread_id,
            status=status,
            current_node=current_node,
            next_node=next_node,
            suspension_reason=suspension_reason,
            checkpoint_revision=revision,
            event_seq=event_seq,
            interrupt_id=interrupt_id,
            pending_tool_use_id=pending_tool_use_id,
            pending_tool_name=pending_tool_name,
            pending_param_preview=pending_param_preview,
            pending_expires_at=(
                pending_interrupt.expires_at if pending_interrupt is not None else None
            ),
            resumable=(
                self.is_durable and status == "suspended" and suspension_reason != "outcome_unknown"
            ),
        )

    @staticmethod
    def _revision_from_checkpoint(checkpoint: CheckpointTuple) -> str:
        configurable = checkpoint.config.get("configurable", {})
        revision = configurable.get("checkpoint_id")
        if not isinstance(revision, str) or not revision:
            raise GraphStateConflictError("Checkpoint revision is missing.")
        return revision

    async def close(self) -> None:
        """Close resources, preserving durable SQLite checkpoints."""

        async with self._close_lock:
            if self._closed:
                return

            # This synchronous gate is raised before the first cleanup await.
            # Existing registered scopes are represented by reserved entries;
            # later scopes fail before they can create a second lock.
            self._closing = True
            reserved = sorted(self._locks.items())
            for _thread_id, entry in reserved:
                entry.users += 1

            completed = False
            try:
                for thread_id, entry in reserved:
                    await entry.lock.acquire()
                    try:
                        if thread_id in self._threads and not self.is_durable:
                            await self._delete_thread_unlocked(thread_id)
                    finally:
                        entry.lock.release()
                owner = self._checkpoint_owner
                if owner is not None:
                    await owner.close()
                self._threads.clear()
                completed = True
            finally:
                if completed:
                    self._closed = True
                self._closing = False
                for thread_id, entry in reserved:
                    self._release_entry(thread_id, entry)
