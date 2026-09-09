"""Bounded in-memory identity journal, owned by the trusted broker.

No eviction and no per-entry expiry. Identities live until the Pod dies; after a
generation loss Core must reject the old Session. This is not a durable journal.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from agent_runtime.sandbox_server.protocol import (
    MAX_RESULT_BYTES,
    Operation,
    encode,
    error_result,
)


class JournalError(RuntimeError):
    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


@dataclass
class Entry:
    input_hash: str
    reserved: int
    result: bytes | None = None
    task: asyncio.Task[bytes] | None = None


class Journal:
    def __init__(self, max_entries: int = 1024, max_bytes: int = 16777216) -> None:
        self.max_entries = max_entries
        self.max_bytes = max_bytes
        self.entries: dict[tuple[str, str, str], Entry] = {}
        self.bytes_used = 0
        self.closed = False

    def _claim(self, op: Operation) -> tuple[Entry, bool]:
        key = op.identity.key
        previous = self.entries.get(key)
        if previous is not None:
            if previous.input_hash != op.input_hash:
                raise JournalError("idempotency_conflict")
            return previous, False
        if self.closed:
            raise JournalError("workspace_lost")
        # Reserve worst-case serialized result plus bounded key/entry overhead
        # BEFORE dispatch; even all concurrently-running commands can finish.
        reserve = MAX_RESULT_BYTES + 2048
        if len(self.entries) >= self.max_entries or self.bytes_used + reserve > self.max_bytes:
            raise JournalError("idempotency_capacity")
        entry = Entry(op.input_hash, reserve)
        self.entries[key] = entry  # No await between lookup, capacity, and insert.
        self.bytes_used += reserve
        return entry, True

    def _finish(self, entry: Entry, payload: bytes) -> bytes:
        if len(payload) > MAX_RESULT_BYTES:
            payload = encode(error_result("outcome_unknown", "Worker result exceeded its bound"))
        self.bytes_used -= entry.reserved - len(payload) - 2048
        entry.reserved = len(payload) + 2048
        entry.result = payload
        return payload

    async def execute(
        self, op: Operation, dispatch: Callable[[], Awaitable[dict[str, Any]]]
    ) -> bytes:
        entry, fresh = self._claim(op)
        if fresh:
            entry.task = asyncio.create_task(self._execute(entry, dispatch))
        if entry.result is not None:
            return entry.result
        assert entry.task is not None
        # Disconnects cannot erase a dispatched identity or run a second process.
        return await asyncio.shield(entry.task)

    async def _execute(
        self, entry: Entry, dispatch: Callable[[], Awaitable[dict[str, Any]]]
    ) -> bytes:
        try:
            result = await dispatch()
        except asyncio.CancelledError:
            result = error_result("cancelled", "Command cancelled")
        except Exception:
            # Never echo transport exceptions: they may contain request headers.
            result = error_result("outcome_unknown", "Execution outcome is unknown")
        try:
            payload = encode(result)
        except (TypeError, ValueError):
            payload = encode(
                error_result("outcome_unknown", "Execution result could not be serialized")
            )
        return self._finish(entry, payload)

    async def cancel(self, op: Operation) -> None:
        entry, fresh = self._claim(op)
        if fresh:
            self._finish(entry, encode(error_result("cancelled", "Cancelled before dispatch")))
        elif entry.task is not None and not entry.task.done():
            entry.task.cancel()
            try:
                await asyncio.shield(entry.task)
            except asyncio.CancelledError:
                if not entry.task.cancelled():
                    raise
                # cancel() can win before _execute starts. Preserve that identity
                # as a terminal tombstone too; a late retry must never execute it.
                self._finish(entry, encode(error_result("cancelled", "Cancelled before dispatch")))

    async def close(self) -> None:
        self.closed = True
        tasks = [
            entry.task
            for entry in self.entries.values()
            if entry.task is not None and not entry.task.done()
        ]
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
