from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass

from langgraph.checkpoint.memory import InMemorySaver


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


class GraphRuntime:
    """App-scoped owner for the process-local LangGraph saver and thread locks."""

    def __init__(self, saver: InMemorySaver | None = None) -> None:
        self.saver = saver or InMemorySaver()
        self._locks: dict[str, _ThreadLockEntry] = {}
        self._threads: set[str] = set()
        self._close_lock = asyncio.Lock()
        self._closing = False
        self._closed = False

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

    async def close(self) -> None:
        """Clear only the threads owned by this runtime instance."""

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
                        if thread_id in self._threads:
                            await self._delete_thread_unlocked(thread_id)
                    finally:
                        entry.lock.release()
                completed = True
            finally:
                if completed:
                    self._closed = True
                self._closing = False
                for thread_id, entry in reserved:
                    self._release_entry(thread_id, entry)
