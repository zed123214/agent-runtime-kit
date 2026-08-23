from __future__ import annotations

import asyncio

import pytest

pytest.importorskip("langgraph")

from agent_runtime.core.graph.runtime import GraphRuntime  # noqa: E402

pytestmark = pytest.mark.graph

_WAIT_S = 2.0


class _ClosingGateRuntime(GraphRuntime):
    def __init__(self) -> None:
        super().__init__()
        self.delete_entered = asyncio.Event()
        self.release_delete = asyncio.Event()

    async def _delete_thread_unlocked(self, thread_id: str) -> None:
        if thread_id == "old-thread":
            self.delete_entered.set()
            await self.release_delete.wait()
        await super()._delete_thread_unlocked(thread_id)


async def test_close_rejects_new_thread_scope_before_cleanup_finishes() -> None:
    runtime = _ClosingGateRuntime()
    async with runtime.thread_scope("old-thread"):
        pass

    close_task = asyncio.create_task(runtime.close())
    await asyncio.wait_for(runtime.delete_entered.wait(), timeout=_WAIT_S)

    async def enter_new_thread() -> None:
        async with runtime.thread_scope("new-thread"):
            pass

    try:
        with pytest.raises(RuntimeError, match="clos"):
            await asyncio.wait_for(enter_new_thread(), timeout=_WAIT_S)
    finally:
        runtime.release_delete.set()
        await asyncio.wait_for(close_task, timeout=_WAIT_S)

    assert runtime.known_threads == frozenset()
    assert runtime._locks == {}


async def test_deleted_thread_lock_entries_are_reclaimed() -> None:
    runtime = GraphRuntime()

    for index in range(25):
        await asyncio.wait_for(runtime.delete_thread(f"one-shot-{index}"), timeout=_WAIT_S)

    assert runtime.known_threads == frozenset()
    assert runtime._locks == {}


async def test_lock_reclamation_does_not_create_parallel_locks_for_waiters() -> None:
    runtime = GraphRuntime()
    first_entered = asyncio.Event()
    release_first = asyncio.Event()
    second_entered = asyncio.Event()
    release_second = asyncio.Event()
    third_entered = asyncio.Event()

    async def first_scope() -> None:
        async with runtime.thread_scope("shared"):
            first_entered.set()
            await release_first.wait()

    async def second_scope() -> None:
        async with runtime.thread_scope("shared"):
            second_entered.set()
            await release_second.wait()

    async def third_scope() -> None:
        async with runtime.thread_scope("shared"):
            third_entered.set()

    first_task = asyncio.create_task(first_scope())
    await asyncio.wait_for(first_entered.wait(), timeout=_WAIT_S)
    delete_task = asyncio.create_task(runtime.delete_thread("shared"))
    await asyncio.sleep(0)
    second_task = asyncio.create_task(second_scope())
    await asyncio.sleep(0)
    release_first.set()

    await asyncio.wait_for(delete_task, timeout=_WAIT_S)
    await asyncio.wait_for(second_entered.wait(), timeout=_WAIT_S)
    third_task = asyncio.create_task(third_scope())
    await asyncio.sleep(0)
    assert not third_entered.is_set()

    release_second.set()
    await asyncio.wait_for(second_task, timeout=_WAIT_S)
    await asyncio.wait_for(third_task, timeout=_WAIT_S)
    await asyncio.wait_for(first_task, timeout=_WAIT_S)
    assert third_entered.is_set()

    await asyncio.wait_for(runtime.delete_thread("shared"), timeout=_WAIT_S)
    assert runtime.known_threads == frozenset()
    assert runtime._locks == {}
