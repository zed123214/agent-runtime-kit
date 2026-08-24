from __future__ import annotations

import asyncio
import fnmatch
import logging
import uuid
import weakref
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from pydantic import BaseModel

from agent_runtime.core.bus.envelope import EventPushEnvelope
from agent_runtime.core.trace.record import TraceRecord
from agent_runtime.core.trace.writer import TraceWriter

logger = logging.getLogger(__name__)

_DRAIN_TIMEOUT_S = 1.0
_MAX_PAUSED_EVENTS = 1_000


def _now() -> str:
    return datetime.now(UTC).isoformat()


@dataclass
class _Subscription:
    sub_id: str
    writer: asyncio.StreamWriter
    topics: list[str]
    scope: str
    paused: bool = False
    activating: bool = False
    buffered_events: list[dict[str, Any]] = field(default_factory=list)


class IpcEventBroadcaster:
    def __init__(
        self,
        trace: TraceWriter | None = None,
        *,
        on_disconnect: Callable[[frozenset[str]], None] | None = None,
    ) -> None:
        self._subscriptions: list[_Subscription] = []
        self._session_ids_by_writer: dict[asyncio.StreamWriter, set[str]] = {}
        self._writer_by_session_id: dict[str, asyncio.StreamWriter] = {}
        self._pending_writer_by_session_id: dict[str, asyncio.StreamWriter] = {}
        self._disconnected_writers: weakref.WeakSet[asyncio.StreamWriter] = weakref.WeakSet()
        self._trace = trace
        self._on_disconnect = on_disconnect

    # 将新 session 独占绑定到当前活连接；断连 writer 和其他 owner 的 session 均拒绝绑定
    def bind_session(self, writer: asyncio.StreamWriter, session_id: str) -> bool:
        if writer in self._disconnected_writers:
            return False
        owner = self._writer_by_session_id.get(session_id)
        if owner is not None and owner is not writer:
            return False
        pending_owner = self._pending_writer_by_session_id.get(session_id)
        if pending_owner is not None and pending_owner is not writer:
            return False
        self._session_ids_by_writer.setdefault(writer, set()).add(session_id)
        self._writer_by_session_id[session_id] = writer
        self._pending_writer_by_session_id.pop(session_id, None)
        return True

    def reserve_session(self, writer: asyncio.StreamWriter, session_id: str) -> bool:
        """Reserve exclusive attach ownership without granting owner privileges."""

        if writer in self._disconnected_writers:
            return False
        owner = self._writer_by_session_id.get(session_id)
        pending_owner = self._pending_writer_by_session_id.get(session_id)
        if pending_owner is not None or (owner is not None and owner is not writer):
            return False
        if owner is writer:
            return True
        self._pending_writer_by_session_id[session_id] = writer
        return True

    def commit_reserved_session(self, writer: asyncio.StreamWriter, session_id: str) -> bool:
        """Activate one reservation after the capability CAS commits."""

        if writer in self._disconnected_writers:
            return False
        if self._writer_by_session_id.get(session_id) is writer:
            return True
        if self._pending_writer_by_session_id.get(session_id) is not writer:
            return False
        self._pending_writer_by_session_id.pop(session_id, None)
        self._session_ids_by_writer.setdefault(writer, set()).add(session_id)
        self._writer_by_session_id[session_id] = writer
        return True

    def release_reserved_session(self, writer: asyncio.StreamWriter, session_id: str) -> bool:
        if self._pending_writer_by_session_id.get(session_id) is not writer:
            return False
        self._pending_writer_by_session_id.pop(session_id, None)
        return True

    # 返回连接拥有的 session 快照，调用方不能修改内部绑定状态
    def session_ids_for(self, writer: asyncio.StreamWriter) -> frozenset[str]:
        if writer in self._disconnected_writers:
            return frozenset()
        return frozenset(self._session_ids_by_writer.get(writer, ()))

    def owns_session(self, writer: asyncio.StreamWriter, session_id: str) -> bool:
        return (
            writer not in self._disconnected_writers
            and self._writer_by_session_id.get(session_id) is writer
        )

    def unbind_session(self, writer: asyncio.StreamWriter, session_id: str) -> bool:
        """Release one exact ownership binding without affecting sibling sessions."""

        if self._writer_by_session_id.get(session_id) is not writer:
            return False
        self._writer_by_session_id.pop(session_id, None)
        session_ids = self._session_ids_by_writer.get(writer)
        if session_ids is not None:
            session_ids.discard(session_id)
            if not session_ids:
                self._session_ids_by_writer.pop(writer, None)
        return True

    # 注册一个客户端订阅，返回 subscription_id
    def subscribe(
        self,
        writer: asyncio.StreamWriter,
        topics: list[str],
        scope: str = "global",
        *,
        paused: bool = False,
    ) -> str:
        sub_id = f"sub-{uuid.uuid4().hex[:8]}"
        if writer in self._disconnected_writers:
            return sub_id
        sub = _Subscription(
            sub_id=sub_id,
            writer=writer,
            topics=topics,
            scope=scope,
            paused=paused,
        )
        self._subscriptions.append(sub)
        return sub_id

    async def activate_buffered(
        self,
        sub_id: str,
        *,
        replay_run_id: str,
        after_event_seq: int,
    ) -> bool:
        """Drain a paused subscription after a replay snapshot, then make it live.

        Events persisted while the snapshot is read remain buffered. Rows at or
        below the snapshot high-water mark are replay duplicates and are
        discarded; later sequenced rows are delivered once in sequence order.
        The subscription stays paused while each batch drains, so publishers can
        append the next batch without waiting for the client writer.
        """

        if after_event_seq < 0:
            raise ValueError("after_event_seq must not be negative")
        sub = next((item for item in self._subscriptions if item.sub_id == sub_id), None)
        if sub is None or not sub.paused or sub.activating:
            return False

        sub.activating = True
        delivered_event_seq = after_event_seq
        try:
            while self._is_active(sub):
                batch = sub.buffered_events
                sub.buffered_events = []
                if not batch:
                    # No await separates the empty check and live transition.
                    # A concurrent handle therefore either joined the buffer or
                    # observes live state; there is no replay-to-live gap.
                    sub.paused = False
                    return True

                for event in batch:
                    event_seq = event.get("event_seq")
                    is_replayed_run = event.get("run_id") == replay_run_id
                    if (
                        is_replayed_run
                        and isinstance(event_seq, int)
                        and event_seq <= delivered_event_seq
                    ):
                        continue
                    if not await self._deliver(sub, event):
                        return False
                    if is_replayed_run and isinstance(event_seq, int):
                        delivered_event_seq = event_seq
            return False
        finally:
            sub.activating = False

    async def drain_replay(self, writer: asyncio.StreamWriter) -> bool:
        """Bound replay backpressure with the same disconnect semantics as live push."""

        try:
            await asyncio.wait_for(writer.drain(), timeout=_DRAIN_TIMEOUT_S)
            return True
        except (TimeoutError, ConnectionResetError, BrokenPipeError, OSError):
            logger.debug("dead replay connection, cleaning up")
            self.disconnect(writer)
            return False

    def unsubscribe_id(self, sub_id: str) -> None:
        """Remove one subscription without changing connection ownership."""

        self._subscriptions = [sub for sub in self._subscriptions if sub.sub_id != sub_id]

    # 移除指定 writer 的订阅和 ownership，但不把活连接永久标记为断开
    def unsubscribe(self, writer: asyncio.StreamWriter) -> frozenset[str]:
        self._subscriptions = [s for s in self._subscriptions if s.writer is not writer]
        session_ids = frozenset(self._session_ids_by_writer.pop(writer, ()))
        for session_id in session_ids:
            if self._writer_by_session_id.get(session_id) is writer:
                self._writer_by_session_id.pop(session_id, None)
        for session_id, pending_writer in list(self._pending_writer_by_session_id.items()):
            if pending_writer is writer:
                self._pending_writer_by_session_id.pop(session_id, None)
        return session_ids

    def mark_disconnected(self, writer: asyncio.StreamWriter) -> None:
        self._disconnected_writers.add(writer)

    # 断连清理带 tombstone，阻止仍在收尾的 handler 重新绑定 owner
    def disconnect(self, writer: asyncio.StreamWriter) -> frozenset[str]:
        self.mark_disconnected(writer)
        session_ids = self.unsubscribe(writer)
        if session_ids and self._on_disconnect is not None:
            self._on_disconnect(session_ids)
        return session_ids

    # 检查连接是否可接收事件；permission.* 必须属于该连接创建的 session
    def can_receive(
        self,
        writer: asyncio.StreamWriter,
        event: dict[str, Any],
        scope: str = "global",
    ) -> bool:
        if writer in self._disconnected_writers:
            return False

        run_id = event.get("run_id")
        if not self._matches_scope(run_id if isinstance(run_id, str) else None, scope):
            return False

        event_type = event.get("type")
        session_id = event.get("session_id")
        if isinstance(session_id, str) and session_id:
            return self.owns_session(writer, session_id) or (
                self._pending_writer_by_session_id.get(session_id) is writer
            )
        if session_id not in (None, ""):
            return False

        # Only explicit daemon-global core lifecycle events are public. Unknown
        # ownerless event types fail closed so future engines/plugins cannot
        # accidentally expose data by omitting RunEvent metadata.
        return isinstance(event_type, str) and event_type.startswith("core.")

    # 安全 replay_from_run 将已校验 run_id 作为只读历史定位 capability；不授予 live ownership
    def can_replay(
        self,
        writer: asyncio.StreamWriter,
        event: dict[str, Any],
        scope: str = "global",
    ) -> bool:
        if writer in self._disconnected_writers:
            return False
        run_id = event.get("run_id")
        if not self._matches_scope(run_id if isinstance(run_id, str) else None, scope):
            return False
        # _replay_events has already established either session ownership or a
        # strong replay capability, resolved a controlled path, and required an
        # exact per-record run_id match. Legacy records may omit optional
        # session/correlation metadata after that path-level authorization.
        return isinstance(run_id, str) and bool(run_id)

    # 将事件推送到所有匹配的订阅客户端；超时或写入失败立即清理死连接
    async def handle(self, event: BaseModel) -> None:
        event_dict = event.model_dump()
        event_type: str = event_dict.get("type", "")

        for sub in list(self._subscriptions):
            if not self._matches_topic(event_type, sub.topics):
                continue
            if not self.can_receive(sub.writer, event_dict, sub.scope):
                continue
            if sub.paused:
                if len(sub.buffered_events) >= _MAX_PAUSED_EVENTS:
                    self.disconnect(sub.writer)
                    continue
                sub.buffered_events.append(event_dict)
                continue
            await self._deliver(sub, event_dict)

    async def _deliver(self, sub: _Subscription, event: dict[str, Any]) -> bool:
        if not self._is_active(sub):
            return False
        try:
            envelope = EventPushEnvelope(event=event)
            sub.writer.write(envelope.model_dump_json().encode() + b"\n")
            await asyncio.wait_for(sub.writer.drain(), timeout=_DRAIN_TIMEOUT_S)
            if self._trace is not None:
                client_id = str(sub.writer.get_extra_info("peername", "<unknown>"))
                self._trace.emit(
                    TraceRecord(
                        ts=_now(),
                        direction="CORE→CLIENT",
                        layer="ipc",
                        kind="push",
                        run_id=event.get("run_id"),
                        client_id=client_id,
                        data={"sub_id": sub.sub_id, "event_type": event.get("type", "")},
                    )
                )
            return True
        except (TimeoutError, ConnectionResetError, BrokenPipeError, OSError):
            logger.debug("dead connection for sub %s, cleaning up", sub.sub_id)
            self.disconnect(sub.writer)
            return False

    def _is_active(self, sub: _Subscription) -> bool:
        return sub.writer not in self._disconnected_writers and any(
            item is sub for item in self._subscriptions
        )

    # 检查事件类型是否匹配订阅的 topic 列表（支持 fnmatch glob 模式）
    @staticmethod
    def _matches_topic(event_type: str, topics: list[str]) -> bool:
        return any(fnmatch.fnmatch(event_type, pattern) for pattern in topics)

    # 检查事件 run_id 是否匹配订阅的 scope（global 全通，run:<id> 精确匹配）
    @staticmethod
    def _matches_scope(run_id: str | None, scope: str) -> bool:
        if scope == "global":
            return True
        if scope.startswith("run:"):
            return run_id == scope[4:]
        return False
