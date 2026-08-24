from __future__ import annotations

from collections.abc import Awaitable, Callable

from pydantic import BaseModel

type EventHandler = Callable[[BaseModel], Awaitable[None]]


class EventBus:
    def __init__(
        self,
        *,
        correlation_id: str | None = None,
        session_id: str | None = None,
        node_id: str | None = None,
        initial_event_seq: int = 0,
        assign_event_seq: bool | None = None,
    ) -> None:
        if initial_event_seq < 0:
            raise ValueError("initial_event_seq must not be negative")
        self._subscribers: list[EventHandler] = []
        self._correlation_id = correlation_id
        self._session_id = session_id
        self._node_id = node_id
        self._event_seq = initial_event_seq
        self._assign_event_seq = (
            correlation_id is not None and node_id is None
            if assign_event_seq is None
            else assign_event_seq
        )
        self._sequence_run_id: str | None = None

    @property
    def correlation_id(self) -> str | None:
        return self._correlation_id

    @property
    def session_id(self) -> str | None:
        return self._session_id

    @property
    def last_event_seq(self) -> int:
        return self._event_seq

    @property
    def event_seq(self) -> int:
        return self._event_seq

    def resume_event_seq(self, last_event_seq: int) -> None:
        """Advance the next root event beyond an already durable cursor."""

        if last_event_seq < 0:
            raise ValueError("last_event_seq must not be negative")
        self._event_seq = max(self._event_seq, last_event_seq)

    # 注册一个事件处理函数
    def subscribe(self, handler: EventHandler) -> None:
        self._subscribers.append(handler)

    def subscribe_first(self, handler: EventHandler) -> None:
        """Register the authoritative durable sink ahead of live fan-out."""

        self._subscribers.insert(0, handler)

    # 按注册顺序依次调用所有订阅者
    async def publish(self, event: BaseModel) -> None:
        updates: dict[str, object] = {}
        fields = type(event).model_fields
        for field_name, value in (
            ("correlation_id", self._correlation_id),
            ("session_id", self._session_id),
            ("node_id", self._node_id),
        ):
            if (
                value is not None
                and field_name in fields
                and getattr(event, field_name, None) is None
            ):
                updates[field_name] = value
        if "run_id" in fields and "event_seq" in fields and self._assign_event_seq:
            event_run_id = getattr(event, "run_id")
            event_seq = getattr(event, "event_seq")
            if self._sequence_run_id is None:
                self._sequence_run_id = event_run_id
            if event_run_id == self._sequence_run_id and event_seq is None:
                self._event_seq += 1
                updates["event_seq"] = self._event_seq
            elif (
                event_seq is not None
                and event_run_id == self._sequence_run_id
                and event_seq > self._event_seq
            ):
                self._event_seq = event_seq
        published = event.model_copy(update=updates) if updates else event
        for handler in self._subscribers:
            await handler(published)
