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
    ) -> None:
        self._subscribers: list[EventHandler] = []
        self._correlation_id = correlation_id
        self._session_id = session_id
        self._node_id = node_id

    @property
    def correlation_id(self) -> str | None:
        return self._correlation_id

    @property
    def session_id(self) -> str | None:
        return self._session_id

    # 注册一个事件处理函数
    def subscribe(self, handler: EventHandler) -> None:
        self._subscribers.append(handler)

    # 按注册顺序依次调用所有订阅者
    async def publish(self, event: BaseModel) -> None:
        updates: dict[str, str] = {}
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
        published = event.model_copy(update=updates) if updates else event
        for handler in self._subscribers:
            await handler(published)
