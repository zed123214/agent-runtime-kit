from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import IO

from pydantic import BaseModel

from agent_runtime.core.events.bus import EventBus
from agent_runtime.core.graph.event_log import EventSequenceError, read_event_log

logger = logging.getLogger(__name__)


class EventWriter:
    def __init__(self, path: Path, *, run_id: str | None = None) -> None:
        self._path = path
        self._run_id = run_id
        self._file: IO[str] | None = None
        self._last_event_seq = 0

    @property
    def last_event_seq(self) -> int:
        return self._last_event_seq

    # 打开事件文件（追加模式），供 async with 使用
    async def __aenter__(self) -> EventWriter:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        snapshot = read_event_log(self._path, repair_tail=True)
        self._last_event_seq = snapshot.last_event_seq
        self._file = open(self._path, "a", encoding="utf-8", newline="\n")
        return self

    # 关闭事件文件
    async def __aexit__(self, *args: object) -> None:
        if self._file is not None:
            self._file.close()
            self._file = None

    # 将事件序列化为 JSON 行并写入文件，写入失败时记录日志但不抛出异常
    async def handle(self, event: BaseModel) -> None:
        if self._file is None:
            return
        if self._run_id is not None and getattr(event, "run_id", None) != self._run_id:
            return
        event_seq = getattr(event, "event_seq", None)
        if event_seq is not None:
            expected = self._last_event_seq + 1
            if event_seq != expected:
                raise EventSequenceError(
                    f"Expected event_seq {expected} while writing, got {event_seq}."
                )
        try:
            self._file.write(event.model_dump_json() + "\n")
            self._file.flush()
            os.fsync(self._file.fileno())
        except (OSError, ValueError) as e:
            logger.error("EventWriter: failed to write event: %s", e)
            if self._run_id is not None:
                raise
        else:
            if isinstance(event_seq, int):
                self._last_event_seq = event_seq

    # 将 handle 注册为 bus 的订阅者
    def subscribe(self, bus: EventBus) -> None:
        bus.resume_event_seq(self._last_event_seq)
        bus.subscribe_first(self.handle)
