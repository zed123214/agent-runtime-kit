from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any


class EventLogError(RuntimeError):
    code = "event_log_error"


class EventLogCorruptionError(EventLogError):
    code = "event_log_corruption"


class EventSequenceError(EventLogCorruptionError):
    code = "event_sequence_error"


@dataclass(frozen=True, slots=True)
class EventLogSnapshot:
    events: tuple[dict[str, Any], ...]
    last_event_seq: int
    truncated_tail: bool


def _truncate_tail(path: Path, size: int) -> None:
    with path.open("r+b") as stream:
        stream.truncate(size)
        stream.flush()
        os.fsync(stream.fileno())


def read_event_log(path: Path, *, repair_tail: bool = False) -> EventLogSnapshot:
    """Read committed JSONL records and reject corruption before the tail."""

    if not path.exists():
        return EventLogSnapshot(events=(), last_event_seq=0, truncated_tail=False)

    payload = path.read_bytes()
    complete_size = len(payload)
    truncated_tail = bool(payload) and not payload.endswith(b"\n")
    if truncated_tail:
        last_newline = payload.rfind(b"\n")
        complete_size = last_newline + 1
        payload = payload[:complete_size]
        if repair_tail:
            _truncate_tail(path, complete_size)

    events: list[dict[str, Any]] = []
    last_event_seq = 0
    sequenced_rows_started = False
    lines = payload.split(b"\n")
    for line_no, raw_line in enumerate(lines, start=1):
        if not raw_line and line_no == len(lines):
            continue
        if raw_line.endswith(b"\r"):
            raw_line = raw_line[:-1]
        if not raw_line:
            raise EventLogCorruptionError(f"Empty event log row at line {line_no}.")
        try:
            decoded = raw_line.decode("utf-8")
            row = json.loads(decoded)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise EventLogCorruptionError(f"Invalid event log row at line {line_no}.") from exc
        if not isinstance(row, dict):
            raise EventLogCorruptionError(f"Event log row at line {line_no} must be a JSON object.")

        raw_seq = row.get("event_seq")
        if raw_seq is None:
            if sequenced_rows_started:
                raise EventSequenceError(
                    f"Missing event_seq after sequenced rows at line {line_no}."
                )
        else:
            if isinstance(raw_seq, bool) or not isinstance(raw_seq, int) or raw_seq <= 0:
                raise EventSequenceError(f"Invalid event_seq at line {line_no}.")
            expected = last_event_seq + 1
            if raw_seq != expected:
                raise EventSequenceError(
                    f"Expected event_seq {expected} at line {line_no}, got {raw_seq}."
                )
            last_event_seq = raw_seq
            sequenced_rows_started = True
        events.append(row)

    return EventLogSnapshot(
        events=tuple(events),
        last_event_seq=last_event_seq,
        truncated_tail=truncated_tail,
    )


def replay_events(
    path: Path,
    *,
    after_event_seq: int = 0,
    repair_tail: bool = False,
) -> list[dict[str, Any]]:
    """Return committed events strictly after the supplied durable cursor."""

    if after_event_seq < 0:
        raise ValueError("after_event_seq must not be negative")
    snapshot = read_event_log(path, repair_tail=repair_tail)
    replayed: list[dict[str, Any]] = []
    for event in snapshot.events:
        event_seq = event.get("event_seq")
        if event_seq is None:
            if after_event_seq == 0:
                replayed.append(dict(event))
        elif isinstance(event_seq, int) and event_seq > after_event_seq:
            replayed.append(dict(event))
    return replayed
