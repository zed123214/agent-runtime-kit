from __future__ import annotations

import json
from pathlib import Path

import pytest
from pydantic import BaseModel, TypeAdapter

from agent_runtime.core.bus.events import (
    Event,
    NodeStartedEvent,
    RunFinishedEvent,
    RunResumedEvent,
    RunStartedEvent,
    RunSuspendedEvent,
    StepStartedEvent,
    SubagentStartedEvent,
)
from agent_runtime.core.events.bus import EventBus
from agent_runtime.core.events.writer import EventWriter
from agent_runtime.core.graph.event_log import (
    EventLogCorruptionError,
    EventSequenceError,
    read_event_log,
    replay_events,
)
from agent_runtime.core.session.store import (
    SessionStore,
    TranscriptConflictError,
    TranscriptCorruptionError,
    canonical_transcript_commit,
)


def _started(run_id: str) -> RunStartedEvent:
    return RunStartedEvent(run_id=run_id, goal="goal", ts="2026-08-24T00:00:00Z")


async def test_root_cursor_includes_node_events_while_child_root_is_independent() -> None:
    root = EventBus(correlation_id="root-run", session_id="session-1")
    node = EventBus(
        correlation_id=root.correlation_id,
        session_id=root.session_id,
        node_id="model",
    )
    child = EventBus(correlation_id=root.correlation_id, session_id=root.session_id)
    root_events: list[BaseModel] = []
    child_events: list[BaseModel] = []

    async def collect_root(event: BaseModel) -> None:
        root_events.append(event)

    async def collect_child(event: BaseModel) -> None:
        child_events.append(event)

    root.subscribe(collect_root)
    node.subscribe(root.publish)
    child.subscribe(collect_child)
    child.subscribe(root.publish)

    root_started = _started("root-run")
    node_started = NodeStartedEvent(run_id="root-run", node_id="model", ts="t1")
    child_started = SubagentStartedEvent(
        run_id="child-run",
        parent_run_id="root-run",
        description="child",
        ts="t2",
    )
    await root.publish(root_started)
    await node.publish(node_started)
    await child.publish(child_started)
    await root.publish(RunFinishedEvent(run_id="root-run", status="success", steps=1, ts="t3"))

    assert root_started.event_seq is None
    assert node_started.event_seq is None
    assert child_started.event_seq is None
    assert [
        event.event_seq for event in root_events if getattr(event, "run_id", None) == "root-run"
    ] == [1, 2, 3]
    assert child_events[0].event_seq == 1  # type: ignore[attr-defined]
    assert root_events[2].event_seq == 1  # type: ignore[attr-defined]
    assert root.event_seq == 3
    assert root.last_event_seq == 3
    assert child.event_seq == 1


def test_suspend_resume_events_are_typed_and_legacy_event_seq_stays_optional() -> None:
    adapter = TypeAdapter(Event)
    legacy = adapter.validate_python(
        {"type": "run.started", "run_id": "run-1", "goal": "goal", "ts": "t0"}
    )
    suspended = adapter.validate_python(
        {
            "type": "run.suspended",
            "run_id": "run-1",
            "session_id": "session-1",
            "reason": "permission",
            "checkpoint_revision": "rev-1",
            "interrupt_id": "interrupt-1",
            "event_seq": 2,
            "ts": "t1",
        }
    )
    resumed = adapter.validate_python(
        {
            "type": "run.resumed",
            "run_id": "run-1",
            "session_id": "session-1",
            "checkpoint_revision": "rev-1",
            "resume_epoch": 1,
            "interrupt_id": "interrupt-1",
            "event_seq": 3,
            "ts": "t2",
        }
    )

    assert legacy.event_seq is None  # type: ignore[attr-defined]
    assert isinstance(suspended, RunSuspendedEvent)
    assert isinstance(resumed, RunResumedEvent)
    assert suspended.event_seq == 2
    assert resumed.event_seq == 3


async def test_writer_is_first_and_event_is_durable_before_live_fanout(tmp_path: Path) -> None:
    path = tmp_path / "events.jsonl"
    bus = EventBus(correlation_id="run-1")
    observed_rows: list[dict[str, object]] = []

    async def live_handler(_event: BaseModel) -> None:
        observed_rows.extend(
            json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()
        )

    bus.subscribe(live_handler)
    async with EventWriter(path, run_id="run-1") as writer:
        writer.subscribe(bus)
        await bus.publish(_started("run-1"))

    assert observed_rows[-1]["event_seq"] == 1
    assert read_event_log(path).last_event_seq == 1


async def test_writer_failure_stops_run_fanout(tmp_path: Path) -> None:
    path = tmp_path / "events.jsonl"
    bus = EventBus(correlation_id="run-1")
    fanout_called = False

    async def live_handler(_event: BaseModel) -> None:
        nonlocal fanout_called
        fanout_called = True

    async with EventWriter(path, run_id="run-1") as writer:
        bus.subscribe(live_handler)
        writer.subscribe(bus)
        assert writer._file is not None
        writer._file.close()
        with pytest.raises(ValueError):
            await bus.publish(_started("run-1"))

    assert not fanout_called


async def test_resume_seeds_cursor_and_replay_excludes_acknowledged_events(
    tmp_path: Path,
) -> None:
    path = tmp_path / "events.jsonl"
    first_bus = EventBus(correlation_id="run-1")
    async with EventWriter(path, run_id="run-1") as writer:
        writer.subscribe(first_bus)
        await first_bus.publish(_started("run-1"))
        await first_bus.publish(StepStartedEvent(run_id="run-1", step=1, ts="t1"))

    resumed_bus = EventBus(correlation_id="run-1")
    async with EventWriter(path, run_id="run-1") as writer:
        writer.subscribe(resumed_bus)
        assert resumed_bus.event_seq == 2
        await resumed_bus.publish(
            RunResumedEvent(
                run_id="run-1",
                session_id="session-1",
                checkpoint_revision="rev-1",
                resume_epoch=1,
                ts="t2",
            )
        )
        await resumed_bus.publish(
            RunFinishedEvent(run_id="run-1", status="success", steps=1, ts="t3")
        )

    snapshot = read_event_log(path)
    assert snapshot.last_event_seq == 4
    assert [event["event_seq"] for event in replay_events(path, after_event_seq=2)] == [3, 4]


async def test_incomplete_tail_is_repaired_before_cursor_continues(tmp_path: Path) -> None:
    path = tmp_path / "events.jsonl"
    first = _started("run-1").model_copy(update={"event_seq": 1}).model_dump_json()
    path.write_bytes((first + "\n").encode("utf-8") + b'{"event_seq":2')

    damaged = read_event_log(path)
    assert damaged.truncated_tail
    assert damaged.last_event_seq == 1

    bus = EventBus(correlation_id="run-1")
    async with EventWriter(path, run_id="run-1") as writer:
        writer.subscribe(bus)
        await bus.publish(StepStartedEvent(run_id="run-1", step=1, ts="t1"))

    repaired = read_event_log(path)
    assert not repaired.truncated_tail
    assert repaired.last_event_seq == 2
    assert [event["event_seq"] for event in repaired.events] == [1, 2]


def test_middle_event_corruption_and_sequence_gaps_fail_closed(tmp_path: Path) -> None:
    corrupted = tmp_path / "corrupted.jsonl"
    corrupted.write_text(
        '{"type":"run.started","event_seq":1}\n{broken}\n{"type":"run.finished","event_seq":2}\n',
        encoding="utf-8",
    )
    with pytest.raises(EventLogCorruptionError):
        read_event_log(corrupted, repair_tail=True)

    gap = tmp_path / "gap.jsonl"
    gap.write_text(
        '{"type":"run.started","event_seq":1}\n{"type":"run.finished","event_seq":3}\n',
        encoding="utf-8",
    )
    with pytest.raises(EventSequenceError):
        read_event_log(gap)


def test_transcript_batch_is_idempotent_and_exposes_canonical_commit(tmp_path: Path) -> None:
    store = SessionStore(tmp_path / "sessions")
    messages = [
        {"role": "assistant", "content": "answer"},
        {
            "role": "user",
            "content": [{"type": "tool_result", "tool_use_id": "tool-1", "content": "ok"}],
        },
        {"role": "assistant", "content": "done"},
    ]

    first = store.append_messages("session-1", messages, run_id="run-1")
    second = store.append_messages("session-1", messages, run_id="run-1")
    observed = store.transcript_commit("session-1", "run-1")
    canonical = canonical_transcript_commit(messages)

    assert first.appended_count == 3
    assert second.appended_count == 0
    assert observed.transcript_commit_count == 3
    assert observed.transcript_commit_hash == canonical.transcript_commit_hash
    assert len(store.read_messages("session-1")) == 3


def test_transcript_recovery_appends_only_missing_legacy_suffix(tmp_path: Path) -> None:
    store = SessionStore(tmp_path / "sessions")
    messages = [
        {"role": "assistant", "content": "first"},
        {"role": "user", "content": "second"},
        {"role": "assistant", "content": "third"},
    ]
    store.append_message("session-1", "assistant", "first", run_id="run-1")

    commit = store.append_messages("session-1", messages, run_id="run-1")

    assert commit.appended_count == 2
    assert store.read_messages("session-1") == messages
    assert store.transcript_commit("session-1", "run-1").transcript_commit_hash == (
        canonical_transcript_commit(messages).transcript_commit_hash
    )


def test_transcript_rejects_conflicting_increment_without_consuming_rows(
    tmp_path: Path,
) -> None:
    store = SessionStore(tmp_path / "sessions")
    store.append_message("session-1", "assistant", "existing", run_id="run-1")
    path = store.session_dir("session-1") / "thread.jsonl"
    before = path.read_bytes()

    with pytest.raises(TranscriptConflictError):
        store.append_messages(
            "session-1",
            [{"role": "assistant", "content": "different"}],
            run_id="run-1",
        )

    assert path.read_bytes() == before


def test_transcript_atomic_replace_preserves_old_file_on_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = SessionStore(tmp_path / "sessions")
    store.append_message("session-1", "user", "existing")
    path = store.session_dir("session-1") / "thread.jsonl"
    before = path.read_bytes()

    def fail_replace(_source: Path, _target: Path) -> None:
        raise OSError("replace failed")

    monkeypatch.setattr("agent_runtime.core.session.store.os.replace", fail_replace)
    with pytest.raises(OSError, match="replace failed"):
        store.append_messages(
            "session-1",
            [{"role": "assistant", "content": "new"}],
            run_id="run-1",
        )

    assert path.read_bytes() == before
    assert list(path.parent.glob(".thread.jsonl.*.tmp")) == []


def test_transcript_discards_incomplete_tail_but_rejects_middle_corruption(
    tmp_path: Path,
) -> None:
    store = SessionStore(tmp_path / "sessions")
    store.append_message("session-1", "user", "committed")
    path = store.session_dir("session-1") / "thread.jsonl"
    path.write_bytes(path.read_bytes() + b'{"role":"assistant"')

    store.append_messages(
        "session-1",
        [{"role": "assistant", "content": "recovered"}],
        run_id="run-1",
    )
    assert store.read_messages("session-1") == [
        {"role": "user", "content": "committed"},
        {"role": "assistant", "content": "recovered"},
    ]

    path.write_bytes(path.read_bytes() + b"{broken}\n")
    with pytest.raises(TranscriptCorruptionError):
        store.append_messages(
            "session-1",
            [{"role": "assistant", "content": "another"}],
            run_id="run-2",
        )


def test_durable_transcript_reader_rejects_orphan_tool_use(tmp_path: Path) -> None:
    store = SessionStore(tmp_path / "sessions")
    orphan = [
        {
            "type": "tool_use",
            "id": "tool-1",
            "name": "echo",
            "input": {},
        }
    ]
    store.append_message("session-1", "assistant", orphan, run_id="run-1")

    assert store.read_messages("session-1") == []
    with pytest.raises(TranscriptConflictError, match="orphan tool_use"):
        store.read_messages_strict("session-1")
