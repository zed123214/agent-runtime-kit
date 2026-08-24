from __future__ import annotations

import asyncio
import json
import os
import socket
import sqlite3
import sys
import uuid
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

from recovery_helpers.scenario_runtime import TOOL_USE_ID, RecoveryScenario

from agent_runtime.core.transport.socket_client import IpcError, SocketClient

_REPO_ROOT = Path(__file__).resolve().parents[2]
_TESTS_ROOT = _REPO_ROOT / "tests"


def guid_data_root(parent: Path) -> Path:
    root = parent / f"kitagent-recovery-{uuid.uuid4().hex}"
    root.mkdir(parents=True)
    return root


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as stream:
        stream.bind(("127.0.0.1", 0))
        return int(stream.getsockname()[1])


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    for raw_line in path.read_bytes().splitlines():
        if not raw_line:
            continue
        row = json.loads(raw_line.decode("utf-8"))
        if not isinstance(row, dict):
            raise AssertionError(f"expected an object row in {path}")
        rows.append(cast(dict[str, Any], row))
    return rows


class DaemonExitedError(RuntimeError):
    pass


class DaemonProcess:
    def __init__(
        self,
        data_root: Path,
        control_dir: Path,
        scenario: RecoveryScenario,
        generation: str,
    ) -> None:
        self.data_root = data_root
        self.control_dir = control_dir
        self.scenario = scenario
        self.generation = generation
        self.port = _free_port()
        self.process: asyncio.subprocess.Process | None = None
        self.stdout = ""
        self.stderr = ""

    async def start(self) -> None:
        env = os.environ.copy()
        python_path = str(_TESTS_ROOT)
        if env.get("PYTHONPATH"):
            python_path += os.pathsep + env["PYTHONPATH"]
        env.update(
            {
                "PYTHONPATH": python_path,
                "AGENTRT_HOST": "127.0.0.1",
                "AGENTRT_PORT": str(self.port),
                "AGENTRT_DATA_ROOT": str(self.data_root),
                "AGENTRT_ENGINE": "graph",
                "AGENTRT_GRAPH_CHECKPOINT_BACKEND": "sqlite",
                "AGENTRT_GRAPH_CHECKPOINT_PATH": "graph-checkpoints.sqlite3",
                "AGENTRT_GRAPH_WALL_TIME_S": "60",
                "AGENTRT_MAX_STEPS": "6",
                "AGENTRT_PERMISSION_TIMEOUT_S": "0",
                "AGENTRT_TRACE_ENABLED": "0",
                "AGENTRT_LOG_LEVEL": "WARNING",
                "AGENTRT_LOG_FILE": str(self.data_root / "logs" / f"daemon-{self.generation}.log"),
                "PYTHONDONTWRITEBYTECODE": "1",
            }
        )
        self.process = await asyncio.create_subprocess_exec(
            sys.executable,
            "-m",
            "recovery_helpers.daemon_process",
            "--scenario",
            self.scenario,
            "--generation",
            self.generation,
            "--control-dir",
            str(self.control_dir),
            cwd=_REPO_ROOT,
            env=env,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        await self._wait_until_listening()

    async def _wait_until_listening(self, timeout: float = 20.0) -> None:
        assert self.process is not None
        deadline = asyncio.get_running_loop().time() + timeout
        while asyncio.get_running_loop().time() < deadline:
            if self.process.returncode is not None:
                await self._collect_output()
                raise DaemonExitedError(
                    f"daemon {self.generation} exited with {self.process.returncode}\n"
                    f"stdout:\n{self.stdout}\nstderr:\n{self.stderr}"
                )
            try:
                _reader, writer = await asyncio.open_connection("127.0.0.1", self.port)
            except (ConnectionRefusedError, OSError):
                await asyncio.sleep(0.02)
                continue
            writer.close()
            await writer.wait_closed()
            return
        await self.kill()
        raise TimeoutError(f"daemon {self.generation} did not listen within {timeout}s")

    async def kill(self) -> None:
        if self.process is None:
            return
        if self.process.returncode is None:
            self.process.kill()
        await asyncio.wait_for(self.process.wait(), timeout=10.0)
        await self._collect_output()

    async def _collect_output(self) -> None:
        assert self.process is not None
        stdout, stderr = await self.process.communicate()
        self.stdout = stdout.decode("utf-8", errors="replace")
        self.stderr = stderr.decode("utf-8", errors="replace")


class RecoveryClient:
    def __init__(self, port: int) -> None:
        self._client = SocketClient("127.0.0.1", port)
        self._loop_task: asyncio.Task[None] | None = None
        self._changed = asyncio.Event()
        self.events: list[dict[str, Any]] = []

    async def connect(self) -> None:
        async def collect(event: dict[str, Any]) -> None:
            self.events.append(event)
            self._changed.set()

        self._client.on_event(collect)
        await self._client.connect()
        self._loop_task = asyncio.create_task(self._client.run_event_loop())
        await self.send(
            "event.subscribe",
            {"topics": ["*"], "scope": "global", "after_event_seq": 0},
        )

    async def send(
        self,
        method: str,
        params: dict[str, object],
        *,
        timeout: float = 30.0,
    ) -> dict[str, Any]:
        return await asyncio.wait_for(
            self._client.send_command(method, params),
            timeout=timeout,
        )

    def send_task(self, method: str, params: dict[str, object]) -> asyncio.Task[dict[str, Any]]:
        return asyncio.create_task(self._client.send_command(method, params))

    async def wait_event(
        self,
        event_type: str,
        *,
        run_id: str | None = None,
        timeout: float = 20.0,
    ) -> dict[str, Any]:
        deadline = asyncio.get_running_loop().time() + timeout
        while True:
            for event in self.events:
                if event.get("type") != event_type:
                    continue
                if run_id is not None and event.get("run_id") != run_id:
                    continue
                return event
            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                raise TimeoutError(f"event {event_type!r} was not observed")
            self._changed.clear()
            if any(
                event.get("type") == event_type
                and (run_id is None or event.get("run_id") == run_id)
                for event in self.events
            ):
                continue
            await asyncio.wait_for(self._changed.wait(), timeout=remaining)

    async def close(self) -> None:
        try:
            await self._client.close()
        except (ConnectionResetError, OSError):
            pass
        if self._loop_task is None:
            return
        try:
            await asyncio.wait_for(self._loop_task, timeout=2.0)
        except TimeoutError:
            self._loop_task.cancel()
            await asyncio.gather(self._loop_task, return_exceptions=True)


@dataclass(frozen=True, slots=True)
class RecoveryResult:
    scenario: RecoveryScenario
    backend: str
    session_id: str
    run_id: str
    thread_id: str
    initial_revision: str | None
    final_revision: str | None
    recovered_node: str
    event_cursor: int
    tool_call_count: int
    provider_call_count: int
    status: str
    suspension_reason: str | None
    events: tuple[dict[str, Any], ...]
    transcript_rows: tuple[dict[str, Any], ...]
    manifest: dict[str, Any]
    journal_status: str | None
    resume_success_count: int = 1
    resume_failure_count: int = 0


async def _wait_gate(
    path: Path,
    daemon: DaemonProcess,
    *,
    timeout: float = 20.0,
) -> dict[str, Any]:
    assert daemon.process is not None
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        if path.exists():
            raw = json.loads(path.read_text(encoding="utf-8"))
            if not isinstance(raw, dict):
                raise AssertionError(f"gate {path} must contain an object")
            return cast(dict[str, Any], raw)
        if daemon.process.returncode is not None:
            await daemon._collect_output()
            raise DaemonExitedError(
                f"daemon {daemon.generation} exited before gate {path.name}\n"
                f"stdout:\n{daemon.stdout}\nstderr:\n{daemon.stderr}"
            )
        await asyncio.sleep(0.01)
    raise TimeoutError(f"gate {path.name} was not reached")


async def _discard_command(task: asyncio.Task[dict[str, Any]]) -> None:
    if not task.done():
        task.cancel()
    await asyncio.gather(task, return_exceptions=True)


def _write_policy(data_root: Path, scenario: RecoveryScenario) -> None:
    if scenario == "permission_interrupt":
        return
    data_root.mkdir(parents=True, exist_ok=True)
    (data_root / "policy.toml").write_text(
        '[always]\nrecovery_counter = "allow"\n',
        encoding="utf-8",
        newline="\n",
    )


def _read_manifest(data_root: Path, session_id: str, run_id: str) -> dict[str, Any]:
    database = data_root / "recovery.sqlite3"
    with closing(sqlite3.connect(database)) as connection:
        connection.row_factory = sqlite3.Row
        row = connection.execute(
            "SELECT * FROM runs WHERE session_id = ? AND run_id = ?",
            (session_id, run_id),
        ).fetchone()
    if row is None:
        raise AssertionError("recovery run manifest is missing")
    return dict(row)


def _read_journal_status(data_root: Path, session_id: str, run_id: str) -> str | None:
    database = data_root / "recovery.sqlite3"
    with closing(sqlite3.connect(database)) as connection:
        row = connection.execute(
            "SELECT status FROM tool_invocations "
            "WHERE session_id = ? AND run_id = ? AND tool_use_id = ?",
            (session_id, run_id, TOOL_USE_ID),
        ).fetchone()
    return str(row[0]) if row is not None else None


def _approval_params(state: dict[str, Any]) -> dict[str, object]:
    summary = state.get("pending_approval_summary")
    if not isinstance(summary, dict):
        raise AssertionError("durable permission state has no approval summary")
    interrupt_id = summary.get("interrupt_id")
    if not isinstance(interrupt_id, str) or not interrupt_id:
        raise AssertionError("durable permission state has no interrupt_id")
    revision = state.get("checkpoint_revision")
    if not isinstance(revision, str) or not revision:
        raise AssertionError("durable permission state has no checkpoint revision")
    return {
        "session_id": str(state["session_id"]),
        "run_id": str(state["run_id"]),
        "tool_use_id": str(summary["tool_use_id"]),
        "interrupt_id": interrupt_id,
        "expected_revision": revision,
        "decision": "allow_once",
    }


async def run_recovery_scenario(
    parent: Path,
    scenario: RecoveryScenario,
    *,
    concurrent_resume: bool = False,
) -> RecoveryResult:
    if concurrent_resume and scenario != "permission_interrupt":
        raise ValueError("concurrent resume scenario requires permission_interrupt")
    data_root = guid_data_root(parent)
    control_dir = data_root / "control"
    control_dir.mkdir(parents=True)
    _write_policy(data_root, scenario)

    daemon_a = DaemonProcess(data_root, control_dir, scenario, "A")
    daemon_b = DaemonProcess(data_root, control_dir, scenario, "B")
    client_a: RecoveryClient | None = None
    client_b: RecoveryClient | None = None
    client_b2: RecoveryClient | None = None
    send_task: asyncio.Task[dict[str, Any]] | None = None
    session_id = ""
    run_id = ""
    resume_token = ""
    initial_revision: str | None = None
    final_state: dict[str, Any] = {}
    resume_success_count = 1
    resume_failure_count = 0
    try:
        await daemon_a.start()
        client_a = RecoveryClient(daemon_a.port)
        await client_a.connect()
        created = await client_a.send(
            "session.create",
            {"mode": "chat", "title": f"recovery-{scenario}"},
        )
        session_id = str(created["session_id"])
        raw_token = created.get("resume_token")
        if not isinstance(raw_token, str) or not raw_token:
            raise AssertionError("durable session.create did not return a resume token")
        resume_token = raw_token

        send_task = client_a.send_task(
            "session.send_message",
            {"session_id": session_id, "content": f"exercise {scenario}"},
        )
        started = await client_a.wait_event("run.started")
        run_id = str(started["run_id"])

        if scenario == "model_inflight":
            gate = await _wait_gate(control_dir / "model_inflight.json", daemon_a)
            assert gate["run_id"] == run_id
        elif scenario == "tool_pre_dispatch":
            gate = await _wait_gate(control_dir / "tool_pre_dispatch.json", daemon_a)
            assert gate == {"run_id": run_id, "pending_tool_count": 1}
        elif scenario == "journal_completed":
            gate = await _wait_gate(control_dir / "journal_completed.json", daemon_a)
            assert gate["run_id"] == run_id
        elif scenario == "outcome_unknown":
            await _wait_gate(control_dir / "side_effect.json", daemon_a)
        else:
            suspended = await client_a.wait_event("run.suspended", run_id=run_id)
            initial_revision = str(suspended["checkpoint_revision"])
            await asyncio.wait_for(send_task, timeout=20.0)
            permission_state = await client_a.send(
                "run.get_state",
                {"session_id": session_id, "run_id": run_id},
            )
            assert permission_state["suspension_reason"] == "permission"

        await daemon_a.kill()
        if send_task is not None:
            await _discard_command(send_task)
        await client_a.close()
        client_a = None

        await daemon_b.start()
        client_b = RecoveryClient(daemon_b.port)
        await client_b.connect()
        if concurrent_resume:
            if initial_revision is None:
                raise AssertionError("concurrent resume requires a checkpoint revision")
            client_b2 = RecoveryClient(daemon_b.port)
            await client_b2.connect()
            clients = [client_b, client_b2]
            attempts = await asyncio.gather(
                *(
                    client.send(
                        "session.resume",
                        {
                            "session_id": session_id,
                            "resume_token": resume_token,
                            "expected_revision": initial_revision,
                        },
                        timeout=40.0,
                    )
                    for client in clients
                ),
                return_exceptions=True,
            )
            successes = [index for index, result in enumerate(attempts) if isinstance(result, dict)]
            failures = [
                index for index, result in enumerate(attempts) if isinstance(result, IpcError)
            ]
            assert len(successes) == len(failures) == 1
            conflict = cast(IpcError, attempts[failures[0]])
            assert conflict.code == -32031
            assert str(conflict) == "[-32031] durable resume conflict"
            resume_success_count = len(successes)
            resume_failure_count = len(failures)
            winner_index = successes[0]
            resumed = cast(dict[str, Any], attempts[winner_index])
            loser = clients[failures[0]]
            await loser.close()
            if winner_index == 1:
                client_b = client_b2
            client_b2 = None
        else:
            resumed = await client_b.send(
                "session.resume",
                {"session_id": session_id, "resume_token": resume_token},
                timeout=40.0,
            )
        raw_revision = resumed.get("checkpoint_revision")
        if isinstance(raw_revision, str):
            initial_revision = initial_revision or raw_revision

        if scenario == "permission_interrupt":
            attached = await client_b.send(
                "run.get_state",
                {"session_id": session_id, "run_id": run_id},
            )
            assert attached["suspension_reason"] == "permission"
            response = await client_b.send(
                "permission.respond",
                _approval_params(attached),
                timeout=40.0,
            )
            assert response["ok"] is True
            await client_b.wait_event("run.finished", run_id=run_id)
        elif scenario in ("model_inflight", "tool_pre_dispatch", "journal_completed"):
            await client_b.wait_event("run.finished", run_id=run_id)

        final_state = await client_b.send(
            "run.get_state",
            {"session_id": session_id, "run_id": run_id},
        )
        await daemon_b.kill()
        await client_b.close()
        client_b = None
    finally:
        if daemon_a.process is not None and daemon_a.process.returncode is None:
            await daemon_a.kill()
        if daemon_b.process is not None and daemon_b.process.returncode is None:
            await daemon_b.kill()
        if send_task is not None:
            await _discard_command(send_task)
        if client_a is not None:
            await client_a.close()
        if client_b is not None:
            await client_b.close()
        if client_b2 is not None:
            await client_b2.close()

    event_path = data_root / "sessions" / session_id / "runs" / run_id / "events.jsonl"
    transcript_path = data_root / "sessions" / session_id / "thread.jsonl"
    events = _read_jsonl(event_path)
    transcript_rows = _read_jsonl(transcript_path)
    manifest = _read_manifest(data_root, session_id, run_id)
    provider_calls = _read_jsonl(control_dir / "provider_calls.jsonl")
    tool_calls = _read_jsonl(control_dir / "tool_calls.jsonl")
    final_revision = final_state.get("checkpoint_revision")
    return RecoveryResult(
        scenario=scenario,
        backend="sqlite",
        session_id=session_id,
        run_id=run_id,
        thread_id=session_id,
        initial_revision=initial_revision,
        final_revision=(str(final_revision) if isinstance(final_revision, str) else None),
        recovered_node=("model" if scenario == "model_inflight" else "kit_tools"),
        event_cursor=int(final_state["event_seq"]),
        tool_call_count=len(tool_calls),
        provider_call_count=len(provider_calls),
        status=str(final_state["status"]),
        suspension_reason=(
            str(final_state["suspension_reason"])
            if final_state.get("suspension_reason") is not None
            else None
        ),
        events=tuple(events),
        transcript_rows=tuple(transcript_rows),
        manifest=manifest,
        journal_status=_read_journal_status(data_root, session_id, run_id),
        resume_success_count=resume_success_count,
        resume_failure_count=resume_failure_count,
    )


def assert_event_and_transcript_contract(
    result: RecoveryResult,
    *,
    terminal: bool,
) -> None:
    root_events = [event for event in result.events if event.get("run_id") == result.run_id]
    sequences = [event.get("event_seq") for event in root_events]
    assert sequences == list(range(1, len(root_events) + 1))
    assert sum(event.get("type") == "run.started" for event in root_events) == 1
    assert sum(event.get("type") == "run.finished" for event in root_events) == int(terminal)
    if terminal:
        assert root_events[-1]["type"] == "run.finished"
    else:
        assert all(event.get("type") != "run.finished" for event in root_events)
    assert result.event_cursor == sequences[-1]
    assert int(result.manifest["event_seq"]) == result.event_cursor

    committed = [
        {"role": row.get("role"), "content": row.get("content")}
        for row in result.transcript_rows
        if row.get("run_id") == result.run_id
    ]
    assert len(committed) == int(result.manifest["transcript_commit_count"])
    canonical_rows = [json.dumps(row, ensure_ascii=False, sort_keys=True) for row in committed]
    assert len(canonical_rows) == len(set(canonical_rows))
