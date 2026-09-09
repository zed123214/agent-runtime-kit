from __future__ import annotations

import json
from pathlib import Path

import pytest

from agent_runtime.core.sandbox.local import LocalSandboxBackend
from agent_runtime.core.sandbox.manager import SandboxManager
from agent_runtime.core.sandbox.models import (
    ExecRequest,
    SandboxCallContext,
    SandboxHandle,
    SandboxKey,
    SandboxSpec,
    WriteRequest,
)


class ReceiptBackend(LocalSandboxBackend):
    """Local execution test double; these tests make no isolation claim."""

    async def ensure(self, key: SandboxKey, spec: SandboxSpec) -> SandboxHandle:
        return await super().ensure(key, SandboxSpec())


async def test_receipts_correlate_calls_without_recording_payloads(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    path = tmp_path / "receipts.jsonl"
    manager = SandboxManager(
        ReceiptBackend(),
        spec=SandboxSpec(backend="kubernetes"),
        lifecycle_path=path,
    )
    key = SandboxKey("session", "receipt-session")
    runtime = manager.runtime_for(key)
    context = SandboxCallContext(key, "child-run", "write", session_id=key.id)
    await runtime.write_text(WriteRequest(context, "private-name", "private-content"))
    await runtime.exec(ExecRequest(SandboxCallContext(key, "root-run", "exec"), "exit 7"))
    await manager.close()
    raw = path.read_text()
    assert "private-name" not in raw and "private-content" not in raw and "exit 7" not in raw
    rows = [json.loads(line) for line in raw.splitlines()]
    execution = [row for row in rows if row["type"] == "sandbox.execution"]
    assert len(execution) == 2
    first, second = execution
    assert (first["run_id"], first["tool_call_id"]) == ("child-run", "write")
    assert first["cold"] is True and second["cold"] is False
    assert first["ready_overhead_ms"] is None
    assert second["exit_code"] == 7 and second["is_error"] is True
    assert second["total_ms"] >= second["queue_ms"]
    assert isinstance(second["ready_overhead_ms"], float)
    assert len(first["input_hash"]) == 64 and first["input_hash"] != second["input_hash"]
    assert "signal" in second and "stdout_hash" in second and "stderr_hash" in second
    assert all(row["schema_version"] == 1 for row in rows if row["type"] != "sandbox.reconcile")
    assert next(row for row in rows if row["type"] == "sandbox.ready")["phase_duration_ms"] >= 0
    assert next(row for row in rows if row["type"] == "sandbox.terminated")["run_id"] is None


async def test_failed_closed_call_still_has_a_receipt(tmp_path: Path) -> None:
    manager = SandboxManager(
        LocalSandboxBackend(),
        spec=SandboxSpec(backend="kubernetes"),
        lifecycle_path=tmp_path / "receipts.jsonl",
    )
    key = SandboxKey("session", "closed")
    runtime = manager.runtime_for(key)
    await manager.release(key)
    with pytest.raises(RuntimeError, match="closed"):
        await runtime.exec(ExecRequest(SandboxCallContext(key, "r", "t"), "not executed"))
    await manager.close()
    rows = [json.loads(line) for line in (tmp_path / "receipts.jsonl").read_text().splitlines()]
    receipt = next(row for row in rows if row["type"] == "sandbox.execution")
    assert receipt["error_type"] == "SandboxClosedError" and receipt["is_error"]
    assert receipt["worker_reported_duration_ms"] is None
