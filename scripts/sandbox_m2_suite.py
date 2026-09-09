"""In-cluster M2 experiments. Every attempted sample is retained, including failures."""

from __future__ import annotations

import asyncio
import copy
import json
import math
import os
import signal
import sys
import time
from collections.abc import Awaitable, Callable
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

from agent_runtime.core.config import get_config
from agent_runtime.core.sandbox.factory import create_sandbox_manager
from agent_runtime.core.sandbox.kube_api import ApiError
from agent_runtime.core.sandbox.models import (
    ExecRequest,
    ReadRequest,
    SandboxCallContext,
    SandboxKey,
    WorkspaceLostError,
    WriteRequest,
)
from agent_runtime.core.sandbox.pod_spec import POLICY_VERSION, PREFIX, pod_spec


def percentile(values: list[float], q: float = 0.95) -> float | None:
    """Nearest-rank percentile, with the method recorded in the manifest."""
    return sorted(values)[math.ceil(len(values) * q) - 1] if values else None


class Experiments:
    def __init__(self, output: Path) -> None:
        self.output = output
        output.mkdir(parents=True, exist_ok=True)
        self.cfg = get_config().sandbox
        self.rows: list[dict[str, Any]] = []
        self.receipts: list[dict[str, object]] = []
        self.sequence = 0

    def record(self, row: dict[str, Any]) -> None:
        self.rows.append(row)
        with (self.output / "samples.jsonl").open("a") as stream:
            stream.write(json.dumps(row) + "\n")

    def context(self, key: SandboxKey) -> SandboxCallContext:
        self.sequence += 1
        return SandboxCallContext(key, "m2-" + key.id, f"call-{self.sequence}", session_id=key.id)

    def key(self, name: str) -> SandboxKey:
        return SandboxKey("session", f"m2-{name}-{uuid4().hex}")

    def manager(self, name: str, **changes: Any) -> Any:
        cfg = copy.deepcopy(self.cfg)
        for key, value in changes.items():
            setattr(cfg, key, value)
        return create_sandbox_manager(cfg, self.output / name, trace_sink=self.receipts.append)

    async def sample(
        self, case: str, index: int, operation: Callable[[], Awaitable[dict[str, Any]]]
    ) -> None:
        started = time.monotonic()
        row: dict[str, Any] = {
            "case": case,
            "iteration": index,
            "started_at": datetime.now(UTC).isoformat(),
        }
        try:
            row.update(await operation())
            row.setdefault("passed", True)
        except Exception as exc:
            row.update(passed=False, error_type=type(exc).__name__, error=str(exc)[:1000])
        finally:
            row["total_ms"] = (time.monotonic() - started) * 1000
            self.record(row)
            print(json.dumps(row), flush=True)

    async def gone(self, manager: Any, name: str, timeout: float = 60) -> float:
        start = time.monotonic()
        async with asyncio.timeout(timeout):
            while any(
                [
                    await manager._backend.api.get(kind, name)
                    for kind in ("pod", "service", "secret")
                ]
            ):
                await asyncio.sleep(0.2)
        return (time.monotonic() - start) * 1000

    async def lifecycle(self) -> None:
        manager = self.manager("lifecycle", idle_timeout_s=4)
        await manager.start()

        async def cycle(i: int) -> dict[str, Any]:
            key = self.key(f"cycle-{i}")
            runtime = manager.runtime_for(key)
            name = "sb-" + manager._backend.identity_for(key)
            try:
                initial = await asyncio.gather(
                    *[
                        runtime.write_text(WriteRequest(self.context(key), "value", str(i)))
                        for _ in range(3)
                    ]
                )
                assert all(not r.is_error for r in initial)
                handle = manager.handle_for(key)
                assert handle is not None
                # These competing authorized calls must retain exactly one Pod UID.
                results = await asyncio.gather(
                    *[runtime.read_text(ReadRequest(self.context(key), "value")) for _ in range(3)]
                )
                assert all(r.content == str(i) for r in results)
                pods = await manager._backend.api.list(
                    "pod", selector=PREFIX + "id=" + handle.sandbox_id
                )
                assert len(pods["items"]) == 1
                ready = next(
                    r
                    for r in self.receipts
                    if r.get("type") == "sandbox.ready" and r.get("sandbox_id") == handle.sandbox_id
                )
                close_started = time.monotonic()
                reason = "close" if i < 30 or i % 2 == 0 else "ttl"
                if reason == "close":
                    await manager.release(key, "m2_close")
                    await manager.release(key, "m2_double_close")
                else:
                    # Real elapsed idle time and the production maintenance loop.
                    await self.gone(manager, name)
                    with_error = False
                    try:
                        await runtime.read_text(ReadRequest(self.context(key), "value"))
                    except WorkspaceLostError:
                        with_error = True
                    assert with_error
                await self.gone(manager, name)
                return {
                    "sandbox_id": handle.sandbox_id,
                    "pod_uid": handle.pod_uid,
                    "cold_ready_ms": ready["phase_duration_ms"],
                    "cold_sample": i < 30,
                    "concurrency": 1 if i < 30 else 10,
                    "duplicate_active_pods": len(pods["items"]) - 1,
                    "release_reason": reason,
                    "release_ms": (time.monotonic() - close_started) * 1000,
                    "leaked_resources": 0,
                }
            finally:
                await manager.release(key, "m2_cycle_finally")

        try:
            absent = self.key("absent")
            await manager.release(absent)
            await manager.release(absent)
            assert manager.handle_for(absent) is None
            for i in range(30):
                await self.sample("lifecycle", i, lambda i=i: cycle(i))
                if i == 2 and all(not r["passed"] for r in self.rows[-3:]):
                    raise RuntimeError(
                        "three lifecycle failures; remaining samples were not attempted"
                    )
            for start in range(30, 100, 10):
                await asyncio.gather(
                    *[
                        self.sample("lifecycle", i, lambda i=i: cycle(i))
                        for i in range(start, start + 10)
                    ]
                )
        finally:
            await manager.close()

    async def continuity_and_exec(self) -> None:
        manager = self.manager("continuity")
        await manager.start()
        key = self.key("continuity")
        runtime = manager.runtime_for(key)

        async def continuity(i: int) -> dict[str, Any]:
            write = await runtime.write_text(WriteRequest(self.context(key), "state", str(i)))
            execute = await runtime.exec(ExecRequest(self.context(key), "cat state > observed"))
            read = await runtime.read_text(ReadRequest(self.context(key), "observed"))
            assert not write.is_error and execute.exit_code == 0 and read.content == str(i)
            return {"pod_uid": manager.handle_for(key).pod_uid}

        async def empty() -> dict[str, Any]:
            context = self.context(key)
            result = await runtime.exec(ExecRequest(context, ":"))
            assert result.exit_code == 0
            receipt = next(
                r
                for r in reversed(self.receipts)
                if r.get("tool_call_id") == context.tool_call_id
                and r.get("type") == "sandbox.execution"
            )
            return {"overhead_ms": receipt["ready_overhead_ms"], "worker_ms": result.duration_ms}

        async def timeout(i: int) -> dict[str, Any]:
            started = time.monotonic()
            result = await runtime.exec(
                ExecRequest(self.context(key), f"(sleep 3; touch late-{i}) & wait", timeout_s=1)
            )
            elapsed = time.monotonic() - started
            await asyncio.sleep(3)
            marker = await runtime.read_text(ReadRequest(self.context(key), f"late-{i}"))
            assert result.terminal_reason == "timeout" and elapsed <= 3 and marker.is_error
            return {
                "terminal_ms": elapsed * 1000,
                "deadline_ms": 3000,
                "marker_absent": marker.is_error,
            }

        try:
            for i in range(30):
                await self.sample("continuity", i, lambda i=i: continuity(i))
            for i in range(100):
                await self.sample("ready_exec", i, empty)
            for i in range(5):
                await self.sample("timeout", i, lambda i=i: timeout(i))
        finally:
            await manager.close()

    async def isolation(self) -> None:
        manager = self.manager("isolation")
        await manager.start()
        keys = [self.key(f"isolation-{i}") for i in range(20)]
        runtimes = [manager.runtime_for(key) for key in keys]
        try:
            writes = await asyncio.gather(
                *[
                    runtime.write_text(WriteRequest(self.context(key), "private", f"owner-{i}"))
                    for i, (key, runtime) in enumerate(zip(keys, runtimes, strict=True))
                ]
            )
            assert all(not result.is_error for result in writes)
            handles = [manager.handle_for(key) for key in keys]
            assert len({h.pod_uid for h in handles}) == 20
            # At the declared 20 Session capacity the next Pod must be rejected.
            body = {
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {"name": "m2-over-quota", "namespace": self.cfg.kubernetes.namespace},
                "spec": pod_spec(self.cfg, "0" * 32, "no-secret"),
            }

            async def quota() -> dict[str, Any]:
                try:
                    pod = await manager._backend.api.create("pod", body)
                except ApiError as exc:
                    assert exc.status == 403
                    return {"http_status": exc.status, "active_sessions": 20}
                await manager._backend.api.delete("pod", "m2-over-quota", pod["metadata"]["uid"])
                raise AssertionError("21st Pod admitted despite capacity quota")

            await self.sample("quota_capacity", 0, quota)
            for key, runtime in zip(keys, runtimes, strict=True):
                result = await runtime.exec(
                    ExecRequest(self.context(key), "ln -s /etc/passwd escape")
                )
                assert result.exit_code == 0

            async def probes(i: int) -> None:
                key, runtime = keys[i], runtimes[i]
                paths = [
                    "../private",
                    "/workspace/../private",
                    "/etc/passwd",
                    "escape",
                    f"../session-{(i + 1) % 20}/private",
                ]
                for j in range(10):

                    async def probe(j: int = j) -> dict[str, Any]:
                        path = paths[j % 5]
                        result = (
                            await runtime.read_text(ReadRequest(self.context(key), path))
                            if j < 5
                            else await runtime.write_text(
                                WriteRequest(self.context(key), path, "forbidden-marker")
                            )
                        )
                        assert result.is_error
                        return {
                            "session_index": i,
                            "pod_uid": handles[i].pod_uid,
                            "path": path,
                            "operation": "read" if j < 5 else "write",
                            "leaked": False,
                            "concurrency": 20,
                        }

                    await self.sample("isolation", i * 10 + j, probe)

            await asyncio.gather(*[probes(i) for i in range(20)])
            for i, (key, runtime) in enumerate(zip(keys, runtimes, strict=True)):
                result = await runtime.read_text(ReadRequest(self.context(key), "private"))
                assert result.content == f"owner-{i}"
            # A Worker must not establish a TCP connection to another Session.
            target = (await manager._backend.api.get("pod", handles[1].pod_name))["status"]["podIP"]

            async def network() -> dict[str, Any]:
                code = f"import socket\ntry: socket.create_connection(({target!r},8080),2)\nexcept OSError: pass\nelse: raise SystemExit(9)"
                result = await runtimes[0].exec(
                    ExecRequest(self.context(keys[0]), "python - <<'PY'\n" + code + "\nPY")
                )
                assert result.exit_code == 0
                return {"target_pod_uid": handles[1].pod_uid, "tcp_connected": False}

            await self.sample("cross_session_network", 0, network)
        finally:
            await manager.close()

    async def security_and_resources(self) -> None:
        manager = self.manager("resources")
        await manager.start()
        key = self.key("resources")
        runtime = manager.runtime_for(key)

        async def execute(code: str, timeout: int = 30) -> Any:
            try:
                result = await runtime.exec(
                    ExecRequest(
                        self.context(key), "python - <<'PY'\n" + code + "\nPY", timeout_s=timeout
                    )
                )
            except Exception as exc:
                with (self.output / "resource-probes.jsonl").open("a") as stream:
                    stream.write(
                        json.dumps(
                            {
                                "probe": self.sequence,
                                "error_type": type(exc).__name__,
                                "terminal_reason": getattr(exc, "terminal_reason", None),
                            }
                        )
                        + "\n"
                    )
                raise
            # These are fixed, credential-free validation probes. Preserve their
            # raw observations before any assertions, including failed probes.
            with (self.output / "resource-probes.jsonl").open("a") as stream:
                stream.write(json.dumps({"probe": self.sequence, **asdict(result)}) + "\n")
            return result

        async def security() -> dict[str, Any]:
            result = await execute("""import os,json,errno
from pathlib import Path
status=dict(line.split(':',1) for line in Path('/proc/self/status').read_text().splitlines() if ':' in line)
root_readonly=False
try: Path('/m2-root-write').write_text('forbidden')
except OSError as e: root_readonly=e.errno==errno.EROFS
data=dict(uid=os.getuid(),no_new_privs=status['NoNewPrivs'].strip(),cap_eff=status['CapEff'].strip(),seccomp=status['Seccomp'].strip(),root_readonly=root_readonly,service_account_token=Path('/var/run/secrets/kubernetes.io/serviceaccount/token').exists(),broker_token=Path('/run/credential/token').exists(),cpu_max=Path('/sys/fs/cgroup/cpu.max').read_text().strip(),memory_max=Path('/sys/fs/cgroup/memory.max').read_text().strip())
print(json.dumps(data))
assert data['uid']==10001 and data['no_new_privs']=='1' and int(data['cap_eff'],16)==0 and data['seccomp']=='2' and root_readonly and not data['service_account_token'] and not data['broker_token']
""")
            assert result.exit_code == 0
            handle = manager.handle_for(key)
            pod = await manager._backend.api.get("pod", handle.pod_name)
            self.output.joinpath("worker-pod.json").write_text(json.dumps(pod, indent=2) + "\n")
            statuses = {}
            for method, path in [
                ("GET", "/healthz"),
                ("POST", "/v1/exec"),
                ("GET", "/v1/files"),
                ("PUT", "/v1/files"),
                ("GET", "/v1/dirs"),
                ("POST", "/v1/cancel"),
            ]:
                response = await manager._backend.http.request(
                    method,
                    str(handle.endpoint) + path,
                    headers={"Authorization": "Bearer invalid-test-credential"},
                    timeout=3,
                )
                statuses[method + " " + path] = response.status_code
            (self.output / "auth-endpoints.json").write_text(json.dumps(statuses, indent=2) + "\n")
            assert all(code == 401 for code in statuses.values()), statuses
            return {**json.loads(result.stdout), "unauthenticated_endpoints": statuses}

        async def cpu() -> dict[str, Any]:
            result = await execute("""import subprocess,sys,json
from pathlib import Path
def stats(): return dict((k,int(v)) for k,v in (line.split() for line in Path('/sys/fs/cgroup/cpu.stat').read_text().splitlines()))
before=stats()
children=[subprocess.Popen([sys.executable,'-c','import time; end=time.monotonic()+4\\nwhile time.monotonic()<end: pass']) for _ in range(4)]
for child in children: child.wait()
after=stats()
data=dict(before=before,after=after,throttled_periods=after['nr_throttled']-before['nr_throttled'])
print(json.dumps(data))
assert data['throttled_periods']>0
""")
            assert result.exit_code == 0
            return json.loads(result.stdout)

        async def oom() -> dict[str, Any]:
            try:
                result = await execute("x=bytearray(800*1024*1024); print(len(x))")
            except WorkspaceLostError as exc:
                assert exc.terminal_reason == "OOMKilled"
                evidence = next(
                    r
                    for r in reversed(self.receipts)
                    if r.get("type") == "sandbox.pod_terminal"
                    and r.get("terminal_reason") == "OOMKilled"
                )
                return {
                    "classification": exc.terminal_reason,
                    "operation_outcome": "unknown",
                    "workspace_lost": True,
                    "pod_evidence": evidence,
                }
            observations = result.resource_observations
            assert result.terminal_reason == "oom_killed" and result.signal == 9
            assert observations and observations["oom_kill_after"] > observations["oom_kill_before"]
            return {
                "classification": result.terminal_reason,
                "exit_code": result.exit_code,
                "signal": result.signal,
                "cgroup": observations,
            }

        async def storage() -> dict[str, Any]:
            try:
                result = await execute("""import os
with open('/workspace/oversize','wb') as f:
 for i in range(576): f.write(b'x'*(1024*1024))
 f.flush(); os.fsync(f.fileno())
""")
            except WorkspaceLostError as exc:
                assert exc.terminal_reason == "Evicted"
                evidence = next(
                    r
                    for r in reversed(self.receipts)
                    if r.get("type") == "sandbox.pod_terminal"
                    and r.get("terminal_reason") == "Evicted"
                )
                return {
                    "classification": "pod_evicted_during_write",
                    "operation_outcome": "unknown",
                    "terminal_reason": exc.terminal_reason,
                    "pod_evidence": evidence,
                }
            assert result.exit_code == 0
            handle = manager.handle_for(key)
            deadline = time.monotonic() + 180
            while time.monotonic() < deadline:
                pod = await manager._backend.api.get("pod", handle.pod_name)
                if pod and pod.get("status", {}).get("phase") in {"Failed", "Succeeded"}:
                    status = pod["status"]
                    self.output.joinpath("storage-pod-status.json").write_text(
                        json.dumps(status, indent=2) + "\n"
                    )
                    events = await manager._backend.api.events_for_pod(str(handle.pod_uid))
                    self.output.joinpath("storage-events.json").write_text(
                        json.dumps(events, indent=2) + "\n"
                    )
                    assert any(
                        e.get("reason") == "Evicted" and "emptydir" in e.get("message", "").lower()
                        for e in events.get("items", [])
                    )
                    try:
                        await runtime.read_text(ReadRequest(self.context(key), "oversize"))
                    except WorkspaceLostError as exc:
                        assert exc.terminal_reason == "Evicted"
                        return {
                            "classification": "pod_evicted_emptydir",
                            "pod_status": status,
                            "runtime_error": "workspace_lost",
                            "terminal_reason": exc.terminal_reason,
                        }
                    raise AssertionError("evicted workspace accepted another operation")
                await asyncio.sleep(1)
            raise AssertionError("emptyDir excess not evicted within 180 seconds")

        try:
            for case, operation in [
                ("pod_security", security),
                ("cpu_throttle", cpu),
                ("oom", oom),
                ("storage_eviction", storage),
            ]:
                if case == "storage_eviction":
                    key = self.key("storage")
                    runtime = manager.runtime_for(key)
                await self.sample(case, 0, operation)
        finally:
            await manager.close()

    async def orphan_reconcile(self) -> None:
        key = self.key("orphan")
        child_root = self.output / "orphan-child"
        code = """import asyncio,json,os,sys
from pathlib import Path
from agent_runtime.core.config import get_config
from agent_runtime.core.sandbox.factory import create_sandbox_manager
from agent_runtime.core.sandbox.models import SandboxKey,SandboxCallContext,WriteRequest
async def run():
 cfg=get_config().sandbox
 cfg.reconcile_grace_s=1
 manager=create_sandbox_manager(cfg,Path(sys.argv[2]))
 key=SandboxKey('session',sys.argv[1])
 await manager.runtime_for(key).write_text(WriteRequest(SandboxCallContext(key,'orphan-child','write'),'marker','owned'))
 h=manager.handle_for(key)
 print(json.dumps(dict(name=h.pod_name,uid=h.pod_uid,sandbox_id=h.sandbox_id)),flush=True)
 os._exit(0)
asyncio.run(run())
"""

        async def operation() -> dict[str, Any]:
            child = await asyncio.create_subprocess_exec(
                sys.executable,
                "-c",
                code,
                key.id,
                str(child_root),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                start_new_session=True,
            )
            manager = self.manager("orphan-reconcile", reconcile_grace_s=1)
            communication = asyncio.create_task(child.communicate())
            try:
                stdout, stderr = await asyncio.wait_for(asyncio.shield(communication), timeout=150)
                assert child.returncode == 0
                identity = json.loads(stdout)
                await asyncio.sleep(1.1)
                before = len(self.receipts)
                await manager.start()
                await self.gone(manager, identity["name"])
                reports = [
                    r for r in self.receipts[before:] if r.get("type") == "sandbox.reconcile"
                ]
                assert reports and reports[0]["destroyed"] >= 1 and not reports[0]["errors"]
                return {"orphan": identity, "reconcile_report": reports[0], "leaked_resources": 0}
            finally:

                async def cleanup() -> None:
                    if child.returncode is None:
                        try:
                            os.killpg(child.pid, signal.SIGKILL)
                        except ProcessLookupError:
                            pass
                    stdout, stderr = await asyncio.wait_for(
                        asyncio.shield(communication), timeout=5
                    )
                    (self.output / "orphan-process.json").write_text(
                        json.dumps(
                            {
                                "exit_code": child.returncode,
                                "stdout": stdout.decode(),
                                "stderr": stderr.decode(),
                            }
                        )
                        + "\n"
                    )
                    try:
                        # Join the producer before scanning; an abandoned child
                        # must never create resources after final inventory.
                        await asyncio.sleep(1.1)
                        await manager.start()
                        for _ in range(3):
                            report = await manager._backend.reconcile()
                            manager._record(
                                {"type": "sandbox.reconcile", "run_id": None, **asdict(report)}
                            )
                            await asyncio.sleep(0.5)
                    finally:
                        await manager.close()

                from agent_runtime.sandbox_server.executor import finish_cleanup

                await finish_cleanup(asyncio.create_task(cleanup()))

        await self.sample("orphan_reconcile", 0, operation)

    async def run(self) -> dict[str, Any]:
        try:
            await self.continuity_and_exec()
            await self.isolation()
            await self.security_and_resources()
            await self.lifecycle()
            await self.orphan_reconcile()
        finally:
            (self.output / "receipts.jsonl").write_text(
                "".join(json.dumps(r) + "\n" for r in self.receipts)
            )
        cases = sorted({row["case"] for row in self.rows})
        summary = {
            case: {
                "attempted": len([r for r in self.rows if r["case"] == case]),
                "passed": len([r for r in self.rows if r["case"] == case and r["passed"]]),
            }
            for case in cases
        }
        cold = [
            r["cold_ready_ms"]
            for r in self.rows
            if r["case"] == "lifecycle" and r.get("cold_sample") and r["passed"]
        ]
        overhead = [
            r["overhead_ms"] for r in self.rows if r["case"] == "ready_exec" and r["passed"]
        ]
        release = [r["release_ms"] for r in self.rows if r["case"] == "lifecycle" and r["passed"]]
        metrics = {
            "cold_ready_p95_ms": percentile(cold),
            "ready_exec_overhead_p95_ms": percentile(overhead),
            "release_p95_ms": percentile(release),
            "percentile_method": "nearest-rank",
        }
        passed = (
            all(row["passed"] for row in self.rows)
            and len(cold) == 30
            and len(overhead) == 100
            and len(release) == 100
            and percentile(cold) <= 15000
            and percentile(overhead) <= 500
            and percentile(release) <= 60000
        )
        return {
            "cases": summary,
            "metrics": metrics,
            "passed": passed,
            "attempted": len(self.rows),
            "failed": sum(not r["passed"] for r in self.rows),
            "resource_profile": asdict(self.cfg.kubernetes),
            "policy_version": POLICY_VERSION,
        }


async def main(output: Path) -> dict[str, Any]:
    if not os.environ.get("KUBERNETES_SERVICE_HOST"):
        raise RuntimeError("M2 requires an in-cluster Core identity")
    return await Experiments(output).run()
