"""Opt-in kind E2E, run INSIDE the cluster as the single Core identity.

Never automatically runs against a workstation context. No test in this file has
been executed for M1 delivery. A fake LLM keeps these tests offline; Pods, API,
HTTP, volumes, cancellation and deletion are real when explicitly enabled.
"""

from __future__ import annotations

import asyncio
import os
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest

from agent_runtime.core.app import CoreApp
from agent_runtime.core.config import get_config
from agent_runtime.core.events.bus import EventBus
from agent_runtime.core.llm.types import LlmResponse, ToolCallBlock
from agent_runtime.core.runner import AgentRunner
from agent_runtime.core.sandbox import (
    ExecRequest,
    ListRequest,
    ReadRequest,
    SandboxCallContext,
    SandboxKey,
    WriteRequest,
)
from agent_runtime.core.sandbox.factory import create_sandbox_manager
from agent_runtime.core.sandbox.models import WorkspaceLostError
from agent_runtime.core.session.manager import SessionManager
from agent_runtime.core.session.store import SessionStore

pytestmark = [
    pytest.mark.kubernetes,
    pytest.mark.skipif(
        os.environ.get("AGENTRT_K8S_E2E") != "1",
        reason="explicit opt-in and in-cluster Core identity required",
    ),
]


@pytest.fixture
async def live(tmp_path: Path) -> Any:
    if not os.environ.get("KUBERNETES_SERVICE_HOST"):
        pytest.fail("kind E2E must run in-cluster; no host kubeconfig fallback")
    cfg = get_config()
    assert cfg.sandbox.backend == "kubernetes"
    manager = create_sandbox_manager(cfg.sandbox, tmp_path)
    await manager.start()
    try:
        yield cfg, manager, manager._backend
    finally:
        await manager.close()
        await manager._backend.aclose()


def context(
    key: SandboxKey, call: str, *, run: str = "root", attempt: int = 1
) -> SandboxCallContext:
    return SandboxCallContext(
        key, run, call, attempt=attempt, session_id=key.id if key.kind == "session" else ""
    )


async def test_real_write_exec_read_list_and_atomic_identity_reuse(live: Any) -> None:
    _, manager, _ = live
    key = SandboxKey("session", "e2e-" + uuid4().hex)
    runtime = manager.runtime_for(key)
    assert manager.handle_for(key) is None
    assert not (
        await runtime.write_text(WriteRequest(context(key, "write"), "data/a", "hello"))
    ).is_error
    result = await runtime.exec(
        ExecRequest(context(key, "append"), "printf x >> counter; cat data/a; printf err >&2")
    )
    assert result.exit_code == 0 and result.stdout == "hello" and result.stderr == "err"
    duplicate = await runtime.exec(
        ExecRequest(
            context(key, "append", attempt=2), "printf x >> counter; cat data/a; printf err >&2"
        )
    )
    assert duplicate == result
    assert (await runtime.read_text(ReadRequest(context(key, "counter"), "counter"))).content == "x"
    conflict = await runtime.exec(ExecRequest(context(key, "append"), "printf replay >> counter"))
    assert conflict.error_type == "idempotency_conflict"
    assert "data/" in (await runtime.list_dir(ListRequest(context(key, "list")))).content


async def test_timeout_kills_group_and_delayed_marker_stays_absent(live: Any) -> None:
    _, manager, _ = live
    key = SandboxKey("session", "e2e-timeout-" + uuid4().hex)
    runtime = manager.runtime_for(key)
    result = await runtime.exec(
        ExecRequest(context(key, "timeout"), "(sleep 3; touch delayed) & wait", timeout_s=1)
    )
    assert result.terminal_reason == "timeout"
    await asyncio.sleep(3)
    assert (await runtime.read_text(ReadRequest(context(key, "read"), "delayed"))).is_error


async def test_auth_path_and_cross_session_negative_cases(live: Any) -> None:
    _, manager, backend = live
    first = SandboxKey("session", "e2e-first-" + uuid4().hex)
    second = SandboxKey("session", "e2e-second-" + uuid4().hex)
    a, b = manager.runtime_for(first), manager.runtime_for(second)
    await a.write_text(WriteRequest(context(first, "secret"), "private", "only-first"))
    assert (await b.read_text(ReadRequest(context(second, "read"), "private"))).is_error
    assert (await a.read_text(ReadRequest(context(first, "escape"), "/etc/passwd"))).is_error
    await a.exec(ExecRequest(context(first, "symlink"), "ln -s /etc/passwd escape"))
    assert (await a.read_text(ReadRequest(context(first, "link-read"), "escape"))).is_error
    handle = manager.handle_for(first)
    assert handle is not None
    response = await backend.http.get(str(handle.endpoint) + "/healthz", timeout=5)
    assert response.status_code == 401
    result = await a.exec(
        ExecRequest(
            context(first, "credentials"),
            "test ! -e /run/credential/token && test ! -e /var/run/secrets/kubernetes.io/serviceaccount/token",
        )
    )
    assert result.exit_code == 0
    # Pod-local connectivity bypasses CNI but still needs the broker token.
    result = await a.exec(
        ExecRequest(
            context(first, "loopback-auth"),
            "python - <<'PY'\nimport urllib.request, urllib.error\n"
            "try:\n urllib.request.urlopen('http://127.0.0.1:8080/healthz')\n"
            "except urllib.error.HTTPError as e:\n assert e.code == 401\n"
            "else:\n raise SystemExit(9)\nPY",
        )
    )
    assert result.exit_code == 0


async def test_default_egress_denies_kubernetes_api(live: Any) -> None:
    _, manager, _ = live
    key = SandboxKey("session", "e2e-egress-" + uuid4().hex)
    runtime = manager.runtime_for(key)
    target = repr(os.environ["KUBERNETES_SERVICE_HOST"])
    # A successful TCP connect is failure even if API authorization later rejects
    # the request. Network isolation cannot be inferred from an HTTP 401/403.
    command = (
        "python - <<'PY'\nimport socket\n"
        f"try:\n s = socket.create_connection(({target}, 443), timeout=2)\n"
        "except OSError:\n pass\nelse:\n s.close(); raise SystemExit(9)\nPY"
    )
    assert (await runtime.exec(ExecRequest(context(key, "egress"), command))).exit_code == 0


async def test_ttl_and_deleted_pod_require_new_session(live: Any) -> None:
    cfg, manager, backend = live
    for reason in ("ttl", "deleted"):
        key = SandboxKey("session", "e2e-lost-" + uuid4().hex)
        runtime = manager.runtime_for(key)
        await runtime.write_text(WriteRequest(context(key, "write"), "marker", "persistent"))
        handle = manager.handle_for(key)
        assert handle is not None
        if reason == "ttl":
            manager._entries[key].last_completed -= cfg.sandbox.idle_timeout_s + 1
            await manager.reap_idle()
        else:
            await backend.api.delete("pod", str(handle.pod_name), str(handle.pod_uid))
            async with asyncio.timeout(cfg.sandbox.cleanup_margin_s):
                while await backend.api.get("pod", str(handle.pod_name)) is not None:
                    await asyncio.sleep(0.1)
        with pytest.raises(WorkspaceLostError):
            await runtime.read_text(ReadRequest(context(key, "after"), "marker"))


async def test_cancel_during_real_pod_creation_cleans_partial_resources(
    live: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, manager, backend = live
    entered = asyncio.Event()

    async def hold_ready(plan: Any, deadline: float) -> None:
        entered.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(backend, "_wait_ready", hold_ready)
    key = SandboxKey("session", "e2e-cancel-" + uuid4().hex)
    task = asyncio.create_task(
        manager.runtime_for(key).exec(ExecRequest(context(key, "exec"), "touch forbidden"))
    )
    await asyncio.wait_for(entered.wait(), timeout=120)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    name = "sb-" + backend.identity_for(key)
    for kind in ("pod", "service", "secret"):
        assert await backend.api.get(kind, name) is None


async def test_chat_turns_keep_one_pod_and_scope_identical_tool_ids_by_run(
    live: Any,
    tmp_path: Path,
) -> None:
    cfg, manager, backend = live
    bus = EventBus()

    class TurnProvider:
        def __init__(self) -> None:
            self.calls: dict[str, int] = {}

        async def chat(
            self,
            messages: Any,
            tool_schemas: Any,
            bus: Any,
            run_id: str,
            **kwargs: Any,
        ) -> LlmResponse:
            count = self.calls.get(run_id, 0)
            self.calls[run_id] = count + 1
            if count == 0:
                return LlmResponse(
                    stop_reason="tool_use",
                    tool_calls=[
                        ToolCallBlock(
                            id="same-tool-id",
                            name="bash",
                            input={
                                "command": "printf x >> turn-log",
                            },
                        ),
                    ],
                )
            return LlmResponse(stop_reason="end_turn", text="done")

    provider = TurnProvider()
    sessions = SessionManager(
        SessionStore(tmp_path / "sessions"),
        lambda: AgentRunner(
            cfg,
            provider=provider,
            bus=bus,
            sandbox_manager=manager,
            runs_dir=tmp_path,
        ),
        bus,
        on_session_closed=lambda sid: manager.release(SandboxKey("session", sid)),
    )
    session = await sessions.create("chat")
    key = SandboxKey("session", session.id)
    await sessions.send_message(session.id, "first turn", run_id="turn-one-" + uuid4().hex)
    first = manager.handle_for(key)
    assert first is not None and session.status == "waiting_for_input"

    await sessions.send_message(session.id, "second turn", run_id="turn-two-" + uuid4().hex)
    second = manager.handle_for(key)
    assert second is not None and second.pod_uid == first.pod_uid
    assert second.generation == first.generation
    result = await manager.runtime_for(key).read_text(
        ReadRequest(context(key, "check-turns"), "turn-log"),
    )
    assert result.content == "xx"
    await sessions.close(session.id)
    for kind in ("pod", "service", "secret"):
        assert await backend.api.get(kind, str(first.pod_name)) is None


class NestedProvider:
    def __init__(self, root: str, background: bool) -> None:
        self.roles = {root: "root"}
        self.calls: dict[str, int] = {}
        self.background = background
        self.child_done = asyncio.Event()

    async def chat(
        self, messages: Any, tool_schemas: Any, bus: Any, run_id: str, **kwargs: Any
    ) -> LlmResponse:
        if run_id not in self.roles:
            self.roles[run_id] = "child" if len(self.roles) == 1 else "nested"
        role = self.roles[run_id]
        count = self.calls.get(run_id, 0)
        self.calls[run_id] = count + 1
        if count == 0:
            calls = [
                ToolCallBlock(
                    id="same-tool-id",
                    name="bash",
                    input={"command": f"printf '{role}\\n' >> shared"},
                )
            ]
            if role != "nested":
                calls.append(
                    ToolCallBlock(
                        id="spawn",
                        name="spawn_agent",
                        input={
                            "description": role,
                            "prompt": "continue",
                            "run_in_background": self.background and role == "root",
                        },
                    )
                )
            return LlmResponse(stop_reason="tool_use", tool_calls=calls)
        if role == "root":
            await asyncio.wait_for(self.child_done.wait(), timeout=120)
        return LlmResponse(stop_reason="end_turn", text="done")


@pytest.mark.parametrize("background", [False, True])
async def test_root_foreground_background_nested_share_real_workspace(
    live: Any,
    tmp_path: Path,
    background: bool,
) -> None:
    cfg, manager, backend = live
    root = "e2e-root-" + uuid4().hex
    provider = NestedProvider(root, background)
    bus = EventBus()

    async def observe(event: Any) -> None:
        if event.type == "subagent.finished" and provider.roles.get(event.run_id) == "child":
            provider.child_done.set()

    bus.subscribe(observe)
    store = SessionStore(tmp_path / "sessions")
    sessions = SessionManager(
        store,
        lambda: AgentRunner(
            cfg, provider=provider, bus=bus, sandbox_manager=manager, runs_dir=tmp_path
        ),
        bus,
        on_session_closed=lambda sid: manager.release(SandboxKey("session", sid)),
    )
    session = await sessions.create("chat")
    await sessions.send_message(session.id, "go", run_id=root)
    key = SandboxKey("session", session.id)
    result = await manager.runtime_for(key).read_text(ReadRequest(context(key, "check"), "shared"))
    assert result.content.splitlines() == ["root", "child", "nested"]
    handle = manager.handle_for(key)
    assert handle is not None
    await sessions.close(session.id)
    assert await backend.api.get("pod", str(handle.pod_name)) is None


@pytest.mark.parametrize("exit_path", ["one-shot", "disconnect", "shutdown", "direct"])
async def test_real_lifecycle_exit_paths(live: Any, tmp_path: Path, exit_path: str) -> None:
    cfg, manager, backend = live

    class Provider:
        count = 0

        async def chat(self, *args: Any, **kwargs: Any) -> LlmResponse:
            self.count += 1
            if self.count == 1:
                return LlmResponse(
                    stop_reason="tool_use",
                    tool_calls=[
                        ToolCallBlock(
                            id="write", name="write_file", input={"path": "x", "content": "x"}
                        )
                    ],
                )
            return LlmResponse(stop_reason="end_turn", text="done")

    app = CoreApp(sandbox_manager=manager)
    sessions = SessionManager(
        SessionStore(tmp_path / "sessions"),
        lambda: AgentRunner(cfg, provider=Provider(), sandbox_manager=manager, runs_dir=tmp_path),
        EventBus(),
        on_session_closed=app._delete_session_resources,
    )
    app._sessions = sessions
    if exit_path == "direct":
        root = "e2e-direct-" + uuid4().hex
        await AgentRunner(
            cfg, provider=Provider(), sandbox_manager=manager, runs_dir=tmp_path
        ).run_and_capture("go", run_id=root)
        key = SandboxKey("direct_run", root)
    else:
        session = await sessions.create("one_shot" if exit_path == "one-shot" else "chat")
        key = SandboxKey("session", session.id)
        await sessions.send_message(session.id, "go")
        if exit_path == "disconnect":
            await app._cleanup_disconnected_sessions(frozenset({session.id}))
        elif exit_path == "shutdown":
            await app._shutdown()
    name = "sb-" + backend.identity_for(key)
    # Shutdown closes the API transport; a fresh API connection is a read-only
    # observation of disappearance, not a replacement execution generation.
    for kind in ("pod", "service", "secret"):
        assert await backend.api.get(kind, name) is None
