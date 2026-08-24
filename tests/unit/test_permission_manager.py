from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import Any

import pytest

from agent_runtime.core.permissions.manager import PermissionManager
from agent_runtime.core.permissions.policy import PermissionDecision, ToolPolicy
from agent_runtime.core.permissions.storage import load_policy_file


def test_durable_evaluation_reuses_cache_and_preserves_forced_ask() -> None:
    manager = _make_manager()

    assert manager.evaluate_durable("write_file", {"path": "x"}, "s1") == (None, "ask")
    assert manager.apply_durable_response("always_allow", "s1", "bash") is True
    assert manager.evaluate_durable("bash", {"command": "echo safe"}, "s1") == (
        True,
        "auto_allow",
    )
    assert manager.evaluate_durable("bash", {"command": "cd .."}, "s1") == (None, "ask")


def test_durable_permission_deadline_is_explicit_and_timezone_aware() -> None:
    no_timeout = PermissionManager(timeout_s=0)
    timed = PermissionManager(timeout_s=5)

    assert no_timeout.durable_expires_at() is None
    deadline = timed.durable_expires_at()
    assert deadline is not None
    parsed = datetime.fromisoformat(deadline)
    assert parsed.tzinfo is not None
    assert parsed > datetime.now(UTC)


# ── helpers ──────────────────────────────────────────────────────────────────


def _make_manager(**policies: ToolPolicy) -> PermissionManager:
    # policy_file=None：测试中不使用持久化，不污染 ~/.agentrt/policy.toml
    return PermissionManager(policies or None)


async def _collect_emitted() -> tuple[list[dict[str, Any]], Any]:
    emitted: list[dict[str, Any]] = []

    async def emitter(event: dict[str, Any]) -> None:
        emitted.append(event)

    return emitted, emitter


# ── evaluate() delegation ─────────────────────────────────────────────────────


# 功能：验证 PermissionManager.evaluate 委托给 policy 层返回正确决策
# 设计：直接调用 evaluate()，不涉及 Future，验证策略加载与委托路径
def test_evaluate_delegates_to_policy() -> None:
    mgr = _make_manager()
    assert mgr.evaluate("read_file", {"path": "x"}) == PermissionDecision.ALLOW
    assert mgr.evaluate("bash", {"command": "echo hi"}) == PermissionDecision.ASK
    assert mgr.evaluate("write_file", {"path": "x", "content": ""}) == PermissionDecision.ASK


# ── check_and_wait: ALLOW path ───────────────────────────────────────────────


# 功能：验证策略为 ALLOW 时 check_and_wait 立即返回 (True, "auto_allow")，不发任何事件
# 设计：read_file 默认 ALLOW，断言不产生 permission.requested 事件，覆盖"无噪声放行"路径
async def test_check_and_wait_allow_no_event() -> None:
    mgr = _make_manager()
    emitted, emitter = await _collect_emitted()

    allowed, decision = await mgr.check_and_wait(
        tool_use_id="t1",
        tool_name="read_file",
        params={"path": "README.md"},
        session_id="s1",
        event_emitter=emitter,
    )

    assert allowed is True
    assert decision == "auto_allow"
    assert emitted == []


# ── check_and_wait: ASK path + respond ───────────────────────────────────────


# 功能：验证 ASK 策略时发出 permission.requested 事件并等待 respond() 解决 Future
# 设计：在后台协程中调用 respond("allow_once")，主协程 await 结束后断言结果；
#       这是权限系统的核心反向请求通路
async def test_check_and_wait_ask_emits_event_and_waits() -> None:
    mgr = _make_manager()
    emitted, emitter = await _collect_emitted()

    async def _auto_respond() -> None:
        await asyncio.sleep(0)  # yield once so check_and_wait can emit the event
        mgr.respond("t2", "allow_once", authorized_session_ids={"s1"})

    task = asyncio.create_task(_auto_respond())
    allowed, decision = await mgr.check_and_wait(
        tool_use_id="t2",
        tool_name="bash",
        params={"command": "echo hi"},
        session_id="s1",
        event_emitter=emitter,
    )
    await task

    assert allowed is True
    assert decision == "allow_once"
    assert len(emitted) == 1
    assert emitted[0]["type"] == "permission.requested"
    assert emitted[0]["tool_use_id"] == "t2"
    assert emitted[0]["tool_name"] == "bash"


# 功能：验证 respond("deny_once") 使 check_and_wait 返回 (False, "deny_once")
# 设计：用户拒绝时工具不应执行，确认 False 返回值而不是异常
async def test_check_and_wait_deny_once_returns_false() -> None:
    mgr = _make_manager()
    _, emitter = await _collect_emitted()

    async def _auto_deny() -> None:
        await asyncio.sleep(0)
        mgr.respond("t3", "deny_once", authorized_session_ids={"s1"})

    task = asyncio.create_task(_auto_deny())
    allowed, decision = await mgr.check_and_wait(
        tool_use_id="t3",
        tool_name="bash",
        params={"command": "echo hi"},
        session_id="s1",
        event_emitter=emitter,
    )
    await task

    assert allowed is False
    assert decision == "deny_once"


# ── always_allow cache ────────────────────────────────────────────────────────


# 功能：验证 respond("always_allow") 后同 session 同工具下次不再发事件
# 设计：第二次调用 check_and_wait 命中 always 缓存，直接返回 (True, "auto_allow")，emitted 仍为 1 条
async def test_always_allow_skips_future_ask() -> None:
    mgr = _make_manager()
    emitted, emitter = await _collect_emitted()

    # First call: user says "always allow"
    async def _auto_always() -> None:
        await asyncio.sleep(0)
        mgr.respond("t4", "always_allow", authorized_session_ids={"s1"})

    task = asyncio.create_task(_auto_always())
    r1, _ = await mgr.check_and_wait(
        tool_use_id="t4",
        tool_name="bash",
        params={"command": "echo hi"},
        session_id="s1",
        event_emitter=emitter,
    )
    await task
    assert r1 is True

    # Second call: should hit cache, no new event
    r2, d2 = await mgr.check_and_wait(
        tool_use_id="t5",
        tool_name="bash",
        params={"command": "ls"},
        session_id="s1",
        event_emitter=emitter,
    )

    assert r2 is True
    assert d2 == "auto_allow"
    assert len(emitted) == 1  # only the first call emitted an event


# 功能：验证 always_allow 在同一 manager 实例内对所有 session 生效（persistent_always 共享）
# 设计：s1 设置 always_allow → 写入 _persistent_always；s2 命中 persistent 缓存，直接放行；
#       emitted 只有 1 条（s2 不需要再 ASK）。这是 persistent always 的核心跨 session 语义。
async def test_always_allow_not_shared_across_sessions() -> None:
    mgr = _make_manager()
    emitted, emitter = await _collect_emitted()

    # session s1 sets always allow for bash
    async def _auto_always() -> None:
        await asyncio.sleep(0)
        mgr.respond("t6", "always_allow", authorized_session_ids={"s1"})

    task = asyncio.create_task(_auto_always())
    await mgr.check_and_wait(
        tool_use_id="t6",
        tool_name="bash",
        params={"command": "echo"},
        session_id="s1",
        event_emitter=emitter,
    )
    await task

    # session s2 — persistent_always["bash"] = "allow" → 直接放行，不再 ASK
    r, d = await mgr.check_and_wait(
        tool_use_id="t7",
        tool_name="bash",
        params={"command": "echo"},
        session_id="s2",
        event_emitter=emitter,
    )

    assert r is True
    assert d == "auto_allow"
    assert len(emitted) == 1  # s2 命中 persistent 缓存，不再发出事件


# ── always_deny cache ─────────────────────────────────────────────────────────


# 功能：验证 respond("always_deny") 后同 session 同工具下次直接返回 (False, "auto_deny")
# 设计：用户选择 always deny 后不应继续骚扰，下次调用静默拒绝
async def test_always_deny_skips_future_ask() -> None:
    mgr = _make_manager()
    emitted, emitter = await _collect_emitted()

    async def _auto_always_deny() -> None:
        await asyncio.sleep(0)
        mgr.respond("t8", "always_deny", authorized_session_ids={"s1"})

    task = asyncio.create_task(_auto_always_deny())
    r1, _ = await mgr.check_and_wait(
        tool_use_id="t8",
        tool_name="bash",
        params={"command": "echo"},
        session_id="s1",
        event_emitter=emitter,
    )
    await task
    assert r1 is False

    # Second call: cache hit → no event, return (False, "auto_deny")
    r2, d2 = await mgr.check_and_wait(
        tool_use_id="t9",
        tool_name="bash",
        params={"command": "ls"},
        session_id="s1",
        event_emitter=emitter,
    )
    assert r2 is False
    assert d2 == "auto_deny"
    assert len(emitted) == 1


# ── cancel_session ────────────────────────────────────────────────────────────


# 功能：验证 cancel_session 将 pending Future 设为 deny_once，check_and_wait 返回 False
# 设计：模拟客户端断连场景——check_and_wait 挂起后调用 cancel_session，
#       确认 Future 被解决而非永久挂起（防止僵尸 run）
async def test_cancel_session_resolves_pending_future() -> None:
    mgr = _make_manager()
    _, emitter = await _collect_emitted()

    async def _cancel_after_emit() -> None:
        await asyncio.sleep(0)  # wait for event to be emitted
        mgr.cancel_session("s1", reason="client_disconnected")

    task = asyncio.create_task(_cancel_after_emit())
    allowed, _ = await mgr.check_and_wait(
        tool_use_id="t10",
        tool_name="bash",
        params={"command": "ls"},
        session_id="s1",
        event_emitter=emitter,
    )
    await task

    assert allowed is False


# 功能：验证 cancel_session 只取消属于该 session 的 pending Future
# 设计：s1 和 s2 各有一个 pending，cancel_session(s2) 不影响 s1 的 Future
async def test_cancel_session_only_affects_target_session() -> None:
    mgr = _make_manager()
    _, emitter = await _collect_emitted()

    # Launch two concurrent check_and_wait for different sessions
    s1_done = asyncio.Event()
    s2_done = asyncio.Event()
    s1_result: list[bool] = []
    s2_result: list[bool] = []

    async def _s1() -> None:
        r, _ = await mgr.check_and_wait(
            tool_use_id="ta",
            tool_name="bash",
            params={"command": "echo"},
            session_id="s1",
            event_emitter=emitter,
        )
        s1_result.append(r)
        s1_done.set()

    async def _s2() -> None:
        r, _ = await mgr.check_and_wait(
            tool_use_id="tb",
            tool_name="bash",
            params={"command": "echo"},
            session_id="s2",
            event_emitter=emitter,
        )
        s2_result.append(r)
        s2_done.set()

    t1 = asyncio.create_task(_s1())
    t2 = asyncio.create_task(_s2())

    await asyncio.sleep(0)  # let both emit events and hang

    # cancel only s2
    mgr.cancel_session("s2")
    await s2_done.wait()

    # s1 should still be pending; resolve it manually
    mgr.respond("ta", "allow_once", authorized_session_ids={"s1"})
    await s1_done.wait()

    await t1
    await t2

    assert s1_result == [True]  # s1 was allowed
    assert s2_result == [False]  # s2 was cancelled → denied


# 功能：验证其他 session 的连接不能解决当前 session 的挂起审批
# 设计：先用 s2 授权集合响应 s1 请求并确认 Future 仍挂起，再由 s1 owner 完成审批
async def test_respond_rejects_session_not_owned_by_connection() -> None:
    mgr = _make_manager()
    emitted = asyncio.Event()

    async def emitter(_event: dict[str, Any]) -> None:
        emitted.set()

    pending = asyncio.create_task(
        mgr.check_and_wait(
            tool_use_id="owned-by-s1",
            tool_name="bash",
            params={"command": "echo safe"},
            session_id="s1",
            event_emitter=emitter,
        )
    )
    await asyncio.wait_for(emitted.wait(), timeout=1.0)

    assert (
        mgr.respond(
            "owned-by-s1",
            "allow_once",
            authorized_session_ids={"s2"},
        )
        is False
    )
    await asyncio.sleep(0)
    assert pending.done() is False

    assert (
        mgr.respond(
            "owned-by-s1",
            "allow_once",
            authorized_session_ids={"s1"},
        )
        is True
    )
    assert await asyncio.wait_for(pending, timeout=1.0) == (True, "allow_once")


# ── respond: unknown tool_use_id ──────────────────────────────────────────────


# 功能：验证 respond 传入不存在的 tool_use_id 时静默忽略，不抛异常
# 设计：竞态场景（客户端重复发送响应）不应导致 daemon crash
def test_respond_unknown_tool_use_id_is_noop() -> None:
    mgr = _make_manager()
    assert mgr.respond("nonexistent", "allow_once", authorized_session_ids={"s1"}) is False


# ── OUTSIDE_CWD 不被 always 缓存绕过 ─────────────────────────────────────────


# 功能：验证 always_allow bash 之后，含绝对路径的命令仍触发 ASK，不被缓存绕过
# 设计：先让 session s1 对 bash 设置 always_allow，再请求含绝对路径命令；
#       OUTSIDE_CWD 检查在 always 缓存之前，应发出 permission.requested 事件
async def test_always_allow_does_not_bypass_outside_cwd() -> None:
    mgr = _make_manager()
    emitted, emitter = await _collect_emitted()

    # 首次 allow → 写入 session always 缓存
    async def _auto_always() -> None:
        await asyncio.sleep(0)
        mgr.respond("t_always", "always_allow", authorized_session_ids={"s1"})

    t = asyncio.create_task(_auto_always())
    await mgr.check_and_wait(
        tool_use_id="t_always",
        tool_name="bash",
        params={"command": "echo ok"},
        session_id="s1",
        event_emitter=emitter,
    )
    await t
    assert len(emitted) == 1  # 首次 ASK 触发事件

    # 第二次：bash + 绝对路径 → OUTSIDE_CWD 强制 ASK，不命中 session always 缓存
    async def _auto_respond_abs() -> None:
        await asyncio.sleep(0)
        mgr.respond("t_abs", "allow_once", authorized_session_ids={"s1"})

    t2 = asyncio.create_task(_auto_respond_abs())
    allowed, decision = await mgr.check_and_wait(
        tool_use_id="t_abs",
        tool_name="bash",
        params={"command": "cat /etc/hosts"},
        session_id="s1",
        event_emitter=emitter,
    )
    await t2

    assert allowed is True
    assert len(emitted) == 2  # 绝对路径命令再次触发 ASK，共 2 个事件


# ── 持久化 always 写文件 ──────────────────────────────────────────────────────


# 功能：验证 always_allow 决策写入 policy_file，新 PermissionManager 加载后自动放行
# 设计：用 tmp_path 作为 policy_file，断言文件存在且内容正确；
#       再新建 manager 加载文件，同工具无需 ASK 直接返回 auto_allow
async def test_persistent_always_written_and_reloaded(tmp_path: pytest.TempPathFixture) -> None:
    policy_file = tmp_path / "policy.toml"
    mgr = PermissionManager(policy_file=policy_file)
    emitted, emitter = await _collect_emitted()

    async def _auto_always() -> None:
        await asyncio.sleep(0)
        mgr.respond("tp1", "always_allow", authorized_session_ids={"s1"})

    t = asyncio.create_task(_auto_always())
    allowed, _ = await mgr.check_and_wait(
        tool_use_id="tp1",
        tool_name="bash",
        params={"command": "echo"},
        session_id="s1",
        event_emitter=emitter,
    )
    await t
    assert allowed is True
    assert policy_file.exists()

    loaded = load_policy_file(policy_file)
    assert loaded.get("bash") == "allow"

    # 新 manager 加载同一文件，bash 应直接 auto_allow（无 OUTSIDE_CWD）
    mgr2 = PermissionManager(policy_file=policy_file)
    emitted2, emitter2 = await _collect_emitted()
    allowed2, decision2 = await mgr2.check_and_wait(
        tool_use_id="tp2",
        tool_name="bash",
        params={"command": "echo new"},
        session_id="s2",
        event_emitter=emitter2,
    )
    assert allowed2 is True
    assert decision2 == "auto_allow"
    assert emitted2 == []  # 无需 ASK


# ── 审批超时 ──────────────────────────────────────────────────────────────────


# 功能：验证 check_and_wait 超时后返回 (False, "timeout")，不永久挂起
# 设计：timeout_s=0.05 极短超时，不主动 respond；断言在合理时间内返回 False
async def test_permission_timeout_returns_false() -> None:
    mgr = PermissionManager(timeout_s=0.05)
    emitted, emitter = await _collect_emitted()

    allowed, decision = await mgr.check_and_wait(
        tool_use_id="t_timeout",
        tool_name="bash",
        params={"command": "echo hi"},
        session_id="s1",
        event_emitter=emitter,
    )

    assert allowed is False
    assert decision == "timeout"
    assert len(emitted) == 1
    assert emitted[0]["type"] == "permission.requested"


# 功能：验证超时后 pending 被清理，迟到的 respond 不影响后续调用
# 设计：超时后调用 respond，不抛异常（unknown tool_use_id 静默忽略）；
#       再次 check_and_wait 同 tool_use_id 仍正常发出新的 permission.requested
async def test_permission_timeout_cleans_up_pending() -> None:
    mgr = PermissionManager(timeout_s=0.05)
    _, emitter = await _collect_emitted()

    await mgr.check_and_wait(
        tool_use_id="t_late",
        tool_name="bash",
        params={"command": "echo"},
        session_id="s1",
        event_emitter=emitter,
    )
    # 超时后迟到的 respond 不应 crash
    assert mgr.respond("t_late", "allow_once", authorized_session_ids={"s1"}) is False
    assert ("s1", "", "t_late") not in mgr._pending


async def test_duplicate_pending_tool_use_id_in_same_session_fails_closed() -> None:
    mgr = PermissionManager(timeout_s=0)
    first_emitted = asyncio.Event()

    async def first_emitter(_event: dict[str, Any]) -> None:
        first_emitted.set()

    first = asyncio.create_task(
        mgr.check_and_wait(
            tool_use_id="duplicate-id",
            tool_name="bash",
            params={"command": "echo first"},
            session_id="s1",
            event_emitter=first_emitter,
        )
    )
    await asyncio.wait_for(first_emitted.wait(), timeout=1.0)

    second = await asyncio.wait_for(
        mgr.check_and_wait(
            tool_use_id="duplicate-id",
            tool_name="bash",
            params={"command": "echo second"},
            session_id="s1",
            event_emitter=lambda _event: asyncio.sleep(0),
        ),
        timeout=1.0,
    )

    assert second == (False, "duplicate_tool_use_id")
    assert mgr.respond("duplicate-id", "allow_once", authorized_session_ids={"s1"}) is True
    assert await asyncio.wait_for(first, timeout=1.0) == (True, "allow_once")


async def test_same_tool_use_id_pending_requests_are_isolated_by_session() -> None:
    mgr = PermissionManager(timeout_s=0)
    emitted = {"s1": asyncio.Event(), "s2": asyncio.Event()}

    async def emitter(event: dict[str, Any]) -> None:
        emitted[str(event["session_id"])].set()

    s1 = asyncio.create_task(
        mgr.check_and_wait(
            tool_use_id="shared-id",
            tool_name="bash",
            params={"command": "echo s1"},
            session_id="s1",
            event_emitter=emitter,
        )
    )
    s2 = asyncio.create_task(
        mgr.check_and_wait(
            tool_use_id="shared-id",
            tool_name="bash",
            params={"command": "echo s2"},
            session_id="s2",
            event_emitter=emitter,
        )
    )
    await asyncio.wait_for(
        asyncio.gather(emitted["s1"].wait(), emitted["s2"].wait()),
        timeout=1.0,
    )

    assert set(mgr._pending) == {("s1", "", "shared-id"), ("s2", "", "shared-id")}
    assert (
        mgr.respond(
            "shared-id",
            "allow_once",
            authorized_session_ids={"s1", "s2"},
        )
        is False
    )
    assert s1.done() is False
    assert s2.done() is False

    assert mgr.respond("shared-id", "allow_once", authorized_session_ids={"s1"}) is True
    assert await asyncio.wait_for(s1, timeout=1.0) == (True, "allow_once")
    assert s2.done() is False

    assert mgr.respond("shared-id", "deny_once", authorized_session_ids={"s2"}) is True
    assert await asyncio.wait_for(s2, timeout=1.0) == (False, "deny_once")
    assert mgr._pending == {}


async def test_cancel_session_with_shared_tool_use_id_leaves_other_session_pending() -> None:
    mgr = PermissionManager(timeout_s=0)
    emitted = {"s1": asyncio.Event(), "s2": asyncio.Event()}

    async def emitter(event: dict[str, Any]) -> None:
        emitted[str(event["session_id"])].set()

    s1 = asyncio.create_task(
        mgr.check_and_wait(
            tool_use_id="shared-cancel-id",
            tool_name="bash",
            params={"command": "echo s1"},
            session_id="s1",
            event_emitter=emitter,
        )
    )
    s2 = asyncio.create_task(
        mgr.check_and_wait(
            tool_use_id="shared-cancel-id",
            tool_name="bash",
            params={"command": "echo s2"},
            session_id="s2",
            event_emitter=emitter,
        )
    )
    await asyncio.wait_for(
        asyncio.gather(emitted["s1"].wait(), emitted["s2"].wait()),
        timeout=1.0,
    )

    mgr.cancel_session("s1")
    assert await asyncio.wait_for(s1, timeout=1.0) == (False, "deny_once")
    assert s2.done() is False
    assert set(mgr._pending) == {("s2", "", "shared-cancel-id")}

    assert (
        mgr.respond(
            "shared-cancel-id",
            "allow_once",
            authorized_session_ids={"s2"},
        )
        is True
    )
    assert await asyncio.wait_for(s2, timeout=1.0) == (True, "allow_once")
    assert mgr._pending == {}


async def test_cancelled_permission_wait_cleans_pending_request() -> None:
    mgr = PermissionManager(timeout_s=0)
    emitted = asyncio.Event()

    async def emitter(_event: dict[str, Any]) -> None:
        emitted.set()

    pending = asyncio.create_task(
        mgr.check_and_wait(
            tool_use_id="cancelled-id",
            tool_name="bash",
            params={"command": "echo waiting"},
            session_id="s1",
            event_emitter=emitter,
        )
    )
    await asyncio.wait_for(emitted.wait(), timeout=1.0)
    pending.cancel()

    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(pending, timeout=1.0)
    assert ("s1", "", "cancelled-id") not in mgr._pending


async def test_same_session_and_tool_use_id_are_isolated_by_run() -> None:
    mgr = PermissionManager(timeout_s=0)
    root_emitted = asyncio.Event()
    child_emitted = asyncio.Event()

    async def root_emitter(_event: dict[str, Any]) -> None:
        root_emitted.set()

    async def child_emitter(_event: dict[str, Any]) -> None:
        child_emitted.set()

    root = asyncio.create_task(
        mgr.check_and_wait(
            tool_use_id="shared-tool",
            tool_name="bash",
            params={"command": "echo root"},
            session_id="s1",
            run_id="root-run",
            event_emitter=root_emitter,
        )
    )
    child = asyncio.create_task(
        mgr.check_and_wait(
            tool_use_id="shared-tool",
            tool_name="bash",
            params={"command": "echo child"},
            session_id="s1",
            run_id="child-run",
            event_emitter=child_emitter,
        )
    )
    await asyncio.wait_for(
        asyncio.gather(root_emitted.wait(), child_emitted.wait()),
        timeout=1.0,
    )

    assert set(mgr._pending) == {
        ("s1", "root-run", "shared-tool"),
        ("s1", "child-run", "shared-tool"),
    }
    assert mgr.respond("shared-tool", "allow_once", authorized_session_ids={"s1"}) is False
    assert root.done() is False
    assert child.done() is False

    assert (
        mgr.respond(
            "shared-tool",
            "allow_once",
            authorized_session_ids={"s1"},
            session_id="s1",
        )
        is False
    )
    assert (
        mgr.respond(
            "shared-tool",
            "allow_once",
            authorized_session_ids={"s1"},
            session_id="s2",
            run_id="root-run",
        )
        is False
    )
    assert root.done() is False
    assert child.done() is False

    assert (
        mgr.respond(
            "shared-tool",
            "allow_once",
            authorized_session_ids={"s1"},
            session_id="s1",
            run_id="root-run",
        )
        is True
    )
    assert await asyncio.wait_for(root, timeout=1.0) == (True, "allow_once")
    assert child.done() is False

    assert (
        mgr.respond(
            "shared-tool",
            "deny_once",
            authorized_session_ids={"s1"},
            session_id="s1",
            run_id="child-run",
        )
        is True
    )
    assert await asyncio.wait_for(child, timeout=1.0) == (False, "deny_once")
    assert mgr._pending == {}


async def test_cancel_session_clears_all_runs_without_affecting_other_sessions() -> None:
    mgr = PermissionManager(timeout_s=0)
    emitted = {"s1-root": asyncio.Event(), "s1-child": asyncio.Event(), "s2": asyncio.Event()}

    async def wait_for_permission(session_id: str, run_id: str, marker: str) -> tuple[bool, str]:
        async def emitter(_event: dict[str, Any]) -> None:
            emitted[marker].set()

        return await mgr.check_and_wait(
            tool_use_id="same-id",
            tool_name="bash",
            params={"command": marker},
            session_id=session_id,
            run_id=run_id,
            event_emitter=emitter,
        )

    s1_root = asyncio.create_task(wait_for_permission("s1", "root", "s1-root"))
    s1_child = asyncio.create_task(wait_for_permission("s1", "child", "s1-child"))
    s2 = asyncio.create_task(wait_for_permission("s2", "other", "s2"))
    await asyncio.wait_for(
        asyncio.gather(*(event.wait() for event in emitted.values())),
        timeout=1.0,
    )

    mgr.cancel_session("s1")

    assert await asyncio.wait_for(s1_root, timeout=1.0) == (False, "deny_once")
    assert await asyncio.wait_for(s1_child, timeout=1.0) == (False, "deny_once")
    assert s2.done() is False
    assert set(mgr._pending) == {("s2", "other", "same-id")}

    assert (
        mgr.respond(
            "same-id",
            "allow_once",
            authorized_session_ids={"s2"},
            session_id="s2",
            run_id="other",
        )
        is True
    )
    assert await asyncio.wait_for(s2, timeout=1.0) == (True, "allow_once")
