from __future__ import annotations

import asyncio
import datetime
import logging
import re
from collections.abc import Awaitable, Callable, Collection
from dataclasses import dataclass
from datetime import UTC, timedelta
from pathlib import Path
from typing import Any

from agent_runtime.core.permissions.policy import (
    DEFAULT_POLICIES,
    PermissionDecision,
    PermissionMode,
    SessionPermissions,
    ToolPolicy,
    matches_outside_cwd,
    param_preview,
)
from agent_runtime.core.permissions.storage import load_policy_file, save_policy_file

logger = logging.getLogger(__name__)


def _now() -> str:
    return datetime.datetime.now(UTC).isoformat()


@dataclass
class _PendingRequest:
    future: asyncio.Future[str]
    session_id: str
    run_id: str
    tool_name: str
    params: dict[str, Any]


# 管理工具调用权限：策略评估、用户审批挂起、session 级和持久化 always 缓存、超时
class PermissionManager:
    def __init__(
        self,
        policies: dict[str, ToolPolicy] | None = None,
        *,
        policy_file: Path | None = None,
        timeout_s: float = 60.0,
    ) -> None:
        self._policies: dict[str, ToolPolicy] = policies or dict(DEFAULT_POLICIES)
        # (session_id, run_id, tool_use_id) → pending Future + metadata
        self._pending: dict[tuple[str, str, str], _PendingRequest] = {}
        # (session_id, tool_name) → "allow" | "deny"（session 内存，重启丢失）
        self._session_always: dict[tuple[str, str], str] = {}
        self._session_permissions: dict[str, SessionPermissions] = {}
        # tool_name → "allow" | "deny"（持久化，从 policy_file 加载）
        self._policy_file = policy_file
        self._persistent_always: dict[str, str] = (
            load_policy_file(policy_file) if policy_file is not None else {}
        )
        # 0 表示不超时
        self._timeout_s = timeout_s

    def session_permissions(self, session_id: str) -> SessionPermissions:
        return self._session_permissions.get(session_id, SessionPermissions()).model_copy(deep=True)

    def configure_session(
        self,
        session_id: str,
        *,
        mode: PermissionMode,
        expected_revision: int,
        revoke_tool: str | None = None,
        reset_grants: bool = False,
    ) -> SessionPermissions:
        """Apply a revision-checked change and reconsider calls still awaiting consent."""
        current = self.session_permissions(session_id)
        if current.revision != expected_revision:
            raise ValueError("session permissions changed; refresh before retrying")
        grants = (
            []
            if reset_grants or mode != current.mode or mode == "read_only"
            else current.allowed_tools
        )
        state = SessionPermissions(
            mode=mode,
            allowed_tools=[tool for tool in grants if tool != revoke_tool],
            revision=current.revision + 1,
        )
        self._session_permissions[session_id] = state
        logger.info(
            "session permissions changed session=%s mode=%s revision=%s",
            session_id,
            mode,
            state.revision,
        )
        self._recheck_pending(session_id)
        return state.model_copy(deep=True)

    def _recheck_pending(self, session_id: str) -> None:
        for key, request in list(self._pending.items()):
            if request.session_id != session_id or request.future.done():
                continue
            allowed, decision = self._evaluate_without_prompt(
                request.tool_name, request.params, session_id
            )
            if allowed is not None:
                self._pending.pop(key)
                request.future.set_result(decision)

    # 对工具名 + 参数执行 4 层静态评估，不挂起
    def evaluate(self, tool_name: str, params: dict[str, Any]) -> PermissionDecision:
        from agent_runtime.core.permissions.policy import evaluate

        policy = self._policies.get(tool_name)
        return evaluate(tool_name, params, policy)

    def evaluate_durable(
        self,
        tool_name: str,
        params: dict[str, Any],
        session_id: str,
    ) -> tuple[bool | None, str]:
        """Evaluate every policy/cache tier without creating an in-memory waiter."""

        return self._evaluate_without_prompt(tool_name, params, session_id)

    def durable_expires_at(self) -> str | None:
        """Return the deadline persisted with a durable ASK interrupt."""

        if self._timeout_s <= 0:
            return None
        return (datetime.datetime.now(UTC) + timedelta(seconds=self._timeout_s)).isoformat()

    def _evaluate_without_prompt(
        self,
        tool_name: str,
        params: dict[str, Any],
        session_id: str,
    ) -> tuple[bool | None, str]:
        command = str(params.get("command", "")) if tool_name == "bash" else ""
        policy = self._policies.get(tool_name)

        # Tier 1: deny_patterns（bash only，不可被缓存绕过）
        if command and policy:
            for pat in policy.deny_patterns:
                if re.search(pat, command):
                    logger.debug("permission: deny_pattern hit tool=%s", tool_name)
                    return False, "auto_deny"

        # Explicit denials also constrain the new session-wide permissions.
        permissions = self._session_permissions.get(session_id)
        if permissions is not None:
            if (
                (policy is not None and policy.default == PermissionDecision.DENY)
                or self._session_always.get((session_id, tool_name)) == "deny"
                or self._persistent_always.get(tool_name) == "deny"
            ):
                return False, "auto_deny"
            if permissions.mode == "read_only":
                return (
                    (True, "auto_allow")
                    if tool_name in {"read_file", "list_dir"}
                    else (False, "read_only")
                )
            if permissions.mode == "auto":
                return True, "session_auto"
            if tool_name in permissions.allowed_tools:
                return True, "session_allow"

        # Legacy always rules still cannot bypass the outside-cwd ASK heuristic.
        outside_cwd = bool(command and matches_outside_cwd(command))
        if outside_cwd:
            return None, "ask"

        # Tier 3: session always 缓存
        session_key = (session_id, tool_name)
        if session_key in self._session_always:
            cached = self._session_always[session_key]
            logger.debug("permission: session cache hit tool=%s decision=%s", tool_name, cached)
            return cached == "allow", f"auto_{cached}"

        # Tier 4: persistent always（跨 session）
        if tool_name in self._persistent_always:
            cached = self._persistent_always[tool_name]
            logger.debug("permission: persistent cache hit tool=%s decision=%s", tool_name, cached)
            return cached == "allow", f"auto_{cached}"

        # Tier 5: allow_patterns（bash only）
        if command and policy:
            for pat in policy.allow_patterns:
                if re.search(pat, command):
                    return True, "auto_allow"

        # Tier 6: tool default
        if policy is not None:
            if policy.default == PermissionDecision.ALLOW:
                return True, "auto_allow"
            if policy.default == PermissionDecision.DENY:
                return False, "auto_deny"
        return None, "ask"

    # 检查权限；如需 ask 则向客户端发事件并等待响应；返回 (allowed, decision_str)
    async def check_and_wait(
        self,
        tool_use_id: str,
        tool_name: str,
        params: dict[str, Any],
        session_id: str,
        event_emitter: Callable[[dict[str, Any]], Awaitable[None]],
        run_id: str = "",
    ) -> tuple[bool, str]:
        allowed, decision = self._evaluate_without_prompt(tool_name, params, session_id)
        if allowed is not None:
            return allowed, decision

        # ASK 路径（来自 OUTSIDE_CWD 强制 ASK，或 default=ASK）
        pending_key = (session_id, run_id, tool_use_id)
        if pending_key in self._pending:
            logger.warning(
                "permission: duplicate pending session_id=%s run_id=%s tool_use_id=%s",
                session_id,
                run_id,
                tool_use_id,
            )
            return False, "duplicate_tool_use_id"
        loop = asyncio.get_event_loop()
        future: asyncio.Future[str] = loop.create_future()
        request = _PendingRequest(
            future=future,
            session_id=session_id,
            run_id=run_id,
            tool_name=tool_name,
            params=dict(params),
        )
        self._pending[pending_key] = request

        try:
            await event_emitter(
                {
                    "type": "permission.requested",
                    "tool_use_id": tool_use_id,
                    "tool_name": tool_name,
                    "params": params,
                    "param_preview": param_preview(tool_name, params),
                    "session_id": session_id,
                    "ts": _now(),
                }
            )
            if self._timeout_s > 0:
                raw = await asyncio.wait_for(future, timeout=self._timeout_s)
            else:
                raw = await future
        except TimeoutError:
            logger.info("permission: timeout tool_use_id=%s tool=%s", tool_use_id, tool_name)
            return False, "timeout"
        finally:
            if self._pending.get(pending_key) is request:
                self._pending.pop(pending_key, None)

        # Session grants are installed synchronously in respond(), never after a
        # later revoke/close has already removed them.
        allowed = self._apply_response(raw, session_id, tool_name)
        return allowed, raw

    # 仅当挂起请求属于调用连接拥有的 session 时，resolve 对应 Future
    def respond(
        self,
        tool_use_id: str,
        decision: str,
        *,
        authorized_session_ids: Collection[str],
        session_id: str | None = None,
        run_id: str | None = None,
    ) -> bool:
        if decision not in {
            "allow_once",
            "allow_session",
            "always_allow",
            "deny_once",
            "always_deny",
        }:
            return False
        if (session_id is None) != (run_id is None):
            logger.warning("permission.respond: session_id and run_id must be provided together")
            return False

        if session_id is not None and run_id is not None:
            if session_id not in authorized_session_ids:
                logger.warning("permission.respond: request is not owned by this connection")
                return False
            pending_key = (session_id, run_id, tool_use_id)
            if pending_key not in self._pending:
                logger.warning(
                    "permission.respond: unknown session_id=%s run_id=%s tool_use_id=%s",
                    session_id,
                    run_id,
                    tool_use_id,
                )
                return False
        else:
            authorized_keys = [
                key
                for key in self._pending
                if key[2] == tool_use_id and key[0] in authorized_session_ids
            ]
            if not authorized_keys:
                logger.warning("permission.respond: unknown or unowned tool_use_id=%s", tool_use_id)
                return False
            if len(authorized_keys) > 1:
                logger.warning(
                    "permission.respond: ambiguous tool_use_id=%s across authorized runs",
                    tool_use_id,
                )
                return False
            pending_key = authorized_keys[0]

        req = self._pending.pop(pending_key)
        if req.future.done():
            logger.warning(
                "permission.respond: request already resolved tool_use_id=%s",
                tool_use_id,
            )
            return False
        if decision == "allow_session":
            state = self.session_permissions(req.session_id)
            if req.tool_name not in state.allowed_tools:
                state.allowed_tools = sorted([*state.allowed_tools, req.tool_name])
                state.revision += 1
                self._session_permissions[req.session_id] = state
        req.future.set_result(decision)
        if decision == "allow_session":
            self._recheck_pending(req.session_id)
        return True

    # 应用审批决策，更新 session + persistent 缓存，返回是否放行
    def apply_durable_response(self, decision: str, session_id: str, tool_name: str) -> bool:
        return self._apply_response(decision, session_id, tool_name)

    def _apply_response(self, decision: str, session_id: str, tool_name: str) -> bool:
        allow = decision in (
            "allow_once",
            "always_allow",
            "allow_session",
            "auto_allow",
            "session_allow",
            "session_auto",
        )
        if decision == "always_allow":
            self._session_always[(session_id, tool_name)] = "allow"
            self._persistent_always[tool_name] = "allow"
            logger.info(
                "permission: always allow tool=%s policy_file=%s persistent=%s",
                tool_name,
                self._policy_file,
                self._persistent_always,
            )
            if self._policy_file is not None:
                try:
                    save_policy_file(self._persistent_always, self._policy_file)
                    logger.info("permission: policy.toml written path=%s", self._policy_file)
                except Exception:
                    logger.exception(
                        "permission: failed to write policy.toml path=%s", self._policy_file
                    )
            else:
                logger.warning("permission: policy_file is None, skipping persistence")
        elif decision == "always_deny":
            self._session_always[(session_id, tool_name)] = "deny"
            self._persistent_always[tool_name] = "deny"
            logger.info(
                "permission: always deny tool=%s policy_file=%s persistent=%s",
                tool_name,
                self._policy_file,
                self._persistent_always,
            )
            if self._policy_file is not None:
                try:
                    save_policy_file(self._persistent_always, self._policy_file)
                    logger.info("permission: policy.toml written path=%s", self._policy_file)
                except Exception:
                    logger.exception(
                        "permission: failed to write policy.toml path=%s", self._policy_file
                    )
            else:
                logger.warning("permission: policy_file is None, skipping persistence")
        return allow

    # 客户端断连时拒绝该 session 所有待审批请求，防止 Future 永久挂起
    def cancel_session(self, session_id: str, reason: str = "client_disconnected") -> None:
        self._session_permissions.pop(session_id, None)
        for cached in [key for key in self._session_always if key[0] == session_id]:
            self._session_always.pop(cached)
        to_cancel = [key for key, req in self._pending.items() if req.session_id == session_id]
        for key in to_cancel:
            req = self._pending.pop(key)
            if not req.future.done():
                logger.debug(
                    "permission: cancel pending run_id=%s tool_use_id=%s reason=%s",
                    key[1],
                    key[2],
                    reason,
                )
                req.future.set_result("deny_once")
