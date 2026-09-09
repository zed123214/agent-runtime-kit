from __future__ import annotations

import asyncio
import json
import time
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from pydantic import ValidationError

from agent_runtime.core.bus.events import (
    PermissionDeniedEvent,
    PermissionGrantedEvent,
    PermissionRequestedEvent,
    ToolCallFailedEvent,
    ToolCallFinishedEvent,
    ToolCallStartedEvent,
)
from agent_runtime.core.events.bus import EventBus
from agent_runtime.core.graph.recovery import (
    ToolInvocationConflictError,
    hash_tool_input,
)
from agent_runtime.core.llm.types import ToolCallBlock
from agent_runtime.core.tools.base import ToolInvocationContext, ToolResult
from agent_runtime.core.tools.errors import RateLimitedError
from agent_runtime.core.tools.registry import ToolRegistry

if TYPE_CHECKING:
    from agent_runtime.core.graph.recovery import RecoveryStore
    from agent_runtime.core.permissions.manager import PermissionManager

_DEFAULT_TIMEOUT: float = 120.0
_MAX_RETRIES: int = 2
_RETRY_BASE_S: float = 2.0  # backoff base; tests can monkeypatch to 0
_RETRYABLE: frozenset[str] = frozenset({"runtime_error", "rate_limited"})


class ToolOutcomeUnknownError(RuntimeError):
    code = "tool_outcome_unknown"


def _serialize_result(result: ToolResult) -> str:
    return json.dumps(
        {
            "content": result.content,
            "error_type": result.error_type,
            "is_error": result.is_error,
        },
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def _deserialize_result(serialized: str | None) -> ToolResult:
    if serialized is None:
        raise ToolInvocationConflictError("Completed tool invocation has no result.")
    try:
        raw = json.loads(serialized)
    except json.JSONDecodeError as exc:
        raise ToolInvocationConflictError("Completed tool result is invalid JSON.") from exc
    if not isinstance(raw, dict):
        raise ToolInvocationConflictError("Completed tool result is not an object.")
    content = raw.get("content")
    is_error = raw.get("is_error")
    error_type = raw.get("error_type")
    if (
        not isinstance(content, str)
        or not isinstance(is_error, bool)
        or (error_type is not None and not isinstance(error_type, str))
    ):
        raise ToolInvocationConflictError("Completed tool result has invalid fields.")
    return ToolResult(content=content, is_error=is_error, error_type=error_type)


def _now() -> str:
    return datetime.now(UTC).isoformat()


# 发布 ToolCallFailedEvent 并返回对应 ToolResult
async def _fail(
    bus: EventBus,
    run_id: str,
    tool_call: ToolCallBlock,
    error_class: str,
    error_message: str,
    elapsed_ms: int,
    *,
    attempt: int = 1,
    before_publish: Callable[[ToolResult], Awaitable[object]] | None = None,
) -> ToolResult:
    result = ToolResult(content=error_message, is_error=True, error_type=error_class)
    if before_publish is not None:
        await before_publish(result)
    await bus.publish(
        ToolCallFailedEvent(
            run_id=run_id,
            tool_use_id=tool_call.id,
            tool_name=tool_call.name,
            error_class=error_class,
            error_message=error_message,
            elapsed_ms=elapsed_ms,
            attempt=attempt,
            ts=_now(),
        )
    )
    return result


# 校验参数、检查权限、限时调用工具、发布进度事件，失败时指数退避重试，返回 ToolResult（不抛异常）
async def invoke_tool(
    registry: ToolRegistry,
    tool_call: ToolCallBlock,
    bus: EventBus,
    run_id: str,
    timeout: float = _DEFAULT_TIMEOUT,
    *,
    permission_manager: PermissionManager | None = None,
    session_id: str = "",
    recovery_store: RecoveryStore | None = None,
    durable_permission: Callable[[], tuple[bool, str]] | None = None,
) -> ToolResult:
    t0 = time.monotonic()

    async def publish_started() -> None:
        await bus.publish(
            ToolCallStartedEvent(
                run_id=run_id,
                tool_use_id=tool_call.id,
                tool_name=tool_call.name,
                params=dict(tool_call.input),
                ts=_now(),
            )
        )

    if recovery_store is None:
        await publish_started()

    def elapsed() -> int:
        return int((time.monotonic() - t0) * 1000)

    tool = registry.get(tool_call.name)
    if tool is None:
        if recovery_store is not None:
            await publish_started()
        return await _fail(
            bus,
            run_id,
            tool_call,
            "runtime_error",
            f"unknown tool: {tool_call.name}",
            elapsed(),
        )

    if tool.params_model is not None:
        try:
            tool.params_model.model_validate(dict(tool_call.input))
        except ValidationError as exc:
            if recovery_store is not None:
                await publish_started()
            return await _fail(
                bus,
                run_id,
                tool_call,
                "schema_error",
                str(exc),
                elapsed(),
            )

    input_hash: str | None = None
    if recovery_store is not None:
        input_hash = hash_tool_input(tool_call.input)
        existing = await recovery_store.get_tool(session_id, run_id, tool_call.id)
        if existing is not None:
            if existing.tool_name != tool_call.name or existing.input_hash != input_hash:
                raise ToolInvocationConflictError(
                    "Tool invocation identity does not match the journal."
                )
            if existing.status == "completed":
                return _deserialize_result(existing.serialized_result)
            raise ToolOutcomeUnknownError(
                f"Tool invocation {tool_call.id!r} has no safely replayable outcome."
            )

    if durable_permission is not None or permission_manager is not None:

        async def _emit_permission(raw: dict[str, Any]) -> None:
            await bus.publish(PermissionRequestedEvent(**raw, run_id=run_id))

        if durable_permission is not None:
            allowed, decision = durable_permission()
        else:
            assert permission_manager is not None
            allowed, decision = await permission_manager.check_and_wait(
                tool_use_id=tool_call.id,
                tool_name=tool_call.name,
                params=dict(tool_call.input),
                session_id=session_id,
                run_id=run_id,
                event_emitter=_emit_permission,
            )
        if allowed:
            if decision not in ("auto_allow",):
                await bus.publish(
                    PermissionGrantedEvent(
                        run_id=run_id,
                        tool_use_id=tool_call.id,
                        decision=decision,
                        ts=_now(),
                    )
                )
        else:
            if decision != "auto_deny":
                await bus.publish(
                    PermissionDeniedEvent(
                        run_id=run_id,
                        tool_use_id=tool_call.id,
                        decision=decision,
                        ts=_now(),
                    )
                )
            if recovery_store is not None:
                await publish_started()
            return await _fail(
                bus,
                run_id,
                tool_call,
                "permission_denied",
                "Permission denied by user. You may not execute this command. "
                "Try an alternative approach or ask the user what to do.",
                elapsed(),
            )

    if recovery_store is not None:
        assert input_hash is not None
        claim = await recovery_store.claim_tool(
            session_id=session_id,
            run_id=run_id,
            tool_use_id=tool_call.id,
            tool_name=tool_call.name,
            input_hash=input_hash,
        )
        if claim.action == "reuse":
            return _deserialize_result(claim.record.serialized_result)
        if claim.action in ("in_progress", "outcome_unknown"):
            raise ToolOutcomeUnknownError(
                f"Tool invocation {tool_call.id!r} has no safely replayable outcome."
            )
        await publish_started()

    async def persist(result: ToolResult) -> ToolResult:
        if recovery_store is not None:
            assert input_hash is not None
            await recovery_store.complete_tool(
                session_id=session_id,
                run_id=run_id,
                tool_use_id=tool_call.id,
                tool_name=tool_call.name,
                input_hash=input_hash,
                serialized_result=_serialize_result(result),
            )
        return result

    async def fail_outcome_unknown(
        error_class: str,
        error_message: str,
        *,
        attempt: int,
    ) -> None:
        assert recovery_store is not None
        assert input_hash is not None
        await recovery_store.mark_tool_outcome_unknown(
            session_id=session_id,
            run_id=run_id,
            tool_use_id=tool_call.id,
            tool_name=tool_call.name,
            input_hash=input_hash,
        )
        await _fail(
            bus,
            run_id,
            tool_call,
            error_class,
            error_message,
            elapsed(),
            attempt=attempt,
        )
        raise ToolOutcomeUnknownError(
            f"Tool invocation {tool_call.id!r} has no safely replayable outcome."
        )

    # Remote runtime owns independent queue/provision/command/cleanup deadlines.
    # Its first authorized call cannot fit the historical Local 120s wrapper.
    # Provisioning retries stay inside the backend, before command dispatch.
    remote = tool.remote_execution
    for attempt in range(1, (1 if remote else _MAX_RETRIES + 1) + 1):
        error_class: str | None = None
        error_message: str | None = None
        result: ToolResult | None = None

        try:
            result = await asyncio.wait_for(
                tool.invoke_with_context(
                    dict(tool_call.input),
                    ToolInvocationContext(
                        run_id=run_id,
                        tool_call_id=tool_call.id,
                        attempt=attempt,
                        session_id=session_id,
                        event_sink=bus.publish,
                    ),
                ),
                timeout=None if remote else timeout,
            )
        except RateLimitedError as exc:
            error_class = "rate_limited"
            error_message = str(exc)
        except TimeoutError:
            if recovery_store is not None:
                await fail_outcome_unknown(
                    "timeout",
                    f"tool timed out after {timeout}s",
                    attempt=attempt,
                )
            return await _fail(
                bus,
                run_id,
                tool_call,
                "timeout",
                f"tool timed out after {timeout}s",
                elapsed(),
                attempt=attempt,
                before_publish=persist,
            )
        except Exception as exc:
            if remote:
                from agent_runtime.core.sandbox.models import SandboxError

                error_class = exc.code if isinstance(exc, SandboxError) else "sandbox_error"
                error_message = (
                    str(exc) if isinstance(exc, SandboxError) else "Sandbox operation failed"
                )
            else:
                error_class = "runtime_error"
                error_message = str(exc)
        else:
            ms = elapsed()
            if result.is_error:
                error_class = result.error_type or "runtime_error"
                error_message = result.content
            else:
                # The retry boundary ends with the external invocation. Once a
                # side effect may have occurred, journal or event-sink failures
                # must propagate instead of dispatching the tool again.
                result = await persist(result)
                await bus.publish(
                    ToolCallFinishedEvent(
                        run_id=run_id,
                        tool_use_id=tool_call.id,
                        tool_name=tool_call.name,
                        elapsed_ms=ms,
                        output=result.content,
                        ts=_now(),
                    )
                )
                return result

        assert error_class is not None and error_message is not None
        ms = elapsed()

        if recovery_store is not None and error_class in _RETRYABLE:
            if result is not None and result.is_error:
                return await _fail(
                    bus,
                    run_id,
                    tool_call,
                    error_class,
                    error_message,
                    ms,
                    attempt=attempt,
                    before_publish=persist,
                )
            await fail_outcome_unknown(
                error_class,
                error_message,
                attempt=attempt,
            )

        if not remote and error_class in _RETRYABLE and attempt <= _MAX_RETRIES:
            await bus.publish(
                ToolCallFailedEvent(
                    run_id=run_id,
                    tool_use_id=tool_call.id,
                    tool_name=tool_call.name,
                    error_class=error_class,
                    error_message=error_message,
                    elapsed_ms=ms,
                    attempt=attempt,
                    ts=_now(),
                )
            )
            await asyncio.sleep(_RETRY_BASE_S * (2 ** (attempt - 1)))
            continue

        return await _fail(
            bus,
            run_id,
            tool_call,
            error_class,
            error_message,
            ms,
            attempt=attempt,
            before_publish=persist,
        )

    # unreachable, but keeps mypy happy
    return ToolResult(content="internal error", is_error=True, error_type="runtime_error")
