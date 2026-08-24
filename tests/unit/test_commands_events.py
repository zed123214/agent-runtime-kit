from __future__ import annotations

import subprocess
import sys

import pytest
from pydantic import ValidationError

from agent_runtime.core.bus.commands import (
    EventSubscribeCommand,
    PermissionRespondCommand,
    PingCommand,
    PongResult,
    RunGetStateCommand,
    SessionResumeCommand,
    SessionResumeResult,
)
from agent_runtime.core.bus.events import CoreStartedEvent


def test_engine_base_and_command_models_are_cold_importable_together() -> None:
    completed = subprocess.run(
        [
            sys.executable,
            "-c",
            "import agent_runtime.core.engine.base; import agent_runtime.core.bus.commands",
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0, completed.stderr


# 功能：验证 PingCommand 序列化后再反序列化，client 和 type 字段完整保留
# 设计：JSON 往返测试确认 wire 协议的序列化正确性，type 字段是 discriminated union 的判别键
def test_ping_command_roundtrip() -> None:
    cmd = PingCommand(client="cli/0.0.1")
    cmd2 = PingCommand.model_validate_json(cmd.model_dump_json())
    assert cmd2.client == "cli/0.0.1"
    assert cmd2.type == "core.ping"


# 功能：验证 PingCommand 的 type 字段默认值为 "core.ping"
# 设计：Literal 默认值测试，type 是 Command union 的判别键，必须与 union 定义完全一致，否则反序列化时会路由到错误类型
def test_ping_command_default_type() -> None:
    cmd = PingCommand(client="x")
    assert cmd.type == "core.ping"


# 功能：验证缺少必填 client 字段时 pydantic 校验失败
# 设计：传入空 dict 触发校验，确认 client 是必填字段，防止 daemon 收到不完整的 ping 命令进入 handler
def test_ping_command_missing_client_raises() -> None:
    with pytest.raises(ValidationError):
        PingCommand.model_validate({})


# 功能：验证 PongResult 序列化往返后所有字段完整保留
# 设计：与 PingCommand 对称，测试命令-响应对的两端序列化，确认 int 和 str 字段类型在往返中不变
def test_pong_result_roundtrip() -> None:
    pong = PongResult(server_version="0.0.1", uptime_ms=42, received_at="2026-05-11T00:00:00Z")
    pong2 = PongResult.model_validate(pong.model_dump())
    assert pong2.server_version == "0.0.1"
    assert pong2.uptime_ms == 42


# 功能：验证 CoreStartedEvent 序列化往返后 listen_addr 和 type 字段正确保留
# 设计：CoreStartedEvent 是 daemon 启动通知，往返测试确认 type 的 Literal 约束在反序列化后保持（不被字段名覆盖）
def test_core_started_event_roundtrip() -> None:
    evt = CoreStartedEvent(listen_addr="127.0.0.1:7437", version="0.0.1")
    evt2 = CoreStartedEvent.model_validate_json(evt.model_dump_json())
    assert evt2.listen_addr == "127.0.0.1:7437"
    assert evt2.type == "core.started"


def test_permission_respond_command_keeps_legacy_and_full_identity_compatible() -> None:
    legacy = PermissionRespondCommand(tool_use_id="tool-1", decision="allow_once")
    precise = PermissionRespondCommand(
        tool_use_id="tool-1",
        session_id="session-1",
        run_id="run-1",
        decision="deny_once",
    )

    assert legacy.session_id is None
    assert legacy.run_id is None
    assert precise.model_dump(exclude_none=True) == {
        "type": "permission.respond",
        "tool_use_id": "tool-1",
        "session_id": "session-1",
        "run_id": "run-1",
        "decision": "deny_once",
    }


def test_durable_recovery_commands_roundtrip_with_revision_and_cursor() -> None:
    resume = SessionResumeCommand(
        session_id="session-1",
        resume_token="opaque-capability",
        expected_revision="revision-1",
    )
    state = RunGetStateCommand(session_id="session-1", run_id="run-1")
    subscribe = EventSubscribeCommand(
        topics=["run.*"],
        replay_from_run="run-1",
        after_event_seq=7,
    )

    assert SessionResumeCommand.model_validate_json(resume.model_dump_json()) == resume
    assert state.type == "run.get_state"
    assert subscribe.after_event_seq == 7
    with pytest.raises(ValidationError):
        EventSubscribeCommand(topics=["run.*"], after_event_seq=-1)


def test_session_resume_result_exposes_rotated_capability() -> None:
    result = SessionResumeResult(
        session_id="session-1",
        run_id="run-1",
        status="suspended",
        suspension_reason="permission",
        checkpoint_revision="revision-1",
        event_seq=8,
        next_resume_token="opaque-next-capability",
    )

    assert result.model_dump()["next_resume_token"] == "opaque-next-capability"


def test_permission_respond_accepts_durable_interrupt_identity() -> None:
    command = PermissionRespondCommand(
        session_id="session-1",
        run_id="run-1",
        tool_use_id="tool-1",
        interrupt_id="interrupt-1",
        expected_revision="revision-1",
        decision="allow_once",
    )

    assert command.interrupt_id == "interrupt-1"
    assert command.expected_revision == "revision-1"
