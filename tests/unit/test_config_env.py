from __future__ import annotations

from pathlib import Path

import pytest

from agent_runtime.core.config import (
    RuntimeConfig,
    get_config,
    resolve_data_root,
    resolve_graph_checkpoint_path,
)

_GRAPH_ENV_NAMES = (
    "AGENTRT_GRAPH_RECURSION_LIMIT",
    "AGENTRT_GRAPH_TOOL_CALL_BUDGET",
    "AGENTRT_GRAPH_WALL_TIME_S",
    "AGENTRT_GRAPH_TRACE_EVENT_LIMIT",
    "AGENTRT_GRAPH_CHECKPOINT_BACKEND",
    "AGENTRT_GRAPH_CHECKPOINT_PATH",
)


def _write_env(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8")


# 功能：验证 .env 文件中的值被正确加载并覆盖内建默认值
# 设计：写 .env 到临时目录并 chdir 进去，清除同名系统环境变量排除干扰，确认 .env 加载路径有效
def test_dotenv_base_loaded(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    env_file = tmp_path / ".env"
    _write_env(env_file, "AGENTRT_PORT=9999\n")
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("AGENTRT_PORT", raising=False)

    cfg = get_config()

    assert cfg.port == 9999


# 功能：验证系统环境变量的优先级高于 .env 文件中的值
# 设计：.env 写 9999，系统环境变量写 8888，确认最终值为 8888，对应四级优先链的顶层约束
def test_system_env_overrides_dotenv(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    env_file = tmp_path / ".env"
    _write_env(env_file, "AGENTRT_PORT=9999\n")
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("AGENTRT_PORT", "8888")

    cfg = get_config()

    assert cfg.port == 8888


# 功能：验证 .env 文件不存在时静默跳过，使用内建默认值（不抛异常）
# 设计：chdir 到空目录，清除系统环境变量，确认 get_config() 不因 .env 缺失而崩溃，默认端口为 7437
def test_missing_env_file_silent(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("AGENTRT_PORT", raising=False)

    cfg = get_config()

    assert cfg.port == 7437


# 功能：验证 .env 中设置的 AGENTRT_CONFIG 能正确影响 TOML 配置文件的加载路径
# 设计：.env 指向自定义 TOML 文件，TOML 中写入不同端口，确认 .env 在 TOML 加载前被读取（优先级链的正确顺序）
def test_dotenv_before_toml_agentrt_config(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    toml_path = tmp_path / "custom.toml"
    toml_path.write_bytes(b"[core]\nport = 5555\n")

    env_file = tmp_path / ".env"
    _write_env(env_file, f"AGENTRT_CONFIG={toml_path}\n")
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("AGENTRT_CONFIG", raising=False)
    monkeypatch.delenv("AGENTRT_PORT", raising=False)

    cfg = get_config()

    assert cfg.port == 5555


# 功能：验证同一变量经过完整四级优先链后，最终值为最高优先级来源（系统环境变量）
# 设计：同时设置默认值(7437)/TOML(6000)/.env(7000)/系统环境变量(8000)，确认最终值为 8000，是优先级链的综合正确性验证
def test_priority_chain_full(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    # 默认值：7437
    # TOML：6000
    # .env：7000
    # 系统环境变量：8000（最高）
    toml_path = tmp_path / "agentrt.toml"
    toml_path.write_bytes(b"[core]\nport = 6000\n")

    env_file = tmp_path / ".env"
    _write_env(env_file, "AGENTRT_PORT=7000\n")

    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("AGENTRT_CONFIG", str(toml_path))
    monkeypatch.setenv("AGENTRT_PORT", "8000")

    cfg = get_config()

    assert cfg.port == 8000


def test_graph_config_defaults() -> None:
    graph = RuntimeConfig().graph

    assert graph.recursion_limit is None
    assert graph.tool_call_budget == 64
    assert graph.wall_time_s == 300.0
    assert graph.trace_event_limit == 64
    assert graph.checkpoint_backend == "memory"
    assert graph.checkpoint_path is None


def test_sqlite_checkpoint_path_is_resolved_inside_data_root(tmp_path: Path) -> None:
    data_root = tmp_path / "daemon-data"
    config = RuntimeConfig(data_root=str(data_root))
    config.graph.checkpoint_backend = "sqlite"
    config.graph.checkpoint_path = "state/checkpoints.sqlite3"

    resolved_root = resolve_data_root(config)
    checkpoint_path = resolve_graph_checkpoint_path(config, resolved_root)

    assert checkpoint_path == (data_root / "state" / "checkpoints.sqlite3").resolve()


def test_sqlite_checkpoint_path_cannot_escape_data_root(tmp_path: Path) -> None:
    data_root = tmp_path / "daemon-data"
    config = RuntimeConfig(data_root=str(data_root))
    config.graph.checkpoint_backend = "sqlite"
    config.graph.checkpoint_path = str(tmp_path / "outside.sqlite3")

    with pytest.raises(SystemExit, match="must resolve inside core.data_root"):
        resolve_graph_checkpoint_path(config, resolve_data_root(config))


def test_data_root_and_sqlite_backend_load_from_environment(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_root = tmp_path / "daemon-data"
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("AGENTRT_CONFIG", raising=False)
    monkeypatch.setenv("AGENTRT_DATA_ROOT", str(data_root))
    monkeypatch.setenv("AGENTRT_GRAPH_CHECKPOINT_BACKEND", "sqlite")
    monkeypatch.setenv("AGENTRT_GRAPH_CHECKPOINT_PATH", "checkpoint.sqlite3")

    config = get_config()
    resolved_root = resolve_data_root(config)

    assert resolved_root == data_root.resolve()
    assert config.graph.checkpoint_backend == "sqlite"
    assert (
        resolve_graph_checkpoint_path(config, resolved_root)
        == (data_root / "checkpoint.sqlite3").resolve()
    )
    assert Path(config.logging.file) == data_root / "logs" / "core.log"
    assert Path(config.trace.file) == data_root / "traces" / "daemon.jsonl"


def test_graph_config_loads_toml_and_environment_override(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config_path = tmp_path / "graph.toml"
    config_path.write_bytes(
        b"""[graph]
recursion_limit = 21
tool_call_budget = 22
wall_time_s = 23.5
trace_event_limit = 24
"""
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("AGENTRT_CONFIG", str(config_path))
    monkeypatch.setenv("AGENTRT_GRAPH_RECURSION_LIMIT", "31")
    monkeypatch.setenv("AGENTRT_GRAPH_TOOL_CALL_BUDGET", "32")
    monkeypatch.setenv("AGENTRT_GRAPH_WALL_TIME_S", "33.5")
    monkeypatch.setenv("AGENTRT_GRAPH_TRACE_EVENT_LIMIT", "34")

    graph = get_config().graph

    assert graph.recursion_limit == 31
    assert graph.tool_call_budget == 32
    assert graph.wall_time_s == 33.5
    assert graph.trace_event_limit == 34


@pytest.mark.parametrize(
    "entry",
    [
        "recursion_limit = 0",
        "tool_call_budget = -1",
        "wall_time_s = 0.0",
        "trace_event_limit = true",
    ],
)
def test_graph_toml_requires_strictly_positive_non_boolean_values(
    entry: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config_path = tmp_path / "invalid-graph.toml"
    config_path.write_text(f"[graph]\n{entry}\n", encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("AGENTRT_CONFIG", str(config_path))
    for name in _GRAPH_ENV_NAMES:
        monkeypatch.delenv(name, raising=False)

    with pytest.raises(SystemExit, match=r"graph\..*positive"):
        get_config()


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("AGENTRT_GRAPH_RECURSION_LIMIT", "0"),
        ("AGENTRT_GRAPH_TOOL_CALL_BUDGET", "-1"),
        ("AGENTRT_GRAPH_WALL_TIME_S", "0"),
        ("AGENTRT_GRAPH_TRACE_EVENT_LIMIT", "false"),
    ],
)
def test_graph_environment_requires_strictly_positive_values(
    name: str,
    value: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("AGENTRT_CONFIG", raising=False)
    for env_name in _GRAPH_ENV_NAMES:
        monkeypatch.delenv(env_name, raising=False)
    monkeypatch.setenv(name, value)

    with pytest.raises(SystemExit, match="Config error"):
        get_config()
