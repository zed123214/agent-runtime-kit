from __future__ import annotations

import os
import tomllib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal, cast

from dotenv import load_dotenv

_DEFAULT_HOST = "127.0.0.1"
_DEFAULT_PORT = 7437
_DEFAULT_LOG_LEVEL = "INFO"
_DEFAULT_LOG_FILE = "~/.agentrt/logs/core.log"
_DEFAULT_LOG_FORMAT = "text"
_DEFAULT_CONFIG_PATH = "~/.agentrt/config.toml"
_DEFAULT_MAX_STEPS = 20
_DEFAULT_MODEL = "claude-sonnet-4-6"
_DEFAULT_TRACE_FILE = "~/.agentrt/traces/daemon.jsonl"
_DEFAULT_DATA_ROOT = "~/.agentrt"
_DEFAULT_SQLITE_CHECKPOINT_FILE = "graph-checkpoints.sqlite3"

type EngineName = Literal["loop", "graph"]
type CheckpointBackend = Literal["memory", "sqlite"]


@dataclass
class LoggingConfig:
    level: str = _DEFAULT_LOG_LEVEL
    file: str = _DEFAULT_LOG_FILE
    format: str = _DEFAULT_LOG_FORMAT  # "text" | "json"


@dataclass
class AgentConfig:
    max_steps: int = _DEFAULT_MAX_STEPS
    engine: EngineName = "loop"


@dataclass
class GraphConfig:
    recursion_limit: int | None = None
    tool_call_budget: int = 64
    wall_time_s: float = 300.0
    trace_event_limit: int = 64
    checkpoint_backend: CheckpointBackend = "memory"
    checkpoint_path: str | None = None


@dataclass
class LlmConfig:
    default_model: str = _DEFAULT_MODEL
    router: str = "static"  # "static" | "rule_based" (S4) | "cost_budget" (S6)


@dataclass
class TraceConfig:
    enabled: bool = True
    file: str = _DEFAULT_TRACE_FILE
    include_llm_payload: bool = True  # false 时 LLM 记录只保留摘要


@dataclass
class PermissionConfig:
    timeout_s: float = 60.0  # 审批超时秒数；0 表示不超时


@dataclass
class CompactionConfig:
    auto_threshold: float = 0.0  # context_pct 触发自动压缩的阈值（0 表示禁用，推荐用手动 /compact）
    tool_result_limit: int = 8_000  # tool_result 截断触发字符数
    tool_result_keep: int = 4_000  # 截断后保留的前缀字符数


@dataclass
class McpServerConfig:
    name: str
    transport: str = "stdio"  # "stdio" | "tcp"
    command: str = ""  # stdio 专用：可执行文件路径
    args: list[str] = field(default_factory=list)
    env: dict[str, str] = field(default_factory=dict)
    host: str = "localhost"  # tcp 专用
    port: int = 3000  # tcp 专用


@dataclass
class McpConfig:
    servers: list[McpServerConfig] = field(default_factory=list)


@dataclass
class RuntimeConfig:
    host: str = _DEFAULT_HOST
    port: int = _DEFAULT_PORT
    data_root: str = _DEFAULT_DATA_ROOT
    logging: LoggingConfig = field(default_factory=LoggingConfig)
    agent: AgentConfig = field(default_factory=AgentConfig)
    graph: GraphConfig = field(default_factory=GraphConfig)
    llm: LlmConfig = field(default_factory=LlmConfig)
    trace: TraceConfig = field(default_factory=TraceConfig)
    permission: PermissionConfig = field(default_factory=PermissionConfig)
    compaction: CompactionConfig = field(default_factory=CompactionConfig)
    mcp: McpConfig = field(default_factory=McpConfig)


# 构建并返回运行时配置：默认值 → 全局 TOML → 项目本地 TOML → .env → 系统环境变量（后者优先级最高）
def get_config() -> RuntimeConfig:
    config = RuntimeConfig()

    # .env 必须在读取 AGENTRT_CONFIG 之前加载，以便 .env 中的 AGENTRT_CONFIG 能影响 TOML 路径
    load_dotenv(".env", override=False)

    # 若显式指定 AGENTRT_CONFIG，只读该文件；否则按优先级叠加：全局 → 项目本地
    explicit = os.environ.get("AGENTRT_CONFIG")
    if explicit:
        config_paths = [Path(explicit).expanduser()]
    else:
        config_paths = [
            Path(_DEFAULT_CONFIG_PATH).expanduser(),
            Path(".agentrt/config.toml"),
        ]

    for config_path in config_paths:
        if config_path.exists():
            try:
                with open(config_path, "rb") as f:
                    data = tomllib.load(f)
            except tomllib.TOMLDecodeError as e:
                raise SystemExit(f"Config parse error ({config_path}): {e}") from e
            _apply_toml(config, data)

    _apply_env(config)
    data_root = Path(config.data_root).expanduser()
    # A custom data root defines an isolated daemon instance.  Keep explicitly
    # configured observability paths intact, but scope the built-in defaults to
    # that same root so two offline daemons cannot share logs or traces.
    if config.logging.file == _DEFAULT_LOG_FILE:
        config.logging.file = str(data_root / "logs" / "core.log")
    if config.trace.file == _DEFAULT_TRACE_FILE:
        config.trace.file = str(data_root / "traces" / "daemon.jsonl")
    return config


def resolve_data_root(config: RuntimeConfig) -> Path:
    """Resolve the daemon-owned data root used for all durable state."""

    if not config.data_root.strip():
        raise SystemExit("Config error: core.data_root must be a non-empty path")
    try:
        return Path(config.data_root).expanduser().resolve()
    except (OSError, RuntimeError) as exc:
        raise SystemExit("Config error: core.data_root could not be resolved") from exc


def resolve_graph_checkpoint_path(config: RuntimeConfig, data_root: Path) -> Path | None:
    """Resolve the configured SQLite file while confining it to ``data_root``."""

    if config.graph.checkpoint_backend == "memory":
        return None

    configured = config.graph.checkpoint_path
    if configured is not None and not configured.strip():
        raise SystemExit("Config error: graph.checkpoint_path must be a non-empty path")
    candidate = Path(configured or _DEFAULT_SQLITE_CHECKPOINT_FILE).expanduser()
    if not candidate.is_absolute():
        candidate = data_root / candidate
    try:
        resolved_root = data_root.resolve()
        resolved = candidate.resolve()
    except (OSError, RuntimeError) as exc:
        raise SystemExit("Config error: graph.checkpoint_path could not be resolved") from exc
    if resolved == resolved_root or not resolved.is_relative_to(resolved_root):
        raise SystemExit("Config error: graph.checkpoint_path must resolve inside core.data_root")
    return resolved


# 将已解析的 TOML 根表写入 config；未知小节或类型错误时退出进程
def _apply_toml(config: RuntimeConfig, data: dict[str, Any]) -> None:
    unknown = set(data.keys()) - {
        "core",
        "logging",
        "agent",
        "graph",
        "llm",
        "trace",
        "permission",
        "compaction",
        "mcp",
    }
    if unknown:
        raise SystemExit(f"Unknown top-level config keys: {', '.join(sorted(unknown))}")

    if "core" in data:
        core = data["core"]
        if not isinstance(core, dict):
            raise SystemExit("Config error: [core] must be a table")
        unknown_core: set[str] = set(core.keys()) - {"host", "port", "data_root"}
        if unknown_core:
            raise SystemExit(f"Unknown [core] keys: {', '.join(sorted(unknown_core))}")
        if "host" in core:
            val = core["host"]
            if not isinstance(val, str):
                raise SystemExit("Config error: core.host must be a string")
            config.host = val
        if "port" in core:
            val = core["port"]
            if not isinstance(val, int):
                raise SystemExit("Config error: core.port must be an integer")
            config.port = val
        if "data_root" in core:
            val = core["data_root"]
            if not isinstance(val, str) or not val.strip():
                raise SystemExit("Config error: core.data_root must be a non-empty string")
            config.data_root = val

    if "logging" in data:
        log = data["logging"]
        if not isinstance(log, dict):
            raise SystemExit("Config error: [logging] must be a table")
        unknown_log: set[str] = set(log.keys()) - {"level", "file", "format"}
        if unknown_log:
            raise SystemExit(f"Unknown [logging] keys: {', '.join(sorted(unknown_log))}")
        for key in ("level", "file", "format"):
            if key in log:
                val = log[key]
                if not isinstance(val, str):
                    raise SystemExit(f"Config error: logging.{key} must be a string")
                setattr(config.logging, key, val)

    if "agent" in data:
        agent = data["agent"]
        if not isinstance(agent, dict):
            raise SystemExit("Config error: [agent] must be a table")
        unknown_agent: set[str] = set(agent.keys()) - {"max_steps", "engine"}
        if unknown_agent:
            raise SystemExit(f"Unknown [agent] keys: {', '.join(sorted(unknown_agent))}")
        if "max_steps" in agent:
            val = agent["max_steps"]
            if not isinstance(val, int) or val <= 0:
                raise SystemExit("Config error: agent.max_steps must be a positive integer")
            config.agent.max_steps = val
        if "engine" in agent:
            val = agent["engine"]
            if not isinstance(val, str) or val not in ("loop", "graph"):
                raise SystemExit("Config error: agent.engine must be 'loop' or 'graph'")
            config.agent.engine = cast(EngineName, val)

    if "graph" in data:
        graph = data["graph"]
        if not isinstance(graph, dict):
            raise SystemExit("Config error: [graph] must be a table")
        unknown_graph: set[str] = set(graph.keys()) - {
            "recursion_limit",
            "tool_call_budget",
            "wall_time_s",
            "trace_event_limit",
            "checkpoint_backend",
            "checkpoint_path",
        }
        if unknown_graph:
            raise SystemExit(f"Unknown [graph] keys: {', '.join(sorted(unknown_graph))}")

        for key in ("recursion_limit", "tool_call_budget", "trace_event_limit"):
            if key not in graph:
                continue
            val = graph[key]
            if isinstance(val, bool) or not isinstance(val, int) or val <= 0:
                raise SystemExit(f"Config error: graph.{key} must be a positive integer")
            setattr(config.graph, key, val)

        if "wall_time_s" in graph:
            val = graph["wall_time_s"]
            if isinstance(val, bool) or not isinstance(val, (int, float)) or not val > 0:
                raise SystemExit("Config error: graph.wall_time_s must be a positive number")
            config.graph.wall_time_s = float(val)

        if "checkpoint_backend" in graph:
            val = graph["checkpoint_backend"]
            if not isinstance(val, str) or val not in ("memory", "sqlite"):
                raise SystemExit(
                    "Config error: graph.checkpoint_backend must be 'memory' or 'sqlite'"
                )
            config.graph.checkpoint_backend = cast(CheckpointBackend, val)

        if "checkpoint_path" in graph:
            val = graph["checkpoint_path"]
            if not isinstance(val, str) or not val.strip():
                raise SystemExit("Config error: graph.checkpoint_path must be a non-empty string")
            config.graph.checkpoint_path = val

    if "llm" in data:
        llm = data["llm"]
        if not isinstance(llm, dict):
            raise SystemExit("Config error: [llm] must be a table")
        unknown_llm: set[str] = set(llm.keys()) - {"default_model", "router"}
        if unknown_llm:
            raise SystemExit(f"Unknown [llm] keys: {', '.join(sorted(unknown_llm))}")
        if "default_model" in llm:
            val = llm["default_model"]
            if not isinstance(val, str):
                raise SystemExit("Config error: llm.default_model must be a string")
            config.llm.default_model = val
        if "router" in llm:
            val = llm["router"]
            if not isinstance(val, str):
                raise SystemExit("Config error: llm.router must be a string")
            config.llm.router = val

    if "trace" in data:
        trace = data["trace"]
        if not isinstance(trace, dict):
            raise SystemExit("Config error: [trace] must be a table")
        unknown_trace: set[str] = set(trace.keys()) - {"enabled", "file", "include_llm_payload"}
        if unknown_trace:
            raise SystemExit(f"Unknown [trace] keys: {', '.join(sorted(unknown_trace))}")
        if "enabled" in trace:
            val = trace["enabled"]
            if not isinstance(val, bool):
                raise SystemExit("Config error: trace.enabled must be a boolean")
            config.trace.enabled = val
        if "file" in trace:
            val = trace["file"]
            if not isinstance(val, str):
                raise SystemExit("Config error: trace.file must be a string")
            config.trace.file = val
        if "include_llm_payload" in trace:
            val = trace["include_llm_payload"]
            if not isinstance(val, bool):
                raise SystemExit("Config error: trace.include_llm_payload must be a boolean")
            config.trace.include_llm_payload = val

    if "permission" in data:
        perm = data["permission"]
        if not isinstance(perm, dict):
            raise SystemExit("Config error: [permission] must be a table")
        unknown_perm: set[str] = set(perm.keys()) - {"timeout_s"}
        if unknown_perm:
            raise SystemExit(f"Unknown [permission] keys: {', '.join(sorted(unknown_perm))}")
        if "timeout_s" in perm:
            val = perm["timeout_s"]
            if not isinstance(val, (int, float)) or val < 0:
                raise SystemExit("Config error: permission.timeout_s must be a non-negative number")
            config.permission.timeout_s = float(val)

    if "compaction" in data:
        comp = data["compaction"]
        if not isinstance(comp, dict):
            raise SystemExit("Config error: [compaction] must be a table")
        unknown_comp: set[str] = set(comp.keys()) - {
            "auto_threshold",
            "tool_result_limit",
            "tool_result_keep",
        }
        if unknown_comp:
            raise SystemExit(f"Unknown [compaction] keys: {', '.join(sorted(unknown_comp))}")
        if "auto_threshold" in comp:
            val = comp["auto_threshold"]
            if not isinstance(val, (int, float)) or not (0.0 <= val <= 1.0):
                raise SystemExit("Config error: compaction.auto_threshold must be between 0 and 1")
            config.compaction.auto_threshold = float(val)
        if "tool_result_limit" in comp:
            val = comp["tool_result_limit"]
            if not isinstance(val, int) or val <= 0:
                raise SystemExit(
                    "Config error: compaction.tool_result_limit must be a positive integer"
                )
            config.compaction.tool_result_limit = val
        if "tool_result_keep" in comp:
            val = comp["tool_result_keep"]
            if not isinstance(val, int) or val <= 0:
                raise SystemExit(
                    "Config error: compaction.tool_result_keep must be a positive integer"
                )
            config.compaction.tool_result_keep = val

    if "mcp" in data:
        mcp = data["mcp"]
        if not isinstance(mcp, dict):
            raise SystemExit("Config error: [mcp] must be a table")
        unknown_mcp: set[str] = set(mcp.keys()) - {"servers"}
        if unknown_mcp:
            raise SystemExit(f"Unknown [mcp] keys: {', '.join(sorted(unknown_mcp))}")
        servers_raw = mcp.get("servers", [])
        if not isinstance(servers_raw, list):
            raise SystemExit("Config error: mcp.servers must be an array of tables")
        for i, srv in enumerate(servers_raw):
            if not isinstance(srv, dict):
                raise SystemExit(f"Config error: mcp.servers[{i}] must be a table")
            name = srv.get("name")
            if not isinstance(name, str) or not name:
                raise SystemExit(f"Config error: mcp.servers[{i}].name must be a non-empty string")
            transport = srv.get("transport", "stdio")
            if transport not in ("stdio", "tcp"):
                raise SystemExit(
                    f"Config error: mcp.servers[{i}].transport must be 'stdio' or 'tcp'"
                )
            s = McpServerConfig(name=name, transport=transport)
            if "command" in srv:
                val = srv["command"]
                if not isinstance(val, str):
                    raise SystemExit(f"Config error: mcp.servers[{i}].command must be a string")
                s.command = val
            if "args" in srv:
                val = srv["args"]
                if not isinstance(val, list):
                    raise SystemExit(f"Config error: mcp.servers[{i}].args must be an array")
                s.args = [str(a) for a in val]
            if "env" in srv:
                val = srv["env"]
                if not isinstance(val, dict):
                    raise SystemExit(f"Config error: mcp.servers[{i}].env must be a table")
                s.env = {str(k): str(v) for k, v in val.items()}
            if "host" in srv:
                val = srv["host"]
                if not isinstance(val, str):
                    raise SystemExit(f"Config error: mcp.servers[{i}].host must be a string")
                s.host = val
            if "port" in srv:
                val = srv["port"]
                if not isinstance(val, int):
                    raise SystemExit(f"Config error: mcp.servers[{i}].port must be an integer")
                s.port = val
            config.mcp.servers.append(s)


# 用 AGENTRT_* 环境变量覆盖 config 中对应字段（若变量已设置）
def _apply_env(config: RuntimeConfig) -> None:
    host = os.environ.get("AGENTRT_HOST")
    if host is not None:
        config.host = host

    data_root = os.environ.get("AGENTRT_DATA_ROOT")
    if data_root is not None:
        if not data_root.strip():
            raise SystemExit("Config error: AGENTRT_DATA_ROOT must be a non-empty path")
        config.data_root = data_root

    port_str = os.environ.get("AGENTRT_PORT")
    if port_str is not None:
        try:
            config.port = int(port_str)
        except ValueError:
            raise SystemExit(f"Config error: AGENTRT_PORT must be an integer, got: {port_str!r}")

    log_level = os.environ.get("AGENTRT_LOG_LEVEL")
    if log_level is not None:
        config.logging.level = log_level

    log_file = os.environ.get("AGENTRT_LOG_FILE")
    if log_file is not None:
        config.logging.file = log_file

    log_format = os.environ.get("AGENTRT_LOG_FORMAT")
    if log_format is not None:
        config.logging.format = log_format

    max_steps_str = os.environ.get("AGENTRT_MAX_STEPS")
    if max_steps_str is not None:
        try:
            val = int(max_steps_str)
            if val <= 0:
                raise SystemExit(
                    "Config error: AGENTRT_MAX_STEPS must be a positive integer,"
                    f" got: {max_steps_str!r}"
                )
            config.agent.max_steps = val
        except ValueError:
            raise SystemExit(
                f"Config error: AGENTRT_MAX_STEPS must be an integer, got: {max_steps_str!r}"
            )

    engine = os.environ.get("AGENTRT_ENGINE")
    if engine is not None:
        if engine not in ("loop", "graph"):
            raise SystemExit("Config error: AGENTRT_ENGINE must be 'loop' or 'graph'")
        config.agent.engine = cast(EngineName, engine)

    checkpoint_backend = os.environ.get("AGENTRT_GRAPH_CHECKPOINT_BACKEND")
    if checkpoint_backend is not None:
        if checkpoint_backend not in ("memory", "sqlite"):
            raise SystemExit(
                "Config error: AGENTRT_GRAPH_CHECKPOINT_BACKEND must be 'memory' or 'sqlite'"
            )
        config.graph.checkpoint_backend = cast(CheckpointBackend, checkpoint_backend)

    checkpoint_path = os.environ.get("AGENTRT_GRAPH_CHECKPOINT_PATH")
    if checkpoint_path is not None:
        if not checkpoint_path.strip():
            raise SystemExit("Config error: AGENTRT_GRAPH_CHECKPOINT_PATH must be a non-empty path")
        config.graph.checkpoint_path = checkpoint_path

    graph_recursion_limit = os.environ.get("AGENTRT_GRAPH_RECURSION_LIMIT")
    if graph_recursion_limit is not None:
        try:
            recursion_limit_value = int(graph_recursion_limit)
        except ValueError:
            raise SystemExit(
                "Config error: AGENTRT_GRAPH_RECURSION_LIMIT must be an integer, "
                f"got: {graph_recursion_limit!r}"
            ) from None
        if recursion_limit_value <= 0:
            raise SystemExit(
                "Config error: AGENTRT_GRAPH_RECURSION_LIMIT must be a positive integer, "
                f"got: {graph_recursion_limit!r}"
            )
        config.graph.recursion_limit = recursion_limit_value

    graph_tool_call_budget = os.environ.get("AGENTRT_GRAPH_TOOL_CALL_BUDGET")
    if graph_tool_call_budget is not None:
        try:
            tool_call_budget_value = int(graph_tool_call_budget)
        except ValueError:
            raise SystemExit(
                "Config error: AGENTRT_GRAPH_TOOL_CALL_BUDGET must be an integer, "
                f"got: {graph_tool_call_budget!r}"
            ) from None
        if tool_call_budget_value <= 0:
            raise SystemExit(
                "Config error: AGENTRT_GRAPH_TOOL_CALL_BUDGET must be a positive integer, "
                f"got: {graph_tool_call_budget!r}"
            )
        config.graph.tool_call_budget = tool_call_budget_value

    graph_wall_time = os.environ.get("AGENTRT_GRAPH_WALL_TIME_S")
    if graph_wall_time is not None:
        try:
            wall_time_value = float(graph_wall_time)
        except ValueError:
            raise SystemExit(
                "Config error: AGENTRT_GRAPH_WALL_TIME_S must be a number, "
                f"got: {graph_wall_time!r}"
            ) from None
        if not wall_time_value > 0:
            raise SystemExit(
                "Config error: AGENTRT_GRAPH_WALL_TIME_S must be a positive number, "
                f"got: {graph_wall_time!r}"
            )
        config.graph.wall_time_s = wall_time_value

    graph_trace_event_limit = os.environ.get("AGENTRT_GRAPH_TRACE_EVENT_LIMIT")
    if graph_trace_event_limit is not None:
        try:
            trace_event_limit_value = int(graph_trace_event_limit)
        except ValueError:
            raise SystemExit(
                "Config error: AGENTRT_GRAPH_TRACE_EVENT_LIMIT must be an integer, "
                f"got: {graph_trace_event_limit!r}"
            ) from None
        if trace_event_limit_value <= 0:
            raise SystemExit(
                "Config error: AGENTRT_GRAPH_TRACE_EVENT_LIMIT must be a positive integer, "
                f"got: {graph_trace_event_limit!r}"
            )
        config.graph.trace_event_limit = trace_event_limit_value

    default_model = os.environ.get("AGENTRT_LLM_DEFAULT_MODEL")
    if default_model is not None:
        config.llm.default_model = default_model

    trace_enabled = os.environ.get("AGENTRT_TRACE_ENABLED")
    if trace_enabled is not None:
        config.trace.enabled = trace_enabled.lower() not in ("0", "false", "no")

    trace_file = os.environ.get("AGENTRT_TRACE_FILE")
    if trace_file is not None:
        config.trace.file = trace_file

    trace_payload = os.environ.get("AGENTRT_TRACE_INCLUDE_LLM_PAYLOAD")
    if trace_payload is not None:
        config.trace.include_llm_payload = trace_payload.lower() not in ("0", "false", "no")

    perm_timeout = os.environ.get("AGENTRT_PERMISSION_TIMEOUT_S")
    if perm_timeout is not None:
        try:
            perm_timeout_val = float(perm_timeout)
            if perm_timeout_val < 0:
                raise SystemExit(
                    "Config error: AGENTRT_PERMISSION_TIMEOUT_S must be >= 0, "
                    f"got: {perm_timeout!r}"
                )
            config.permission.timeout_s = perm_timeout_val
        except ValueError:
            raise SystemExit(
                "Config error: AGENTRT_PERMISSION_TIMEOUT_S must be a number, "
                f"got: {perm_timeout!r}"
            )

    compact_threshold = os.environ.get("AGENTRT_COMPACT_THRESHOLD")
    if compact_threshold is not None:
        try:
            compact_threshold_val = float(compact_threshold)
            if not (0.0 <= compact_threshold_val <= 1.0):
                raise SystemExit(
                    "Config error: AGENTRT_COMPACT_THRESHOLD must be between 0 and 1, "
                    f"got: {compact_threshold!r}"
                )
            config.compaction.auto_threshold = compact_threshold_val
        except ValueError:
            raise SystemExit(
                "Config error: AGENTRT_COMPACT_THRESHOLD must be a number, "
                f"got: {compact_threshold!r}"
            )

    compact_tool_limit = os.environ.get("AGENTRT_COMPACT_TOOL_LIMIT")
    if compact_tool_limit is not None:
        try:
            compact_tool_limit_val = int(compact_tool_limit)
            if compact_tool_limit_val <= 0:
                raise SystemExit(
                    "Config error: AGENTRT_COMPACT_TOOL_LIMIT must be a positive "
                    f"integer, got: {compact_tool_limit!r}"
                )
            config.compaction.tool_result_limit = compact_tool_limit_val
        except ValueError:
            raise SystemExit(
                "Config error: AGENTRT_COMPACT_TOOL_LIMIT must be an integer, "
                f"got: {compact_tool_limit!r}"
            )

    compact_tool_keep = os.environ.get("AGENTRT_COMPACT_TOOL_KEEP")
    if compact_tool_keep is not None:
        try:
            compact_tool_keep_val = int(compact_tool_keep)
            if compact_tool_keep_val <= 0:
                raise SystemExit(
                    "Config error: AGENTRT_COMPACT_TOOL_KEEP must be a positive "
                    f"integer, got: {compact_tool_keep!r}"
                )
            config.compaction.tool_result_keep = compact_tool_keep_val
        except ValueError:
            raise SystemExit(
                "Config error: AGENTRT_COMPACT_TOOL_KEEP must be an integer, "
                f"got: {compact_tool_keep!r}"
            )
