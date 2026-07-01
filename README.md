# Agent Runtime Kit

面向本地自动化任务的 AI Agent 运行时框架，支持 daemon 常驻执行、类型化 IPC、实时事件流、工具权限控制、会话记忆、上下文压缩、子 Agent 以及 MCP 工具接入。

中文 | [English](README.en.md)

[![CI](https://github.com/zed123214/agent-runtime-kit/actions/workflows/ci.yml/badge.svg)](https://github.com/zed123214/agent-runtime-kit/actions/workflows/ci.yml)
[![Python](https://img.shields.io/badge/Python-3.12-blue)](https://www.python.org/)
[![Pydantic](https://img.shields.io/badge/Pydantic-v2-E92063)](https://docs.pydantic.dev/)
[![Textual](https://img.shields.io/badge/Textual-TUI-7B2CBF)](https://textual.textualize.io/)
[![License](https://img.shields.io/badge/License-MIT-yellow)](LICENSE)

## 项目定位

现代 AI Agent 不应只是一次 LLM API 调用封装。一个可用的本地 Agent 运行时需要具备：长期运行的执行进程、类型化 IPC、可观察的事件流、安全的本地工具执行、可持久化的会话记忆，以及统一的扩展模型，用于接入工具、Skills、子 Agent 和 MCP Server。

Agent Runtime Kit 使用 Python 实现这些 Agent Runtime 基础能力。当前 provider 实现基于 Anthropic 模型，但系统核心围绕 provider 边界设计：项目重点不是模型本身，而是模型外围的 daemon、通信协议、工具调用、权限审批、会话管理和事件基础设施。

## 支持的运行环境

本仓库主要面向 Linux/macOS 风格环境。由于当前运行手册、进程控制命令和 Shell 工具行为依赖 POSIX 语义，暂不将原生 Windows 作为主要运行目标。

在 Windows 机器上，建议通过 WSL2 或 Docker 进行运行实验。源码和文档仍然可以直接在 Windows 中阅读和审查。

## 系统架构

```mermaid
graph TD
    User((开发者)) --> CLI["agentrt CLI"]
    User --> TUI["agentrt-tui"]

    CLI -->|JSON-RPC 2.0 over NDJSON TCP| Core["agentrt-core daemon"]
    TUI -->|订阅 / 回放事件| Core

    subgraph Runtime["Agent Runtime"]
        Core --> Session["SessionManager\nthread.jsonl / notes.md"]
        Core --> Runner["AgentRunner"]
        Runner --> Loop["AgentLoop\n规划-执行-观察"]
        Loop --> LLM["LLM Provider\n流式输出 + usage"]
        Loop --> Registry["ToolRegistry"]
        Registry --> Permission["PermissionManager"]
        Registry --> Builtins["内置工具\nread/write/list/bash/task"]
        Registry --> MCP["MCP tools"]
        Registry --> SubAgent["Subagents"]
        Runner --> Compact["Compactor\n上下文预算"]
    end

    subgraph Observability["Observability"]
        Loop --> EventBus["EventBus"]
        Permission --> EventBus
        SubAgent --> EventBus
        EventBus --> Writer["events.jsonl"]
        EventBus --> Broadcast["IPC broadcaster"]
        Broadcast --> TUI
    end
```

## 核心能力

1. **Daemon + CLI/TUI 客户端架构**：将长期运行的 Agent 执行过程与前端生命周期解耦。CLI/TUI 可以断开连接，daemon 继续维护运行状态。
2. **类型化 IPC**：使用 Pydantic 建模请求、响应、错误和事件，并通过 JSON-RPC 2.0 over NDJSON TCP 暴露进程间通信协议。
3. **协议文档自动生成**：`WIRE_PROTOCOL.md` 从源码协议模型生成，降低手写协议文档与代码实现发生漂移的风险。
4. **ReAct 风格 AgentLoop**：运行时统一处理 LLM 流式输出、`tool_use` 解析、工具参数校验、工具执行、`tool_result` 回注、最大步数限制、取消语义和失败恢复。
5. **ToolRegistry + PermissionManager**：内置工具和 MCP 工具共享 schema 校验、权限判断、事件发布和结构化结果返回机制。
6. **Session 记忆**：完整消息历史保存到 `thread.jsonl`，经过整理的长期事实保存到 `notes.md`。
7. **上下文治理**：支持 tool result 截断、context 水位监控，以及用 compact 摘要替换过大的历史上下文。
8. **Skills、Subagents 与 MCP**：Markdown Skills、隔离上下文的子 Agent 和 MCP 工具复用同一套工具注册、权限、事件和运行器基础设施。

## 简历映射

| 简历表述 | 仓库 |
| --- | --- |
| Daemon + CLI/TUI 多进程架构 | `src/agent_runtime/core/app.py`、`src/agent_runtime/cli/`、`src/agent_runtime/tui/`、`docs/architecture.md` |
| JSON-RPC 2.0 over NDJSON TCP | `src/agent_runtime/core/bus/`、`src/agent_runtime/core/transport/`、`WIRE_PROTOCOL.md` |
| 类型安全的协议边界 | Pydantic 协议模型、strict `mypy`、自动生成的 `WIRE_PROTOCOL.md` |
| 可观察事件流 | `EventBus`、`events.jsonl`、可回放的客户端事件订阅 |
| Runtime 层工具权限控制 | `src/agent_runtime/core/permissions/`、`docs/tool-permissions.md`、`examples/permissions/` |
| 可恢复的 LLM/工具执行闭环 | `src/agent_runtime/core/loop.py`、`src/agent_runtime/core/runner.py`、单元测试和集成测试 |
| Session 记忆与上下文治理 | `src/agent_runtime/core/session/`、`src/agent_runtime/core/compact/`、`docs/session-memory.md` |
| 统一扩展模型 | `src/agent_runtime/core/skills/`、`src/agent_runtime/core/subagent/`、`src/agent_runtime/core/mcp/`、`examples/` |

## 快速开始

### 环境要求

- Linux/macOS、WSL2 或 Docker
- Python 3.12
- [uv](https://docs.astral.sh/uv/)
- 真实 LLM 运行需要配置 `ANTHROPIC_API_KEY`

### 安装

```bash
git clone https://github.com/zed123214/agent-runtime-kit.git
cd agent-runtime-kit
uv sync
```

### 配置

```bash
cp .env.example .env
```

示例：

```env
AGENTRT_HOST=127.0.0.1
AGENTRT_PORT=7437
AGENTRT_LOG_LEVEL=INFO
AGENTRT_LOG_FILE=~/.agentrt/logs/core.log
AGENTRT_LOG_FORMAT=text
# ANTHROPIC_API_KEY=sk-ant-your-key-here
# AGENTRT_LLM_DEFAULT_MODEL=claude-sonnet-4-6
# AGENTRT_MAX_STEPS=20
```

不要提交真实 API Key。请将本地密钥保存在 `.env` 或 shell 环境变量中。

### 运行

```bash
uv run agentrt-core
uv run agentrt ping
uv run agentrt run --goal "Inspect this repository and summarize the project structure"
uv run agentrt-tui
```

## 事件流示例

```json
{"type":"run.started","run_id":"20260629-101500-a1b2c3","goal":"...","ts":"..."}
{"type":"llm.token","run_id":"20260629-101500-a1b2c3","token":"I","ts":"..."}
{"type":"tool.started","run_id":"20260629-101500-a1b2c3","tool_name":"list_dir","ts":"..."}
{"type":"permission.requested","run_id":"20260629-101500-a1b2c3","tool_name":"bash","ts":"..."}
{"type":"tool.finished","run_id":"20260629-101500-a1b2c3","tool_name":"list_dir","is_error":false,"ts":"..."}
{"type":"run.finished","run_id":"20260629-101500-a1b2c3","status":"success","ts":"..."}
```

## 仓库结构

```text
agent-runtime-kit/
|-- README.md
|-- README.en.md
|-- RUNBOOK.md
|-- WIRE_PROTOCOL.md
|-- docs/
|   |-- architecture.md
|   |-- agent-loop.md
|   |-- tool-permissions.md
|   |-- session-memory.md
|   |-- skills-subagents-mcp.md
|   `-- project-highlights.md
|-- examples/
|   |-- basic_run/
|   |-- permissions/
|   |   `-- trace_permission_flow.py
|   |-- skills/
|   `-- mcp/
|-- scripts/
|   |-- generate_wire_protocol.py
|   `-- check_wire_protocol.py
|-- src/agent_runtime/
|   |-- cli/
|   |-- tui/
|   `-- core/
|       |-- app.py
|       |-- runner.py
|       |-- loop.py
|       |-- bus/
|       |-- transport/
|       |-- tools/
|       |-- permissions/
|       |-- session/
|       |-- compact/
|       |-- skills/
|       |-- subagent/
|       |-- mcp/
|       `-- trace/
`-- tests/
    |-- unit/
    `-- integration/
```

## 开发与检查

```bash
uv run ruff check src tests scripts
uv run ruff format --check src tests scripts
uv run mypy src
uv run pytest tests/ -v
uv run python scripts/check_wire_protocol.py --check
```

原生 Windows 下，如果 `uv` 脚本入口出现 trampoline path 错误，可以改用 Python 模块方式运行工具：

```bash
uv run python -m mypy src
uv run python -m pytest tests/ -v
```

修改 `src/agent_runtime/core/bus/` 下的协议模型后，重新生成协议文档：

```bash
uv run python scripts/generate_wire_protocol.py
```

## 文档

- [Architecture](docs/architecture.md)
- [Agent Loop](docs/agent-loop.md)
- [Tool Permissions](docs/tool-permissions.md)
- [Session Memory](docs/session-memory.md)
- [Skills, Subagents, and MCP](docs/skills-subagents-mcp.md)
- [Project Highlights](docs/project-highlights.md)
- [Runbook](RUNBOOK.md)
- [Wire Protocol](WIRE_PROTOCOL.md)

## 安全说明

- `.env`、日志、会话数据、缓存、虚拟环境和本地工作区不应提交到仓库。
- Shell、文件写入和外部 MCP 工具在执行前都会经过权限系统。
- 这是一个作品集和学习项目。若用于生产环境，还需要补充沙箱隔离、安全审查、资源隔离和运维加固。

## License

MIT License. See [LICENSE](LICENSE).
