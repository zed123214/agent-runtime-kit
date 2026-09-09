# Agent Runtime Kit Kubernetes Sandbox Runtime PRD

> 实施与验收基准。M0 交付执行契约及 Local 兼容；M1–M3 逐步交付 Kubernetes、隔离证据与恢复。

## 1. 文档信息

| 字段 | 内容 |
| --- | --- |
| 产品名称 | Agent Runtime Kit Sandbox Runtime |
| 定位 | Kubernetes-native Agent Code Execution Harness |
| PRD 版本 | v0.2（保留原文件名，便于已有引用） |
| 日期 | 2026-09-05 |
| 当前阶段 | M0/M1 实现及 M2 本机真实验收完成；M3 待实施。实际数据与边界见 `docs/sandbox-m2-validation.md` |
| 源码基线 | `main` / `fbb7612`，执行会话启动后记录完整 SHA 与 dirty 状态 |
| 测试基线 | 2026-09-05 Windows 现有 `.venv` 中 `python -m pytest --collect-only -q` 收集 489 条；这只是收集结果，不代表测试通过或可选依赖齐全 |
| 支持环境 | Python 3.12；运行与进程测试按仓库规范使用 Linux/macOS，Windows 使用 WSL2 或 Docker |

### 1.1 本次修订与实施约束

- 将 M0 展开为可执行任务、兼容矩阵和退出条件，统一第 12、14、15 节的阶段含义。
- 明确 Local 是宿主执行适配器，不承诺 Session 间文件、进程或网络隔离。
- 补齐调用身份如何穿过 `invoke_tool()`、懒创建、根/子 Run 所有权、取消与销毁竞态。
- 将 Kubernetes 安全配置前置到首次运行不可信命令之前；M2 负责完整验证与证据，不补装最低安全边界。
- 区分执行结果、可重试错误与未知结果，明确 Pod 丢失、TTL 回收会丢失 `emptyDir` 工作区。
- 本文的“首版/P0”指 M1+M2；M0 仅受第 12.1 节明确列出的实现范围约束。设计完成不等于 M0 完成。
- **本次执行指令（2026-09-05）**：用户要求“写完代码别测试”。M0 会话实现代码与必要测试用例，但不运行测试、lint、类型检查或验收脚本；保留验收计划，将交付状态写为“代码已实现，未测试/未验收”，不得宣称检查通过。
- **后续执行指令（2026-09-09 更新）**：用户要求修复阻断 M2 的问题后新开任务执行 M2，已进入测试与验收阶段；上述不运行检查的限制仅属于原编码交付。执行证据与环境见 `docs/sandbox-m2-handoff.md`，未经实际执行的集群指标仍不得宣称通过。

一句话定义：

> 为 Agent Runtime Kit 增加 Session 级持久沙箱，把 Agent 的命令与文件操作路由到受资源、网络和生命周期策略约束的 Kubernetes 执行环境，并为创建、执行和回收生成可关联、可回放、可核验的运行回执。

## 2. 背景与问题

Agent Runtime Kit 已经具备常驻 Core、Session、Loop/Graph 双执行引擎、参数校验、权限审批、事件日志、恢复以及子 Agent 等 Harness 能力，但工具副作用仍落在 Core 所在环境：

- `BashTool.invoke()` 通过 `asyncio.create_subprocess_shell()` 启动本机 shell；
- `ReadFileTool`、`WriteFileTool`、`ListDirTool` 直接访问 Core 当前文件系统；
- 根 Agent 与子 Agent 分别构造本地工具实例，共享的是 Python 进程而不是操作系统级执行边界；
- 当前超时和权限治理位于工具调用链，尚未形成 CPU、内存、临时存储和网络层面的统一隔离。

这使得模型生成代码的“写入 → 执行 → 观察 → 修改”闭环与 Core 的运行环境耦合。Sandbox Runtime 的目标不是替换 Agent Loop，而是把副作用执行从控制面剥离出来。

## 3. 产品目标

### 3.1 Goals

1. 定义稳定的 Sandbox 生命周期与执行契约，使 Local、Kubernetes 以及未来更强隔离后端可以复用同一工具层；共用行为与后端能力差异必须分别声明。
2. 建立 `Session → SandboxHandle → Pod UID` 的唯一映射；同一 Session 的根 Agent 和子 Agent 共享工作区。
3. 将 `bash/read_file/write_file/list_dir` 路由到 Sandbox Runtime，同时保持现有工具名称、参数校验、权限审批和 ToolResult 回写链不变。
4. 提供 CPU、内存、临时存储、命令超时、空闲 TTL、NetworkPolicy 和 Pod Security 基线。
5. 建立幂等创建、显式释放、TTL 回收和孤儿资源扫描的生命周期闭环。
6. 生成可关联 `session_id/run_id/tool_call_id/sandbox_id` 的生命周期与执行回执。
7. 在固定 kind 环境完成可复现的功能、隔离、故障和并发验收，为简历指标提供证据。

### 3.2 Non-goals（首版）

- 不开发通用 Kubernetes 调度平台、自定义调度器或自研 CRD/Operator。
- 不首发 WarmPool、快照、暂停/恢复、GPU、多集群、浏览器桌面或 Jupyter。
- 不允许模型选择任意镜像、PodSpec、ServiceAccount、挂载或 RuntimeClass。
- 不把 LLM Provider Key、kubeconfig 或 Core 环境变量注入 Sandbox Pod。
- 不改变 Agent Loop、Graph 调度、模型路由、MCP 协议或 Skill 解析语义。
- 不在首版支持 Core 多副本共同接管同一 Sandbox。
- 不在首版为 durable session 承诺跨 Core 重启的工作区恢复；该能力进入 M3。
- 不在首版自动同步宿主机项目目录、PVC 或对象存储快照；Kubernetes 工作区从空的 `/workspace` 开始。
- 不在首版暴露每个 Sandbox 的 Ingress/NodePort，也不支持 Core 在集群外直连 Sandbox Server。

## 4. 业界开源参考与取舍

下列是架构参考，不是 M0 的依赖或实现验收依据。外部仓库的移动分支链接仅作定位；如后续直接采用其实现，应记录具体 commit。本次修订核对了第 17 节 Kubernetes 官方规范，未重新审计所有参考项目的当前代码。

| 参考项目 | 参考模式 | Agent Runtime Kit 采用方式 |
| --- | --- | --- |
| [OpenHands Runtime](https://docs.openhands.dev/openhands/usage/architecture/runtime) | Agent 后端通过稳定 API 把 Action 发送给容器内执行服务，再接收 Observation | Pod 内部署轻量 Sandbox Server；Core 不直接承载 shell 和文件副作用 |
| [SWE-ReX](https://github.com/SWE-agent/SWE-ReX) | Deployment 与 Runtime 分层，同一 Agent 逻辑可切换 Local、Docker、Remote 等执行环境 | 分离 `SandboxBackend` 生命周期接口与 `SandboxRuntime` 执行接口 |
| [DeerFlow Sandbox](https://github.com/bytedance/deer-flow/blob/main/backend/packages/harness/deerflow/sandbox/sandbox_provider.py) | Provider 提供 acquire/get/release；同一 thread 使用稳定 Sandbox；K8s Provisioner 为每个 Sandbox 建立 Pod 与 ClusterIP Service | 采用 Session 级身份、生命周期 Provider 和集群内服务访问；首版不引入热池与跨实例租约 |
| [E2B Infra](https://github.com/e2b-dev/infra/blob/main/docs/ARCHITECTURE.md) | 控制面负责生命周期与路由，执行数据面负责进程和文件系统 API | Core 保存治理状态，Sandbox Server 只执行受约束操作 |
| [OpenSandbox](https://github.com/opensandbox-group/OpenSandbox/blob/main/specs/sandbox-lifecycle.yml) | 生命周期 API 与执行 API 分离，并显式建模状态、资源、网络和 TTL | 采用显式状态机与结构化 Lifecycle/Exec Receipt |
| [Kubernetes SIG Agent Sandbox](https://github.com/kubernetes-sigs/agent-sandbox) | `Sandbox` 提供稳定 Pod-backed 身份；扩展层提供 `SandboxClaim`、`SandboxTemplate`、`SandboxWarmPool` | 首版直接管理 Pod/Service；契约稳定后评估增加 `AgentSandboxBackend`，不自研同类 CRD |

设计原则是组合采用成熟模式，不复制任一项目的完整平台范围。

## 5. 当前 Agent Runtime Kit 接入点

| 当前模块 | 现有职责 | Sandbox 改造 |
| --- | --- | --- |
| [`core/tools/invocation.py`](src/agent_runtime/core/tools/invocation.py) | schema 校验、PermissionManager、超时、重试、ToolResult、事件和恢复 journal | 保持治理顺序；在权限通过后由工具访问 Sandbox Runtime |
| [`core/runner.py`](src/agent_runtime/core/runner.py) | 每次 run 组装 ToolRegistry、Provider、EventBus 和 Engine | 注入共享 `SandboxManager`，用同一 Session 的 Runtime 构造副作用工具 |
| [`core/subagent/tool.py`](src/agent_runtime/core/subagent/tool.py) | 子 Agent 独立 Run、事件持久化/桥接、子 registry | 子 Agent 继承父 Session 的 SandboxHandle，不重复创建 Pod |
| [`core/session/manager.py`](src/agent_runtime/core/session/manager.py) | Session 创建、消息执行、关闭和恢复 | 生命周期以 Session 为归属；首次副作用调用懒创建 Sandbox |
| [`core/app.py`](src/agent_runtime/core/app.py) | Core 单例依赖、SocketServer、SessionManager 和资源清理 | 创建/关闭 SandboxManager；在 session 资源清理中幂等销毁 Sandbox |
| [`core/config.py`](src/agent_runtime/core/config.py) | TOML/env 配置解析与严格未知字段检查 | 新增 `[sandbox]` 与 `[sandbox.kubernetes]` 配置模型 |
| [`core/tools/builtin/`](src/agent_runtime/core/tools/builtin/) | 本机 bash 和文件工具 | 工具 schema 保持稳定，执行委托给 SandboxRuntime |

现有 `invoke_tool()` 的执行门控包括工具解析、参数校验、已有 recovery journal 查询、权限审批、journal claim、工具执行与结果持久化。普通与 durable 模式的 `tool.call_started` 时机不同，M0 必须保留已有事件顺序，不能为了统一示意图重新排序。Sandbox 首次创建位于真正执行工具的入口，必须晚于权限允许与已有 journal 复用判断；被拒绝、参数无效或复用已完成结果的调用不得创建 Sandbox。

本次仅迁移 `bash/read_file/write_file/list_dir`。`note_save`、任务存储、事件/恢复文件和 MCP 工具仍遵循原有执行归属，不因名称涉及文件而自动迁移。

## 6. 总体架构

```mermaid
flowchart LR
    Client[CLI / TUI] --> Core[Agent Runtime Kit Core]
    Core --> Session[SessionManager]
    Core --> Governance[Permission / Event / Recovery]
    Session --> Manager[SandboxManager]
    Manager --> Backend[SandboxBackend]
    Backend --> K8s[Kubernetes API]
    K8s --> Pod[Session Sandbox Pod]
    K8s --> Svc[ClusterIP Service]
    Pod --> Worker[Sandbox Server]
    Manager --> Runtime[SandboxRuntime Client]
    Runtime --> Svc
    Governance --> Runtime
    Pod --> Workspace[/workspace emptyDir]
```

### 6.1 控制面

Agent Runtime Kit Core 继续持有：

- Session、Run 和根/子 Agent 关系；
- 工具 schema 与 ToolRegistry；
- PermissionManager；
- EventBus、JSONL 和 RecoveryStore；
- Session 与 SandboxHandle 映射；
- Sandbox 策略、创建、Ready、释放和 Reconcile。

首版验证拓扑为单副本 Core 与 Sandbox Pod 同集群部署，Sandbox Server 只通过 ClusterIP 暴露；CLI/TUI 通过 Core Service 的本地端口转发或受控内网入口连接。日常本机开发继续使用 Local backend。

### 6.2 执行面

每个 Sandbox Pod 运行固定版本的 Sandbox Server：

- 仅暴露命令和文本文件操作；
- 文件 API 路径限制在 `/workspace`，shell 初始 cwd 为 `/workspace`；cwd 本身不能限制任意 shell 的文件访问；
- 返回 stdout、stderr、exit code、duration 和 terminal reason；
- 保持当前 Bash 64 KiB 输出、Read 512 KiB、Write 1 MiB 的边界；
- 使用 `{sandbox_id, run_id, tool_call_id}` 作为执行请求幂等键；
- 不持有模型密钥、Kubernetes 凭据或用户 kubeconfig。

## 7. 核心领域模型

### 7.1 双层接口

```python
class SandboxBackend(Protocol):
    async def ensure(self, key: SandboxKey, spec: SandboxSpec) -> SandboxHandle: ...
    async def status(self, handle: SandboxHandle) -> SandboxStatus: ...
    async def destroy(self, handle: SandboxHandle, reason: str) -> None: ...
    async def reconcile(self) -> ReconcileReport: ...


class SandboxRuntime(Protocol):
    async def exec(self, request: ExecRequest) -> ExecResult: ...
    async def read_text(self, request: ReadRequest) -> FileResult: ...
    async def write_text(self, request: WriteRequest) -> FileResult: ...
    async def list_dir(self, request: ListRequest) -> ListResult: ...
```

`SandboxBackend` 只负责生命周期；`SandboxRuntime` 只负责执行协议。Tool 不感知 Pod、Service、CRD 或具体云平台。

### 7.1.1 M0 最小类型与装配契约

接口示意不是完整代码，实际命名可遵循现有仓库风格，但以下语义不可省略：

| 类型/入口 | 最低要求 |
| --- | --- |
| `SandboxKey` | 区分 `session` 与 `direct_run` 类型及原始 ID，避免同名碰撞；子 Run 继承父 key |
| `SandboxSpec` | backend、工作区策略、policy version；M0 不强制填写 Kubernetes 字段 |
| `SandboxHandle` | key、sandbox_id、backend、状态；Kubernetes 的 namespace/pod_uid/endpoint 等字段可空，Local 不伪造 Pod 身份 |
| `SandboxCallContext` | 实际 run_id、tool_call_id、attempt，及所属 key/session_id；只由可信调用层产生，不能加入模型可见参数 |
| `ExecRequest` | 调用上下文、command、timeout_s；M0 保留 Bash 默认 60 秒、参数范围 1–120 秒 |
| `ReadRequest / WriteRequest / ListRequest` | 调用上下文、原始 path；分别携带 content 或 max_depth；保留输入路径用于兼容展示 |
| `ExecResult` | 输出、exit_code、terminal_reason、duration_ms、truncated；支持 Local 的原有合并流，M1 增加分离 stdout/stderr，不能虚构原流顺序 |
| `FileResult / ListResult` | 内容/树条目或稳定展示信息、截断信息及错误；与 ToolResult 的映射只实现一次 |
| `SandboxManager.runtime_for(key)` | 获取按 key 共享的懒执行 facade；构建 registry 时可以调用，但此入口不得 ensure 或产生外部副作用 |
| `SandboxManager.release(key, reason) / close()` | 幂等释放一个归属或 Manager 管理的全部资源；未创建过的 key 不触发 backend.ensure |

调用身份的推荐接入是为 `BaseTool` 增加带上下文的内部调用钩子，默认委托原有 `invoke(params)`；四个工具覆盖该钩子，`invoke_tool()` 在治理通过后传入上下文。也可使用等价的任务局部绑定，但不得在共享 Runtime 上写可变 `current_run_id` 等字段。现有第三方工具、`extra_tools` 和直接 `Tool().invoke(params)` 用法保持兼容；直接调用工具可生成本次调用身份，但不承诺未经治理的调用具有跨重试幂等性。

### 7.2 身份映射

```text
session_id
  └─ sandbox_id = sha256(canonical_json([deployment_scope, key_kind, key_id]))[:32]
       ├─ namespace
       ├─ pod_name / pod_uid
       ├─ service_name / endpoint
       ├─ image_digest
       ├─ resource_profile
       └─ policy_version
```

其中有 Session 时 `key_kind=session, key_id=session_id`；没有 Session 的 direct Runner 使用 `key_kind=direct_run, key_id=root_run_id`。`deployment_scope` 是跨 Core 重启保持稳定的部署配置，不使用进程启动时随机值；M0 的 Local 身份只用于关联，不用于接管宿主资源。

- 同一 Session 的所有 root run 复用同一 Sandbox。
- 子 Agent 继承父 key 与懒 Runtime，不要求启动子 Agent 时已有 Handle；foreground/background/nested child 都保留独立 `child_run_id` 和事件文件，不能独立释放父 Sandbox。
- one-shot Session 在 run 终态后销毁 Sandbox。
- chat Session 在显式关闭、非 durable 断连或 idle TTL 后销毁 Sandbox。
- 无 Session 的 direct Runner 以根 run_id 为 key，在终态清理完所有子任务后由 Runner 释放 Sandbox。

### 7.3 状态机

```text
ABSENT -> CREATING -> READY <-> BUSY -> TERMINATING -> TERMINATED
                    |   |                  ^
                    |   +------ idle TTL --+
                    +----------> FAILED ----+
```

状态转换必须写入生命周期回执；重复 `ensure()` 返回同一合法 Sandbox，重复 `destroy()` 视为成功终态。

异常/关闭路径还包括 `CREATING → FAILED`、`CREATING → TERMINATING`、`BUSY → TERMINATING`；创建失败和取消不能等待一次正常 READY 后才进入清理。

M0 至少实现可观察的状态与关闭竞态；外部事件及持久化回执按 M1/M2 分阶段交付。`BUSY` 表示有执行占用，不能被 idle 回收；取消创建必须阻止晚到的 READY 重新加入已关闭 key。重复关闭共享一次清理结果，关闭后旧 facade 不得重新创建资源。Local release 释放管理状态及本实现拥有的活动进程，不删除 cwd 或用户文件。

Kubernetes 的 Pod UID 是工作区实例边界。M3 前 Pod 丢失/重启导致状态不可信或 TTL 销毁后，将该 Session 的工作区标为 `workspace_lost` 并要求新 Session；不得为旧调用静默换一个空工作区。M1 用 `restartPolicy: Never` 避免 Worker 重启丢失内存幂等表后继续接收旧身份请求。

## 8. 功能需求

### FR-01 Backend 可插拔

- `sandbox.backend=local|kubernetes`。
- 默认 `local` 保持当前用户流程和工具 schema。
- M0 Local backend 先保留现有 cwd 与绝对路径兼容；严格工作区根限制作为独立迁移，Kubernetes backend 从首版即限制在 `/workspace`。
- Kubernetes 依赖作为可选安装项，未安装时返回明确的配置错误。
- Local/Kubernetes 共享 exec/file 基础行为契约；隔离、远端去重和资源限制按 backend capability 单独验收。Fake backend 不能替代真实集群证据。

### FR-02 Session 级生命周期

- 第一次已授权的副作用工具调用执行 `ensure(session_id)`。
- 同一 Session 并发 ensure 只能产生一个 Ready Sandbox。
- Ready 判断采用初始 get/list + watch；断线在总 ready deadline 内重连，遇 `410 Gone` 重新 list 获取 resourceVersion，不使用无期限固定轮询。Pod Ready 后还需确认 Service 路由与 Worker 协议可用。
- Sandbox 创建失败时产生唯一失败终态和清理动作。
- session close、one-shot 完成、非 durable 断连和 Core shutdown 触发幂等 destroy。
- 无 Session 的 direct run 在自身终态触发幂等 destroy。

### FR-03 工作区连续性与隔离

本项的物理隔离要求适用于 Kubernetes。M0 Local 共享宿主 cwd、文件系统与网络，只隔离 Manager 的逻辑身份和状态。

- Pod 以 `emptyDir` 挂载 `/workspace`，以独立 `emptyDir` 挂载 `/tmp`。
- 同一 Session 的多轮工具调用和子 Agent 可观察前序写入。
- 不同 Session 不共享 volume、进程命名空间、网络身份或 Runtime client。
- 首版工作区与 Pod 同生命周期；PVC 和对象存储快照进入后续版本。

### FR-04 Sandbox Server 协议

首版最小端点：

| Method | Endpoint | 作用 |
| --- | --- | --- |
| `GET` | `/healthz` | readiness/liveness |
| `POST` | `/v1/exec` | 执行非交互命令 |
| `GET` | `/v1/files?path=` | 读取文本文件 |
| `PUT` | `/v1/files?path=` | 写入文本文件 |
| `GET` | `/v1/dirs?path=` | 列出目录 |

协议要求：

- 每个副作用请求携带 `sandbox_id/run_id/tool_call_id/attempt`；
- 文件 API 路径必须属于 `/workspace`；除词法与符号链接校验外，实际 open/create 使用目录 fd 与不跟随符号链接的安全遍历等机制，覆盖检查后替换路径的竞态；读取、创建父目录、写入和目录递归均适用；
- 命令使用固定工作目录 `/workspace`，stdin 默认关闭；
- 超时终止整个进程组并返回明确 terminal reason；
- M1 stdout/stderr 分离传输，额外保留有界 merged_output 供 Tool 展示；M0 Local 保留已有合并流，不承诺从合并内容恢复原始 stdout/stderr；
- P0 在当前 Pod 生命周期内缓存完成结果，重复幂等键返回原终态；M3 将 journal 持久化用于 Core 重启对账。
- Sandbox Server 使用每个 Sandbox 独立的随机访问令牌，禁止放入命令环境、argv、工作区、日志或 receipt；令牌保护与命令身份隔离必须在 M1 实现设计中明确，不能仅依赖“没有继承 env”。同 UID 进程和 `/proc` 可能暴露凭据，若 Worker 与不可信命令共享身份且无法防护，须先采用独立受控认证代理等边界再执行不可信命令。NetworkPolicy 同时限制只有 Core 可访问服务端口。

### FR-05 工具治理兼容

- 保持 `bash/read_file/write_file/list_dir` 名称和模型可见 schema。
- PermissionManager 在 Sandbox 副作用前完成审批。
- `tool.call_started/finished/failed` 继续由现有治理路径发布。
- Loop 和 Graph 都调用同一 SandboxRuntime 工具实例。
- durable 模式在 M3 前对 Kubernetes backend 明确返回能力不可用状态。
- M3 前若配置会自动创建 durable chat（例如 Graph SQLite recovery）且 backend 为 Kubernetes，Core 在监听端口前 fail-fast 拒绝该组合。
- 命令已经 dispatch 后发生连接中断时进入 `outcome_unknown`，不作为普通 `runtime_error` 自动重试副作用。
- M1 对已经执行并返回的非零退出码也不得自动重发命令；`attempt` 只是传输尝试编号，不构成新执行身份。M0 保持已有非 durable 重试策略与 durable journal 安全边界，不在契约迁移中宣称修复所有历史重复执行风险。
- M1 分开 queue、provisioning、command 与 cleanup 预算；command 从进程实际启动计时，权限等待沿用 PermissionManager 的预算。首次调用的执行外层 deadline 至少覆盖 `queue_timeout + ready_timeout + command_timeout + cleanup_margin`，不复用当前固定 120 秒作为远程总预算；M0 保留现有本地 deadline 行为。

### FR-06 生命周期事件与回执

新增内部/持久化事件：

- `sandbox.creating`
- `sandbox.ready`
- `sandbox.failed`
- `sandbox.terminating`
- `sandbox.terminated`

`creating/ready/failed` 携带触发调用的 `run_id`，进入对应 run 的 EventWriter；空闲 TTL 等无活跃 run 的回收事件写入 `data_root/sandboxes/lifecycle.jsonl` 与 daemon trace，避免写入错误的 run JSONL。

生命周期回执：

```text
SandboxLifecycleReceipt
- sandbox_id / session_id / backend
- namespace / pod_uid / image_digest
- policy_version / resource_profile
- create_time / ready_ms / terminal_reason
```

执行回执：

```text
SandboxExecReceipt
- sandbox_id / run_id / tool_call_id / attempt
- operation / input_hash
- start_time / end_time / duration_ms
- exit_code / signal / terminal_reason
- stdout_hash / stderr_hash
- policy_version
```

回执不记录模型密钥、kubeconfig、完整环境变量或超出既有工具日志范围的文件内容。

### FR-07 回收与 Reconcile

- 显式关闭立即触发销毁。
- M1 idle TTL 从最后一次操作完成起按单调时钟计算，仅回收无排队/执行中操作的 Ready Sandbox；新请求与 TTL 通过同一状态门控竞争。回收后同一 Kubernetes Session 不静默重建空工作区。
- Pod/Service 使用 Agent Runtime Kit owner label 和创建时间 annotation。
- M1 Core 启动时扫描本 deployment scope 标签且超过 grace period 的孤儿资源，并输出 ReconcileReport；M3 扩展为先恢复映射/对账，再判定孤儿。只能通过可信 ownership 元数据接管或删除资源。
- Service 通过 ownerReference 或同一幂等清理路径随 Pod 删除。
- Kubernetes `404` 视为 destroy 已完成；其他错误保留失败状态供下一轮 reconcile。

### FR-08 配置

M0 配置仅实现 `[sandbox].backend = "local"` 与对应 `AGENTRT_SANDBOX_BACKEND` 环境变量，沿用现有默认值 → TOML → 环境变量的优先级与严格未知字段校验。值为 `kubernetes` 时在启动监听之前明确报“本阶段尚未实现”，不得悄悄回退 Local。其他 backend 拼写直接报配置错误；M0 不加入 Kubernetes 依赖。

以下为 M1 配置草案，不是 M0 可用配置。全部字段要同时定义默认值、正数/枚举校验与环境变量映射；部署前必须替换 scope 与镜像 digest 占位符：

```toml
[sandbox]
backend = "local"
deployment_scope = "<stable-deployment-id>"
create_on = "first_tool"
idle_timeout_s = 900
queue_timeout_s = 120
command_timeout_s = 120
cleanup_margin_s = 5
reconcile_grace_s = 120

[sandbox.kubernetes]
namespace = "kitagent-sandboxes"
image = "ghcr.io/example/kitagent-sandbox@sha256:<digest>"
service_port = 8080
ready_timeout_s = 120
runtime_class_name = ""
cpu_request = "100m"
cpu_limit = "1"
memory_request = "128Mi"
memory_limit = "512Mi"
ephemeral_storage_limit = "1Gi"
ephemeral_storage_request = "256Mi"
workspace_size_limit = "512Mi"
tmp_size_limit = "128Mi"
```

首版只开放经过校验的固定字段，不接受原始 PodSpec 透传。

## 9. Kubernetes 安全与资源基线

### 9.1 Namespace 与 RBAC

- Sandbox 使用专属 namespace `kitagent-sandboxes`。
- Core ServiceAccount 使用 namespace 内最小 Role：Pod/Service 按需 create/get/list/watch/delete，Event 只读 get/list/watch；认证若采用 Secret，必须另列实际所需权限与泄漏防护，禁止通配 RBAC。Core 不获得 `pods/exec` 或集群级资源权限。
- Sandbox Pod 设置 `automountServiceAccountToken: false`。
- Sandbox 容器不具备访问 Kubernetes API 的凭据。
- Sandbox Server 的访问令牌只用于 Core→Worker 认证，按 Sandbox 隔离并在销毁时失效。

### 9.2 Pod Security

```yaml
securityContext:
  runAsNonRoot: true
  seccompProfile:
    type: RuntimeDefault

containers:
  - securityContext:
      allowPrivilegeEscalation: false
      readOnlyRootFilesystem: true
      capabilities:
        drop: ["ALL"]
```

同时固定：

- `privileged: false`；
- 不使用 `hostPath`、`hostNetwork`、`hostPID` 或 `hostIPC`；
- 仅 `/workspace` 与 `/tmp` 可写；
- 镜像使用 digest 固定；
- 更强隔离档通过 `runtimeClassName` 选择 gVisor/Kata。

### 9.3 网络

- Namespace 默认 deny ingress/egress。
- 仅允许 Agent Runtime Kit Core 访问 Sandbox Server 端口。
- Sandbox 默认不访问模型 API、Kubernetes API、集群内网和云元数据地址。
- 如后续允许下载依赖，使用独立 egress profile 与 allowlist，并在回执中记录 policy version。
- kind 验收环境必须选择实际执行 NetworkPolicy 的 CNI。

### 9.4 资源

- 每个 Pod 显式设置 CPU、memory、ephemeral-storage requests/limits。
- `/workspace` 的 `emptyDir.sizeLimit` 与 ephemeral-storage limit 协同配置。
- Namespace 使用 ResourceQuota 与 LimitRange 限制 Sandbox 数和总体资源。
- OOM、磁盘超限、命令超时和 Pod 驱逐映射为结构化 terminal reason。

以上 non-root、禁提权、drop ALL、seccomp、无宿主挂载等配置以 [Pod Security Standards](https://kubernetes.io/docs/concepts/security/pod-security-standards/) 为基准。Kubernetes 提供编排、cgroup、Namespace 和网络治理；更强内核边界由 RuntimeClass 对应的隔离运行时提供。M1 在执行模型命令前必须具备最低安全配置和认证；M2 才发布完整验证结论。

## 10. 可靠性设计

### 10.1 幂等创建

- `sandbox_id` 由稳定 deployment scope 与 sandbox key 派生，可跨同一部署的 Core 重启重发现。
- create 前查询同名且 label/annotation/镜像摘要一致的资源。
- 已存在且 Ready 且 UID/协议有效：复用；Creating：继续 watch；首次创建未分配成功的临时错误可以在 ready 预算内重试。已经拥有工作区的 Failed/Terminating 实例清理后报告 workspace_lost，不静默对旧 Session 重建。
- 身份元数据不一致：返回冲突，不接管未知资源。

### 10.2 幂等执行

- 键为 `{sandbox_id, run_id, tool_call_id}`，与现有 session/run/tool journal 身份保持一致，避免根/子 Run 的工具 ID 碰撞。
- P0 Worker 在 dispatch 前原子登记 `started` 并缓存序列化终态；同键进行中的请求等待或返回 in_progress，不开启第二个进程。M3 将 journal 写入可恢复介质。
- 同键同 input hash 返回已有结果；同键不同 input hash 返回 conflict。
- Core 在 dispatch 后网络中断时将结果标记为 outcome unknown，不触发普通副作用重试；M3 通过 Worker journal 对账。
- 去重表设置条目和字节上限，满时拒绝新身份并返回明确容量错误，不通过驱逐旧键重新开放执行；Pod/Worker generation 失效后禁止旧请求投递到新实例。保留上限及 TTL 必须在 M1 实施文档明确。
- journal 无法让任意外部副作用和结果落盘自动成为同一事务。M3 对“已经产生副作用但结果未提交”的崩溃窗口仍返回 outcome_unknown，不承诺普遍 exactly-once。

### 10.3 并发边界

- SessionManager 继续约束同一 Session 的根消息并发。
- SandboxManager 为每个 session_id 提供独立 ensure/destroy 锁。
- 根 Agent 与子 Agent 共享按 key 绑定的 Runtime；M0 起对同一 key 的四个 Runtime 操作串行化。操作锁不得包裹整个 Run、`spawn_agent` 或等待 child 的过程，避免父任务持锁等待子任务的死锁；不同 key 之间不设全局执行锁。
- M3 再评估交互式进程和并行执行；首版不引入分布式文件锁。

### 10.4 P0 单实例边界

- Core 首版单副本运行，Sandbox ownership 保存在本进程及持久化 receipt 中。
- startup reconcile 只处理带有本 deployment scope 标签且超过 grace period 的资源。
- 多 Core 副本、租约转移和跨实例 warm pool 属于后续版本。

## 11. 验收计划

以下均为计划验收目标；只有生成对应 TestRunManifest 和原始回执后，指标才进入简历。

| 能力 | 计划验收目标 | 主要证据 |
| --- | ---: | --- |
| Session 映射 | 100 次 create/close，重复活跃 Pod 为 0 | 生命周期测试、Pod UID 清单 |
| 工作区连续性 | 30/30 次 write→exec→read 保持状态 | Exec/File receipt |
| 跨 Session 隔离 | 20 个并发 Session、200 次越权读写探测，泄漏数 0 | 负向用例与路径记录 |
| Cold Ready | 固定 kind、预拉取镜像，30 次样本 p95 ≤ 15s | 环境清单、镜像 digest、原始样本 |
| Ready 后执行开销 | 100 次空命令，调度开销 p95 ≤ 500ms | Exec receipt 明细 |
| 命令超时 | 100% 在 `timeout + 2s` 内进入终态 | 进程与事件时间戳 |
| 资源治理 | CPU 超限观察到节流；OOM、存储超限/驱逐与超时分别有符合事实的结果分类 | cgroup 指标、Pod status、事件、receipt |
| 网络策略 | allowlist 目标成功；未授权目标和集群 API 探测全部拒绝 | NetworkPolicy 与探测日志 |
| Pod 安全基线 | non-root、只读根、禁提权、drop ALL、seccomp、关闭 SA token 全部生效 | 渲染 PodSpec 与策略检查 |
| 生命周期回收 | close/TTL 后 p95 60s 内回收，100 次无孤儿 Pod/Service | ReconcileReport |
| 幂等恢复（M3） | 10 类重试/竞态 × 20 轮，共 200/200；重复副作用为 0 | Worker journal、Core recovery journal |
| 现有能力兼容 | 当前离线测试全绿；新增 Local/K8s contract tests 全绿 | pytest 报告 |

### 11.1 测试分层

1. Unit：SandboxManager 状态机、规范化路径、配置、幂等键、回执序列化。
2. Contract：共同行为运行于真实 Local 与 Fake backend；隔离/资源/网络能力专属测试运行于真实 Kubernetes，不以 Fake 通过冒充集群通过。
3. kind Integration：Pod/Service 创建、Ready watch、命令、文件、超时和删除。
4. Security：路径逃逸、跨 Session、ServiceAccount、NetworkPolicy、资源超限。
5. Recovery：Core 重启、Pod 删除、网络中断、重复响应、outcome unknown。
6. Regression：Loop、Graph、Permission、子 Agent、one-shot/chat 与现有事件顺序。

命令超时用例除检查返回时间外，还要在超时窗口后验证远端 marker 文件未生成，证明终止的是进程组，而不只是断开 HTTP 连接。

生命周期矩阵必须覆盖：Sandbox 尚未创建时 close、重复 close、创建中取消、one-shot、chat 跨 Turn、非 durable 断连、graceful shutdown、root/foreground/background/nested child 共享、并发 ensure 单创建，以及 M3 的 durable suspended 保留/重连/恢复后关闭。

指标口径：Cold Ready 从首次授权调用进入 ensure 到 Worker API 可用，排队/镜像是否预拉取分别记录；Ready 后开销记录 Core 总耗时减 Worker 执行耗时；失败样本单列而不从分母消失。CPU limit 通常导致节流，ephemeral storage 超限可触发驱逐，不能统一称为命令终态。参考 [资源管理](https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/)。

NetworkPolicy 需要实际执行策略的 CNI；Core→Worker 的允许规则与 Sandbox 默认拒绝出站要独立验证，不能将创建策略对象视为隔离成功。参考 [NetworkPolicy](https://kubernetes.io/docs/concepts/services-networking/network-policies/)。

### 11.2 TestRunManifest

每次可用于简历数字的测试必须记录：

```text
commit_sha
test_time
os / cpu / memory
kubernetes_version / kind_version / cni
node_count
core_image_digest / sandbox_image_digest
resource_profile / policy_version
case_count / iteration_count / concurrency
passed / failed / leaked_resources
raw_receipt_paths
```

## 12. 里程碑

### 12.1 M0：契约与本地兼容

**目标**：默认 Local 路径实际经过 SandboxRuntime，具备可注入、可共享、可关闭的归属；现有 Agent Loop/Graph、审批、事件和本地工具行为继续成立。仅新增抽象而未替换真实工具执行路径，不算完成。

**实现范围**：

1. 在 `src/agent_runtime/core/sandbox/` 定义第 7.1.1 节的最小类型、Backend/Runtime Protocol、Local backend/runtime 和 Manager。用清晰依赖方向避免 Runtime 反过来实例化四个 builtin Tool 造成递归；原有 subprocess/文件实现迁入 Local，Tool 负责参数与结果适配。
2. 为四个工具增加可选 Runtime 注入和内部调用上下文接入，保留无参数构造与直接 `invoke(params)`。不向模型 schema 添加 session、Pod、权限或路径策略参数。
3. CoreApp 持有共享 Manager，经 Runner 的可选依赖传入 root registry；SpawnAgentTool 将相同 key/facade 传给 foreground/background/nested child registry。子 Agent 运行时的请求 run_id 必须是 child_run_id。
4. Session.close、one-shot 完成、非 durable 断连和 shutdown 复用现有清理链：阻止新工作，取消/等待已有根与子任务，再 release。无 Session Runner 只清理本次根 run key；不关闭外部注入 Manager 的其他 key。Local durable suspend/resume 继续遵循原有恢复语义，不因一次暂停而误判 Session 已关闭。
5. Manager 的单 key ensure 合并并发创建，四类操作按 key 串行；创建失败、操作异常、取消及重复 close 不泄漏锁/任务。释放与创建交错时，晚到 Handle 也必须清理；新调用不得复活已关闭归属。
6. 配置只交付第 FR-08 的 M0 子集，默认 Local；backend 未实现或未知时 fail-fast。模块导入不要求安装 Kubernetes，也不读取 kubeconfig。
7. 添加有意义的 Manager、Local 契约与集成回归用例及用户说明，记录本阶段能力边界。**按本次用户指令，不运行这些用例或任何测试/检查脚本。**

**不在 M0 实现**：Kubernetes SDK、Pod/Service、HTTP Worker、镜像与部署 YAML、严格 `/workspace` 隔离、默认拒绝网络、资源配额、TTL 后台任务、集群 reconcile、新 wire event、持久化 receipt、远端去重或恢复重连。Backend.reconcile 在 Local 可返回空报告；不是扫描/删除宿主目录。

**Local 兼容矩阵**：

| 项目 | M0 要保留的行为 |
| --- | --- |
| 工具 API | 名称、description、input_schema、Pydantic 默认值/校验、ToolResult 结构及常见错误文本；第三方 BaseTool 无需新增必填参数 |
| cwd 与路径 | 相对路径沿用调用时宿主 cwd；接受既有绝对路径行为；文件工具继续拒绝含 `..` 路径分量；不强加 `/workspace` 或新项目根 |
| Bash | 原 shell 语义、默认 60 秒/最大 120 秒、stdout+stderr 合并、`[no output]`、`[exit N]`、timeout 文本和现有截断展示；M0 不顺带重写非 durable 重试策略 |
| Read/Write | UTF-8 与读时替换非法字节、Read 512 KiB 截断、Write 1 MiB 拒绝、自动创建父目录和写入字节数文本 |
| List | 默认 path `.`、默认深度 2/最大 4、200 条上限、隐藏条目、原排序/树形展示 |
| 退出/取消 | 保留原有取消与终态事件语义；清理新层持有的任务/进程，不吞取消，不扩大到其他 Session 或宿主进程 |
| 持久化与事件 | 原 recovery result reuse/outcome_unknown 逻辑、事件字段/顺序、子 Run 独立事件文件；M0 不发布新增 sandbox wire event |

64 KiB 是现有 Bash 声明的输出上限；基线实现存在先完整读取、再按字符切片的问题。M0 记录这个限制并保留展示兼容，不把它当作有界内存的安全保证；M1 Worker 必须独立实现按字节有界收集及 Unicode 边界处理。

**实施顺序与后续验收条件**：

| ID | 工作 | 验收用例/可观察结果 |
| --- | --- | --- |
| M0-01 | 类型、Local、Manager | 真实 Local write→exec→read/list；相同 key 复用，不同 key 的逻辑状态不串；不得宣称文件隔离 |
| M0-02 | 四个 Tool 委托 | 原构造和直接调用仍有效；Fake Runtime 可捕获完整请求；模型 schema 无新增字段 |
| M0-03 | invocation 上下文 | 参数错误、权限拒绝、完成 journal 复用均为零 ensure；root/child 的 run_id 与 tool_call_id 不串 |
| M0-04 | 所有权与关闭 | 同 Session 跨 Turn 复用；foreground/background/nested child 共享；child 完成不 destroy；direct root 完成释放 |
| M0-05 | 竞态与取消 | 并发 ensure 单创建；close-before-create、close-during-create、double-close、异常后可释放；释放不删除用户 marker 文件 |
| M0-06 | 配置与回归 | 默认/环境覆盖、非法值及未实现 Kubernetes fail-fast；Loop/Graph、Permission、Event、Recovery 行为保持 |
| M0-07 | 交付记录 | 列出变更、未完成项、测试文件与执行状态；不填写未经执行的通过数 |

正常 M0 验收需要以上用例及仓库质量检查通过。**本次只交付实现与测试代码；由于用户明确要求不测试，验收门保持待执行，不因代码已写而标记“测试通过”。** 日后经用户要求再按 `CONTRIBUTING.md` 在受支持环境运行：

```bash
uv sync --frozen --extra graph-sqlite
uv run ruff check src tests scripts
uv run ruff format --check src tests scripts
uv run mypy src
uv run pytest tests/unit -q
# 离线 integration：按标记/文件选择，不触发真实 LLM 用例
uv run pytest tests/integration -m "not integration" -q
uv run python scripts/check_wire_protocol.py --check
```

以上是后续验收清单，不是本次会话要执行的命令。Graph 可选依赖、平台或既有失败应单独列出，不降低标准也不伪报全绿。

### M1：Kubernetes cold-start 闭环

- Core 管理 Pod/Service；
- Sandbox Server 提供 exec/file API；
- 一 Session 一 Pod，根/子 Agent 共享；
- session close、one-shot、断连和 shutdown 清理；
- 最低 Pod Security、认证、无 SA token、资源上限及实际生效的默认拒绝网络，在执行不可信命令前启用；
- 内存幂等表、远端不安全重试抑制、deadline、空闲 TTL、基本孤儿扫描与 workspace_lost 行为；
- 新增 run-scoped Sandbox 事件并同步 wire protocol 文档；
- kind 完成功能 E2E。

### M2：安全与证据闭环

- 完整验证 Pod Security、RBAC、ResourceQuota/LimitRange、NetworkPolicy，完善容量与边界配置；
- 生命周期/执行 receipt 与 TestRunManifest；
- 完成隔离、资源、网络、并发和回收验收；
- 形成可复现实验报告和演示脚本。

### M3：可靠恢复

- Worker 幂等 journal；
- Core 重启后的 Sandbox 重连和 outcome unknown 对账；
- 将 M1 孤儿 grace/reconcile 扩展为恢复映射、重连对账和所有权判定；
- durable session 接入；
- 评估 Kubernetes SIG Agent Sandbox backend。

### M4：规模化演进（可选）

- WarmPool 与冷启动优化；
- Core 多副本租约/所有权；
- PVC/快照；
- gVisor/Kata 隔离档；
- 多租户认证、配额与成本治理。

## 13. 交付物

- `src/agent_runtime/core/sandbox/`：接口、Manager、Local/Kubernetes backend、Runtime client；
- `src/agent_runtime/sandbox_server/`：Pod 内执行服务；
- `deploy/kubernetes/`：Namespace、RBAC、Quota、LimitRange、NetworkPolicy、Core 和 Sandbox 模板；
- `tests/unit/test_sandbox_*.py`：状态机和契约测试；
- `tests/integration/test_kubernetes_sandbox.py`：kind E2E；
- `scripts/run_sandbox_validation.py`：固定环境验收入口；
- `artifacts/sandbox-validation/<timestamp>/`：manifest、摘要与原始回执；
- `docs/kubernetes-sandbox.md`：实现后的用户/运维文档；
- 演示流程：创建 Session → 首次授权工具 → Pod Ready → write/exec/read → 子 Agent 共享 → close 回收。

## 14. 简历证据成熟度

### 设计阶段：PRD 与方案完成后

> 完成 Agent Runtime Kit Kubernetes Sandbox Runtime 方案设计，定义 Session 级生命周期、命令/文件执行契约，以及资源、网络、回收与运行回执的量化验收体系。

### M0：接口、Local backend 与契约验收完成后

> 为 Agent Runtime Kit 抽象 Sandbox 生命周期与执行契约，实现 Local 适配和 Session/子 Agent 的共享注入，并通过本地契约测试验证工具兼容性。

只写完代码而未运行测试时，改写为“已实现 Local 适配与共享注入，契约测试待执行”；不能使用上述“通过”表述。

### M1：Kubernetes 功能闭环验收后

> 实现一 Session 一 Sandbox Pod 的 cold-start 执行链，打通命令、文件操作与关闭回收；完整隔离和性能数据进入 M2 验收。

### M2：kind E2E 与安全验收完成后

> 实现 Agent Runtime Kit Kubernetes Sandbox Runtime，将命令与文件操作路由至 Session 级持久 Pod，使根 Agent 与子 Agent 共享受限工作区，并在固定 kind 环境完成 `[实际通过数/总数]` 条端到端验证。

> 建立 non-root、资源配额、默认拒绝网络和 TTL 回收策略，在 `[并发数]` 个 Sandbox、`[探测次数]` 次隔离用例中取得跨工作区泄漏数 `[实测值]`、回收成功率 `[实测值]`。

### M3：恢复验收完成后

> 以 `run_id + tool_call_id` 建立幂等执行与恢复机制，覆盖超时、OOM、Pod 删除、Core 重启和并发重试等 `[实际类别数]` 类场景，连续 `[轮次]` 轮回归通过 `[实际结果]`。

简历只替换已经由 TestRunManifest 与原始回执确认的占位符。

## 15. 面试讲述

### 15.1 30 秒版本（设计阶段，M0 验收前）

> 我把 Agent Runtime Kit 的 Agent 控制面和代码执行面拆开：Loop、Session、权限和恢复仍由 Core 管理，命令与文件操作进入 Kubernetes Sandbox。一个 Session 绑定一个稳定 Pod，保证多轮写代码、运行和观察时工作区连续；同时用资源、网络、TTL 和执行回执治理副作用。当前完成了与仓库接入点对应的 PRD、接口契约和量化验收设计。

### 15.2 90 秒版本（M2/M3 验收后替换指标）

> 这个设计解决的不是简单容器化，而是 Agent 反复执行模型生成代码时的状态、隔离和恢复问题。我参考 OpenHands 的 Action-Observation Runtime、SWE-ReX 的 Deployment/Runtime 分层、DeerFlow 的 Session 级 SandboxProvider，以及 Kubernetes SIG Agent Sandbox 的稳定身份模型。在 Agent Runtime Kit 中，Core 保存 Session 到 SandboxHandle 的映射，Pod 内执行服务处理命令和文件操作；根 Agent 和子 Agent 继承同一 SandboxHandle，不同 Session 使用独立工作区。执行请求以 run_id 与 tool_call_id 联合去重，结果进入结构化回执；Kubernetes 负责 CPU、内存、临时存储、NetworkPolicy 和生命周期治理。最终验收覆盖跨 Session 隔离、孤儿 Pod、超时/OOM 终态、并发重试以及 Ready/Exec p95。

### 15.3 高频追问

1. **为什么不是每条命令一个 Job？**  
   Agent Loop 需要跨轮保留代码、依赖和中间产物；Session 级 Pod 同时满足工作区连续性和启动成本要求。

2. **为什么不直接使用 `pods/exec`？**  
   Pod 内执行服务提供稳定的类型化协议、文件 API、幂等键和终态回执，控制面只管理生命周期，降低对 Kubernetes exec transport 的耦合。

3. **为什么保留 SandboxBackend 与 SandboxRuntime 两层？**  
   生命周期与命令/文件执行的变化频率不同；分层后可替换 Kubernetes、SIG Agent Sandbox 或更强隔离后端，而不改变 Tool schema。

4. **Kubernetes Pod 是否等同于强安全沙箱？**  
   Kubernetes 提供资源、Namespace 和网络治理；更强内核边界由 gVisor、Kata 等 RuntimeClass 实现。

5. **如何避免重复副作用？**  
   使用 `{sandbox_id, run_id, tool_call_id}` 作为幂等键，Worker 先登记执行记录；同键同输入复用终态，同键不同输入返回冲突。

6. **根 Agent 和子 Agent 如何共享环境？**  
   子 Agent 继承父 Session 的 SandboxHandle，共享 `/workspace`，但保留独立 run_id、ExecutionContext 和事件链。

7. **Core 重启后如何恢复？**  
   M3 计划通过持久化映射、Pod label/UID 和 Worker journal 对账；完成结果可复用，证据不足的执行仍保持 outcome_unknown。M1/M2 不承诺恢复工作区，只有基本孤儿回收。

8. **与通用 Sandbox 平台的区别是什么？**  
   本项目重点是把隔离执行嵌入 Agent Runtime Kit 已有的 Permission、Event、Recovery 和子 Agent 语义，而不是复制通用云 Sandbox 产品。

## 16. 决策记录

1. 首版选择“一 Session 一 Pod”，不选择“一 Tool Call 一 Job”。
2. 首版使用 Pod 内 Sandbox Server，不以 `pods/exec` 作为工具执行协议。
3. 首版直接管理 Pod/Service，不自研 CRD；接口稳定后评估 Kubernetes SIG Agent Sandbox。
4. 首版默认 `emptyDir`，持久化工作区进入后续版本。
5. 首版单 Core 副本，先验证生命周期和恢复契约，再引入跨实例所有权。
6. Local backend 保持默认，Kubernetes backend 作为可选依赖和部署能力。

## 17. 参考资料

- [OpenHands Runtime Architecture](https://docs.openhands.dev/openhands/usage/architecture/runtime)
- [SWE-ReX](https://github.com/SWE-agent/SWE-ReX)
- [DeerFlow SandboxProvider](https://github.com/bytedance/deer-flow/blob/main/backend/packages/harness/deerflow/sandbox/sandbox_provider.py)
- [DeerFlow Kubernetes Provisioner / Helm](https://github.com/bytedance/deer-flow/blob/main/deploy/helm/deer-flow/README.md)
- [E2B Infrastructure Architecture](https://github.com/e2b-dev/infra/blob/main/docs/ARCHITECTURE.md)
- [OpenSandbox Lifecycle API](https://github.com/opensandbox-group/OpenSandbox/blob/main/specs/sandbox-lifecycle.yml)
- [Kubernetes SIG Agent Sandbox](https://github.com/kubernetes-sigs/agent-sandbox)
- [Kubernetes Pod Security Standards](https://kubernetes.io/docs/concepts/security/pod-security-standards/)
- [Kubernetes NetworkPolicy](https://kubernetes.io/docs/concepts/services-networking/network-policies/)
- [Kubernetes Resource Management](https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/)
- [Kubernetes API Concepts / watch 恢复](https://kubernetes.io/docs/reference/using-api/api-concepts/)
- [Kubernetes Pod Lifecycle / restartPolicy](https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/)
