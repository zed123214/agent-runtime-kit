# Sandbox M0 实现与使用说明

交付状态：**代码已实现，未测试/未验收**。本次依据用户指令“写完代码别测试”，
只编写实现、测试代码和说明，没有运行 pytest（含收集）、lint、类型检查、编译验证、
wire protocol 检查、smoke 或验收脚本。本文没有测试通过数或集群验收指标。

实施依据是仓库根目录 `kubernetes-sandbox-prd-v1.md` v0.2 的 1.1、7.1.1、FR-08
和 12.1 节。启动基线为 `main / fbb761291c706d15a080373926c7caca271f8342`，
实施分支为 `feat/sandbox-m0`；没有提交或推送。原有 `.cursor/`、
`docs/interview-prep.md` 和已优化的 PRD 保留原状。

## 使用

默认 Local，无需新增配置。在项目 `.agentrt/config.toml` 或现有全局配置中可以显式写：

```toml
[sandbox]
backend = "local"
```

环境变量 `AGENTRT_SANDBOX_BACKEND=local` 覆盖 TOML。配置沿用现有的默认值、
全局/项目 TOML、`.env` 与系统环境变量优先级；系统环境变量优先于 `.env`。
后端能力在合并后验证，因此 TOML 中的 `kubernetes` 可以被环境变量 `local` 覆盖。
非法拼写或错误类型仍按配置错误拒绝。

本阶段只交付 `backend` 字段。最终值为 `kubernetes` 时明确报
`not implemented in M0 (本阶段尚未实现)`，Core 在创建监听前拒绝配置，
直接构造 Runner 也会验证。`[sandbox.kubernetes]`、`deployment_scope`、TTL、
资源和工作区等后续字段均属于未知配置，不能当作已生效策略使用。

开发者可将一个 `SandboxManager` 传给 `AgentRunner(..., sandbox_manager=manager)`
或 `CoreApp(..., sandbox_manager=manager)`。Core 将 Manager 作为应用资源关闭；
direct Runner 只释放本次根 Run 的 key，保留注入 Manager 的其他环境。
独立调用 `BashTool()`、`ReadFileTool()`、`WriteFileTool()`、`ListDirTool()` 的
`invoke(params)` 继续可用。这类直接调用绕过既有工具治理，不承诺跨重试幂等。

运行目标仍为 Python 3.12 和 Linux/macOS；Windows 运行使用 WSL2 或 Docker。
本次未安装环境或增加原生 Windows 支持。

## 实际接入

| 工作项 | 实现位置与行为 | 交付状态 |
| --- | --- | --- |
| M0-01 契约与 Local | `core/sandbox/` 定义身份、请求、结果、Backend/Runtime Protocol、Local 与 Manager；四种宿主副作用移入 Local | 已实现，未验收 |
| M0-02 四工具委托 | `core/tools/sandbox.py` 统一适配结果与身份；四个 builtin 仅校验参数、构造请求、委托 Runtime | 已实现，未验收 |
| M0-03 内部身份 | `BaseTool.invoke_with_context()` 默认委托原 `invoke()`；`invocation.py` 在原执行位置提供不可变上下文 | 已实现，未验收 |
| M0-04 归属与释放 | Core/Runner/Session/Spawn 传递共享 facade，按根任务归属等待子任务后释放 | 已实现，未验收 |
| M0-05 并发与关闭 | 按 key 单次操作锁、懒创建、关闭墓碑、共享清理任务、取消与晚到创建清理 | 已实现，未验收 |
| M0-06 最小配置 | `core/config.py` 和 `.env.example`，默认 Local、环境覆盖、未知字段和不支持后端拒绝 | 已实现，未验收 |
| M0-07 交付记录 | 本文、中英文 README 与新增测试文件的未执行说明 | 已记录 |

本次 M0-01～M0-07 的编码交付已完成。收尾仅阅读源码与 diff 并修复清理边界，
没有启动项目；未发现本次范围内仍待编码的必做项，运行验证与验收仍待执行。

`SandboxKey(kind, id)` 区分 `session` 和 `direct_run`，避免相同字符串 ID 碰撞。
Session 的多轮根 Run 共用一个 facade；前台、后台与嵌套 child 继承相同 key/facade，
请求携带各自的真实 `child_run_id`。子 Agent 事件文件继续独立，child 结束不释放父环境。
没有 Session 的 Runner 使用根 Run ID 作为 key。

可信调用身份为 `run_id/tool_call_id/attempt/session_id`，工具结合已绑定的 key
构造 `SandboxCallContext`。身份不进入模型 schema，不写入共享 Runtime 的可变
`current_run_id`。第三方 `BaseTool` 与 `extra_tools` 不需要新增必填构造参数或实现新钩子。

`runtime_for(key)` 只建立懒 facade，不调用 Backend.ensure、不捕获 cwd。
权限允许且未复用已完成 journal 结果的真正工具执行才会触发 ensure。
每个 key 仅在单次 Runtime 操作中持锁，不在整个 Run、spawn 或等待 child 时持锁；
不同 key 没有全局执行锁。Manager 可通过 `status(key)` 与 `handle_for(key)`
观察逻辑状态；Local Handle 不伪造 Pod UID、namespace 或 endpoint。

关闭先阻止新工作，再取消并等待已接入的根/子任务或 Runtime 操作，最后释放环境。
释放未创建的 key 不会 ensure；关闭后的旧 facade 不能重新创建。重复 release/close
共享清理结果，晚到 Handle 仍进入清理。后台任务注册表按根 Run 归属，避免一个
direct Run 的收尾取消同一个 Runner 上另一个根 Run 的子任务。

创建等待被取消时自动关闭该归属并接回晚到资源。Session 已决定关闭后，即使元数据
写入或关闭事件订阅者失败，资源回调仍执行；同一连接内一个 Session 清理失败不会
跳过其他 Session。Core shutdown 等待在途 Session 资源回调，再关闭共享 Manager。

one-shot 完成、异常或取消、显式 Session close、非 durable 断连和 Core shutdown
进入释放链。Local durable suspend 是暂停，保留 Session facade；显式恢复沿用 key。
Core 重启后 Local 仍使用宿主文件系统，M0 没有新增工作区快照或恢复保证。

## Local 兼容与限制

Local 是宿主执行适配器，**不提供文件、进程或网络物理隔离**。相对路径按操作时的
宿主 cwd 解释，既有绝对路径仍接受；文件工具继续拒绝包含 `..` 路径分量的输入。
release/destroy 只释放管理状态和本实现拥有的活动执行，不删除 cwd、项目目录或用户文件。

保留四个工具名称、description、模型 schema、Pydantic 默认值、ToolResult 结构及
原有常见错误/展示文本。Bash 默认 60 秒，参数范围 1–120 秒，仍合并 stdout/stderr，
保留 `[no output]`、`[exit N]`、超时和截断展示；无法从合并流重建两个原始流。
超时、取消和关闭会清理本实现仍在执行的 POSIX 进程组，包括取消期间晚到的进程创建。
成功 shell 已返回、后台后代已重定向输出或脱离执行会话时，Local 不承诺回收这些后代，
即使命令没有显式使用 `setsid`。保留这一原有 shell 行为，也避免保存可能复用的 PGID
后误杀其他宿主进程；本阶段没有新增进程 supervisor 或物理隔离。

Read 保留 UTF-8 替换非法字节及 512 KiB 截断；Write 保留 1 MiB 拒绝、自动创建
父目录与字节数展示；List 保留默认 `.`、默认深度 2、最大深度 4、200 条、隐藏项、
排序及树形展示。Bash 64 KiB 的历史实现先收集完整输出，再按字符切片，
该限制保留为展示兼容，不能作为有界内存安全保证。

普通调用的既有重试策略、durable journal 的完成结果复用和 `outcome_unknown`
边界保持原调用链。M0 没有新增远端去重，未宣称修复所有历史重复副作用风险。
未改变现有 wire event 类型；任务存储、笔记、事件/恢复文件和 MCP 工具仍由原模块管理。

## 测试代码与执行状态

以下文件均已添加，**全部未运行，未收集，未验收**：

| 文件 | 编写的覆盖范围 |
| --- | --- |
| `tests/unit/test_sandbox_manager.py` | 懒创建、共享与不同 key、串行/并行边界、重复关闭、失败/取消、晚到 Handle |
| `tests/unit/test_sandbox_local.py` | 真实 Local 写/命令/读/目录契约、展示/大小边界、取消进程与不删除用户文件 |
| `tests/unit/test_sandbox_tools.py` | 四工具 Fake Runtime 委托、schema、无参/直接调用、cwd 与内部身份 |
| `tests/unit/test_sandbox_invocation.py` | 参数/权限门控、journal lookup/claim 复用、attempt、普通/durable 错误边界 |
| `tests/unit/test_sandbox_lifecycle.py` | 根/前台/后台/嵌套归属、Session 多轮与关闭、关闭写入失败/多 Session 清理、direct release、durable 暂停、启动拒绝 |
| `tests/unit/test_sandbox_config.py` | 默认、TOML/env/.env 优先级、非法值、Kubernetes 与未交付字段拒绝 |
| `tests/integration/test_sandbox_engines.py` | 离线 Provider 经真实 Loop/可选 Graph 引擎进入注入 Runtime，身份与事件顺序 |

Graph 用例保留可选依赖缺失时的跳过逻辑，但本次也未执行该逻辑。
现有测试、检查与验收同样未运行。后续获用户执行指令后，再在受支持环境按 PRD 12.1
及 CONTRIBUTING 运行相应验证；当前不能据此声称兼容性、取消时限或进程回收已验收。

## 未交付范围

M1–M3 未实施：没有 Kubernetes 依赖、Pod/Service、HTTP Worker、部署 YAML、
严格 `/workspace` 边界、网络策略、资源配额、TTL 后台任务、集群 reconcile、
新增 wire event、持久化 sandbox receipt、远端幂等表或跨 Core 的沙箱恢复。
Local reconcile 返回空报告，不扫描或删除宿主文件。M0 的运行验证与验收仍待执行。
