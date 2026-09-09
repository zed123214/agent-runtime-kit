# Sandbox M1 实现记录

最新状态（2026-09-09）：M1 的 13 条真实 Kubernetes E2E 已通过；后续 M2 的实测、修复和
验收结果见 [M2 验收报告](sandbox-m2-validation.md)。原开工信息见
[M2 交接记录](sandbox-m2-handoff.md)。用户最新指令为修复阻断项后新开任务执行 M2，
旧“不测试”约束不再适用于本次验证阶段。

下文保留原 M1 编码交付时的历史记录，包括当时未执行的检查状态。
原交付状态：**代码交付完成，测试及集群验收未执行**。这不是 M1 验收通过声明。
依据 `kubernetes-sandbox-prd-v1.md` v0.2 及 `CONTRIBUTING.md`；从当前分支
`feat/sandbox-m0` 的 M0 提交 `8780693` 继续实现，没有从旧 main 重做，也没有提交或推送。
用户原有 `.cursor/`、`docs/interview-prep.md`、`docs/sandbox-m0-review.md` 保留。
M0 说明中“没有提交”描述的是当时状态，不是此次基线。

原 M1 编码交付遵守“写完代码别测试”：未运行 pytest（含 collect-only）、ruff/lint、mypy、
编译、wire 生成/检查、smoke、部署渲染、Docker 构建、容器/Worker/Core 启动、kubectl 或网络探测。
实际执行仅限源码/文档/index/diff 阅读、文件编辑、Git 状态阅读及 `uv lock` 依赖解析。
锁文件已加入 kubernetes-asyncio/aiohttp 及传递依赖；这不构成安装或功能验证。

## 先行修复 M0 审阅问题

`SessionManager.close()` 不再永久缓存失败的关闭任务。进行中的并发 close 仍共享同一任务，
取消某个 waiter 不取消清理。关闭查询等尚未提交的失败清除失败任务并恢复准入（shutdown 除外）；
Session 已进入 closed 后保持关闭，重试元数据持久化、待发布的关闭事件和幂等资源回调，
不复活 Runtime。断连路径采用相同的未关闭/已关闭边界。

`CoreApp._session_resource_tasks`、`SandboxManager.release/close` 同步允许失败清理重试，
否则仅修 SessionManager 仍会撞到下层缓存的原失败。关闭事件的待发布标记避免 one-shot
已经发布关闭后再重复发布。回归用例在 `test_sandbox_close_retry.py`，未执行。

## 实际交付

| 范围 | 实现位置与行为 |
| --- | --- |
| 配置与可选依赖 | `core/sandbox/config.py`、`core/config.py`、pyproject/uv.lock；默认 Local，全部固定字段有 TOML/env 映射与类型、范围、资源、namespace/scope/digest 校验 |
| 装配与能力门控 | `sandbox/factory.py`、CoreApp、Runner、SessionManager；Core 监听前拒绝缺配置、缺依赖、未确认 CNI、Kubernetes durable/自动 durable chat |
| Pod/Service 生命周期 | `sandbox/kube_api.py`、`kubernetes.py`、`pod_spec.py`；直接 API 管理 Pod、ClusterIP Service、专属 token Secret，无 pods/exec |
| 幂等创建与 Ready | per-key 锁、稳定 SHA-256 身份、签名归属、UID/镜像/模板冲突拒绝；兼容 API 省略默认 false 字段及等值资源量规范化；初始 get/list、有界 watch、410/断线重列，最后核对 Worker API/协议/代际 |
| Worker | `sandbox_server/`；受控 broker + executor，两种启动角色、health/exec/read/write/list/cancel 端点、独立镜像构建文件 |
| 文件与进程 | dirfd/O_NOFOLLOW 逐级遍历、普通文件检查、有界目录/文本；固定初始 cwd、受控 env、字节有界 stdout/stderr/merged、timeout/cancel 清理进程组 |
| 认证与安全模板 | token 只挂载 broker；不同 UID/PID/根挂载视图；只读根、non-root、drop ALL、seccomp、禁提权、无 SA token/host 挂载、资源上限、默认 deny 与 Core-only ingress |
| 远端幂等 | broker dispatch 前原子内存登记，input hash 冲突拒绝，进行中去重，终态有界保留，容量满拒绝新键，attempt 不改变执行身份 |
| 重试与 deadline | `sandbox/remote.py` 与 invocation：远端不重放未知或非零结果；独立 queue/provision/command/cleanup，避开 Local 固定 120 秒外层限制；Local 原策略保留 |
| 清理与工作区丢失 | Manager TTL/准入墓碑、创建失败/取消/晚到清理、Session/one-shot/断连/shutdown/direct 路径；Pod/代际丢失或 TTL 后 workspace_lost，拒绝重建旧 Session |
| 基本对账 | 启动扫描与周期维护、scope/签名/grace 判定、404 成功、失败重试；Service ownerReference 与精确 UID 删除，保留部分创建的清理债务 |
| 事件与记录 | run-scoped SandboxLifecycleEvent、invocation 显式传入实际 run bus、CLI/TUI 订阅/显示、手工同步 WIRE_PROTOCOL 和生成器；无活跃 run 只写 lifecycle.jsonl/trace |
| 部署与验收入口 | `deploy/kubernetes/` Namespace/RBAC/Quota/LimitRange/Policy/Core/Job/kind 模板和 Dockerfile；渲染器、kind E2E 与日志 receipt 入口，均未运行 |

## 设计决定与边界

1. **一个 key 一个 Pod。** Session 多轮、foreground/background/nested child 复用懒 facade；
   child 请求保留 child_run_id。无 Session 的归属是根 run_id。四种 Runtime 操作按 key 串行，
   不把锁持有到整个 Run 或等待子 Agent，审批在 ensure 之前。
2. **认证代理是实际边界。** broker UID 10000，executor/shell UID 10001，独立 PID namespace；
   token Secret 没有挂到 executor，broker 不挂载 workspace。Unix socket 转发不含 token、Authorization
   或 Core env。同 UID shell 可攻击自己的执行服务，因此执行输出始终不可信；这不赋予访问 broker 凭据的能力。
   具备节点/Namespace 管理权限的操作者仍属于可信部署边界。
3. **网络失败关闭。** 配置的 CNI 运维确认默认 false；Core 在执行前还核对全 Namespace 的策略，
   拒绝额外放行规则。它没有自动证明 CNI enforcement，当前没有探测证据；部署者不能把确认开关当作绕过项。
4. **工作区不偷偷恢复。** `restartPolicy: Never` 加 Pod UID/Worker boot generation，
   任何已知工作区丢失都封闭旧 key。当前进程可以找回一次失去 API 响应的创建；跨 Core 恢复映射/工作区接管留待 M3。
5. **去重不逐出。** 默认 1024 条、16MiB；每个新键预留最坏 4MiB 结果+2KiB 元数据，终态改按实际字节计账。
   无条目 TTL，记录保留到 Pod 终止，满后返回 idempotency_capacity。配置允许 8–64MiB、1–100000 条。
   这是进程内去重，不是崩溃后 exactly-once，M3 仍需讨论外部副作用与持久化不能原子提交的窗口。
6. **未知结果不是失败重试。** 已 dispatch 丢响应保留 outcome_unknown，并清理/封闭此归属。
   非零退出即使 error_type 来自旧 runtime_error 也不重放；Local 原有普通/durable 策略没有统一改写。
7. **分开时钟预算。** queue 只约束锁等待，provision 约束懒启动/创建/Ready，dispatch 前校验另受 ready 预算，
   command 从 subprocess 建立后计时，cleanup 单独限时。Graph 的整体 Run budget 与权限等待仍由其原模块管理。
8. **可重试清理与审计。** Ready 且无 queued/active 操作才可 TTL 回收；门控先封闭再异步删除。
   删除用 UID precondition，已知 UID 不符拒绝删除。未完成清理保持记录供周期/下次启动对账；
   无活跃 run 的 TTL/显式 close/孤儿清理不会附会到上一次 run JSONL。
   创建失败由接收到错误的操作任务启动回收，避免清理任务抢先取消等待者而掩盖原始错误。

`/workspace` 限制是文件 API 的访问边界；shell cwd 不是 chroot。进程组清理不承诺捕获主动 setsid
脱离组的进程，Pod 生命周期提供最终 cgroup 回收。Local 仍没有物理文件/网络隔离。
完整配置、真实部署材料及操作步骤见 [Kubernetes Sandbox 文档](kubernetes-sandbox.md)。

## 运行前需要准备

- 支持 Python 3.12 的 Linux/WSL，安装相应 optional extra；本次未安装。
- 单副本、集群内 Core，明确 namespace、稳定 scope；保管 Core-only ownership key。
- 由实际构建/registry 提供固定 Worker/Core/可选 validation image digest；仓库未填假摘要，也未构建镜像。
- 有执行能力且已经独立验收的 CNI，部署精确的默认 deny/Core-only 策略；当前本机集群未探测。
- Namespace/RBAC/Quota/LimitRange；Core 的 ownership 与 LLM Secret；外部 CLI 只连接 Core 转发端口。
- 运行 kind Job 前停止并等待普通 Core 退出，避免同 scope 两个 owner；固定真实代码来源再记录验收 receipt。

## 用例与执行状态

以下全部为**已编写、未执行**，不报告通过数：

| 文件 | 覆盖目的 |
| --- | --- |
| `tests/unit/test_sandbox_close_retry.py` | 临时 recovery 查询失败、并发 close 去重、取消 waiter、已关闭资源重试不复活 |
| `tests/unit/test_sandbox_kubernetes_config.py` | 合法/非法配置、全字段 env 映射、Local 无依赖、K8s 缺依赖、durable 组合拒绝 |
| `tests/unit/test_sandbox_kubernetes.py` | Fake API 的重复创建/失去响应、watch 410、UID/镜像/重启/OOM 拒绝、部分创建回收、签名/grace 对账、安全模板 |
| `tests/unit/test_sandbox_worker.py` | 内存去重/冲突/容量/取消墓碑、输出字节上限、路径替换竞态、真实 POSIX 进程组 marker 用例 |
| `tests/unit/test_sandbox_worker_auth.py` | 所有端点先认证、非 ASCII 凭据拒绝、正确 token 下身份/代际冲突零 dispatch、health 不暴露凭据、较短 command deadline 不误拒文件操作 |
| `tests/unit/test_sandbox_remote.py` | 不重放、外层 deadline、创建失败保留原错误并回收、审批/参数零创建、TTL 队列竞争、child 独立事件文件 |
| `tests/unit/test_sandbox_remote_client.py` | HTTP dispatch 前后错误分类、代际失效零请求、分离 stdout/stderr/非零结果 |
| `tests/integration/test_kubernetes_sandbox.py` | 仅显式开启的真实 kind Pod/Service/HTTP/file/timeout/认证/出站/TTL/取消/聊天跨 Turn/子 Agent/退出矩阵，LLM 使用离线 provider |
| M0/既有用例 | 配置拒绝旧 M0 占位的断言已适配 M1；其余契约/引擎/权限/恢复回归保留，未重跑 |

`scripts/render_kubernetes.py` 只负责生成可审阅材料；`scripts/run_sandbox_validation.py` 是之后才运行的
in-cluster opt-in E2E 入口。后者执行后才产生 JUnit、实际 exit code/计数和基础 receipt；
当前没有 artifacts、raw receipts、性能百分位、安全通过率或网络隔离通过结论。
WIRE_PROTOCOL 依用户允许手工同步，未运行生成器或一致性检查。

## 未交付及待验收

M1 编码范围已交付，运行正确性和 kind 功能验收仍待执行。M2 的完整安全/资源/性能实验、量化生命周期/执行
receipt、TestRunManifest 与报告未交付；M3 的 durable 重连、持久 Worker journal、Core 恢复映射/结果对账
未交付。多 Core 租约、warm pool、PVC/快照、自动 CNI 安装/迁移和更强 RuntimeClass 实验也不在本阶段。
这些边界没有用 stub 返回成功替代。仓库未提交，不应把“代码交付”写成“验收通过”。
