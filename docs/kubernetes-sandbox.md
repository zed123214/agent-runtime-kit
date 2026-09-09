# Kubernetes Sandbox（M1 / M2）

状态（2026-09-09）：M1 真实集群 E2E 已通过。M2 的安全、资源、性能与回收原始证据和最终状态
见 [M2 验收报告](sandbox-m2-validation.md)。[M2 交接记录](sandbox-m2-handoff.md)保留开工前的历史基线。
Local 仍为默认，保留宿主 cwd、文件和现有恢复语义；
Kubernetes 不会自动回退 Local。

## 拓扑与运行条件

单副本 Core 和 Worker 在同一集群；Core 通过 in-cluster ServiceAccount 连接 API，
通过 ClusterIP 访问 Worker。外部 CLI/TUI 只连接 Core。Backend 不读本机 kubeconfig，
不使用 pods/exec、Sandbox Ingress、NodePort 或宿主挂载。

最小模板使用 `kitagent-core` 与 `kitagent-sandboxes` 两个 Namespace。Core Deployment 为
`replicas: 1`、`strategy: Recreate`；同一 scope 不允许同时运行第二个 Core/验收 Job。
`deployment_scope` 和 ownership key 跨重启保持稳定；改 key 会使旧资源的签名失效，
此时自动扫描拒绝删除它们，需要运维按实际归属处理，不能简单改 scope 来接管。

已检查并保留本机 `wsl-k8s` / kindnet 与 `default/web`；它不作为 NetworkPolicy 验收环境。
独立的 `agentrt-sandbox-e2e` 使用实际验证的 Cilium。另建验收环境时可以使用
`deploy/kubernetes/kind.yaml`（关闭默认 CNI），再按
[Cilium 的 kind 安装说明](https://docs.cilium.io/en/stable/installation/kind/)安装与实际集群版本匹配、
明确固定版本的 CNI。kind 节点镜像、CNI/chart 版本和配置由运维记录。

`network_policy_verified` 默认 false。部署者必须先独立验证 CNI 的 ingress/egress 实际执行，
再设为 true；这是一项运维确认，不是程序自动获得的安全证据。Core 在启动和每次 dispatch 前
还会读取整个 Sandbox Namespace 的 NetworkPolicy，要求恰好存在本实现的两条策略且内容一致。
额外策略可能通过并集开放流量，因此也会拒绝。Core 不具有修改策略的 RBAC 权限。
[Kubernetes NetworkPolicy 文档](https://kubernetes.io/docs/concepts/services-networking/network-policies/)
说明策略依赖 CNI，策略对象存在本身不证明隔离生效。M2 每次准备验收均运行实际探测；启动脚本
还核对集群 UID、CNI 配置/镜像和策略，并为 Job 固定不可变的环境记录。

## 安装与配置

Core 可选依赖：`uv sync --frozen --extra kubernetes`。
Worker 可选依赖：`uv sync --frozen --extra sandbox-worker`。Local 不要求安装这些 extra。
依赖由 `uv.lock` 固定；本次已在独立 WSL venv 与 Linux 镜像中安装、运行并验证。

示例 Core TOML（必须替换 scope、镜像摘要和 key 路径）：

```toml
[sandbox]
backend = "kubernetes"
deployment_scope = "agentrt-dev"
create_on = "first_tool"
idle_timeout_s = 900
queue_timeout_s = 120
command_timeout_s = 120
cleanup_margin_s = 5
reconcile_grace_s = 120

[sandbox.kubernetes]
namespace = "kitagent-sandboxes"
core_namespace = "kitagent-core"
image = "registry.example/agentrt-worker@sha256:<real-64-hex-digest>"
service_port = 8080
ready_timeout_s = 120
runtime_class_name = ""
cpu_request = "100m"
cpu_limit = "1"
memory_request = "128Mi"
memory_limit = "512Mi"
ephemeral_storage_request = "256Mi"
ephemeral_storage_limit = "1Gi"
workspace_size_limit = "512Mi"
tmp_size_limit = "128Mi"
ownership_key_file = "/run/agentrt/ownership/key"
network_policy_verified = false
journal_max_entries = 1024
journal_max_bytes = 16777216
```

所有字段均有环境变量映射：`[sandbox]` 字段为 `AGENTRT_SANDBOX_<字段大写>`，
`[sandbox.kubernetes]` 字段为 `AGENTRT_SANDBOX_KUBERNETES_<字段大写>`。
例如 `AGENTRT_SANDBOX_QUEUE_TIMEOUT_S`、`AGENTRT_SANDBOX_KUBERNETES_CPU_LIMIT`。
配置优先级为默认 → 全局/项目 TOML → 环境变量，系统环境优先于 `.env`。
未知字段、布尔冒充数字、非有限/非正 deadline、非法 namespace/scope、未固定摘要的镜像、
request 大于 limit、workspace+tmp 大于执行容器 ephemeral limit 均拒绝。
CPU 接受整数/小数核心或 `m`（最细 1m）；存储接受正整数 bytes 或 Ki/Mi/Gi/Ti，
没有开放 Kubernetes quantity 的全部表达式。默认 CPU/memory/storage 字段作用于 executor；
broker 另有固定 request `50m/64Mi/16Mi` 和 limit `250m/256Mi/64Mi`。

所有秒数字段须在 `(0, 86400]`，command 上限 120、ready 上限 3600；端口为 1–65535。
幂等表条目上限 1–100000，字节预算 8–64 MiB。字段默认值见上述示例；Local 的 scope/image/key
默认为空，不要求网络确认。仅支持 `create_on=first_tool`。不接受原始 PodSpec、任意 env、volume 或网络透传。

Kubernetes + 显式 durable、Graph SQLite 自动 durable chat 在启动监听或 direct Runner 进入执行前拒绝。
SessionManager 也拒绝创建不支持的 durable Session。Local graph-sqlite recovery 保持原有入口。

## 认证与最低安全边界

每个 Pod 包含两个容器，使用同一固定摘要镜像：

| 容器 | 身份/挂载 | 职责 |
| --- | --- | --- |
| broker | UID 10000；只读 token Secret、只读 `/tmp/control`；不挂载 workspace | 对 Core 认证；持有权威内存幂等表；通过 Unix socket 转发类型化请求 |
| executor | UID 10001；`/workspace`、`/tmp`、`/tmp/control`；没有 token Secret | 命令和文件操作；没有 TCP 监听；不持有模型/API/kubeconfig 凭据 |

两个容器不共享 PID namespace，拥有独立的根挂载视图。随机 256-bit token 仅出现在 Core 内存、
Sandbox 专属 Secret 和 broker 的只读 Secret 挂载/内存中；broker 不创建 shell，
也不把 Authorization、token 或完整 Core 环境转发给 executor。executor 的 `/proc` 和同 UID
进程只能看到执行侧信息。共享 socket 不保存凭据，其路径由 executor 持有，broker 挂载为只读。
不可信命令可以攻击自身执行服务或伪造执行输出，所以返回内容始终是不可信数据；它不能据此取得 broker token
或清空 broker 的权威去重表。Pod 内 loopback 仍需要 token，不能只依赖 NetworkPolicy。

两个容器均为 non-root、禁止提权、drop ALL、RuntimeDefault seccomp、只读根；
Pod 关闭 SA token、hostNetwork/hostPID/hostIPC/进程共享；无 init/ephemeral 容器和 hostPath。
workspace/tmp 分别为受限 emptyDir，control 为独立 1Mi emptyDir，仍位于 `/tmp/control`。
资源显式 request/limit，加 Namespace ResourceQuota/LimitRange。固定模板校验不接受额外容器或挂载。
这些设置参考 [Pod Security Standards](https://kubernetes.io/docs/concepts/security/pod-security-standards/)，
实际 PodSpec、cgroup、只读根及权限负向探测的结果见 M2 验收报告。

默认出站全部拒绝，无 DNS/下载依赖例外；ingress 只允许指定 Core namespace **且**匹配 Core label/scope
的 Pod 到 broker 端口。NetworkPolicy 是 Pod 级策略，并非每容器策略。
运行任意 shell 的初始 cwd 固定为 `/workspace`，它仍能读取执行容器中允许读取的 `/etc` 等文件；
cwd 不是 shell 的文件访问隔离。若需要更强内核边界，另行安装并验收 gVisor/Kata 的 RuntimeClass。

## Worker 协议与执行结果

`GET /healthz`、`POST /v1/exec`、`GET/PUT /v1/files?path=...`、`GET /v1/dirs?path=...`，
以及用于精确取消的 `POST /v1/cancel`。所有公开端点需 bearer token。
操作包含 JSON envelope：identity 的 sandbox_id/run_id/tool_call_id/attempt、pod_uid、generation、
operation 和相应参数。GET 文件/目录也带同一 JSON body；query 的 path 必须一致。
Core 直接访问 ClusterIP，不经过可能丢弃 GET body 的外部缓存/代理。
模型可见的四个工具 schema 不变，这些身份只由可信调用层构造。

文件 API 拒绝 `..`、workspace 以外的绝对路径和所有符号链接（含指向 workspace 内部的链接）。
每个路径分量均通过已打开的目录 fd 和 `O_NOFOLLOW` 遍历；最终 open、mkdir、目录递归也采用同一边界。
仅接受普通文件，FIFO/设备不进入阻塞读写。此实现覆盖检查后替换符号链接的竞态，
并依赖 workspace 是独立挂载：目录改名不会跨入别的可写文件系统。写入不是事务式/崩溃原子写。
Read 最多 512KiB，Write 最多 1MiB，List 最多 200 条、深度 4；超大目录只收集有界条目后排序。

stdout/stderr 各最多 64KiB，merged_output 另最多 64KiB。收集时持续排空管道但丢弃超过预算的字节，
UTF-8 替换后再次保证编码字节边界。merged_output 是接收 chunk 的观察顺序，不承诺重建两个 pipe 的真实写入顺序。
Local 原有合并流仍保留。命令 stdin 关闭；只注入 PATH/HOME/TMPDIR/LANG。
timeout/cancel 发送 SIGKILL 清理该次 POSIX 进程组并等待收尾；正常非交互命令结束也执行组清理。
主动 `setsid` 脱离进程组不在该组保证内，Pod 删除负责最终 cgroup 生命周期；本次不宣称更强进程沙箱实验结论。

Core 对失去响应的 dispatch 返回 `outcome_unknown`，不会把未知结果或非零退出码变成自动重放。
结果未知会关闭这个 Pod 归属；未来调用需新 Session。连接尚未建立的错误归为 provision_failed，
SDK 对 get/create 的暂时失败可在 provisioning deadline 内重试，工具级远端重试禁用。

排队只覆盖每 key 操作锁，创建/Ready 有独立预算，command 从 subprocess 建立后计时，cleanup 有独立预算。
远端绕过旧的工具外层固定 120 秒 wrapper，各阶段自行限定；首次调用包括 queue、provisioning、
dispatch 前重新核对身份/策略、command、cleanup，不能将总耗时当作 command timeout。
权限等待仍属于 PermissionManager 的预算；未授权不会创建 Pod。

## 幂等、代际与清理

broker 在 dispatch 前原子登记 `{sandbox_id, run_id, tool_call_id}` 与 canonical input hash。
attempt 不改变身份；同键同输入进行中共享 task、终态复用字节结果，同键不同输入冲突。
终态保持到 Pod 销毁，没有 per-entry TTL，没有逐出旧键后重开副作用。
每个新身份预留最多 4MiB 结果及 2KiB 管理开销，终态后改为实际序列化大小计账；满时拒绝新键。
executor 也有辅助内存 journal，broker 表才是可信边界。两者均未持久化，M1 不提供崩溃后 exactly-once。

Pod 使用 `restartPolicy: Never`。Core 核对 API UID、镜像/模板、容器 restart/terminated 状态和
Worker boot generation。UID 消失/改变、Worker 代际失效、TTL 回收、远端取消或 outcome_unknown 后，
Manager 保存 workspace_lost 墓碑，不给旧 Session 分配空工作区。跨 Core 重启恢复和接管工作区留待 M3；
M1 的进程内同 key ensure 可以复用自己的已知创建，未知 incarnation 即使名字相同也拒绝接管。

所有权元数据包含 scope、原始 key、稳定 sandbox_id、随机 incarnation、创建时间、镜像/配置指纹，
由 Core 专属稳定 HMAC key 签名。Label 匹配不等于可删除；扫描还需验证签名、名字/namespace、
记录身份与 API UID。Service 使用精确 Pod UID 的 ownerReference；Pod/Service/Secret 也进入同一幂等清理链。
404 为完成，其他删除错误保留；Pod 使用短暂的优雅删除，避免用强制删除提前抹去 API 身份。
成功 close 等待资源消失，超时继续对账。

启动扫描只删除本 scope、可信签名、签名/API 创建时间均超过 grace 的孤儿；当前进程明确失败/关闭的
pending cleanup 可立即重试。维护周期最多 30 秒，TTL 从最后操作完成的单调时钟计时，
只回收 Ready 且没有排队/执行任务的归属。关闭准入与 TTL 在同一个事件循环门控中竞争。
Root、foreground/background/nested child 共享 key；无 Session 使用根 run_id，child 完成不会销毁共享环境。
close、one-shot、非 durable 断连、shutdown、创建失败/取消都进入清理。

`sandbox.creating/ready/failed/terminating/terminated` 在有实际触发 run 时进入它的 EventWriter。
没有活跃 run 的回收只写 `data_root/sandboxes/lifecycle.jsonl` 与 daemon trace；孤儿资源删除另记
内部 `sandbox.reconcile_deleted`，报告记 `sandbox.reconcile`。不把上一次 run_id 填入 TTL 事件。
M2 已提供量化执行/生命周期回执与 TestRunManifest。完整结果和边界见验收报告。

## 构建、渲染与后续验收

下面示例在 POSIX/WSL 使用。先选定明确的构建来源，记录实际 commit、dirty 状态、基础镜像 digest；
仓库这次仍是未提交 M1，不能把 M0 SHA 当作一个已提交 M1 的构建证明。

```bash
# 镜像名/tag 只是构建入口；部署参数必须使用实际产生的 RepoDigest。
docker build -f src/agent_runtime/sandbox_server/Dockerfile -t "$WORKER_TAG" .
docker build -f deploy/kubernetes/Core.Dockerfile -t "$CORE_TAG" .
# 按所选 registry/build 流程发布并记录真实 WORKER_IMAGE / CORE_IMAGE 的 @sha256 引用。
# kind 的 containerd 和 Docker Desktop 镜像存储分开，需导入/拉取匹配摘要的镜像。

uv run python scripts/render_kubernetes.py \
  --scope agentrt-dev --worker-image "$WORKER_IMAGE" --core-image "$CORE_IMAGE" \
  --output .agentrt/kubernetes
```

渲染默认把网络确认设为 false，Core 启动会拒绝；CNI 独立验收后才重新渲染并加
`--network-policy-verified`。运行前以所选 **明确 context** 部署 namespaces、RBAC、limits、policies，
再创建两个 Core Secret：`agentrt-ownership` 的 key 为 `key`（至少 32 随机 bytes，妥善保存、不轮转丢失），
`agentrt-llm` 的 key 为 `ANTHROPIC_API_KEY`。使用受控文件/Secret 管理流程导入，
不要将实际凭据写入 YAML、仓库、argv 或展示日志。最后应用 Core 模板。
Worker Pod/Service/Secret 由 Core 动态创建，`sandbox-specs.json.txt` 仅供审阅固定模板。

```bash
kubectl --context "$TARGET_CONTEXT" apply -f .agentrt/kubernetes/namespaces.yaml
kubectl --context "$TARGET_CONTEXT" apply -f .agentrt/kubernetes/rbac.yaml
kubectl --context "$TARGET_CONTEXT" apply -f .agentrt/kubernetes/limits.yaml
kubectl --context "$TARGET_CONTEXT" apply -f .agentrt/kubernetes/network-policy.yaml
# 此处先准备上述 Core Secrets。
kubectl --context "$TARGET_CONTEXT" apply -f .agentrt/kubernetes/core.yaml
kubectl --context "$TARGET_CONTEXT" -n kitagent-core port-forward service/agentrt-core 7437:7437
# 外部 CLI 使用自己的 Local 配置，只连接端口转发的 Core：
AGENTRT_SANDBOX_BACKEND=local AGENTRT_HOST=127.0.0.1 AGENTRT_PORT=7437 agentrt chat
```

M2 验收采用 `scripts/sandbox_m2_build.sh`：冻结包含 dirty 改动的源码快照，构建并核对三个真实
digest，再导入 kind。Validation 镜像额外包含 `build-source.json` 和测试依赖，启动时逐文件验证来源。
随后由 `sandbox_m2_cluster.py` 运行 CNI/准入探测，`sandbox_m2_run.py launch` 校验单 Core、集群身份、
配置与镜像绑定，并给 Job 分配不可变的环境 ConfigMap。具体命令见 M2 验收报告。

Job 暂时承担唯一 Core 身份；`run_sandbox_validation.py --enable-kubernetes --stage all` 先跑 M1，
再跑 M2。JUnit、样本与回执在 `/artifacts`，以带 SHA256 的压缩包写入 Job 日志，`collect` 负责校验和还原。
不需要 pods/exec、kubectl cp 或真实 LLM。每个 Job 使用新名字，不能把旧完成对象当作新实验。

M1/M2 的本机真实验收已完成。后续 durable 恢复与跨 Core 接管属于 M3，未包含在这些通过数中。
