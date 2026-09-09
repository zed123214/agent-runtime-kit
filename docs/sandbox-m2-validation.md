# Kubernetes Sandbox M2 验收记录

状态：**M2 实施与本机真实验收完成，通过。** 最终运行时间为 2026-09-09 01:59:13–02:05:56（Asia/Shanghai），使用离线 Provider，没有调用真实 LLM。工作区未提交、未推送。

## 最终验收结果

| 项目 | 实测 | PRD 目标 / 口径 |
| --- | --- | --- |
| M1 功能 E2E | 13/13，69.30 s | 真实 Pod/Service、HTTP、文件、聊天、子 Agent 与退出清理 |
| M2 实验 | 442/442，12 类 | 失败样本单独保留，见下方修复历程 |
| 工作区连续性 | 30/30 | write → exec → read 逐步断言 |
| 并发隔离 | 20 个 Session、200 次越权读写，泄漏 0 | 每 Session 5 读 + 5 写；另核对各自私有文件，并探测跨 Pod TCP |
| Session 创建/回收 | 100/100；重复活跃 Pod 0；最终残留 0 | 含首次并发调用、重复 close、65 次 close 与 35 次真实 TTL |
| Cold Ready | 30 次，p95 **3524.51 ms** | ≤15000 ms；镜像已导入，采样并发 1 |
| Ready 后执行开销 | 100 次，p95 **19.26 ms** | ≤500 ms；Core 总耗时减 Worker 报告的命令耗时 |
| close/TTL 回收 | 100 次，p95 **6656.30 ms** | ≤60000 ms；TTL 样本包含 4 s 闲置等待与维护间隔 |
| 命令超时 | 5/5；最慢 **1020.28 ms** 进入终态 | timeout=1 s，要求 ≤3 s；3 s 后再次确认延迟 marker 不存在 |
| CPU | 4 个竞争进程、4 s 负载，节流周期增加 **41** | 实际读取 cgroup `cpu.stat`，CPU limit=1 |
| OOM | `OOMKilled` + `workspace_lost`；未重放 | 保存同一 Pod UID 的容器终态；命令结果仍标为未知 |
| 存储 | 576 MiB 写入触发 512 MiB emptyDir 驱逐；观察约 **55.41 s** | 保留 Succeeded/Completed Pod 状态及同 UID 的 kubelet Evicted 事件 |
| 孤儿扫描 | 真实子 Core 进程直接退出后，回收 Pod/Service/Secret **3/3** | 仅测试启动扫描，不宣称 durable 恢复 |
| 独立网络验证 | 7 类 × 3 次，**21/21** | Core 允许；错误标签/命名空间入口、受控目标/API 出站拒绝 |
| RBAC / 准入 | **18/18** | pods/exec 等越权拒绝；Pod Security 与 LimitRange 负向准入 |
| Pod 安全 | UID 10001、NoNewPrivs=1、CapEff=0、Seccomp=2、只读根、无 SA/broker token | 运行态探测与实际 PodSpec；六个未认证端点均 401 |
| 离线回归与质量门 | **742 passed、14 deselected，91.09 s** | Ruff、format、mypy（120 files）、wire、diff 检查通过 |
| 演示脚本 | 实际运行完成，M1 13/13，残留 0 | `demo_kubernetes_sandbox.sh`，Job `agentrt-m2-demo-20260909-04` |

p95 采用 nearest-rank。性能数据仅代表本报告的单节点、固定镜像与资源配置；未执行多节点、真实模型延迟或生产负载测试。

最终证据目录为 `artifacts/sandbox-validation/m2-final-20260909-04/agentrt-m2-final-20260909-04/`：

- [TestRunManifest](../artifacts/sandbox-validation/m2-final-20260909-04/agentrt-m2-final-20260909-04/manifest.json)
- [逐次样本](../artifacts/sandbox-validation/m2-final-20260909-04/agentrt-m2-final-20260909-04/m2/samples.jsonl)
- [生命周期与执行回执](../artifacts/sandbox-validation/m2-final-20260909-04/agentrt-m2-final-20260909-04/m2/receipts.jsonl)
- [M1 JUnit](../artifacts/sandbox-validation/m2-final-20260909-04/agentrt-m2-final-20260909-04/m1-junit.xml)
- `operator/` 保存了该次验收的 CNI、RBAC/准入、资源配置、镜像检查、宿主信息和离线 JUnit 副本；后续演示不会改写这份证据。

实际构建源码快照 SHA256：`2fe6c8677d040625cecd855dfcacf7c60ce0a10d4b10443aec13fb3fb1bf28b3`。该快照包含未提交源码；验证后仅更新交付文档。

| 镜像用途 | 实际 digest |
| --- | --- |
| Worker | `sha256:8a395f37aaee53d61a0220410145b9535f05fcd7cddfd8e9e06d559fec18f32b` |
| 执行 Core 逻辑的 Validation Job | `sha256:16c559f977cf733f99e2c1e8d6ef725c3cace6ac36ddc118764844d1e04f7b9c` |
| 同源发行 Core 镜像 | `sha256:e8925fe88ae48dd72cc16ae82e210b8fbed752b0f66d4116ed3fef2dae09ee83` |

## 来源与环境

- 仓库 `feat/sandbox-m0`，HEAD `8780693325fe27b4bfb56a792e412bcfd1897fff`。
- M1/M2 为未提交工作区实现。镜像使用 `scripts/snapshot_sandbox_source.py` 保存实际源码、逐文件 SHA256、汇总 SHA256、dirty 状态与压缩快照；HEAD 只说明已提交基线。
- 原有 `wsl-k8s` 与 `default/web` 保留。独立验收集群为 `agentrt-sandbox-e2e`，context 为 `kind-agentrt-sandbox-e2e`，kubeconfig 为 WSL `/root/.kube/agentrt-m2.config`。
- Windows 10 Pro build 19045 / WSL Ubuntu-20.04，AMD Ryzen 5 9600X，6 核/12 逻辑 CPU；Windows 可见约 31.10 GiB，Docker/WSL 可见 16,293,437,440 bytes（约 15.17 GiB）。
- kind v0.33.0，单节点 Kubernetes v1.36.4，Cilium 1.20.1。每次验收重新探测 CNI，不复用跨集群的旧成功结果。
- Docker Desktop 4.67.0.222858、Engine 29.3.1、cgroup v2。原验证集群作为背景负载保留；不把本次单节点结果推广到多节点或生产负载。
- WSL 离线测试使用 Python 3.12.13；固定 Python 基础镜像中的 Python 为 3.12.14。Worker、Core、Validation 均以真实 manifest digest 导入 kind，未推送镜像。

## 修复历程与失败样本

- 候选 01 的 M1 真实 E2E：13/13，69.97 秒。源码快照为 `cd1fc52ca6985e561c6cf2ff0a3e90ac32bd4c9cd65c034fbe74ae8a2734f094`。
- 候选 01 的 M2 基线：441 个尝试，439 通过、2 失败，结束时 Sandbox Pod/Service/Secret 均为 0。
- 基线冷启动 30 个样本 p95 3569.55 ms，Ready 后 100 次空命令的执行开销 p95 20.16 ms，100 次关闭/TTL 回收 p95 6684.65 ms。
- 基线失败为 OOM 后 Core 收到 outcome_unknown，以及存储实验错误地复用已丢失的工作区。未删掉失败样本，也未以资源参数存在代替资源实测。
- 候选 02：M1 13/13；M2 441/442，结束时无残留。OOM 分类和真实进程退出后的孤儿扫描通过；存储已实际驱逐，但 Pod 最终显示 Succeeded，旧识别逻辑漏认，同 UID 的原始 Pod 与 kubelet Event 已保存。
- 修复后定向回归逐步扩大到 41 条；最终全量离线结果为 742 passed、14 deselected。中间 740 条回归及更早的 731 条基线报告也保留。

原始证据位于 `artifacts/sandbox-validation/`。目录包括失败的构建日志、源码快照、镜像检查、CNI/准入原始响应、Job 日志、JUnit、执行/生命周期回执与逐次样本。该目录被 Git 忽略，应另行备份验收材料。

## 实现变化

执行回执保留调用身份、请求参数摘要、有限输出摘要、退出码/信号、排队/创建/操作耗时、策略和资源信息。输入摘要版本为 `request-arguments-v1`，针对请求参数，先于后端超时策略裁剪；它与 Worker journal 的规范化协议摘要分别定义。输出摘要针对有界、解码后的 UTF-8 输出，不声称涵盖被丢弃的尾部。回执不复制命令、文件内容或凭据。

OOM 若只杀死命令，可由进程退出与 cgroup `oom_kill` 增长共同确认；若执行服务也被杀死，Core 在清理前有界观察 Kubernetes 容器 `OOMKilled` 状态，记录 `workspace_lost` 与资源原因。此时命令副作用仍可能未知，不能重放。存储超限按实际 kubelet 驱逐记录 `Evicted`，不要求写入系统调用立即返回 ENOSPC。本机实测存在“Evicted 事件 + Succeeded/Completed 最终 Pod 状态”，后端按不可变 Pod UID、事件种类与 kubelet 来源关联驱逐证据，并保留实际 Pod phase。

基于 review 技能完成了独立的规范与 PRD 审阅。修复了旧 CNI 证据复用、环境 ConfigMap 混用、失败探测输出丢失和异常退出实验的子进程收尾。每个 Job 的环境 ConfigMap 不可变，启动前核对镜像/源码、集群 UID、CNI 配置与策略。发行 Core 镜像另行构建；实测 Core 代码运行在 Validation Job 中，因此最终 Manifest 的 `core_image_digest` 记录实际 Validation 镜像，`core_release_image_digest` 单列发行镜像。Job 使用 2 CPU / 1 GiB 上限。

## 复现入口

在 WSL 中从仓库运行，显式指定目标 kubeconfig/context：

```bash
export AGENTRT_BUILD_PYTHON=/root/.local/share/agentrt-venvs/sandbox-m2/bin/python
bash scripts/sandbox_m2_build.sh <new-run-id>
$AGENTRT_BUILD_PYTHON scripts/sandbox_m2_cluster.py --kubeconfig /root/.kube/agentrt-m2.config --context kind-agentrt-sandbox-e2e --build-run <new-run-id>
$AGENTRT_BUILD_PYTHON scripts/sandbox_m2_run.py launch --kubeconfig /root/.kube/agentrt-m2.config --context kind-agentrt-sandbox-e2e --build-run <new-run-id> --job <unique-job-name> --stage all
# Job 结束后收集；不需要 pods/exec 或 kubectl cp。
$AGENTRT_BUILD_PYTHON scripts/sandbox_m2_run.py collect --kubeconfig /root/.kube/agentrt-m2.config --context kind-agentrt-sandbox-e2e --build-run <new-run-id> --job <unique-job-name>
```

`launch` 校验集群/CNI/策略与源码绑定，拒绝已有活动 Core，并为 Job 创建不可变的环境 ConfigMap。重试构建复用冻结快照；修改代码后必须使用新的 run-id。

已有镜像时，可运行演示脚本，实际经过聊天多轮、write/exec/read、foreground/background/nested child 共享和关闭清理：

```bash
export AGENTRT_M2_KUBECONFIG=/root/.kube/agentrt-m2.config
export AGENTRT_M2_CONTEXT=kind-agentrt-sandbox-e2e
bash scripts/demo_kubernetes_sandbox.sh <build-run> <new-demo-job>
```

需要从零建立独立验收集群时，使用固定节点镜像（保留原集群及其 kubeconfig）：

```bash
kind create cluster --name agentrt-sandbox-e2e --kubeconfig /root/.kube/agentrt-m2.config \
  --config deploy/kubernetes/kind.yaml \
  --image kindest/node:v1.36.4@sha256:099e049362a1526b2db71494e1947aae99bd16290d7c895f2b7ea312e3cbfaed --retain
helm install cilium ./cilium-1.20.1.tgz --namespace kube-system \
  --kubeconfig /root/.kube/agentrt-m2.config --kube-context kind-agentrt-sandbox-e2e \
  --set image.pullPolicy=IfNotPresent --set ipam.mode=kubernetes \
  --set operator.replicas=1 --set kubeProxyReplacement=false \
  --set socketLB.enabled=false --set envoy.enabled=false
kubectl --kubeconfig /root/.kube/agentrt-m2.config --context kind-agentrt-sandbox-e2e \
  -n kube-system rollout status daemonset/cilium --timeout=300s
```

本次 Helm 为 3.19.0，安装包已按官方 SHA256 文件验证。Cilium chart 来自
[官方 Helm 仓库](https://helm.cilium.io/cilium-1.20.1.tgz)，实测 SHA256 为
`06210eef7c23d15f7699c79e2fe3a1ec9c389024c5c5c006ea04022d322449a2`。
配置参考 [Cilium kind 安装](https://docs.cilium.io/en/stable/installation/kind/)；
[兼容列表](https://docs.cilium.io/en/stable/network/kubernetes/requirements/)包含 Kubernetes 1.36。

## 阶段边界

M3 的 durable 恢复、持久 Worker journal、多 Core 租约与 PVC/快照未交付。Local 仍不具备物理隔离，Pod 使用普通 containerd/runc，未宣称 gVisor/Kata 的内核隔离能力。进程组清理不覆盖主动 setsid 逃逸；Pod 回收提供最终生命周期边界。
