# M2 开工交接（2026-09-09）

> 本文保留 M2 开工前的历史基线。后续已构建镜像并执行真实集群实验，当前结果、原始证据与
> 复现入口以 [M2 验收报告](sandbox-m2-validation.md)为准。

用户最新指令：**修复阻断 M2 的问题后，新开任务执行 M2**。本轮已进入验证阶段；
旧 M0/M1 记录中的“写完代码别测试”是原交付约束，不能用它跳过本次已授权的测试和 M2 验收。

工作目录为 `C:\Users\zed\Desktop\kamaAgent\Agent-Runtime-Kit`，对应
`/mnt/c/Users/zed/Desktop/kamaAgent/Agent-Runtime-Kit`。当前分支 `feat/sandbox-m0`，HEAD 为
`8780693325fe27b4bfb56a792e412bcfd1897fff`。M1 及本轮修复仍是工作区改动，包含未跟踪文件，
未提交或推送。新任务直接继续此工作区，不能从旧 main 或仅从 HEAD 重做实现。

## 已修复的五项问题

1. **TTL 后继续聊天**：Runner 在已建立的 Run 事件边界内获取懒 facade，捕获 `SandboxError`，
   输出带 `workspace_lost` 和新建 Session 提示的失败 `run.finished`。Session 完成该次失败 Run
   的元数据收尾，不再返回通用 IPC Internal error，也不重建旧工作区。
2. **正常 shell 退出后的进程组回收**：Worker 观察独立于管道 EOF 的 `Process.returncode`，
   shell 退出即进入组清理，随后在 cleanup 预算内排空输出。`sleep 60 &` 不再误等超时；
   无 `wait` 的后台延迟写入会被终止。主动 setsid 逃逸仍属于文档中列明的 Pod 最终回收边界。
3. **文件操作超时预算**：Worker 协议 `Operation.response_timeout_s()` 统一 Core 与 broker
   的请求预算，文件使用独立 10 秒，命令使用本次请求 timeout；health/cancel 也显式限定。
4. **scope YAML 类型**：Core、NetworkPolicy、validation Job 中所有 scope 插槽均作为字符串
   引用，合法的 `123`、`true`、`null` 等值不会被 YAML 隐式转换。
5. **输出截断标记**：UTF-8 replacement 膨胀后的二次截断纳入 `Output.truncated`，并显示提示。

回归集中在 `tests/unit/test_sandbox_m1_regressions.py`。测试实际使用 POSIX 子进程、
broker 的 Unix-socket HTTP dispatch、渲染器及 Core IPC→Session→Runner 链，覆盖原故障路径。

## 验证过程与当前证据

- 新增用例修复前：11 failed、1 passed；五项原故障均复现。修复后：12 passed。
- Sandbox 相关扩大回归初跑：179 passed、2 failed。两个测试前置条件已修正：Local 回收测试
  先创建可读 marker；TTL 测试使用相对单调时钟，避免 WSL 刚启动不足 900 秒时误判。
- 全量离线初跑暴露旧测试仍向 Runner fallback 注册表注入子任务。M0 已改为按 Run 管理，
  旧测试分别失败或在清理中挂起；该次运行在 420 passed、1 failed、14 deselected 时中断。
  两个测试文件已改接实际 Run 注册表并补测试清理，定向复跑 3 passed。
- 已处理首次运行质量门发现的 5 项类型错误，以及 M0/M1 文件的格式和 import 问题。
- `ruff check src tests scripts`：通过。
- `ruff format --check src tests scripts`：通过，217 个 Python 文件。
- `mypy src`：通过，120 个 source files。
- `python scripts/check_wire_protocol.py --check`：通过。
- 全量离线最终结果：**731 passed、14 deselected，88.02 秒**。使用
  `-m 'not integration and not kubernetes'` 排除需真实 LLM/集群的显式标记用例。
  JUnit 为 `artifacts/sandbox-validation/m1-pre-m2-unit.xml`（已被 Git 忽略）。
- 最终 `git diff --check` 通过；源代码/测试修改后已再次确认 Ruff 与格式检查通过。

**尚未构建 Worker/Core 镜像、部署集群资源或执行 Kubernetes/CNI/资源/性能验收。**
离线回归不代表 M1 集群功能验收或 M2 隔离验收通过。没有调用真实 LLM。

## 可复用的 WSL 测试环境

Windows 原 `.venv` 是 Windows Python，未覆盖。另建了 Linux 环境：

- WSL：`Ubuntu-20.04`，用户 `root`。
- Python：`/root/.local/share/agentrt-venvs/sandbox-m2/bin/python`，CPython 3.12.13。
- uv：`/root/.local/bin/uv`（非 login shell 的 PATH 未必包含它）。
- `UV_PROJECT_ENVIRONMENT=/root/.local/share/agentrt-venvs/sandbox-m2`。
- 使用锁文件安装了 dev、`kubernetes`、`sandbox-worker`、`graph-sqlite` extras。

从仓库目录的 PowerShell 运行：

```powershell
wsl.exe -d Ubuntu-20.04 -u root --exec env UV_PROJECT_ENVIRONMENT=/root/.local/share/agentrt-venvs/sandbox-m2 /root/.local/bin/uv sync --project /mnt/c/Users/zed/Desktop/kamaAgent/Agent-Runtime-Kit --frozen --extra kubernetes --extra sandbox-worker --extra graph-sqlite
wsl.exe -d Ubuntu-20.04 -u root --exec env ANTHROPIC_API_KEY= /root/.local/share/agentrt-venvs/sandbox-m2/bin/python -m pytest tests/ -m 'not integration and not kubernetes' -q --tb=short
```

WSL 仍提示 G 盘挂载失败，本次 Python 与离线回归可用；该提示不代表 Kubernetes 不可用。

## 新任务的 M2 范围

以 `kubernetes-sandbox-prd-v1.md` §11、§11.2、M2 里程碑及 `CONTRIBUTING.md` 为准：

1. 先读取外层 `AGENTS.md` 并检查现有 Docker/kind 状态，明确 kubeconfig/context，核查 CNI
   是否实际执行 NetworkPolicy。已有 `wsl-k8s`/kindnet 只是历史快照，当前尚未做集群探测。
2. 在具备实际网络隔离的环境构建真实固定 digest 镜像，先完成 M1 功能 E2E，再执行 M2 的
   Pod Security、RBAC、Quota/LimitRange、网络、路径/跨 Session、资源、并发和回收实验。
3. 若现有集群不能满足隔离前置条件，可建立专用、隔离的 M2 kind 验收集群；保留原集群及
   `default/web` 示例和数据，不能为图方便删建原集群。CNI attestation 必须由真实探测支持。
4. 实现生命周期/执行 receipt、完整 TestRunManifest、原始样本和可复现实验报告/演示脚本。
   失败样本保留在分母，记录真实硬件/软件/CNI/镜像/并发/时间口径，按 PRD 判断达标情况。
5. 当前 HEAD 仅代表 M0。构建证明必须注明 dirty 状态并记录实际源码快照/摘要，不能把 M0
   SHA 当作已提交 M1/M2 的证明。现有 validation 入口仅收 commit，必要时完善来源记录。
6. 使用离线 Provider，不依赖真实模型调用。凭据仅进入受控文件/Secret，不写到参数、仓库或
   展示日志；同 scope 保持单 Core/Job owner，清理只针对本轮明确创建的资源。
7. M3 durable 恢复、持久 Worker journal、多 Core 租约等继续保持阶段边界。发现影响 M2
   验收的真实问题应修复并复测；未完成的指标须明确标为失败/未完成，不能填造成功数字。

保留用户原有 `.cursor/`、`docs/interview-prep.md`、`docs/sandbox-m0-review.md`。本轮未请求提交或推送。
