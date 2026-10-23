# KitAgent 浏览器工作台

工作台通过本机 FastAPI 网关连接真实 Core，提供多轮聊天、工具时间线、会话权限与人工审批、子任务记录、运行报告和浏览器刷新恢复。当前支持 **Loop + Local**；Core 的执行、权限和消息提交仍是权威。

## 会话权限

输入框旁的权限菜单提供三种模式，新会话默认“按需询问”：

| 模式 | 行为 |
| --- | --- |
| 只读 | 只允许 `read_file`、`list_dir`；拒绝 Shell、文件写入、笔记、MCP 和子 Agent 等其他工具。它是工具级限制，不是操作系统沙箱。 |
| 按需询问 | 沿用 Core 工具策略。审批卡片勾选“记住本会话的此工具授权”后，该工具在本会话内的后续调用无需重复批准。 |
| 自动批准 | 经明确确认后，当前等待和后续工具调用自动通过；显式拒绝策略仍优先。Local 可操作宿主文件和网络，不限于项目目录。 |

记住工具授权允许同一工具的不同参数与路径，例如 `bash` 会允许本会话的任意命令，不会逐条判断命令风险；原有显式拒绝规则仍有效。权限菜单列出已记住的工具，可逐项撤销。切换模式会清除已记住的工具授权；已开始执行的操作不会被撤销。

设置只保存在 Core 内存中，以会话为单位，主任务和子任务共用；不会写入全局 `policy.toml`。浏览器刷新不影响授权，会话关闭、Core 断连或重启后清除，新会话不会继承。权限更改由 Core 校验归属和版本；过期页面不能覆盖较新的决定。旧 Core 未声明权限模式能力时，页面保留单次审批。

## 构建与启动

运行时需要 Linux、Python 3.12 和 uv。Windows 使用 WSL2；前端可以在 Windows 或 WSL 构建，需要 Node 22.12 或以上版本。不要在两种系统之间复用同一份 node_modules，切换构建系统时重新执行 npm ci。

在项目的 `frontend` 目录构建静态资源：

```bash
npm ci
npm run build
```

构建产物写入 `src/agent_runtime/web/static`，由 Python 网关同源提供，不需要另开 Node 服务。构建产物不纳入 Git；源码安装后先构建再运行。没有构建时网页会显示具体操作提示。

在 Linux 或 WSL 的项目根目录安装可选 Web 依赖，uv 默认使用项目内的 `.venv`：

```bash
uv sync --frozen --extra web --dev
```

终端一启动 Core，模型设置沿用现有服务端配置：

```bash
uv run --frozen --extra web agentrt-core
```

终端二在相同项目与环境启动网关：

```bash
uv run --frozen --extra web agentrt-web
```

使用终端输出的 `http://localhost:8765/#bootstrap=…` 启动链接在浏览器打开。一次性口令换取 HttpOnly 本机 Cookie，地址中的口令随即移除；同一浏览器之后可直接访问 localhost:8765。网关重启会换身份和口令，必须使用新链接。不同浏览器配置文件不共享身份。

网关默认只监听 127.0.0.1，默认连接 Core 的 7437 端口，不使用现有示例服务的 8080。可用 `--core-port` 指向其他本机 Core；P0 拒绝 Graph、Kubernetes 和旧协议组合。

## 无 API Key 的真实工具演示

以下命令在 WSL 项目根目录执行。每次演示使用全新的目录；已有目录会被拒绝，避免覆盖用户文件。

```bash
demo_workspace="$(mktemp -d)/workspace"
uv run --frozen --extra web python -m agent_runtime.web.demo \
  --port 7438 --directory "$demo_workspace"
```

另一个终端启动：

```bash
uv run --frozen --extra web agentrt-web --core-port 7438
```

新建会话，输入“检查样例 Python 仓库，执行检查脚本并生成检查报告”。允许检查命令和报告写入两次审批后，可以读到报告，并从结果面板导出运行报告。拒绝命令时检查不会执行。输入含“子任务”会触发真实 `spawn_agent`；输入“模拟工具错误”会访问不存在的样例文件，以观察工具错误与运行终态的区别。

这是脚本化 Provider 与真实 Core、TCP、权限、文件和 Shell 工具组合，界面明确标为离线演示。每个 run 使用独占创建的检查标记，重复命令会失败；测试据此判断有没有重复副作用。

## 刷新与报告的语义

- 浏览器连接只负责观察，网关持有 Core TCP。网关、Core 和该连接持续存活时，刷新不会终止或重新执行任务。
- 命令有 request ID，接收状态未知时查询原请求。创建响应丢失、尚无会话 ID 时也能查询；不会自动重发。
- 查询不到原请求时，可以继续查询或选择“解除等待（不重发）”。该操作仅解除本页提交锁并保留草稿，不取消原请求，也不代表它一定未执行；确认会话状态后再决定是否提交新任务。
- 等待发送回执期间可以编辑下一条草稿。回执只清除本次提交后未被修改的草稿版本；新编辑的内容及其本地存储会保留。
- 快照带实例、会话和序号，后续增量连续衔接。旧订阅不覆盖新会话，慢浏览器需要重新获取快照。
- Core 断线后，普通会话不能恢复执行。Graph/SQLite 的进程恢复属于后续 P1。
- 运行报告来自 Core 按 run 绑定的已提交结果，加上工具与审批记录。它与 Agent 写入磁盘的 `report.md` 是两个对象。
- 报告中的任务名称来自该 run 对应的提交请求，侧栏会话标题保持独立；多轮对话不会使后续报告沿用首轮任务名称。
- 下载报告不会执行工具或读取任意文件。每项工具输出最多 4 KiB，报告输出合计最多 64 KiB，截断会标注。
- “结束会话”会停止其中的工作，不等于暂停或只停止本轮。

## 容量与本机边界

网关单进程运行，不配置多个 Uvicorn worker。最多 5 个未关闭会话，当前实例最多登记 25 个会话；请求登记达到 1,000 条后拒绝新任务，关闭操作保留空间。满额后先结束任务，再重启网关。

每会话保留最近 100 条消息、200 条工具详情、10 份报告，展示预算 4 MiB；控制摘要另限 1 MiB 和 1,000 次调用，待审批最多 64 项。报告和摘要超额时明确提示并结束会话，不能继续执行却丢掉控制记录。空闲且无人观察、没有运行或审批的会话 30 分钟后关闭。

所有 Web 请求检查 Cookie 与归属，修改请求检查 Origin、Host、CSRF 和请求大小。模型 Key 与恢复凭证没有浏览器设置入口。Local 工具仍然作用于 Core 的宿主工作目录，不提供文件系统隔离。

## 开发与验证

开发服务器可使用 Vite 代理。先给网关显式增加开发来源：

```bash
uv run --frozen --extra web agentrt-web --dev-origin http://127.0.0.1:5173
```

在前端运行 `npm run dev`，将启动链接中的端口改为 5173、主机改为 127.0.0.1，保留首次口令。API 和 WebSocket 均经 Vite 同源代理。

Python 测试用真实子进程 Core 和 TCP，不要求真实模型或 Kubernetes：

```bash
uv run --frozen --extra web pytest tests/unit/test_web_gateway.py \
  tests/unit/test_web_projection.py tests/unit/test_web_run_result.py \
  tests/unit/test_web_session.py tests/unit/test_session_permissions.py -q
```

前端状态测试与构建：

```bash
cd frontend
npm test
npm run build
npx playwright install chromium
```

Linux 浏览器验收：设置 `AGENTRT_TEST_PYTHON` 为已安装项目和 web extra 的 Python 3.12 解释器，然后 `npm run test:e2e`。Windows PowerShell 示例：

```powershell
$env:AGENTRT_TEST_PYTHON = '/path/to/agent-runtime-kit/.venv/bin/python'
$env:AGENTRT_TEST_WSL = 'Ubuntu-20.04'
npm run test:e2e
```

验收脚本启动自己的临时 Core/网关，测试完成后停止，不复用生产会话或已有集群。自动验证创建响应丢失后的查询、审批刷新、run 身份、报告下载及三种桌面宽度，并生成 README 截图。截图中的路径来自合成临时目录。

默认浏览器验收在输出、审批和工具执行三个阶段各刷新 10 次；测量只使用浏览器自身时钟。设置 `AGENTRT_RECORD_DEMO=1` 再运行同一命令可录制约 46 秒演示，生成 `docs/assets/web-workbench-demo.webm`；录制模式用一次刷新，测量记录与压力验收分开保存。

只运行请求与草稿相关的浏览器回归可使用 `npx playwright test -g 'regression:'`。设置 `AGENTRT_ASSETS_DIR=../artifacts/web-validation/review-fixes` 可将完整验收的截图写入忽略目录，避免覆盖已归档的 README 素材。

## 实现入口

| 入口 | 作用 |
| --- | --- |
| [Web 网关](../src/agent_runtime/web/gateway.py) | 请求身份、会话归属、连接与容量 |
| [Web 会话](../src/agent_runtime/web/session.py) | 长 RPC、审批、同步和订阅生命周期 |
| [展示投影](../src/agent_runtime/web/projection.py) | 事件映射、实体更新、冻结报告与容量 |
| [HTTP 与 WebSocket](../src/agent_runtime/web/app.py) | 同源访问控制、限定接口与静态资源 |
| [前端状态](../frontend/src/useWorkbench.ts) | 浏览器订阅、重连、请求查询和会话切换 |
| [工作台](../frontend/src/App.tsx) | assistant-ui 外部状态适配、聊天、执行与结果 |
| [Core 协议](../WIRE_PROTOCOL.md) | `core.describe` 和 owner-only `run.get_result` |
