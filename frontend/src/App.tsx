import {
  memo,
  useEffect,
  useMemo,
  useRef,
  useState,
  type ReactNode,
} from "react";
import {
  AssistantRuntimeProvider,
  MessagePrimitive,
  ThreadPrimitive,
  useExternalStoreRuntime,
} from "@assistant-ui/react";
import ReactMarkdown from "react-markdown";
import remarkGfm from "remark-gfm";
import {
  ArrowDown,
  ArrowRight,
  Check,
  CheckCheck,
  ChevronDown,
  ChevronRight,
  Circle,
  Clock3,
  Copy,
  Download,
  FileText,
  GitBranch,
  LoaderCircle,
  Menu,
  MessageSquare,
  MoreHorizontal,
  Plus,
  Radio,
  Send,
  ShieldCheck,
  Sparkles,
  SquareTerminal,
  X,
} from "lucide-react";
import { useWorkbench } from "./useWorkbench";
import { labels } from "./state";
import { PermissionControl, decisionLabels } from "./PermissionControl";
import type {
  Approval,
  ApprovalDecision,
  Message,
  PermissionChange,
  Report,
  Tool,
  View,
} from "./types";

const errorText = (cause: unknown) =>
  cause instanceof Error ? cause.message : "操作失败";

function CodeBlock({ children }: { children?: ReactNode }) {
  const ref = useRef<HTMLPreElement>(null);
  const [copied, setCopied] = useState(false);
  return (
    <div className="code-block">
      <button
        className="copy-code"
        aria-label="复制代码"
        onClick={() => {
          void navigator.clipboard
            .writeText(ref.current?.textContent || "")
            .then(() => setCopied(true));
        }}
      >
        {copied ? <Check size={14} /> : <Copy size={14} />}
      </button>
      <pre ref={ref}>{children}</pre>
    </div>
  );
}

const Markdown = memo(function Markdown({ text }: { text: string }) {
  return (
    <div className="markdown">
      <ReactMarkdown
        skipHtml
        remarkPlugins={[remarkGfm]}
        components={{
          pre: CodeBlock,
          a: ({ children, href }) => (
            <a href={href} target="_blank" rel="noopener noreferrer">
              {children}
            </a>
          ),
          img: ({ alt }) => <span>[图片：{alt || "不加载外部图片"}]</span>,
        }}
      >
        {text}
      </ReactMarkdown>
    </div>
  );
});

function UserMessage() {
  return (
    <MessagePrimitive.Root className="user-message">
      <div className="message-label">你</div>
      <MessagePrimitive.Parts components={{ Text: Markdown }} />
    </MessagePrimitive.Root>
  );
}
function AssistantMessage() {
  return (
    <MessagePrimitive.Root className="assistant-message">
      <div className="message-label">
        <span className="mini-logo">
          <Sparkles size={13} />
        </span>
        KitAgent
      </div>
      <MessagePrimitive.Parts components={{ Text: Markdown }} />
    </MessagePrimitive.Root>
  );
}

function StatusIcon({ status, size = 16 }: { status: string; size?: number }) {
  if (status === "success" || status === "ready" || status === "resolved")
    return <CheckCheck size={size} />;
  if (["failed", "denied", "lost", "sync_error", "unknown"].includes(status))
    return <X size={size} />;
  if (status === "approval" || status === "pending")
    return <ShieldCheck size={size} />;
  if (
    ["running", "processing", "syncing", "submitting", "closing"].includes(
      status,
    )
  )
    return <LoaderCircle size={size} className="spin" />;
  return <Circle size={size} />;
}

function Chat({
  view,
  connected,
  busy,
  onSend,
  onPermissions,
}: {
  view: View;
  connected: boolean;
  busy: boolean;
  onSend: (text: string) => Promise<boolean>;
  onPermissions: (change: PermissionChange) => Promise<boolean>;
}) {
  const key = `agentrt:draft:${view.instance_id}:${view.session_id}`;
  const [draft, setDraft] = useState(sessionStorage.getItem(key) || "");
  const draftRevision = useRef(0);
  const mounted = useRef(true);
  const [newContent, setNewContent] = useState(false);
  const [sendError, setSendError] = useState("");
  const scrolling = useRef<HTMLDivElement>(null);
  const follow = useRef(true);
  const composing = useRef(false);
  useEffect(() => {
    mounted.current = true;
    return () => {
      mounted.current = false;
    };
  }, []);
  function editDraft(text: string) {
    draftRevision.current++;
    setDraft(text);
    sessionStorage.setItem(key, text);
  }
  const messages = useMemo(() => Object.values(view.messages), [view.messages]);
  const running = ["running", "approval", "syncing"].includes(view.state);
  const runtime = useExternalStoreRuntime<Message>({
    messages,
    isRunning: running,
    isDisabled: !connected || view.state !== "ready",
    convertMessage: (message) => ({
      id: message.id,
      role: message.role,
      content: [
        {
          type: "text",
          text:
            message.text + (message.truncated ? "\n\n（展示内容已截断）" : ""),
        },
      ],
    }),
    onNew: async (message) => {
      const text = message.content
        .filter((p) => p.type === "text")
        .map((p) => p.text)
        .join("\n");
      await onSend(text);
    },
  });

  useEffect(() => {
    const saved = sessionStorage.getItem(`agentrt:scroll:${view.session_id}`);
    if (saved && scrolling.current) {
      scrolling.current.scrollTop = Number(saved);
      follow.current =
        scrolling.current.scrollHeight -
          scrolling.current.scrollTop -
          scrolling.current.clientHeight <
        80;
    }
  }, [view.session_id]);
  useEffect(() => {
    const element = scrolling.current;
    if (!element) return;
    if (follow.current) element.scrollTop = element.scrollHeight;
    else setNewContent(true);
  }, [messages]);

  async function send() {
    if (!draft.trim() || busy || !connected || view.state !== "ready") return;
    if (new TextEncoder().encode(draft).length > 32768) {
      setSendError("单条消息不能超过 32 KiB。");
      return;
    }
    const submittedRevision = draftRevision.current;
    const sent = await onSend(draft);
    if (
      sent &&
      mounted.current &&
      draftRevision.current === submittedRevision
    ) {
      setDraft("");
      sessionStorage.removeItem(key);
      setSendError("");
      follow.current = true;
    }
  }

  return (
    <AssistantRuntimeProvider runtime={runtime}>
      <ThreadPrimitive.Root
        className="chat-panel"
        data-run-id={view.current_run_id || ""}
      >
        <div className="conversation-heading">
          <span>
            <MessageSquare size={16} /> 对话
          </span>
          <span className="muted">
            {messages.length ? "任务与公开回复" : "从一个具体任务开始"}
          </span>
        </div>
        <div
          className="messages"
          ref={scrolling}
          onScroll={(event) => {
            const element = event.currentTarget;
            follow.current =
              element.scrollHeight - element.scrollTop - element.clientHeight <
              80;
            sessionStorage.setItem(
              `agentrt:scroll:${view.session_id}`,
              String(element.scrollTop),
            );
            if (follow.current) setNewContent(false);
          }}
        >
          {messages.length === 0 ? (
            <div className="welcome">
              <div className="welcome-mark">
                <SquareTerminal size={29} />
              </div>
              <p className="eyebrow">YOUR AGENT, UNDER CONTROL</p>
              <h1>
                把任务交给 Agent，
                <br />
                把执行过程留在眼前。
              </h1>
              <p>
                从目标到工具调用，再到可核对的结果。
                <br />
                需要你的决定时，工作台会明确提示。
              </p>
              <button
                className="example-task"
                onClick={() => {
                  const text =
                    "检查样例 Python 仓库，执行检查脚本并生成检查报告。";
                  editDraft(text);
                }}
              >
                <FileText size={18} />
                <span>
                  <strong>检查样例仓库</strong>
                  <small>读取说明 · 执行检查 · 交付报告</small>
                </span>
                <ArrowRight size={16} />
              </button>
            </div>
          ) : (
            <ThreadPrimitive.Messages
              components={{ UserMessage, AssistantMessage }}
            />
          )}
          {running && (
            <div className={`run-inline ${view.state}`}>
              <StatusIcon status={view.state} />
              <span>{labels[view.state]}</span>
              <span className="muted">
                {view.state === "approval"
                  ? "请在执行面板确认本次操作"
                  : "工具和运行状态见右侧"}
              </span>
            </div>
          )}
        </div>
        {newContent && (
          <button
            className="new-content"
            onClick={() => {
              follow.current = true;
              scrolling.current?.scrollTo({
                top: scrolling.current.scrollHeight,
              });
              setNewContent(false);
            }}
          >
            <ArrowDown size={14} /> 有新内容
          </button>
        )}
        <form
          className="composer"
          onSubmit={(event) => {
            event.preventDefault();
            void send();
          }}
        >
          <textarea
            aria-label="输入任务"
            placeholder="描述你希望完成的任务…"
            value={draft}
            rows={3}
            onChange={(event) => {
              editDraft(event.target.value);
            }}
            onCompositionStart={() => {
              composing.current = true;
            }}
            onCompositionEnd={() => {
              composing.current = false;
            }}
            onKeyDown={(event) => {
              if (
                event.key === "Enter" &&
                !event.shiftKey &&
                !event.nativeEvent.isComposing &&
                !composing.current
              ) {
                event.preventDefault();
                void send();
              }
            }}
          />
          <div className="composer-footer">
            <div className="composer-context">
              <span>
                <span className="model-dot" /> {view.core.model}
              </span>
              {view.core.permission_modes?.length && view.permissions ? (
                <PermissionControl
                  permissions={view.permissions}
                  disabled={
                    !connected ||
                    ["closing", "closed", "lost"].includes(view.state)
                  }
                  onChange={onPermissions}
                />
              ) : null}
            </div>
            <button
              className="send-button"
              type="submit"
              aria-label="发送任务"
              disabled={
                !draft.trim() || busy || !connected || view.state !== "ready"
              }
            >
              <Send size={16} />
            </button>
          </div>
          {sendError && <p className="error-text">{sendError}</p>}
        </form>
        <div className="input-hint">
          Enter 发送 · Shift + Enter 换行 <span>执行中可编辑下一条草稿</span>
        </div>
      </ThreadPrimitive.Root>
    </AssistantRuntimeProvider>
  );
}

function ApprovalCard({
  approval,
  connected,
  onDecision,
  source,
  onSource,
  sessionPermissions,
}: {
  approval: Approval;
  connected: boolean;
  source: string;
  onSource: () => void;
  sessionPermissions: boolean;
  onDecision: (id: string, decision: ApprovalDecision) => Promise<void>;
}) {
  const [sending, setSending] = useState(false);
  const [remember, setRemember] = useState(false);
  const enabled = connected && approval.status === "pending" && !sending;
  async function decide(decision: ApprovalDecision) {
    setSending(true);
    try {
      await onDecision(approval.id, decision);
    } finally {
      setSending(false);
    }
  }
  return (
    <section className="approval-card" aria-label="工具审批">
      <div className="approval-title">
        <ShieldCheck size={18} />
        <strong>需要你的确认</strong>
        <span>{labels[approval.status]}</span>
      </div>
      <p>
        将通过 <strong>{approval.name}</strong>{" "}
        执行本次调用。可单次批准，或记住本会话的此工具授权。
      </p>
      <button
        className="approval-source"
        onClick={onSource}
        title={approval.run_id}
      >
        <GitBranch size={12} />
        来源：{source} · {approval.run_id.slice(-6)}
        <ChevronRight size={12} />
      </button>
      <pre>{approval.params}</pre>
      {!approval.can_allow && (
        <p className="error-text">参数过长，无法完整审阅；此页面仅允许拒绝。</p>
      )}
      {approval.expires_at && (
        <small>
          有效期至 {new Date(approval.expires_at).toLocaleTimeString()}
        </small>
      )}
      {sessionPermissions && (
        <label className="remember-approval">
          <input
            type="checkbox"
            checked={remember}
            disabled={!enabled || !approval.can_allow}
            onChange={(event) => setRemember(event.target.checked)}
          />
          记住本会话的 {approval.name} 授权
        </label>
      )}
      {remember && (
        <p className="approval-scope">
          本次及后续 {approval.name}{" "}
          调用（含不同参数和路径）不再询问；可在输入框旁的权限菜单撤销。仅对本会话有效。
        </p>
      )}
      <div className="approval-actions">
        <button disabled={!enabled} onClick={() => void decide("deny_once")}>
          拒绝一次
        </button>
        <button
          className="primary"
          disabled={!enabled || !approval.can_allow}
          onClick={() => void decide(remember ? "allow_session" : "allow_once")}
        >
          <Check size={15} />
          {remember ? "允许并记住" : "允许一次"}
        </button>
      </div>
    </section>
  );
}

function ToolDetail({ tool }: { tool: Tool }) {
  return (
    <div className="tool-detail">
      <div className="detail-heading">
        <SquareTerminal size={15} />
        <strong>{tool.name}</strong>
        <span className={`badge ${tool.status}`}>
          {labels[tool.status] || tool.status}
        </span>
      </div>
      <p className="detail-label">调用参数</p>
      {tool.decision && (
        <p className="detail-meta">
          权限：{decisionLabels[tool.decision] || tool.decision}
        </p>
      )}
      <pre>{tool.params || "未提供"}</pre>
      {tool.elapsed_ms !== undefined && (
        <div className="detail-meta">
          <Clock3 size={13} /> 调用总耗时 {(tool.elapsed_ms / 1000).toFixed(2)}{" "}
          s（含审批等待）
        </div>
      )}
      {tool.attempts.map((attempt) => (
        <div className="attempt" key={attempt.attempt}>
          <strong>尝试 {attempt.attempt}</strong>
          <pre>{attempt.error}</pre>
        </div>
      ))}
      <p className="detail-label">工具输出</p>
      <pre>
        {tool.output ||
          (tool.status === "success"
            ? "调用已完成，没有文本输出。"
            : "调用尚未返回输出。")}
      </pre>
      {tool.truncated && <p className="muted">输出已截断，仅展示保留内容。</p>}
      <details>
        <summary>调用标识</summary>
        <code className="break-all">{tool.id}</code>
      </details>
    </div>
  );
}

function ReportPanel({
  report,
  sid,
  onTool,
}: {
  report: Report;
  sid: string;
  onTool: (id: string) => void;
}) {
  const [copied, setCopied] = useState(false);
  const [downloadError, setDownloadError] = useState("");
  async function download() {
    try {
      const response = await fetch(
        `/api/sessions/${sid}/runs/${report.run_id}/report?format=markdown`,
      );
      if (!response.ok) throw new Error("报告尚未生成或已不再保留。");
      const url = URL.createObjectURL(await response.blob());
      const link = document.createElement("a");
      link.href = url;
      link.download = `kitagent-run-${report.run_id.slice(0, 24)}.md`;
      link.click();
      setTimeout(() => URL.revokeObjectURL(url), 1000);
    } catch (cause) {
      setDownloadError(errorText(cause));
    }
  }
  return (
    <div className="report">
      <div className="report-heading">
        <div className="report-mark">
          <FileText size={24} />
        </div>
        <div>
          <p className="eyebrow">RUN REPORT</p>
          <h2>本轮运行报告</h2>
        </div>
        <span className={`badge ${report.result.status}`}>
          <StatusIcon status={report.result.status} />
          {labels[report.result.status]}
        </span>
      </div>
      <p className="detail-label">本轮任务</p>
      <p className="markdown" data-testid="report-task">
        {report.title}
      </p>
      <div className="report-stats">
        <div>
          <strong>{report.tools.length}</strong>
          <span>工具调用</span>
        </div>
        <div>
          <strong>
            {
              report.tools.filter(
                (t) =>
                  t.decision?.endsWith("_once") ||
                  t.decision === "allow_session",
              ).length
            }
          </strong>
          <span>人工决定</span>
        </div>
        <div>
          <strong>{report.core.engine}</strong>
          <span>执行引擎</span>
        </div>
      </div>
      <div className="report-actions">
        <button className="primary" onClick={() => void download()}>
          <Download size={15} />
          导出运行报告
        </button>
        <button
          aria-label="复制本轮结论"
          onClick={() =>
            void navigator.clipboard
              .writeText(report.result.final_answer || "")
              .then(() => setCopied(true))
          }
        >
          {copied ? <Check size={15} /> : <Copy size={15} />}
        </button>
      </div>
      {downloadError && <p className="error-text">{downloadError}</p>}
      <p className="detail-label">本轮模型结论</p>
      <Markdown
        text={report.result.final_answer || "本轮没有已提交的最终答复。"}
      />
      {report.result.truncated && (
        <p className="muted">结论过长，展示和导出均已截断。</p>
      )}
      <p className="detail-label">执行事实与来源</p>
      <div className="evidence-list">
        {report.tools.map((tool) => (
          <button key={tool.id} onClick={() => onTool(tool.id)}>
            <span className={tool.status}>
              <StatusIcon status={tool.status} size={14} />
            </span>
            <span>
              {tool.name}
              <small>
                {tool.decision
                  ? `权限决定 · ${decisionLabels[tool.decision] || tool.decision}`
                  : labels[tool.status]}
              </small>
            </span>
            <ChevronRight size={14} />
          </button>
        ))}
      </div>
      <p className="report-note">{report.note}</p>
      <details>
        <summary>报告来源</summary>
        <code className="break-all">{report.result.transcript_ref}</code>
        <p>{new Date(report.generated_at).toLocaleString()}</p>
      </details>
    </div>
  );
}

function Inspector({
  view,
  connected,
  approve,
}: {
  view: View;
  connected: boolean;
  approve: (id: string, decision: ApprovalDecision) => Promise<void>;
}) {
  const selectionKey = `agentrt:tool:${view.session_id}`;
  const [tab, setTab] = useState<"execution" | "result">("execution");
  const [selected, setSelected] = useState(
    sessionStorage.getItem(selectionKey) || "",
  );
  const [missingSource, setMissingSource] = useState(false);
  const approvals = Object.values(view.approvals).filter(
    (a) => a.status === "pending" || a.status === "submitting",
  );
  const tools = Object.values(view.tools);
  const report = view.current_run_id
    ? view.reports[view.current_run_id]
    : undefined;
  function choose(id: string) {
    setSelected(id);
    sessionStorage.setItem(selectionKey, id);
    setTab("execution");
    setMissingSource(!view.tools[id]);
  }
  return (
    <aside className="inspector">
      <div className="inspector-tabs" role="tablist" aria-label="运行检查">
        <button
          role="tab"
          aria-selected={tab === "execution"}
          onClick={() => setTab("execution")}
        >
          执行过程 <span>{tools.length}</span>
        </button>
        <button
          role="tab"
          aria-selected={tab === "result"}
          onClick={() => setTab("result")}
        >
          结果 {report && <span className="result-dot" />}
        </button>
      </div>
      <div className="inspector-scroll">
        {approvals.length > 0 && tab === "result" && (
          <button className="approval-jump" onClick={() => setTab("execution")}>
            <ShieldCheck size={16} /> {approvals.length} 项操作等待审批{" "}
            <ArrowRight size={15} />
          </button>
        )}
        {tab === "result" ? (
          report ? (
            <ReportPanel
              report={report}
              sid={view.session_id}
              onTool={choose}
            />
          ) : (
            <div className="panel-empty">
              <FileText size={28} />
              <h3>结果将在完成后出现</h3>
              <p>
                本轮结论和执行事实完成同步后，
                <br />
                可以查看并导出运行报告。
              </p>
            </div>
          )
        ) : (
          <>
            <div className="environment-card">
              <span className="detail-label">执行环境</span>
              <span className="environment-kind">
                <SquareTerminal size={15} />
                {view.core.engine} / {view.core.sandbox_backend}
              </span>
              <code title={view.core.working_directory}>
                {view.core.working_directory}
              </code>
              <small>Local 会话共享宿主工作目录</small>
            </div>
            {approvals.map((approval) => (
              <ApprovalCard
                key={approval.id}
                approval={approval}
                connected={connected}
                onDecision={approve}
                sessionPermissions={Boolean(view.core.permission_modes?.length)}
                source={
                  view.runs[approval.run_id]?.parent_run_id
                    ? `子任务 · ${view.runs[approval.run_id].title}`
                    : "主任务"
                }
                onSource={() => choose(approval.tool_id)}
              />
            ))}
            {!tools.length && (
              <div className="panel-empty">
                <Radio size={28} />
                <h3>等待任务开始</h3>
                <p>
                  工具调用、审批与结果
                  <br />
                  将按实际事件显示在这里。
                </p>
              </div>
            )}
            <div className="timeline">
              {Object.values(view.runs).map((run) => {
                const ownTools = tools.filter((tool) => tool.run_id === run.id);
                return (
                  <section className="run-group" key={run.id}>
                    <div className="run-heading">
                      <GitBranch size={14} />
                      <span>
                        {run.parent_run_id ? `子任务 · ${run.title}` : "主任务"}
                      </span>
                      <small>{run.step ? `第 ${run.step} 步` : ""}</small>
                    </div>
                    {run.skill && <p className="muted">Skill · {run.skill}</p>}
                    {run.parent_run_id && run.output && (
                      <details className="child-output">
                        <summary>
                          子任务公开输出 · {labels[run.status] || run.status}
                        </summary>
                        <Markdown text={run.output} />
                        {run.output_truncated && <p>流式记录已截断。</p>}
                      </details>
                    )}
                    {ownTools.map((tool) => (
                      <button
                        className={`tool-row ${tool.id === selected ? "selected" : ""}`}
                        key={tool.id}
                        onClick={() => choose(tool.id)}
                      >
                        <span className={`tool-state ${tool.status}`}>
                          <StatusIcon status={tool.status} />
                        </span>
                        <span className="tool-row-content">
                          <strong>{tool.name}</strong>
                          <small>
                            {labels[tool.status] || tool.status}
                            {tool.decision?.endsWith("_once") ||
                            tool.decision === "allow_session"
                              ? " · 人工决定已记录"
                              : ""}
                          </small>
                        </span>
                        <ChevronRight size={14} />
                      </button>
                    ))}
                  </section>
                );
              })}
            </div>
            {missingSource && (
              <p className="muted">完整正文已不再保留；报告仍保留来源摘要。</p>
            )}
            {selected && view.tools[selected] && (
              <ToolDetail tool={view.tools[selected]} />
            )}
            {report && (
              <button className="view-report" onClick={() => setTab("result")}>
                <FileText size={17} />
                <span>本轮运行报告已就绪</span>
                <ArrowRight size={16} />
              </button>
            )}
          </>
        )}
      </div>
      <div className="inspector-foot">
        <ShieldCheck size={13} />
        执行与审批状态由 Core 确认
      </div>
    </aside>
  );
}

export default function App() {
  const wb = useWorkbench();
  const [sidebar, setSidebar] = useState(false);
  const [inspector, setInspector] = useState(false);
  const [more, setMore] = useState(false);
  const connected = wb.connection === "connected";
  const core = wb.view?.core || wb.status?.core;
  return (
    <div
      className={`workbench ${sidebar ? "sidebar-open" : ""} ${inspector ? "inspector-open" : ""}`}
    >
      <nav className="sidebar" aria-label="会话列表">
        <div className="brand">
          <div className="brand-icon">
            <SquareTerminal size={22} />
          </div>
          <div>
            <strong>KitAgent</strong>
            <small>RUNTIME WORKBENCH</small>
          </div>
          <button
            className="mobile-close"
            aria-label="关闭侧栏"
            onClick={() => setSidebar(false)}
          >
            <X size={16} />
          </button>
        </div>
        <button
          className="new-session"
          disabled={!wb.status?.supported || wb.busy || !!wb.unknown}
          onClick={() => void wb.create().then(() => setSidebar(false))}
        >
          <Plus size={17} /> 新建会话 <span>＋</span>
        </button>
        <div className="section-caption">
          工作会话 <span>{wb.sessions.length}</span>
        </div>
        <div className="session-list">
          {wb.sessions.map((session) => (
            <button
              className={`session-item ${session.id === wb.sid ? "active" : ""}`}
              key={session.id}
              onClick={() => {
                wb.select(session.id);
                setSidebar(false);
              }}
            >
              <MessageSquare size={16} />
              <span>
                <strong>{session.title}</strong>
                <small className={session.state}>
                  <StatusIcon status={session.state} size={11} />
                  {labels[session.state]}
                  {session.pending ? ` · ${session.pending}` : ""}
                </small>
              </span>
            </button>
          ))}
        </div>
        <div className="sidebar-bottom">
          <span className="local-badge">
            <span className="status-dot" />
            本机工作台
          </span>
          <p>由你决定如何执行</p>
          <small>Agent Runtime Kit · Web v0.1</small>
        </div>
      </nav>
      <main className="main">
        <header className="topbar">
          <button
            className="sidebar-toggle icon-button"
            aria-label="打开会话列表"
            onClick={() => setSidebar(!sidebar)}
          >
            <Menu size={19} />
          </button>
          <div className="breadcrumb">
            <span>工作台</span>
            <ChevronRight size={14} />
            <strong>{wb.view?.title || "开始新任务"}</strong>
          </div>
          <div className="topbar-right">
            <span
              className={`connection-pill ${wb.status?.supported && (!wb.sid || connected) ? "online" : ""}`}
            >
              <span className="status-dot" />
              {wb.sid
                ? connected
                  ? "实时连接"
                  : "连接恢复中"
                : wb.status?.supported
                  ? "Core 已连接"
                  : "Core 未就绪"}
            </span>
            <button
              className="inspector-toggle icon-button"
              aria-label="切换执行面板"
              onClick={() => setInspector(!inspector)}
            >
              <Radio size={18} />
            </button>
            {wb.view && (
              <div className="session-menu">
                <button
                  className="icon-button"
                  aria-label="会话操作"
                  onClick={() => setMore(!more)}
                >
                  <MoreHorizontal size={20} />
                </button>
                {more && (
                  <button
                    className="close-session"
                    disabled={!connected || wb.view.state === "closed"}
                    onClick={() => {
                      setMore(false);
                      void wb.close();
                    }}
                  >
                    结束会话
                  </button>
                )}
              </div>
            )}
          </div>
        </header>
        {core?.model === "offline-demo" && (
          <div className="demo-banner">
            <span>DEMO</span>离线演示 · 真实 Core、工具执行与权限链路
            <small>模型决策由脚本固定</small>
          </div>
        )}
        {(wb.error || wb.status?.error || wb.view?.error || wb.unknown) && (
          <div className="error-banner" role="alert">
            <span>
              {wb.error ||
                wb.view?.error ||
                wb.status?.error ||
                "上次请求的接收状态待确认，请查询原请求；不会自动重发。"}
            </span>
            {wb.unknown && (
              <>
                <button onClick={() => void wb.checkPending()}>
                  查询原请求
                </button>
                <button disabled={wb.busy} onClick={wb.leavePending}>
                  解除等待（不重发）
                </button>
              </>
            )}
            {wb.view?.state === "sync_error" && (
              <button onClick={() => void wb.synchronize()}>
                重新同步结果
              </button>
            )}
            <button aria-label="关闭提示" onClick={wb.clearError}>
              <X size={14} />
            </button>
          </div>
        )}
        <div className="workspace">
          {wb.view ? (
            <>
              <Chat
                key={wb.view.session_id}
                view={wb.view}
                connected={connected}
                busy={wb.busy || !!wb.unknown}
                onSend={wb.send}
                onPermissions={wb.configurePermissions}
              />
              <Inspector
                key={`inspector:${wb.view.session_id}`}
                view={wb.view}
                connected={connected}
                approve={wb.approve}
              />
            </>
          ) : (
            <div className="onboarding">
              <div className="welcome-mark">
                <SquareTerminal size={34} />
              </div>
              <p className="eyebrow">AGENT RUNTIME KIT</p>
              <h1>
                让 Agent 的执行过程
                <br />
                清晰、可控、可核对。
              </h1>
              <p>建立一个会话，开始你的第一个任务。</p>
              <button
                className="primary"
                disabled={!wb.status?.supported || wb.busy || !!wb.unknown}
                onClick={() => void wb.create()}
              >
                <Plus size={17} />
                新建会话
              </button>
              <div className="onboarding-features">
                <span>
                  <Radio size={16} />
                  实时执行
                </span>
                <span>
                  <ShieldCheck size={16} />
                  人工审批
                </span>
                <span>
                  <FileText size={16} />
                  运行报告
                </span>
              </div>
              {!wb.status && (
                <p className="muted">
                  请从 agentrt-web 输出的本机启动链接进入。
                </p>
              )}
            </div>
          )}
        </div>
        <footer className="statusbar">
          <span>
            <span className="status-dot" />
            {core ? `${core.engine} / ${core.sandbox_backend}` : "等待 Core"}
          </span>
          <span title={core?.working_directory}>
            {core?.working_directory || "连接后显示执行环境"}
          </span>
          <span aria-live="polite">
            {wb.view ? labels[wb.view.state] : "准备开始"}
          </span>
        </footer>
      </main>
    </div>
  );
}
