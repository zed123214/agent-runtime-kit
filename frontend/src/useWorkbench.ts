import {
  useCallback,
  useEffect,
  useLayoutEffect,
  useRef,
  useState,
} from "react";
import { api, ApiError, setCsrf } from "./api";
import { applyPatch } from "./state";
import type {
  ApprovalDecision,
  Core,
  Frame,
  PermissionChange,
  RequestRecord,
  Summary,
  View,
} from "./types";

interface Status {
  instance_id: string;
  csrf: string;
  core: Core | null;
  error: string | null;
  supported: boolean;
}
interface Pending {
  request_id: string;
  kind: "create" | "send";
  instance_id: string;
  session_id?: string;
}
const pendingKey = "agentrt:pending";
const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

export function useWorkbench() {
  const [status, setStatus] = useState<Status | null>(null);
  const [sessions, setSessions] = useState<Summary[]>([]);
  const [sid, setSid] = useState<string | null>(
    new URL(location.href).searchParams.get("session"),
  );
  const [view, setView] = useState<View | null>(null);
  const [connection, setConnection] = useState("connecting");
  const [error, setError] = useState("");
  const [busy, setBusy] = useState(false);
  const [unknown, setUnknown] = useState<Pending | null>(null);
  const generation = useRef(0);
  const summaryGeneration = useRef(0);
  const current = useRef<View | null>(null);
  const frames = useRef<Frame[]>([]);
  const receivedAt = useRef<number | null>(null);
  const pending = useRef<Pending | null>(null);

  const select = useCallback((id: string) => {
    setSid(id);
    const url = new URL(location.href);
    url.searchParams.set("session", id);
    history.replaceState(null, "", url);
  }, []);

  const refresh = useCallback(async () => {
    const epoch = ++summaryGeneration.current;
    const result = await api<{ instance_id: string; sessions: Summary[] }>(
      "/sessions",
    );
    if (epoch !== summaryGeneration.current) return;
    if (status && result.instance_id !== status.instance_id) {
      setError("网关实例已变化，请使用新的启动链接。旧任务不会重新执行。");
      return;
    }
    setSessions((previous) =>
      result.sessions.map((item) => {
        const existing = previous.find((session) => session.id === item.id);
        return existing && existing.seq > item.seq ? existing : item;
      }),
    );
  }, [status]);

  useEffect(() => {
    let live = true;
    async function initialize() {
      try {
        const fragment = new URLSearchParams(location.hash.slice(1));
        const token = fragment.get("bootstrap");
        // Remove the launch capability before rendering any user content or links.
        if (token) {
          history.replaceState(
            null,
            "",
            `${location.pathname}${location.search}`,
          );
          try {
            await api("/bootstrap", { token });
          } catch {
            /* An already authenticated second tab can use its existing cookie. */
          }
        }
        const next = await api<Status>("/status");
        if (!live) return;
        setCsrf(next.csrf);
        setStatus(next);
        const saved = sessionStorage.getItem(pendingKey);
        if (saved) {
          const record = JSON.parse(saved) as Pending;
          pending.current = record;
          setUnknown(record);
        }
      } catch (cause) {
        if (live) {
          setError(String(cause));
          setConnection("unauthorized");
        }
      }
    }
    void initialize();
    return () => {
      live = false;
    };
  }, []);

  useEffect(() => {
    if (!status) return;
    let live = true;
    let inflight = false;
    const poll = async () => {
      if (document.hidden || inflight) return;
      inflight = true;
      try {
        if (live) await refresh();
      } catch {
        if (live) setError("会话摘要暂时无法同步，显示状态可能滞后。");
      } finally {
        inflight = false;
      }
    };
    void poll();
    const timer = setInterval(() => void poll(), 2000);
    document.addEventListener("visibilitychange", poll);
    return () => {
      live = false;
      clearInterval(timer);
      document.removeEventListener("visibilitychange", poll);
    };
  }, [status, refresh]);

  useEffect(() => {
    if (!sid || !status) return;
    const epoch = ++generation.current;
    let disposed = false;
    let socket: WebSocket | null = null;
    let retry: ReturnType<typeof setTimeout> | undefined;
    let paint: number | null = null;
    let attempt = 0;
    let lastFrame = Date.now();
    current.current = null;
    frames.current = [];
    setView(null);
    setConnection("connecting");

    function publish() {
      paint = null;
      if (disposed || generation.current !== epoch) return;
      try {
        for (const frame of frames.current.splice(0)) {
          if (frame.type === "snapshot") {
            if (
              frame.snapshot.instance_id !== status!.instance_id ||
              frame.snapshot.session_id !== sid
            )
              throw new Error("实例已变化");
            current.current = frame.snapshot;
            setConnection("connected");
          } else if (frame.type === "patch" && current.current) {
            current.current = applyPatch(current.current, frame);
          }
        }
        setView(current.current);
      } catch {
        frames.current = [];
        setConnection("reconnecting");
        socket?.close();
      }
    }
    function connect() {
      if (disposed) return;
      socket = new WebSocket(
        `${location.protocol === "https:" ? "wss:" : "ws:"}//${location.host}/api/sessions/${sid}/events`,
      );
      socket.onmessage = (event) => {
        if (disposed || generation.current !== epoch) return;
        lastFrame = Date.now();
        try {
          const frame = JSON.parse(event.data) as Frame;
          if (frame.type === "resync") {
            socket?.close();
            return;
          }
          if (frame.type === "heartbeat") return;
          if (receivedAt.current === null)
            receivedAt.current = performance.now();
          frames.current.push(frame);
          if (frames.current.length > 256) {
            frames.current = [];
            socket?.close();
            return;
          }
          if (paint === null) paint = requestAnimationFrame(publish);
          attempt = 0;
        } catch {
          socket?.close();
        }
      };
      socket.onclose = () => {
        if (disposed) return;
        setConnection("reconnecting");
        retry = setTimeout(connect, Math.min(500 * 2 ** attempt++, 8000));
      };
      socket.onerror = () => socket?.close();
    }
    connect();
    const heartbeat = setInterval(() => {
      if (!document.hidden && Date.now() - lastFrame > 35000) socket?.close();
    }, 5000);
    return () => {
      disposed = true;
      ++generation.current;
      clearTimeout(retry);
      clearInterval(heartbeat);
      if (paint !== null) cancelAnimationFrame(paint);
      frames.current = [];
      socket?.close();
    };
  }, [sid, status]);

  useLayoutEffect(() => {
    const started = receivedAt.current;
    receivedAt.current = null;
    if (!new URL(location.href).searchParams.has("measure") || started === null)
      return;
    requestAnimationFrame(() => {
      if (performance.getEntriesByName("agentrt:event-render").length < 1000)
        performance.measure("agentrt:event-render", {
          start: started,
          end: performance.now(),
        });
    });
  }, [view]);

  function isPending(record: Pending) {
    return (
      pending.current?.request_id === record.request_id &&
      pending.current?.instance_id === record.instance_id
    );
  }

  function clearPending(record: Pending) {
    if (!isPending(record)) return false;
    sessionStorage.removeItem(pendingKey);
    pending.current = null;
    setUnknown(null);
    return true;
  }

  async function resolveRequest(record: Pending): Promise<boolean> {
    if (record.instance_id !== status?.instance_id)
      throw new Error("该请求属于旧网关，不能在当前实例重新执行。");
    for (let count = 0; count < 80; count++) {
      const result = await api<RequestRecord>(`/requests/${record.request_id}`);
      // An explicit exit can be followed by a new request while this lookup is in flight.
      if (!isPending(record)) return false;
      if (record.kind === "create" && result.state === "accepted") {
        await sleep(125);
        continue;
      }
      if (result.state === "failed") {
        setError(result.error || "请求未执行");
        clearPending(record);
        throw new ApiError(result.error || "请求未执行", 409);
      }
      if (record.kind === "create" && result.session_id)
        select(result.session_id);
      clearPending(record);
      setError("");
      // Summary refresh failure must not turn an acknowledged request back into an unknown one.
      void refresh().catch(() => {});
      return true;
    }
    throw new Error("请求仍在处理中，请查询原请求状态。");
  }

  async function command(kind: "create" | "send", content?: string) {
    if (busy || pending.current || !status) return false;
    setBusy(true);
    setError("");
    const record: Pending = {
      request_id: crypto.randomUUID(),
      kind,
      instance_id: status.instance_id,
      session_id: sid || undefined,
    };
    pending.current = record;
    sessionStorage.setItem(pendingKey, JSON.stringify(record));
    try {
      if (kind === "create")
        await api("/sessions", { request_id: record.request_id });
      else
        await api(`/sessions/${sid}/messages`, {
          request_id: record.request_id,
          content,
        });
      return await resolveRequest(record);
    } catch (cause) {
      setError(String(cause));
      if (cause instanceof ApiError && cause.status < 500) {
        clearPending(record);
      } else setUnknown(record);
      return false;
    } finally {
      setBusy(false);
    }
  }

  async function approve(id: string, decision: ApprovalDecision) {
    if (connection !== "connected" || !sid) return;
    try {
      const result = await api<{ ok: boolean }>(
        `/sessions/${sid}/approvals/${id}`,
        { decision },
      );
      if (!result.ok)
        setError("该审批已失效、已被处理或结果未确认。请查看当前状态。");
    } catch (cause) {
      setError(String(cause));
    }
    await refresh();
  }

  async function close() {
    if (!sid || !view || connection !== "connected") return;
    if (
      !window.confirm("结束会话将停止其中的任务，后续需要新建会话。确定结束？")
    )
      return;
    try {
      await api(`/sessions/${sid}/close`, { request_id: crypto.randomUUID() });
    } catch (cause) {
      setError(String(cause));
    }
  }

  async function configurePermissions(
    change: PermissionChange,
  ): Promise<boolean> {
    if (!sid || !view || connection !== "connected") return false;
    try {
      await api(`/sessions/${sid}/permissions`, change);
      return true;
    } catch (cause) {
      setError(String(cause));
      return false;
    }
  }

  async function synchronize() {
    try {
      await api(`/sessions/${sid}/sync`, {});
    } catch (cause) {
      setError(String(cause));
    }
  }

  async function checkPending() {
    if (!unknown) return;
    const record = unknown;
    try {
      await resolveRequest(record);
    } catch (cause) {
      if (!isPending(record)) return;
      setError(
        cause instanceof ApiError && cause.status === 404
          ? "当前网关未找到该请求，执行结果仍未确认。可继续查询，或解除本页等待；不会自动重发。"
          : String(cause),
      );
    }
  }

  function leavePending() {
    if (!unknown || busy || !isPending(unknown)) return;
    if (
      !window.confirm(
        "仅解除本页等待，不会取消或重发原请求；原请求仍可能被网关接收。请核对会话与执行状态后，再决定是否提交新任务。继续？",
      )
    )
      return;
    if (clearPending(unknown))
      setError("已解除本页等待。原请求未被取消，也未重发；草稿已保留。");
  }

  return {
    status,
    sessions,
    sid,
    view,
    connection,
    error,
    busy,
    unknown,
    select,
    approve,
    configurePermissions,
    close,
    synchronize,
    checkPending,
    leavePending,
    create: () => command("create"),
    send: (content: string) => command("send", content),
    clearError: () => setError(""),
  };
}
