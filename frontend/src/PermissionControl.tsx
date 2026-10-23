import { useEffect, useRef, useState } from "react";
import { Check, ChevronDown, Eye, ShieldCheck, Zap, X } from "lucide-react";
import type {
  PermissionChange,
  PermissionMode,
  SessionPermissions,
} from "./types";

const modes = [
  {
    value: "read_only",
    label: "只读",
    detail: "仅允许读取文件和查看目录，阻止命令与修改。",
    icon: Eye,
  },
  {
    value: "ask",
    label: "按需询问",
    detail: "读取自动通过；命令、写入等操作由你批准。",
    icon: ShieldCheck,
  },
  {
    value: "auto",
    label: "自动批准",
    detail: "本会话的工具调用自动通过，包括命令与文件修改。",
    icon: Zap,
  },
] as const;

export const decisionLabels: Record<string, string> = {
  allow_once: "允许一次",
  deny_once: "拒绝一次",
  allow_session: "本会话允许此工具",
  session_allow: "沿用本会话授权",
  session_auto: "会话自动批准",
  read_only: "只读模式阻止",
  auto_allow: "策略允许",
  auto_deny: "策略拒绝",
  timeout: "审批超时",
  always_allow: "全局策略允许",
  always_deny: "全局策略拒绝",
};

export function PermissionControl({
  permissions,
  disabled,
  onChange,
}: {
  permissions: SessionPermissions;
  disabled: boolean;
  onChange: (change: PermissionChange) => Promise<boolean>;
}) {
  const [open, setOpen] = useState(false);
  const [confirmAuto, setConfirmAuto] = useState(false);
  const [saving, setSaving] = useState(false);
  const root = useRef<HTMLDivElement>(null);
  const selected = modes.find((mode) => mode.value === permissions.mode)!;
  useEffect(() => {
    if (!open) return;
    function close(event: PointerEvent) {
      if (!root.current?.contains(event.target as Node)) {
        setOpen(false);
        setConfirmAuto(false);
      }
    }
    document.addEventListener("pointerdown", close);
    return () => document.removeEventListener("pointerdown", close);
  }, [open]);
  async function apply(mode: PermissionMode, revoke_tool?: string) {
    if (saving || disabled) return;
    setSaving(true);
    try {
      const ok = await onChange({
        mode,
        expected_revision: permissions.revision,
        revoke_tool,
        reset_grants: !revoke_tool,
      });
      if (ok) {
        setConfirmAuto(false);
        if (!revoke_tool) setOpen(false);
      }
    } finally {
      setSaving(false);
    }
  }
  return (
    <div
      className="permission-control"
      ref={root}
      onKeyDown={(event) => {
        if (event.key === "Escape") {
          event.stopPropagation();
          setOpen(false);
          setConfirmAuto(false);
        }
      }}
    >
      <button
        type="button"
        className={`permission-trigger ${permissions.mode}`}
        aria-label={`会话权限：${selected.label}`}
        aria-expanded={open}
        aria-haspopup="dialog"
        disabled={disabled}
        onClick={() => {
          setOpen(!open);
          setConfirmAuto(false);
        }}
      >
        <selected.icon size={14} /> {selected.label}
        {permissions.allowed_tools.length > 0 && (
          <span className="grant-count">
            {permissions.allowed_tools.length} 项授权
          </span>
        )}
        <ChevronDown size={13} />
      </button>
      {open && (
        <div
          className="permission-popover"
          role="dialog"
          aria-label="会话权限设置"
        >
          <div className="permission-heading">
            <strong>如何批准工具操作？</strong>
            <span>仅本会话</span>
          </div>
          {confirmAuto ? (
            <div className="permission-confirm">
              <Zap size={20} />
              <strong>启用本会话自动批准</strong>
              <p>
                将自动允许当前等待及后续的工具调用，包括运行命令、修改或删除文件、访问网络。Local
                模式可访问宿主文件，不限于项目目录。
              </p>
              <p>随时可切回“按需询问”。已开始的操作不会撤回。</p>
              <div className="permission-confirm-actions">
                <button
                  type="button"
                  disabled={saving}
                  onClick={() => setConfirmAuto(false)}
                >
                  取消
                </button>
                <button
                  type="button"
                  className="primary"
                  disabled={saving || disabled}
                  onClick={() => void apply("auto")}
                >
                  启用自动批准
                </button>
              </div>
            </div>
          ) : (
            <>
              <div role="group" aria-label="权限模式">
                {modes.map((mode) => (
                  <button
                    type="button"
                    className={`permission-option ${permissions.mode === mode.value ? "selected" : ""}`}
                    key={mode.value}
                    aria-pressed={permissions.mode === mode.value}
                    disabled={saving || disabled}
                    onClick={() =>
                      mode.value === "auto" && permissions.mode !== "auto"
                        ? setConfirmAuto(true)
                        : void apply(mode.value)
                    }
                  >
                    <mode.icon size={17} />
                    <span>
                      <strong>{mode.label}</strong>
                      <small>{mode.detail}</small>
                    </span>
                    {permissions.mode === mode.value && <Check size={15} />}
                  </button>
                ))}
              </div>
              {permissions.allowed_tools.length > 0 && (
                <div className="permission-grants">
                  <p>本会话已记住</p>
                  {permissions.allowed_tools.map((tool) => (
                    <div key={tool}>
                      <code>{tool}</code>
                      <button
                        type="button"
                        aria-label={`撤销 ${tool} 授权`}
                        disabled={saving || disabled}
                        onClick={() => void apply(permissions.mode, tool)}
                      >
                        <X size={12} />
                        撤销
                      </button>
                    </div>
                  ))}
                </div>
              )}
              <p className="permission-footnote">
                授权随会话结束清除。模式变化由 Core 确认，已开始的操作不受影响。
              </p>
            </>
          )}
        </div>
      )}
    </div>
  );
}
