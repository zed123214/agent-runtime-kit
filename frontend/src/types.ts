export type RunState =
  | "ready"
  | "running"
  | "approval"
  | "syncing"
  | "sync_error"
  | "closing"
  | "closed"
  | "lost";
export interface Core {
  engine: string;
  sandbox_backend: string;
  model: string;
  working_directory: string;
  permission_modes?: PermissionMode[];
}
export type PermissionMode = "ask" | "read_only" | "auto";
export type ApprovalDecision = "allow_once" | "allow_session" | "deny_once";
export interface SessionPermissions {
  mode: PermissionMode;
  allowed_tools: string[];
  revision: number;
}
export interface PermissionChange {
  mode: PermissionMode;
  expected_revision: number;
  reset_grants?: boolean;
  revoke_tool?: string;
}
export interface Message {
  id: string;
  role: "user" | "assistant";
  text: string;
  draft: boolean;
  truncated?: boolean;
}
export interface Tool {
  id: string;
  run_id: string;
  root_id: string;
  tool_use_id: string;
  name: string;
  status: string;
  params?: string;
  output?: string;
  truncated?: boolean;
  elapsed_ms?: number;
  decision?: string;
  attempts: { attempt: number; error: string }[];
}
export interface Approval {
  id: string;
  tool_id: string;
  run_id: string;
  name: string;
  params: string;
  can_allow: boolean;
  status: string;
  decision?: string;
  expires_at?: string;
}
export interface Run {
  id: string;
  parent_run_id?: string;
  root_id?: string;
  title: string;
  status: string;
  step?: number;
  reason?: string;
  skill?: string;
  output?: string;
  output_truncated?: boolean;
}
export interface Report {
  run_id: string;
  title: string;
  generated_at: string;
  core: Core;
  tools: Tool[];
  note: string;
  result: {
    status: string;
    commit_state: string;
    final_answer: string | null;
    no_final_answer: boolean;
    truncated: boolean;
    transcript_ref: string;
    result_revision: string;
  };
}
export interface View {
  schema_version: number;
  instance_id: string;
  session_id: string;
  seq: number;
  title: string;
  state: RunState;
  error: string | null;
  current_run_id: string | null;
  core: Core;
  messages: Record<string, Message>;
  permissions: SessionPermissions;
  tools: Record<string, Tool>;
  approvals: Record<string, Approval>;
  runs: Record<string, Run>;
  reports: Record<string, Report>;
}
export interface Summary {
  id: string;
  title: string;
  state: RunState;
  pending: number;
  seq: number;
}
export type Collection =
  | "messages"
  | "tools"
  | "approvals"
  | "runs"
  | "reports";
export type Operation =
  | { op: "meta"; value: Partial<View> }
  | { op: "put"; collection: Collection; key: string; value: unknown }
  | { op: "remove"; collection: Collection; key: string };
export interface Patch {
  type: "patch";
  instance_id: string;
  session_id: string;
  seq: number;
  operations: Operation[];
}
export type Frame =
  | { type: "snapshot"; snapshot: View }
  | Patch
  | { type: "heartbeat" | "resync" };
export interface RequestRecord {
  request_id: string;
  instance_id: string;
  state: string;
  session_id?: string;
  error?: string;
}
