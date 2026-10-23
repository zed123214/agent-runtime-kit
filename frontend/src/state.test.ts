import { describe, expect, it } from "vitest";
import { applyPatch, SequenceGap } from "./state";
import type { Patch, View } from "./types";

const initial: View = {
  schema_version: 1,
  instance_id: "gateway",
  session_id: "one",
  seq: 3,
  title: "demo",
  state: "running",
  error: null,
  current_run_id: "run",
  core: {
    engine: "loop",
    sandbox_backend: "local",
    model: "demo",
    working_directory: "/tmp",
  },
  messages: {},
  permissions: { mode: "ask", allowed_tools: [], revision: 0 },
  tools: {},
  approvals: {},
  runs: {},
  reports: {},
};
const patch: Patch = {
  type: "patch",
  instance_id: "gateway",
  session_id: "one",
  seq: 4,
  operations: [{ op: "meta", value: { state: "approval" } }],
};

describe("snapshot and increment handoff", () => {
  it("ignores duplicates and late frames from another session or gateway", () => {
    const changed = applyPatch(initial, patch);
    expect(changed.state).toBe("approval");
    expect(applyPatch(changed, patch)).toBe(changed);
    expect(applyPatch(initial, { ...patch, session_id: "two" })).toBe(initial);
    expect(applyPatch(initial, { ...patch, instance_id: "restarted" })).toBe(
      initial,
    );
  });
  it("forces a new snapshot on a sequence gap rather than silently skipping an approval", () => {
    expect(() => applyPatch(initial, { ...patch, seq: 5 })).toThrow(
      SequenceGap,
    );
  });
});
