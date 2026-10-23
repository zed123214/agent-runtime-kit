import { defineConfig } from "@playwright/test";

export default defineConfig({
  testDir: "./e2e",
  timeout: 120000,
  workers: 1,
  use: {
    browserName: "chromium",
    viewport: { width: 1440, height: 900 },
    locale: "zh-CN",
    screenshot: "only-on-failure",
    trace: "off",
    video: process.env.AGENTRT_RECORD_DEMO
      ? { mode: "on", size: { width: 1440, height: 900 } }
      : "off",
  },
});
