import { test, expect, type Page } from "@playwright/test";
import { spawn, type ChildProcessWithoutNullStreams } from "node:child_process";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import path from "node:path";

let serverProcess: ChildProcessWithoutNullStreams;
let origin: string;
let token: string;
let storageState: Awaited<
  ReturnType<import("@playwright/test").BrowserContext["storageState"]>
>;
let completedSession: string;
const assets = path.resolve(process.env.AGENTRT_ASSETS_DIR || "../docs/assets");
const recording = Boolean(process.env.AGENTRT_RECORD_DEMO);
const serverArgs = recording ? [] : ["--slow"];

test.beforeAll(async () => {
  // The runtime itself is Linux; browser acceptance runs in real Windows Chromium.
  const root = path
    .resolve("..")
    .replaceAll("\\", "/")
    .replace(/^([A-Z]):/, (_, drive: string) => `/mnt/${drive.toLowerCase()}`);
  const python = process.env.AGENTRT_TEST_PYTHON || "python3";
  serverProcess =
    process.platform === "win32"
      ? spawn(
          "wsl.exe",
          [
            "-d",
            process.env.AGENTRT_TEST_WSL || "Ubuntu-20.04",
            "-u",
            "root",
            "--exec",
            python,
            `${root}/scripts/web_validation_server.py`,
            ...serverArgs,
          ],
          { windowsHide: true, stdio: "pipe" },
        )
      : spawn(
          python,
          [path.resolve("../scripts/web_validation_server.py"), ...serverArgs],
          {
            stdio: "pipe",
          },
        );
  const ready = await new Promise<{ origin: string; token: string }>(
    (resolve, reject) => {
      let buffer = "";
      const timer = setTimeout(
        () => reject(new Error("Local validation server did not start")),
        25000,
      );
      serverProcess.stdout.on("data", (chunk) => {
        buffer += String(chunk);
        const frame = buffer
          .split("\n")
          .find((line) => line.startsWith("READY "));
        if (frame) {
          clearTimeout(timer);
          resolve(JSON.parse(frame.slice(6)));
        }
      });
      serverProcess.once("exit", (code) => {
        clearTimeout(timer);
        reject(new Error(`Validation server exited: ${code}`));
      });
    },
  );
  origin = ready.origin;
  token = ready.token;
  // WSL can bind before Windows localhost forwarding becomes visible.
  for (let attempt = 0; attempt < 50; attempt++) {
    try {
      if ((await fetch(origin, { signal: AbortSignal.timeout(1000) })).ok)
        break;
    } catch {
      /* readiness probe, not a command retry */
    }
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
  await mkdir(assets, { recursive: true });
  await mkdir(path.resolve("../artifacts/web-validation"), { recursive: true });
});

test.afterAll(async () => {
  if (serverProcess) {
    serverProcess.stdin.write("\n");
    await new Promise<void>((resolve) => {
      const timer = setTimeout(resolve, 20000);
      serverProcess.once("exit", () => {
        clearTimeout(timer);
        resolve();
      });
    });
  }
});

async function openAuthenticated(page: Page) {
  if (storageState) await page.context().addCookies(storageState.cookies);
  else {
    const bootstrap = await page.request.post(`${origin}/api/bootstrap`, {
      data: { token },
      headers: { Origin: origin },
    });
    expect(bootstrap.ok()).toBe(true);
    storageState = await page.context().storageState();
  }
  await page.goto(origin);
  await expect(
    page.getByRole("button", { name: "新建会话", exact: true }),
  ).toBeEnabled();
}

async function closeTestSession(page: Page) {
  const sid = new URL(page.url()).searchParams.get("session");
  if (!sid) return;
  const status = await page.request
    .get(`${origin}/api/status`)
    .then((response) => response.json());
  await page.request.post(`${origin}/api/sessions/${sid}/close`, {
    data: { request_id: crypto.randomUUID() },
    headers: { Origin: origin, "X-CSRF-Token": status.csrf },
  });
}

test("real Core approval survives reload and exports the committed report", async ({
  page,
}, testInfo) => {
  const errors: string[] = [];
  const started = Date.now();
  const reloadSamples: number[] = [];
  const renderSamples: number[] = [];
  const collectRender = async () => {
    renderSamples.push(
      ...(await page.evaluate(() => {
        const samples = performance
          .getEntriesByName("agentrt:event-render")
          .map((entry) => entry.duration);
        performance.clearMeasures("agentrt:event-render");
        return samples;
      })),
    );
  };
  const reload = async (rid: string) => {
    await collectRender();
    await page.reload();
    await expect(page.locator(".connection-pill")).toContainText("实时连接");
    await expect(page.locator(".chat-panel")).toHaveAttribute(
      "data-run-id",
      rid,
    );
    reloadSamples.push(await page.evaluate(() => performance.now()));
  };
  page.on("pageerror", (error) => errors.push(error.message));
  const bootstrap = await page.request.post(`${origin}/api/bootstrap`, {
    data: { token },
    headers: { Origin: origin },
  });
  expect(bootstrap.ok()).toBe(true);
  storageState = await page.context().storageState();
  await page.goto(`${origin}/?measure=1`);
  // Do not retain launch URLs/tokens in browser traces or screenshots.
  await expect(page).not.toHaveURL(/bootstrap=/);
  await page.route("**/api/sessions", async (route) => {
    if (route.request().method() !== "POST") return route.continue();
    await route.fetch();
    await route.abort(); // Lose the response after the real server accepted creation.
  });
  await page.getByRole("button", { name: "新建会话", exact: true }).click();
  await expect(page.getByRole("button", { name: "查询原请求" })).toBeVisible();
  await page.unroute("**/api/sessions");
  await page.reload();
  await expect(page.getByRole("button", { name: "查询原请求" })).toBeVisible();
  await page.getByRole("button", { name: "查询原请求" }).click();
  await expect(page.getByLabel("输入任务")).toBeVisible();
  if (recording) await page.waitForTimeout(3000);
  await page
    .getByLabel("输入任务")
    .fill("检查样例 Python 仓库，执行检查脚本并生成检查报告。");
  await expect(page.getByRole("button", { name: "发送任务" })).toBeEnabled();
  await page.getByRole("button", { name: "发送任务" }).click();
  const sid = new URL(page.url()).searchParams.get("session")!;
  completedSession = sid;
  await expect(page.locator(".assistant-message")).toBeVisible();
  const initial = await page.request
    .get(`${origin}/api/sessions/${sid}/snapshot`)
    .then((r) => r.json());
  if (!recording)
    for (let i = 0; i < 10; i++) await reload(initial.current_run_id);
  await expect(page.getByRole("button", { name: "允许一次" })).toBeVisible({
    timeout: 15000,
  });
  const before = await page.request
    .get(`${origin}/api/sessions/${sid}/snapshot`)
    .then((r) => r.json());
  await expect(page.locator(".session-item.active")).toContainText("等待审批");
  await page.screenshot({
    path: path.join(assets, "web-workbench-approval.png"),
  });
  if (recording) await page.waitForTimeout(5000);
  for (let i = 0; i < (recording ? 1 : 10); i++)
    await reload(before.current_run_id);
  await expect(page.getByRole("button", { name: "允许一次" })).toBeEnabled();
  const after = await page.request
    .get(`${origin}/api/sessions/${sid}/snapshot`)
    .then((r) => r.json());
  expect(after.current_run_id).toBe(before.current_run_id);
  expect(Object.keys(after.approvals)).toEqual(Object.keys(before.approvals));
  await page.getByRole("button", { name: "允许一次" }).click();
  if (!recording)
    for (let i = 0; i < 10; i++) await reload(before.current_run_id);
  await expect(page.locator(".approval-card")).toContainText("write_file", {
    timeout: 15000,
  });
  if (recording) await page.waitForTimeout(5000);
  await page.getByRole("button", { name: "允许一次" }).click();
  await page.getByRole("button", { name: "本轮运行报告已就绪" }).click();
  await expect(
    page.getByRole("button", { name: "导出运行报告" }),
  ).toBeVisible();
  await expect(page.locator(".session-item.active")).toContainText(
    "可继续对话",
  );
  await page.screenshot({
    path: path.join(assets, "web-workbench-overview.png"),
  });
  const downloadPromise = page.waitForEvent("download");
  await page.getByRole("button", { name: "导出运行报告" }).click();
  const download = await downloadPromise;
  const target = testInfo.outputPath("report.md");
  await download.saveAs(target);
  const report = await readFile(target, "utf8");
  expect(report).toContain("检查报告");
  expect(report).toContain(before.current_run_id);
  expect(report).toContain("PASS: sample data is valid");
  expect(report).not.toContain(token);
  expect(errors).toEqual([]);
  await collectRender();
  for (const width of [1280, 1024]) {
    await page.setViewportSize({ width, height: 900 });
    await expect(
      page.getByRole("button", { name: "导出运行报告" }),
    ).toBeVisible();
    const overflow = await page.evaluate(
      () => document.documentElement.scrollWidth > innerWidth,
    );
    expect(overflow).toBe(false);
  }
  await writeFile(
    path.resolve(
      `../artifacts/web-validation/${recording ? "recording" : "browser"}-result.json`,
    ),
    JSON.stringify(
      {
        scenario: "D1-D3",
        viewport: "1440x900",
        browser: `${process.platform} Chromium`,
        browser_version: page.context().browser()?.version(),
        reload_preserved_run: true,
        reload_samples_ms: reloadSamples,
        render_samples_ms: renderSamples,
        report_verified: true,
        viewports: [1440, 1280, 1024],
        date: new Date().toISOString(),
      },
      null,
      2,
    ),
  );
  if (recording) {
    await page.setViewportSize({ width: 1440, height: 900 });
    await page.waitForTimeout(Math.max(0, 46000 - (Date.now() - started)));
    const video = page.video();
    await page.close();
    await video?.saveAs(path.join(assets, "web-workbench-demo.webm"));
  }
});

test("cold pages become usable without resending the completed task", async ({
  browser,
}) => {
  test.skip(recording, "The recording run only produces the walkthrough.");
  const samples: number[] = [];
  for (let index = 0; index < 10; index++) {
    const context = await browser.newContext({
      storageState,
      viewport: { width: 1440, height: 900 },
    });
    try {
      const page = await context.newPage();
      await page.goto(`${origin}/?session=${completedSession}`);
      await expect(page.locator(".connection-pill")).toContainText("实时连接");
      await expect(page.getByLabel("输入任务")).toBeVisible();
      samples.push(await page.evaluate(() => performance.now()));
      expect(samples.at(-1)).toBeLessThan(2000);
      expect(
        await page.request
          .get(`${origin}/api/sessions`)
          .then((r) => r.json())
          .then((r) => r.sessions.length),
      ).toBe(1);
    } finally {
      await context.close();
    }
  }
  await writeFile(
    path.resolve("../artifacts/web-validation/cold-load.json"),
    JSON.stringify(
      {
        browser: browser.version(),
        samples_ms: samples,
        sample_count: samples.length,
        method:
          "Fresh browser context with existing auth cookie; navigation to interactive snapshot. No installation or service startup.",
      },
      null,
      2,
    ),
  );
});

for (const kind of ["create", "send"] as const) {
  test(`regression: missing ${kind} request can leave the unknown state without replay`, async ({
    page,
  }) => {
    await openAuthenticated(page);
    if (kind === "send") {
      await page.getByRole("button", { name: "新建会话", exact: true }).click();
      await page.getByLabel("输入任务").fill("未送达的任务草稿");
      await expect(
        page.getByRole("button", { name: "发送任务" }),
      ).toBeEnabled();
    }
    let posts = 0;
    let requestId = "";
    const pattern =
      kind === "create" ? "**/api/sessions" : "**/api/sessions/*/messages";
    await page.route(pattern, async (route) => {
      if (route.request().method() !== "POST") return route.continue();
      posts++;
      requestId = route.request().postDataJSON().request_id;
      await route.abort(); // Nothing reaches the gateway, unlike the response-loss case.
    });
    await page
      .getByRole("button", {
        name: kind === "create" ? "新建会话" : "发送任务",
        exact: true,
      })
      .click();
    await expect(
      page.getByRole("button", { name: "查询原请求" }),
    ).toBeVisible();
    expect(
      (await page.request.get(`${origin}/api/requests/${requestId}`)).status(),
    ).toBe(404);
    await page.reload();
    await page.getByRole("button", { name: "查询原请求" }).click();
    await expect(page.getByRole("alert")).toContainText("请求");
    await expect(
      page.getByRole("button", { name: "解除等待（不重发）", exact: true }),
    ).toBeVisible();
    page.once("dialog", (dialog) => dialog.dismiss());
    await page
      .getByRole("button", { name: "解除等待（不重发）", exact: true })
      .click();
    await expect(
      page.getByRole("button", { name: "查询原请求" }),
    ).toBeVisible();
    page.once("dialog", (dialog) => dialog.accept());
    await page
      .getByRole("button", { name: "解除等待（不重发）", exact: true })
      .click();
    await expect(page.getByRole("button", { name: "查询原请求" })).toHaveCount(
      0,
    );
    await expect
      .poll(() =>
        page.evaluate(() => sessionStorage.getItem("agentrt:pending")),
      )
      .toBeNull();
    if (kind === "send") {
      await expect(page.getByLabel("输入任务")).toHaveValue("未送达的任务草稿");
      await expect(
        page.getByRole("button", { name: "发送任务" }),
      ).toBeEnabled();
    } else
      await expect(
        page.getByRole("button", { name: "新建会话", exact: true }),
      ).toBeEnabled();
    await page.reload();
    await expect(
      page.getByRole("button", { name: "解除等待（不重发）", exact: true }),
    ).toHaveCount(0);
    expect(posts).toBe(1);
    await closeTestSession(page);
  });
}

for (const variant of ["new text", "edited back", "unchanged"] as const) {
  test(`regression: a late send receipt respects draft revision (${variant})`, async ({
    page,
  }) => {
    await openAuthenticated(page);
    await page.getByRole("button", { name: "新建会话", exact: true }).click();
    const input = page.getByLabel("输入任务");
    await input.fill("第一条提交的任务");
    await expect(page.getByRole("button", { name: "发送任务" })).toBeEnabled();
    let release!: () => void;
    const held = new Promise<void>((resolve) => {
      release = resolve;
    });
    let accepted!: () => void;
    const arrived = new Promise<void>((resolve) => {
      accepted = resolve;
    });
    await page.route("**/api/sessions/*/messages", async (route) => {
      const response = await route.fetch();
      accepted();
      await held;
      await route.fulfill({ response });
    });
    try {
      await page.getByRole("button", { name: "发送任务" }).click();
      await arrived;
      if (variant !== "unchanged")
        await input.fill("等待回执时编辑的下一条草稿");
      if (variant === "edited back") await input.fill("第一条提交的任务");
      const expected =
        variant === "unchanged"
          ? ""
          : variant === "edited back"
            ? "第一条提交的任务"
            : "等待回执时编辑的下一条草稿";
      release();
      await expect
        .poll(() =>
          page.evaluate(() => sessionStorage.getItem("agentrt:pending")),
        )
        .toBeNull();
      await expect(input).toHaveValue(expected);
      await page.reload();
      await expect(page.getByLabel("输入任务")).toHaveValue(expected);
    } finally {
      release();
      await closeTestSession(page);
    }
  });
}

test("regression: an abandoned lookup cannot consume a newer pending request", async ({
  page,
}) => {
  await openAuthenticated(page);
  let firstId = "";
  let secondId = "";
  let posts = 0;
  await page.route("**/api/sessions", async (route) => {
    if (route.request().method() !== "POST") return route.continue();
    posts++;
    if (posts === 1) {
      firstId = route.request().postDataJSON().request_id;
      await route.fetch(); // Accepted by the gateway, but the receipt is lost.
    } else secondId = route.request().postDataJSON().request_id;
    await route.abort();
  });
  await page.getByRole("button", { name: "新建会话", exact: true }).click();
  await expect(page.getByRole("button", { name: "查询原请求" })).toBeVisible();
  await expect
    .poll(async () =>
      (await page.request.get(`${origin}/api/requests/${firstId}`))
        .json()
        .then((r) => r.state),
    )
    .toBe("completed");
  let release!: () => void;
  const held = new Promise<void>((resolve) => {
    release = resolve;
  });
  let received!: () => void;
  const arrived = new Promise<void>((resolve) => {
    received = resolve;
  });
  let delivered!: () => void;
  const finished = new Promise<void>((resolve) => {
    delivered = resolve;
  });
  await page.route(`**/api/requests/${firstId}`, async (route) => {
    const response = await route.fetch();
    received();
    await held;
    await route.fulfill({ response });
    delivered();
  });
  try {
    await page.getByRole("button", { name: "查询原请求" }).click();
    await arrived;
    page.once("dialog", (dialog) => dialog.accept());
    await page
      .getByRole("button", { name: "解除等待（不重发）", exact: true })
      .click();
    await page.getByRole("button", { name: "新建会话", exact: true }).click();
    await expect(
      page.getByRole("button", { name: "查询原请求" }),
    ).toBeVisible();
    expect(secondId).not.toBe(firstId);
    const responseArrived = page.waitForResponse(
      (response) => response.url() === `${origin}/api/requests/${firstId}`,
    );
    release();
    await finished;
    await (await responseArrived).finished();
    await page.evaluate(
      () =>
        new Promise<void>((resolve) =>
          requestAnimationFrame(() => requestAnimationFrame(() => resolve())),
        ),
    );
    await expect(
      page.getByRole("button", { name: "查询原请求" }),
    ).toBeVisible();
    expect(
      await page.evaluate(
        () => JSON.parse(sessionStorage.getItem("agentrt:pending")!).request_id,
      ),
    ).toBe(secondId);
    expect(new URL(page.url()).searchParams.get("session")).toBeNull();
    expect(posts).toBe(2);
  } finally {
    release();
  }
});
