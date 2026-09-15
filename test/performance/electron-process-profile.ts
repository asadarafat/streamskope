import { mkdtemp, readFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { setTimeout as delay } from "node:timers/promises";

import { _electron as electron, expect } from "@playwright/test";
import { Producer } from "@platformatic/kafka";

import { connectElectronToFixture } from "../support/electron-application";
import {
  fixtureClientOptions,
  loadFixtureConfig,
  loadFixtureConnection,
  provisionSeededFixtureTopic,
} from "../support/kafka-fixture";

async function main(): Promise<void> {
  const packaged = JSON.parse(await readFile("dist/package/verification.json", "utf8")) as {
    executablePath: string;
  };
  const directory = await mkdtemp(join(tmpdir(), "streamskope-process-profile-"));
  const streaming = process.argv.includes("--stream");
  const seeded = streaming ? await provisionSeededFixtureTopic() : undefined;
  let producer: Producer<Buffer, Buffer, Buffer, Buffer> | undefined;
  let producing: Promise<void> | undefined;
  let producingFailure: Error | undefined;
  let stopProducing = false;
  let produced = 0;
  const environment = Object.fromEntries(
    Object.entries(process.env).filter(
      (entry): entry is [string, string] => entry[1] !== undefined,
    ),
  );
  delete environment.ELECTRON_RUN_AS_NODE;
  delete environment.STREAMSKOPE_RENDERER_URL;
  environment.NODE_ENV = "production";
  const start = performance.now();
  const application = await electron.launch({
    executablePath: packaged.executablePath,
    args: [`--user-data-dir=${directory}`, ...(process.getuid?.() === 0 ? ["--no-sandbox"] : [])],
    env: environment,
  });
  let hostStderr = "";
  application.process().stderr?.on("data", (data: Buffer) => {
    hostStderr = (hostStderr + data.toString("utf8")).slice(-8192);
  });
  try {
    const page = await application.firstWindow();
    const rendererErrors: string[] = [];
    page.on("pageerror", (error) => {
      if (rendererErrors.length < 10) rendererErrors.push(error.message);
    });
    page.on("console", (message) => {
      if (message.type() === "error" && rendererErrors.length < 10)
        rendererErrors.push(message.text());
    });
    await page.getByRole("banner").waitFor();
    const startupMs = performance.now() - start;
    const cdp = await page.context().newCDPSession(page);
    await cdp.send("Performance.enable");
    const evidence: unknown[] = [];
    for (const state of [
      "idle",
      ...(process.argv.includes("--connect") || streaming ? ["connected"] : []),
      ...(streaming ? ["streaming", "minimized-streaming", "restored-streaming"] : ["minimized"]),
    ]) {
      if (state.startsWith("minimized")) {
        await application.evaluate(({ BrowserWindow }) =>
          BrowserWindow.getAllWindows()[0]?.minimize(),
        );
        await expect
          .poll(() =>
            application.evaluate(({ BrowserWindow }) =>
              BrowserWindow.getAllWindows()[0]?.isMinimized(),
            ),
          )
          .toBe(true);
      }
      if (state === "restored-streaming") {
        await application.evaluate(({ BrowserWindow }) =>
          BrowserWindow.getAllWindows()[0]?.restore(),
        );
        await expect.poll(() => page.evaluate(() => document.visibilityState)).toBe("visible");
      }
      if (state === "connected") {
        await application.evaluate(({ BrowserWindow }) =>
          BrowserWindow.getAllWindows()[0]?.restore(),
        );
        if (!(await page.getByRole("button", { name: "Add profile" }).isEnabled())) {
          evidence.push({
            state,
            unverified:
              "Profile creation is unavailable in this production host; no connected measurement was attempted.",
          });
          process.exitCode = 1;
          continue;
        }
        await connectElectronToFixture(
          page,
          seeded?.config ?? (await loadFixtureConfig()),
          await loadFixtureConnection(),
        );
      }
      if (state === "streaming" && seeded !== undefined) {
        await page.getByRole("searchbox", { name: "Search topics" }).fill(seeded.config.topic);
        await page.getByRole("button", { name: seeded.config.topic, exact: true }).click();
        await expect(page.getByRole("button", { name: "Show message filters" })).toBeVisible();
        producer = new Producer<Buffer, Buffer, Buffer, Buffer>({
          ...(await fixtureClientOptions(
            await loadFixtureConnection(),
            seeded.config,
            "streamskope-process-profiler",
          )),
          autocreateTopics: false,
        });
        const activeProducer = producer;
        producing = (async (): Promise<void> => {
          const start = performance.now();
          const value = Buffer.alloc(256, 120);
          while (!stopProducing) {
            const due = Math.floor(performance.now() - start) - produced;
            if (due < 1) {
              await delay(5);
              continue;
            }
            const count = Math.min(due, 200);
            await activeProducer.send({
              messages: Array.from({ length: count }, (_, index) => ({
                topic: seeded.config.topic,
                key: Buffer.from(`perf-${produced + index}`),
                value,
              })),
            });
            produced += count;
          }
        })().catch((error: unknown) => {
          producingFailure = error instanceof Error ? error : new Error(String(error));
        });
        await expect(page.getByRole("gridcell").filter({ hasText: "perf-" }).first()).toBeVisible({
          timeout: 10000,
        });
      }
      await application.evaluate(({ app }) => app.getAppMetrics());
      await page.waitForTimeout(1000);
      const samples: unknown[] = [];
      const producedBeforeSamples = produced;
      const visibleSequence = async (): Promise<number> =>
        page.evaluate(() => {
          const keys = [...document.querySelectorAll('[role="gridcell"]')].flatMap((cell) => {
            const match = /perf-(\d+)/.exec(cell.textContent ?? "");
            return match?.[1] === undefined ? [] : [Number(match[1])];
          });
          return keys.length === 0 ? -1 : Math.max(...keys);
        });
      const visibleSequenceBefore = state.includes("streaming") ? await visibleSequence() : -1;
      for (let index = 0; index < (state.includes("streaming") ? 20 : 5); index += 1) {
        if (
          state.includes("streaming") &&
          (await page.getByText("Host unavailable", { exact: true }).isVisible())
        ) {
          process.stderr.write(`${hostStderr}\n`);
          process.stderr.write(`${JSON.stringify(rendererErrors)}\n`);
          throw new Error("Production host became unavailable during streaming");
        }
        const main = await application.evaluate(({ app }) => ({
          versions: process.versions,
          memory: process.memoryUsage(),
          metrics: app.getAppMetrics(),
        }));
        const osMemory = [];
        for (const metric of main.metrics) {
          let rollup: string | null = null;
          if (process.platform === "linux") {
            rollup = await readFile(`/proc/${metric.pid}/smaps_rollup`, "utf8").catch(() => null);
          }
          const readKb = (name: string): number | null => {
            const match = rollup?.match(new RegExp(`^${name}:\\s+(\\d+)`, "m"));
            return match?.[1] === undefined ? null : Number(match[1]) * 1024;
          };
          osMemory.push({
            pid: metric.pid,
            type: metric.type,
            rss: readKb("Rss"),
            pss: readKb("Pss"),
            privateClean: readKb("Private_Clean"),
            privateDirty: readKb("Private_Dirty"),
          });
        }
        samples.push({
          main,
          osMemory,
          rendererHeap: await cdp.send("Runtime.getHeapUsage"),
          rendererMetrics: await cdp.send("Performance.getMetrics"),
        });
        await page.waitForTimeout(500);
      }
      const mainTimerDelayMs = await application.evaluate(async () => {
        const delays: number[] = [];
        for (let index = 0; index < 30; index += 1) {
          const started = process.hrtime.bigint();
          await new Promise<void>((resolve) => setTimeout(resolve, 10));
          delays.push(Math.max(0, Number(process.hrtime.bigint() - started) / 1e6 - 10));
        }
        return delays;
      });
      const interactionToFrameMs: number[] = [];
      if (state === "streaming" || state === "restored-streaming") {
        for (let index = 0; index < 40; index += 1) {
          interactionToFrameMs.push(
            await page.evaluate(async () => {
              const control = document.querySelector<HTMLButtonElement>(
                'button[aria-label="Show message filters"], button[aria-label="Hide message filters"]',
              );
              if (control === null) throw new Error("Message filter control missing");
              const showing = control.getAttribute("aria-label") === "Show message filters";
              const start = performance.now();
              control.click();
              await new Promise<void>((resolve) =>
                requestAnimationFrame(() => requestAnimationFrame(() => resolve())),
              );
              const updated = document.querySelector(
                `button[aria-label="${showing ? "Hide" : "Show"} message filters"]`,
              );
              if (updated === null) throw new Error("Filter interaction did not update the UI");
              return performance.now() - start;
            }),
          );
        }
      }
      if (producingFailure !== undefined) throw producingFailure;
      if (state.includes("streaming") && produced <= producedBeforeSamples)
        throw new Error("Streaming producer made no progress");
      const visibleSequenceAfter = state.includes("streaming") ? await visibleSequence() : -1;
      process.stderr.write(
        `${JSON.stringify({ state, produced, visibleSequenceBefore, visibleSequenceAfter })}\n`,
      );
      if (
        (state === "streaming" || state === "restored-streaming") &&
        visibleSequenceAfter <= visibleSequenceBefore
      ) {
        throw new Error("Visible streamed records made no progress");
      }
      evidence.push({
        state,
        visibleSequenceBefore,
        visibleSequenceAfter,
        produced,
        producedDuringSamples: produced - producedBeforeSamples,
        interactionToFrameMs,
        windowIsMinimized: await application.evaluate(({ BrowserWindow }) =>
          BrowserWindow.getAllWindows()[0]?.isMinimized(),
        ),
        rendererVisibility: await page.evaluate(() => document.visibilityState),
        mainTimerDelayMs,
        samples,
      });
    }
    process.stdout.write(
      `${JSON.stringify({ method: "Isolated production package; fresh OS-protected profile; five idle/connected or twenty active process samples at 500 ms. Optional --stream offers 1k/s 256-byte records on one temporary partition; producer runs outside Electron. RSS sums double-count shared pages; Linux PSS included. Thirty 10-ms main-thread timer overshoot probes per state. Interaction probes measure synthetic filter-button click to two animation frames with confirmed UI state, not hardware input or receive-to-visible latency. Renderer external memory is not measured.", startupMs, evidence }, null, 2)}\n`,
    );
  } finally {
    stopProducing = true;
    await producing;
    await producer?.close();
    await application.close();
    await seeded?.dispose();
  }
}

void main().catch((error: unknown) => {
  process.stderr.write(`${error instanceof Error ? error.stack : String(error)}\n`);
  process.exitCode = 1;
});
