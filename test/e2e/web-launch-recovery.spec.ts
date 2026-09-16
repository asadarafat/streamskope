import { createServer, type Server } from "node:http";
import { resolve } from "node:path";

import AxeBuilder from "@axe-core/playwright";
import { expect, test } from "@playwright/test";

import {
  type HostCommand,
  type HostCommandResponse,
  type HostEventListener,
  type StreamSkopeBackend,
} from "../../src/features/kafka/contracts";
import { launchWebDevelopment, type RunningWebDevelopment } from "../../src/platform/dev-host";
import { expectWorkbenchReady } from "../support/workbench-browser";

class RejectingBackend implements StreamSkopeBackend {
  execute(command: HostCommand): Promise<HostCommandResponse> {
    return Promise.reject(new Error(`Unexpected host command: ${command.command}`));
  }

  shutdown(): Promise<void> {
    return Promise.resolve();
  }

  subscribe(_listener: HostEventListener): () => void {
    return (): void => undefined;
  }
}

async function listen(server: Server): Promise<number> {
  await new Promise<void>((resolveListen, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => {
      server.off("error", reject);
      resolveListen();
    });
  });
  const address = server.address();
  if (address === null || typeof address === "string") {
    throw new Error("Expected a temporary TCP endpoint.");
  }
  return address.port;
}

async function reservePort(): Promise<number> {
  const server = createServer();
  const port = await listen(server);
  await new Promise<void>((resolveClose, reject) => {
    server.close((error) => {
      if (error === undefined) {
        resolveClose();
      } else {
        reject(error);
      }
    });
  });
  return port;
}

let launch: RunningWebDevelopment | undefined;

function activeLaunch(): RunningWebDevelopment {
  if (launch === undefined) {
    throw new Error("Web development launch is not ready.");
  }
  return launch;
}

test.describe("StreamSkope browser launch recovery", () => {
  test.beforeAll(async () => {
    launch = await launchWebDevelopment({
      backend: new RejectingBackend(),
      hostPort: await reservePort(),
      rendererPort: await reservePort(),
      rendererRoot: resolve(process.cwd()),
    });
  });

  test.afterAll(async () => {
    await launch?.close();
  });

  test("opens a fresh workbench from the clean renderer address", async ({ page }) => {
    await page.goto(activeLaunch().rendererOrigin);

    await expectWorkbenchReady(page);
    expect(new URL(page.url()).hash).toBe("");
    expect(new URL(page.url()).search).toBe("");
    await expect
      .poll(() =>
        page.evaluate(() => {
          return window.sessionStorage.length;
        }),
      )
      .toBe(0);

    const directStatus = await page.evaluate(async (hostOrigin) => {
      const response = await fetch(`${hostOrigin}/health`, { mode: "cors" });
      return response.status;
    }, activeLaunch().host.origin);
    expect(directStatus).toBe(403);
  });

  test("supports clean reloads and independent new tabs", async ({ context, page }) => {
    await page.goto(activeLaunch().browserUrl);
    await expectWorkbenchReady(page);
    expect(new URL(page.url()).hash).toBe("");

    await page.reload();

    await expectWorkbenchReady(page);
    expect(new URL(page.url()).hash).toBe("");

    const separateTab = await context.newPage();
    await separateTab.goto(activeLaunch().rendererOrigin);
    await expectWorkbenchReady(separateTab);
    expect(new URL(separateTab.url()).hash).toBe("");
    await separateTab.close();
  });

  test("scrubs a legacy fragment without retaining or displaying its credential", async ({
    page,
  }) => {
    const legacyCredential = "legacy-browser-capability-must-not-remain-visible";
    await page.goto(
      `${activeLaunch().rendererOrigin}/?legacy=true#host=${encodeURIComponent(
        activeLaunch().host.origin,
      )}&token=${legacyCredential}`,
    );

    await expectWorkbenchReady(page);
    expect(new URL(page.url()).hash).toBe("");
    expect(new URL(page.url()).search).toBe("");
    await expect(page.locator("body")).not.toContainText(legacyCredential);
    expect(
      await page.evaluate(() => {
        return window.sessionStorage.length;
      }),
    ).toBe(0);
    expect((await new AxeBuilder({ page }).analyze()).violations).toEqual([]);
  });

  test("keeps the workbench visible when the internal gateway is unavailable", async ({ page }) => {
    await page.route("**/__streamskope_host/events", async (route) => {
      await route.fulfill({ status: 403 });
    });
    await page.goto(activeLaunch().rendererOrigin);

    await expectWorkbenchReady(page);
    await expect(page.getByRole("contentinfo")).toContainText("Host unavailable");
    await expect(page.getByRole("button", { name: "Reload workbench" })).toBeVisible();
  });
});
