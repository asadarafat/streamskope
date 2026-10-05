import { mkdtemp, readdir, rm, stat, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { afterEach, describe, expect, it, vi } from "vitest";

import { openDevelopmentBrowser } from "../../tools/dev/launch";
import type { DevelopmentBackend, RunningWebDevelopment } from "../../src/platform/dev-host";
import { ProviderHostRegistry } from "../../src/platform/node/provider-host";
import { createProviderFixture } from "../support/provider-fixture";
import {
  startWebDevelopmentCommand,
  WebDevelopmentSessionConflictError,
  type WebDevelopmentCommandDependencies,
  type WebDevelopmentCommandOptions,
} from "../../tools/dev/session";

const BROWSER_URL = "http://clab.orb.local:5173/";

class FakeBackend implements DevelopmentBackend {
  shutdownCalls = 0;

  execute(): Promise<never> {
    return Promise.reject(new Error("No backend command was expected."));
  }

  shutdown(): Promise<void> {
    this.shutdownCalls += 1;
    return Promise.resolve();
  }

  subscribe(): () => void {
    return () => undefined;
  }
}

interface CommandFixture {
  readonly backends: FakeBackend[];
  readonly browserUrls: string[];
  readonly dependencies: WebDevelopmentCommandDependencies;
  readonly launchCalls: number[];
}

const temporaryDirectories: string[] = [];

afterEach(async () => {
  await Promise.all(
    temporaryDirectories
      .splice(0)
      .map((directory) => rm(directory, { force: true, recursive: true })),
  );
});

async function commandOptions(): Promise<
  WebDevelopmentCommandOptions & { readonly sessionDirectory: string }
> {
  const root = await mkdtemp(join(tmpdir(), "streamskope-command-root-"));
  const sessionDirectory = await mkdtemp(join(tmpdir(), "streamskope-command-state-"));
  temporaryDirectories.push(root, sessionDirectory);
  return {
    hostPort: 4319,
    publicHostname: "clab.orb.local",
    rendererPort: 5173,
    rendererRoot: root,
    sessionDirectory,
  };
}

function commandFixture({
  isExistingSessionReady = (): Promise<boolean> => Promise.resolve(true),
  isProcessAlive,
  ownerPid,
}: {
  readonly isExistingSessionReady?: (browserUrl: string) => Promise<boolean>;
  readonly isProcessAlive: (pid: number) => boolean;
  readonly ownerPid: number;
}): CommandFixture {
  const backends: FakeBackend[] = [];
  const browserUrls: string[] = [];
  const launchCalls: number[] = [];
  return {
    backends,
    browserUrls,
    dependencies: {
      createBackend: (): FakeBackend => {
        const backend = new FakeBackend();
        backends.push(backend);
        return backend;
      },
      isExistingSessionReady,
      isProcessAlive,
      launch: (options): Promise<RunningWebDevelopment> => {
        const backend = options.backend;
        if (backend === undefined) throw new Error("Expected the command's Kafka backend.");
        launchCalls.push(options.hostPort);
        let closePromise: Promise<void> | undefined;
        return Promise.resolve({
          browserUrl: BROWSER_URL,
          close: (): Promise<void> => {
            closePromise ??= backend.shutdown();
            return closePromise;
          },
          host: {
            close: () => Promise.resolve(),
            hostname: "clab.orb.local",
            origin: "http://clab.orb.local:4319",
            port: 4319,
          },
          rendererOrigin: "http://clab.orb.local:5173",
        });
      },
      openBrowser: (browserUrl: string): Promise<void> => {
        browserUrls.push(browserUrl);
        return Promise.resolve();
      },
      ownerPid,
    },
    launchCalls,
  };
}

describe("web-development command session", () => {
  it("passes a composite provider registry and plugin assets through the launcher without a Kafka wrapper", async () => {
    const options = await commandOptions();
    const kafka = createProviderFixture({ id: "kafka", version: 11 });
    const nats = createProviderFixture({ id: "nats", version: 12 });
    const providers = new ProviderHostRegistry([kafka.endpoint, nats.endpoint]);
    const pluginAsset = (): Promise<undefined> => Promise.resolve(undefined);
    const running = await startWebDevelopmentCommand(options, {
      createProviders: () => ({ providers, pluginAsset }),
      ownerPid: 101,
      isProcessAlive: (pid) => pid === 101,
      launch: (submitted): Promise<RunningWebDevelopment> => {
        expect(submitted.providers).toBe(providers);
        expect(submitted.backend).toBeUndefined();
        expect(submitted.pluginAsset).toBe(pluginAsset);
        return Promise.resolve({
          browserUrl: BROWSER_URL,
          close: () => providers.shutdown(),
          host: {
            close: () => Promise.resolve(),
            hostname: "clab.orb.local",
            origin: "http://clab.orb.local:4319",
            port: 4319,
          },
          rendererOrigin: "http://clab.orb.local:5173",
        });
      },
    });
    const request = nats.command();
    await expect(providers.get("nats")?.dispatch(request)).resolves.toEqual(nats.response(request));
    await running.close();
    await running.close();
    expect(kafka.shutdownCalls).toBe(1);
    expect(nats.shutdownCalls).toBe(1);
    expect(await readdir(options.sessionDirectory)).toEqual([]);
  });

  it("attempts sibling registry cleanup and releases the lease while a rejected launch waits for deferred cleanup", async () => {
    const options = await commandOptions();
    const kafka = createProviderFixture({ id: "kafka", version: 11 });
    const nats = createProviderFixture({ id: "nats", version: 12 });
    const providers = new ProviderHostRegistry([kafka.endpoint, nats.endpoint]);
    let finishNats = (): void => undefined;
    kafka.shutdownOperation = (): Promise<void> => {
      throw new Error("Kafka fixture cleanup failed.");
    };
    nats.shutdownOperation = (): Promise<void> =>
      new Promise<void>((resolve) => {
        finishNats = resolve;
      });
    const launchFailure = new Error("Fixture launcher rejected before returning ownership.");
    let settled = false;
    const outcome = startWebDevelopmentCommand(options, {
      createProviders: () => ({ providers }),
      ownerPid: 101,
      isProcessAlive: (pid) => pid === 101,
      launch: (): Promise<never> => Promise.reject(launchFailure),
    }).then(
      () => {
        throw new Error("Expected startup rejection.");
      },
      (error: unknown): unknown => {
        settled = true;
        return error;
      },
    );
    await vi.waitFor(() => expect(nats.shutdownCalls).toBe(1));
    expect(kafka.shutdownCalls).toBe(1);
    expect(settled).toBe(false);
    await vi.waitFor(async () => expect(await readdir(options.sessionDirectory)).toEqual([]));
    expect(settled).toBe(false);
    finishNats();
    const failure = await outcome;
    expect(failure).toBeInstanceOf(AggregateError);
    expect((failure as AggregateError).cause).toBe(launchFailure);
    expect((failure as AggregateError).errors[0]).toBe(launchFailure);
    expect(kafka.shutdownCalls).toBe(1);
    expect(nats.shutdownCalls).toBe(1);
  });

  it("releases the lease when a legacy backend shutdown throws synchronously before launch returns", async () => {
    const options = await commandOptions();
    const backend = new FakeBackend();
    backend.shutdown = (): Promise<void> => {
      backend.shutdownCalls += 1;
      throw new Error("Legacy cleanup fixture failure.");
    };
    const launchFailure = new Error("Legacy launcher fixture failure.");
    await expect(
      startWebDevelopmentCommand(options, {
        createBackend: () => backend,
        ownerPid: 101,
        isProcessAlive: (pid) => pid === 101,
        launch: (): Promise<never> => {
          throw launchFailure;
        },
      }),
    ).rejects.toMatchObject({ cause: launchFailure });
    expect(backend.shutdownCalls).toBe(1);
    expect(await readdir(options.sessionDirectory)).toEqual([]);
  });

  it("awaits development profile preparation before launching a new backend", async () => {
    const options = await commandOptions();
    const backend = new FakeBackend();
    let preparation = "pending";
    let launchedBackend: DevelopmentBackend | undefined;

    const running = await startWebDevelopmentCommand(options, {
      createBackend: async (): Promise<FakeBackend> => {
        await Promise.resolve();
        preparation = "complete";
        return backend;
      },
      isProcessAlive: (pid) => pid === 101,
      launch: (launchOptions): Promise<RunningWebDevelopment> => {
        const backend = launchOptions.backend;
        if (backend === undefined) throw new Error("Expected the command's Kafka backend.");
        expect(preparation).toBe("complete");
        launchedBackend = backend;
        return Promise.resolve({
          browserUrl: BROWSER_URL,
          close: () => backend.shutdown(),
          host: {
            close: () => Promise.resolve(),
            hostname: "clab.orb.local",
            origin: "http://clab.orb.local:4319",
            port: 4319,
          },
          rendererOrigin: "http://clab.orb.local:5173",
        });
      },
      ownerPid: 101,
    });

    expect(launchedBackend).toBe(backend);
    await running.close();
  });

  it("reuses one healthy exact session without constructing a second backend", async () => {
    const options = await commandOptions();
    const fixture = commandFixture({
      isExistingSessionReady: (browserUrl): Promise<boolean> =>
        Promise.resolve(browserUrl === BROWSER_URL),
      isProcessAlive: (pid) => pid === 101 || pid === 202,
      ownerPid: 101,
    });

    let preparations = 0;
    const dependencies = {
      ...fixture.dependencies,
      prepare: (): Promise<void> => {
        preparations += 1;
        return Promise.resolve();
      },
    };

    const first = await startWebDevelopmentCommand(options, dependencies);
    const second = await startWebDevelopmentCommand(options, {
      ...dependencies,
      ownerPid: 202,
    });

    expect(first).toMatchObject({ browserUrl: BROWSER_URL, reused: false });
    expect(preparations).toBe(2);
    expect(second).toMatchObject({ browserUrl: BROWSER_URL, reused: true });
    expect(fixture.backends).toHaveLength(1);
    expect(fixture.launchCalls).toEqual([4319]);
    expect(fixture.browserUrls).toEqual([BROWSER_URL, BROWSER_URL]);

    await second.close();
    expect(fixture.backends[0]?.shutdownCalls).toBe(0);
    await first.close();
    expect(fixture.backends[0]?.shutdownCalls).toBe(1);
  });

  it("replaces a verified same-repository session when its hostname changes", async () => {
    const options = await commandOptions();
    let alive = true;
    const fixture = commandFixture({
      isProcessAlive: (pid) => pid === 101 && alive,
      ownerPid: 101,
    });
    const first = await startWebDevelopmentCommand(options, fixture.dependencies);
    const stopped: number[] = [];
    const second = await startWebDevelopmentCommand(
      { ...options, publicHostname: "127.0.0.1" },
      {
        ...fixture.dependencies,
        ownerPid: 202,
        stopOwnedSession: async (pid, identity) => {
          expect(identity.rendererRoot).toBe(options.rendererRoot);
          stopped.push(pid);
          alive = false;
          await first.close();
        },
      },
    );
    expect(stopped).toEqual([101]);
    expect(second.reused).toBe(false);
    await second.close();
  });

  it("does not stop a conflicting session that fails authenticated readiness", async () => {
    const options = await commandOptions();
    const fixture = commandFixture({ isProcessAlive: () => true, ownerPid: 101 });
    const first = await startWebDevelopmentCommand(options, fixture.dependencies);
    let stopped = false;
    await expect(
      startWebDevelopmentCommand(
        { ...options, publicHostname: "127.0.0.1" },
        {
          ...fixture.dependencies,
          ownerPid: 202,
          isExistingSessionReady: () => Promise.resolve(false),
          stopOwnedSession: () => {
            stopped = true;
            return Promise.resolve();
          },
        },
      ),
    ).rejects.toThrow();
    expect(stopped).toBe(false);
    await first.close();
  });

  it("recovers a dead owner's record without allowing its cleanup to remove the replacement", async () => {
    const options = await commandOptions();
    const firstFixture = commandFixture({
      isProcessAlive: (pid) => pid === 101,
      ownerPid: 101,
    });
    const first = await startWebDevelopmentCommand(options, firstFixture.dependencies);

    const replacementFixture = commandFixture({
      isProcessAlive: (pid) => pid === 202 || pid === 303,
      ownerPid: 202,
    });
    const replacement = await startWebDevelopmentCommand(options, replacementFixture.dependencies);
    expect(replacement.reused).toBe(false);

    await first.close();
    const observer = await startWebDevelopmentCommand(options, {
      ...replacementFixture.dependencies,
      ownerPid: 303,
    });

    expect(observer).toMatchObject({ browserUrl: BROWSER_URL, reused: true });
    expect(replacementFixture.backends).toHaveLength(1);
    await observer.close();
    await replacement.close();
  });

  it("refuses to replace a live session that cannot confirm authenticated readiness", async () => {
    const options = await commandOptions();
    const fixture = commandFixture({
      isExistingSessionReady: (): Promise<boolean> => Promise.resolve(false),
      isProcessAlive: (pid) => pid === 101 || pid === 202,
      ownerPid: 101,
    });
    const first = await startWebDevelopmentCommand(options, fixture.dependencies);

    await expect(
      startWebDevelopmentCommand(options, {
        ...fixture.dependencies,
        ownerPid: 202,
      }),
    ).rejects.toThrow(WebDevelopmentSessionConflictError);
    expect(fixture.backends).toHaveLength(1);
    expect(fixture.launchCalls).toEqual([4319]);

    await first.close();
  });

  it("keeps coordination state private and removes it after graceful shutdown", async () => {
    const options = await commandOptions();
    const fixture = commandFixture({
      isProcessAlive: (pid) => pid === 101,
      ownerPid: 101,
    });
    const running = await startWebDevelopmentCommand(options, fixture.dependencies);

    const entries = await readdir(options.sessionDirectory);
    expect(entries).toHaveLength(1);
    if (process.platform !== "win32") {
      expect((await stat(options.sessionDirectory)).mode & 0o077).toBe(0);
      expect((await stat(join(options.sessionDirectory, entries[0] ?? ""))).mode & 0o077).toBe(0);
    }

    await running.close();
    expect(await readdir(options.sessionDirectory)).toEqual([]);
  });

  it("keeps a ready owned session available when the browser opener fails", async () => {
    const options = await commandOptions();
    const fixture = commandFixture({
      isProcessAlive: (pid) => pid === 101,
      ownerPid: 101,
    });
    const running = await startWebDevelopmentCommand(options, {
      ...fixture.dependencies,
      openBrowser: (): Promise<void> => Promise.reject(new Error("No browser opener")),
    });

    expect(running).toMatchObject({
      browserOpenError: "No browser opener",
      browserUrl: BROWSER_URL,
      reused: false,
    });
    expect(fixture.backends).toHaveLength(1);

    await running.close();
    expect(fixture.backends[0]?.shutdownCalls).toBe(1);
  });

  it.skipIf(process.platform === "win32")(
    "acknowledges a long-lived remote-development opener without killing it",
    async () => {
      const directory = await mkdtemp(join(tmpdir(), "streamskope-browser-opener-"));
      temporaryDirectories.push(directory);
      const opener = join(directory, "browser-opener.sh");
      await writeFile(opener, "#!/bin/sh\nsleep 20\n", { mode: 0o700 });
      const previousBrowser = process.env.BROWSER;
      process.env.BROWSER = opener;
      const startedAt = Date.now();
      try {
        await expect(openDevelopmentBrowser(BROWSER_URL)).resolves.toBeUndefined();
        expect(Date.now() - startedAt).toBeLessThan(5_000);
      } finally {
        if (previousBrowser === undefined) {
          Reflect.deleteProperty(process.env, "BROWSER");
        } else {
          process.env.BROWSER = previousBrowser;
        }
      }
    },
  );
});
