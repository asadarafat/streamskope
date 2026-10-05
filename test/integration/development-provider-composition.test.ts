import { beforeEach, describe, expect, it, vi } from "vitest";

import { HOST_PROTOCOL_VERSION } from "../../src/features/kafka/contracts";
import { NATS_PROTOCOL_VERSION } from "../../src/features/nats/contracts";
import type {
  RunningWebDevelopmentCommand,
  WebDevelopmentCommandDependencies,
  WebDevelopmentProviderApplication,
} from "../../tools/dev/session";

const development = vi.hoisted(() => ({
  application: undefined as WebDevelopmentProviderApplication | undefined,
  startup: undefined as Promise<RunningWebDevelopmentCommand> | undefined,
  kafkaShutdownCalls: 0,
  natsShutdownCalls: 0,
  natsCreateCalls: 0,
  pluginStartCalls: 0,
  natsCreateFailure: undefined as Error | undefined,
  kafkaShutdown: (): Promise<void> => Promise.resolve(),
  asset: { content: new Uint8Array([1, 2, 3]), contentType: "text/javascript" },
}));

vi.mock("../../tools/dev/session", () => ({
  startWebDevelopmentCommand: (
    _options: unknown,
    dependencies: WebDevelopmentCommandDependencies,
  ): Promise<RunningWebDevelopmentCommand> => {
    const task = Promise.resolve().then(async (): Promise<RunningWebDevelopmentCommand> => {
      if (dependencies.createProviders === undefined)
        throw new Error("Expected real development provider composition.");
      development.application = await dependencies.createProviders();
      return {
        browserOpenError: null,
        browserUrl: "http://127.0.0.1:5173/",
        reused: true,
        close: () => development.application!.providers.shutdown(),
      };
    });
    development.startup = task;
    return task;
  },
}));
vi.mock("../../tools/dev/launch", () => ({
  openDevelopmentBrowser: (): Promise<void> => Promise.resolve(),
  webDevelopmentOptions: (): object => ({}),
}));
vi.mock("../../tools/dev/kafka-fixture/development-profile", () => ({
  prepareLocalAioDevelopmentProfile: (): Promise<object> =>
    Promise.resolve({ status: "unavailable", recovery: "Fixture does not start Kafka." }),
}));
vi.mock("../../src/platform/node/plugins/store", () => ({ PluginStore: class {} }));
vi.mock("../../src/platform/node/plugins/runtime", () => ({
  PluginRuntime: class {
    start(): Promise<void> {
      development.pluginStartCalls += 1;
      return Promise.resolve();
    }
    rendererAsset(): Promise<object> {
      return Promise.resolve(development.asset);
    }
  },
}));
vi.mock("../../src/platform/node/kafka-backend", () => ({
  createBrowserKafkaProfileStore: (): object => ({}),
  createKafkaBackend: (): object => ({
    execute: (): Promise<never> =>
      Promise.reject(new Error("Unexpected development Kafka fixture command.")),
    subscribe: (): (() => void) => (): void => undefined,
    stopStream: (): Promise<void> => Promise.resolve(),
    shutdown: (): Promise<void> => {
      development.kafkaShutdownCalls += 1;
      return development.kafkaShutdown();
    },
  }),
}));
vi.mock("../../src/platform/node/nats-backend", () => ({
  createNatsBackend: (): object => {
    development.natsCreateCalls += 1;
    if (development.natsCreateFailure !== undefined) throw development.natsCreateFailure;
    return {
      execute: (): Promise<never> =>
        Promise.reject(new Error("Unexpected development NATS fixture command.")),
      subscribe: (): (() => void) => (): void => undefined,
      stopStream: (): Promise<void> => Promise.resolve(),
      shutdown: (): Promise<void> => {
        development.natsShutdownCalls += 1;
        return Promise.resolve();
      },
    };
  },
}));

beforeEach(() => {
  vi.resetModules();
  development.application = undefined;
  development.startup = undefined;
  development.kafkaShutdownCalls = 0;
  development.natsShutdownCalls = 0;
  development.natsCreateCalls = 0;
  development.pluginStartCalls = 0;
  development.natsCreateFailure = undefined;
  development.kafkaShutdown = (): Promise<void> => Promise.resolve();
  vi.spyOn(process.stdout, "write").mockReturnValue(true);
  vi.spyOn(process.stderr, "write").mockReturnValue(true);
});

describe("actual development provider composition", () => {
  it("selects the distinct Kafka and NATS endpoints and keeps plugin assets under the same lifecycle", async () => {
    await import("../../tools/dev/start");
    await vi.waitFor(() => expect(development.startup).toBeDefined());
    const running = await development.startup!;
    const application = development.application!;
    expect(application.providers.endpoints().map(({ id, version }) => ({ id, version }))).toEqual([
      { id: "kafka", version: HOST_PROTOCOL_VERSION },
      { id: "nats", version: NATS_PROTOCOL_VERSION },
    ]);
    expect(development.pluginStartCalls).toBe(1);
    expect(development.natsCreateCalls).toBe(1);
    await expect(application.pluginAsset?.("/plugins/fixture/renderer.js")).resolves.toBe(
      development.asset,
    );
    await running.close();
    await running.close();
    expect(development.kafkaShutdownCalls).toBe(1);
    expect(development.natsShutdownCalls).toBe(1);
  });

  it.each(["successful", "failed"] as const)(
    "awaits already-created provider cleanup after second-provider startup fails: %s",
    async (cleanup) => {
      const failure = new Error("Development NATS fixture startup failed.");
      development.natsCreateFailure = failure;
      let finish = (): void => undefined;
      let reject = (_error: Error): void => undefined;
      development.kafkaShutdown = (): Promise<void> =>
        new Promise<void>((resolve, fail) => {
          finish = resolve;
          reject = fail;
        });
      const originalExitCode = process.exitCode;
      try {
        await import("../../tools/dev/start");
        await vi.waitFor(() => expect(development.kafkaShutdownCalls).toBe(1));
        let settled = false;
        const outcome = development.startup!.then(
          () => {
            throw new Error("Expected development startup failure.");
          },
          (error: unknown): unknown => {
            settled = true;
            return error;
          },
        );
        expect(settled).toBe(false);
        expect(development.application).toBeUndefined();
        expect(development.natsShutdownCalls).toBe(0);
        if (cleanup === "successful") finish();
        else reject(new Error("Development Kafka fixture cleanup failed."));
        const error = await outcome;
        if (cleanup === "successful") expect(error).toBe(failure);
        else expect(error).toMatchObject({ cause: failure });
        await vi.waitFor(() => expect(process.exitCode).toBe(1));
        expect(development.kafkaShutdownCalls).toBe(1);
      } finally {
        process.exitCode = originalExitCode;
      }
    },
  );
});
