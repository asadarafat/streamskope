import { tmpdir } from "node:os";
import { join } from "node:path";

import { beforeEach, describe, expect, it, vi } from "vitest";

import { createElectronKafkaBackend } from "../../src/platform/electron/main/electron-kafka-backend";
import { ReversibleSafeStorage } from "../support/protected-profile-fixture";

const startup = vi.hoisted(() => ({
  backendCreates: 0,
  shutdownCalls: 0,
  pluginStarts: 0,
  startOperation: (): Promise<void> => Promise.resolve(),
  shutdownOperation: (): Promise<void> => Promise.resolve(),
}));

vi.mock("../../src/platform/node/kafka-backend", () => ({
  createKafkaBackend: (): object => {
    startup.backendCreates += 1;
    return {
      shutdown: (): Promise<void> => {
        startup.shutdownCalls += 1;
        return startup.shutdownOperation();
      },
    };
  },
}));
vi.mock("../../src/platform/node/plugins/runtime", () => ({
  PluginRuntime: class {
    start(): Promise<void> {
      startup.pluginStarts += 1;
      return startup.startOperation();
    }
  },
}));

beforeEach(() => {
  startup.backendCreates = 0;
  startup.shutdownCalls = 0;
  startup.pluginStarts = 0;
  startup.startOperation = (): Promise<void> => Promise.resolve();
  startup.shutdownOperation = (): Promise<void> => Promise.resolve();
});

function construct(): ReturnType<typeof createElectronKafkaBackend> {
  return createElectronKafkaBackend({
    platform: "linux",
    safeStorage: new ReversibleSafeStorage(),
    userDataPath: join(tmpdir(), "streamskope-unopened-backend-startup-fixture"),
    profileProtection: {
      capability: {
        durability: "durable",
        protection: "unavailable",
        state: "unavailable",
        recovery: "Fixture store is disabled.",
      },
    },
  });
}

describe("actual Electron backend factory startup ownership", () => {
  it.each(["synchronous", "asynchronous"] as const)(
    "awaits its unpublished backend after %s plugin startup rejection",
    async (failure) => {
      const startupFailure = new Error("Fixture plugin startup failed.");
      startup.startOperation = (): Promise<void> => {
        if (failure === "synchronous") throw startupFailure;
        return Promise.reject(startupFailure);
      };
      let finish = (): void => undefined;
      startup.shutdownOperation = (): Promise<void> =>
        new Promise<void>((resolve) => {
          finish = resolve;
        });
      let settled = false;
      const outcome = construct().then(
        () => {
          throw new Error("Expected startup failure.");
        },
        (error: unknown): unknown => {
          settled = true;
          return error;
        },
      );
      await vi.waitFor(() => expect(startup.shutdownCalls).toBe(1));
      expect(startup.backendCreates).toBe(1);
      expect(startup.pluginStarts).toBe(1);
      expect(settled).toBe(false);
      finish();
      expect(await outcome).toBe(startupFailure);
      expect(startup.shutdownCalls).toBe(1);
    },
  );

  it.each(["synchronous", "asynchronous"] as const)(
    "preserves both startup and %s cleanup failures instead of reporting successful ownership transfer",
    async (failure) => {
      const startupFailure = new Error("Fixture plugin startup failed.");
      const cleanupFailure = new Error("Fixture backend cleanup failed.");
      startup.startOperation = (): Promise<void> => Promise.reject(startupFailure);
      startup.shutdownOperation = (): Promise<void> => {
        if (failure === "synchronous") throw cleanupFailure;
        return Promise.reject(cleanupFailure);
      };
      const outcome = await construct().catch((error: unknown): unknown => error);
      expect(outcome).toBeInstanceOf(AggregateError);
      expect((outcome as AggregateError).cause).toBe(cleanupFailure);
      expect((outcome as AggregateError).errors).toEqual([startupFailure, cleanupFailure]);
      expect(startup.backendCreates).toBe(1);
      expect(startup.shutdownCalls).toBe(1);
    },
  );
});
