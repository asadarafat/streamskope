import { mkdir, mkdtemp, readFile, readdir, rm, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";

import { afterEach, beforeAll, describe, expect, it } from "vitest";

import { HOST_PROTOCOL_VERSION, type HostEvent } from "../../src/features/kafka/contracts";
import type { KafkaProfileRecord } from "../../src/features/kafka/application";
import { NatsProfileService } from "../../src/features/nats/application/profile-service";
import type {
  NatsProfileCreateInput,
  NatsProfileUpdateInput,
} from "../../src/features/nats/contracts";
import { AtomicKafkaProfileFileStore } from "../../src/platform/node/kafka-profile-file-store";
import type { ProfileProtector } from "../../src/platform/node/profile-protector";
import { createElectronKafkaBackend } from "../../src/platform/electron/main/electron-kafka-backend";
import { createElectronNatsProfileStore } from "../../src/platform/electron/main/electron-nats-profile-store";
import { initializeElectronProfileProtection } from "../../src/platform/electron/main/electron-profile-protection";
import {
  ReversibleSafeStorage,
  protectedProfileFixtureCa,
} from "../support/protected-profile-fixture";

const directories: string[] = [];
const time = "2026-10-05T12:00:00.000Z";
let caPem: string;

beforeAll(async () => {
  caPem = await protectedProfileFixtureCa();
});
afterEach(async () => {
  await Promise.all(
    directories.splice(0).map((directory) => rm(directory, { force: true, recursive: true })),
  );
});

async function userData(): Promise<string> {
  const directory = await mkdtemp(join(tmpdir(), "streamskope-shared-profile-"));
  directories.push(directory);
  return directory;
}

function input(): NatsProfileCreateInput {
  return {
    name: "Protected NATS",
    servers: ["tls://nats.example.test:4222"],
    authentication: {
      mode: "token",
      token: { mode: "replace", value: "nats-secret-only-in-host" },
    },
    tls: { mode: "tls", caPem: { mode: "replace", value: caPem } },
  };
}

function retained(name: string): NatsProfileUpdateInput {
  return {
    ...input(),
    name,
    authentication: { mode: "token", token: { mode: "retain" } },
    tls: { mode: "tls", caPem: { mode: "retain" } },
  };
}

async function seedKafka(path: string, protector: ProfileProtector): Promise<readonly string[]> {
  const profile: KafkaProfileRecord = {
    id: "existing-kafka",
    brokers: ["kafka.example.test:9093"],
    name: "Existing Kafka",
    createdAt: time,
    updatedAt: time,
    trust: { kind: "pem", label: "ca.pem", material: caPem },
  };
  const protectedValue = await protector.protect(
    JSON.stringify({ profileId: profile.id, trust: { material: caPem }, version: 1 }),
  );
  await mkdir(join(path, ".."), { recursive: true });
  await writeFile(
    path,
    JSON.stringify({
      version: 2,
      profiles: [
        {
          id: profile.id,
          name: profile.name,
          brokers: profile.brokers,
          createdAt: time,
          updatedAt: time,
          trust: { kind: "pem", label: "ca.pem", materialPresent: true, passwordPresent: false },
          protectedValue: protectedValue.toString("base64"),
        },
      ],
    }),
    { mode: 0o600 },
  );
  await new AtomicKafkaProfileFileStore(path, protector, {
    durability: "durable",
    protection: "os-protected",
    state: "ready",
  }).commit([{ ...profile, revision: 1, transport: "tls" }]);
  return (await readdir(join(path, ".."))).sort();
}

describe("shared native profile protection", () => {
  it("uses one OS initialization while NATS lifecycle leaves Kafka data and recovery generations unchanged", async () => {
    const userDataPath = await userData();
    const safeStorage = new ReversibleSafeStorage();
    const profileProtection = await initializeElectronProfileProtection(safeStorage, "linux");
    if (profileProtection.protector === undefined)
      throw new Error("Expected protected fixture storage.");
    const kafkaPath = join(userDataPath, "profiles", "kafka-profiles.json");
    const kafkaFiles = await seedKafka(kafkaPath, profileProtection.protector);
    expect(kafkaFiles).toEqual([
      "kafka-profiles.json",
      "kafka-profiles.json.pre-transport-v2",
      "kafka-profiles.json.pre-upgrade.bak",
    ]);
    const before = await Promise.all(
      kafkaFiles.map((name) => readFile(join(kafkaPath, "..", name))),
    );
    const kafka = await createElectronKafkaBackend({
      platform: "linux",
      safeStorage,
      userDataPath,
      profileProtection,
    });
    const events: HostEvent[] = [];
    kafka.subscribe((event) => {
      events.push(event);
    });
    try {
      const store = createElectronNatsProfileStore({ userDataPath, profileProtection });
      const service = new NatsProfileService(store, {
        createId: (): string => "nats-native",
        now: (): Date => new Date(time),
      });
      const created = await service.create(input());
      expect(created.profiles[0]).toMatchObject({
        id: "nats-native",
        revision: 1,
        authentication: { mode: "token", tokenPresent: true },
        tls: { mode: "tls", caPresent: true },
      });
      expect(JSON.stringify(created)).not.toMatch(/nats-secret-only-in-host|BEGIN CERTIFICATE/u);
      await service.update("nats-native", 1, retained("Renamed NATS"));

      const reopened = new NatsProfileService(
        createElectronNatsProfileStore({ userDataPath, profileProtection }),
      );
      const restored = await reopened.resolve("nats-native", 2);
      expect(restored.connection).toEqual({
        servers: input().servers,
        authentication: { mode: "token", token: "nats-secret-only-in-host" },
        tls: { mode: "tls", caPem },
      });
      expect((await reopened.list()).profiles.map((profile) => profile.id)).toEqual([
        "nats-native",
      ]);
      await reopened.delete("nats-native", 2);
      expect(
        (
          await new NatsProfileService(
            createElectronNatsProfileStore({ userDataPath, profileProtection }),
          ).list()
        ).profiles,
      ).toEqual([]);

      await expect(
        kafka.execute({
          version: HOST_PROTOCOL_VERSION,
          command: "profiles.list",
          id: "list-retained-kafka",
          payload: {},
        }),
      ).resolves.toMatchObject({ ok: true });
      const kafkaSnapshot = events
        .filter((event) => event.event === "profiles.changed")
        .at(-1)?.payload;
      expect(kafkaSnapshot?.profiles.map((profile) => profile.id)).toEqual(["existing-kafka"]);
      expect(
        await Promise.all(kafkaFiles.map((name) => readFile(join(kafkaPath, "..", name)))),
      ).toEqual(before);
      expect((await readdir(join(kafkaPath, ".."))).sort()).toEqual(
        [...kafkaFiles, "nats-profiles.json"].sort(),
      );
      expect(safeStorage.availabilityCalls).toBe(1);
      expect(safeStorage.backendCalls).toBe(1);
    } finally {
      await kafka.shutdown();
    }
  });

  it("keeps retained secrets through rename, rejects stale revisions before encryption, and applies explicit replacement/clear", async () => {
    const userDataPath = await userData();
    const safeStorage = new ReversibleSafeStorage();
    const profileProtection = await initializeElectronProfileProtection(safeStorage, "linux");
    const service = new NatsProfileService(
      createElectronNatsProfileStore({ userDataPath, profileProtection }),
      { createId: (): string => "editable-nats", now: (): Date => new Date(time) },
    );
    await service.create(input());
    await service.update("editable-nats", 1, retained("Renamed profile"));
    const path = join(userDataPath, "profiles", "nats-profiles.json");
    const original = await readFile(path);
    const protectedCount = safeStorage.protectedInputs.length;

    await expect(service.update("editable-nats", 1, retained("Stale name"))).rejects.toMatchObject({
      code: "revision-conflict",
    });
    expect(await readFile(path)).toEqual(original);
    expect(safeStorage.protectedInputs).toHaveLength(protectedCount);
    const renamed = await new NatsProfileService(
      createElectronNatsProfileStore({ userDataPath, profileProtection }),
    ).resolve("editable-nats", 2);
    expect(renamed.connection.authentication).toEqual({
      mode: "token",
      token: "nats-secret-only-in-host",
    });
    expect(renamed.connection.tls).toEqual({ mode: "tls", caPem });

    await service.update("editable-nats", 2, {
      ...retained("Replaced token"),
      authentication: {
        mode: "token",
        token: { mode: "replace", value: "deliberately-replaced-token" },
      },
      tls: { mode: "tls", caPem: { mode: "clear" } },
    });
    const replaced = await new NatsProfileService(
      createElectronNatsProfileStore({ userDataPath, profileProtection }),
    ).resolve("editable-nats", 3);
    expect(replaced.connection.authentication).toEqual({
      mode: "token",
      token: "deliberately-replaced-token",
    });
    expect(replaced.connection.tls).toEqual({ mode: "tls" });
    await service.update("editable-nats", 3, {
      name: "No secret",
      servers: ["nats://nats.example.test:4222"],
      authentication: { mode: "none" },
      tls: { mode: "plaintext" },
    });
    const cleared = await new NatsProfileService(
      createElectronNatsProfileStore({ userDataPath, profileProtection }),
    ).resolve("editable-nats", 4);
    expect(cleared.connection.authentication).toEqual({ mode: "none" });
    expect(cleared.connection.tls).toEqual({ mode: "plaintext" });
  });

  it.each([
    "unavailable",
    "basic_text",
    "unknown",
    "availability rejection",
    "backend lookup rejection",
  ])("does not create a durable NATS file when OS protection reports %s", async (failure) => {
    const userDataPath = await userData();
    const safeStorage = new ReversibleSafeStorage();
    if (failure === "unavailable") safeStorage.available = false;
    if (failure === "basic_text" || failure === "unknown") safeStorage.backend = failure;
    if (failure === "availability rejection")
      safeStorage.isAsyncEncryptionAvailable = (): Promise<never> =>
        Promise.reject(new Error("private keyring diagnostic"));
    if (failure === "backend lookup rejection")
      safeStorage.getSelectedStorageBackend = (): never => {
        throw new Error("private selected backend diagnostic");
      };
    const profileProtection = await initializeElectronProfileProtection(safeStorage, "linux");
    const service = new NatsProfileService(
      createElectronNatsProfileStore({ userDataPath, profileProtection }),
    );

    expect(await service.list()).toMatchObject({
      profiles: [],
      capability: { durability: "durable", protection: "unavailable", state: "unavailable" },
    });
    await expect(service.create(input())).rejects.toMatchObject({ code: "storage-unavailable" });
    await expect(service.create(input())).rejects.not.toThrow(
      /private|nats-secret-only-in-host|BEGIN CERTIFICATE/u,
    );
    expect(safeStorage.protectedInputs).toEqual([]);
    await expect(
      readFile(join(userDataPath, "profiles", "nats-profiles.json")),
    ).rejects.toMatchObject({ code: "ENOENT" });
  });

  it("preserves existing NATS bytes without reading them while OS protection is unavailable", async () => {
    const userDataPath = await userData();
    const path = join(userDataPath, "profiles", "nats-profiles.json");
    await mkdir(join(path, ".."), { recursive: true });
    const original = Buffer.from("preserved unavailable encrypted data");
    await writeFile(path, original);
    const safeStorage = new ReversibleSafeStorage();
    safeStorage.available = false;
    const profileProtection = await initializeElectronProfileProtection(safeStorage, "linux");
    const service = new NatsProfileService(
      createElectronNatsProfileStore({ userDataPath, profileProtection }),
    );

    expect((await service.list()).capability.state).toBe("unavailable");
    await expect(service.create(input())).rejects.toMatchObject({ code: "storage-unavailable" });
    expect(await readFile(path)).toEqual(original);
    expect(safeStorage.unprotectedInputs).toEqual([]);
  });
});
