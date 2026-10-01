import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { afterEach, describe, expect, it } from "vitest";

import { DEFAULT_CONNECTION_TEMPLATE_DOCUMENT } from "../support/legacy-template-document";
import { HOST_PROTOCOL_VERSION, type HostEvent } from "../../src/features/kafka/contracts";
import { createElectronKafkaBackend } from "../../src/platform/electron/main/electron-kafka-backend";
import type { ElectronSafeStoragePort } from "../../src/platform/electron/main/electron-profile-protection";
import { trustRecipeInput } from "../support/trust-recipe";

class ReversibleSafeStorage implements ElectronSafeStoragePort {
  decryptStringAsync(
    encrypted: Buffer,
  ): Promise<{ readonly result: string; readonly shouldReEncrypt: boolean }> {
    return Promise.resolve({
      result: Buffer.from(encrypted).reverse().toString("utf8"),
      shouldReEncrypt: false,
    });
  }

  encryptStringAsync(plainText: string): Promise<Buffer> {
    return Promise.resolve(Buffer.from(plainText, "utf8").reverse());
  }

  getSelectedStorageBackend(): "gnome_libsecret" {
    return "gnome_libsecret";
  }

  isAsyncEncryptionAvailable(): Promise<boolean> {
    return Promise.resolve(true);
  }
}

const temporaryDirectories: string[] = [];

async function temporaryUserData(): Promise<string> {
  const directory = await mkdtemp(join(tmpdir(), "streamskope-electron-template-"));
  temporaryDirectories.push(directory);
  return directory;
}

afterEach(async () => {
  await Promise.all(
    temporaryDirectories
      .splice(0)
      .map((directory) => rm(directory, { force: true, recursive: true })),
  );
});

describe("Electron recipe ownership and legacy migration", () => {
  it("restores recipe identity without changing preserved legacy bytes", async () => {
    const userDataPath = await temporaryUserData();
    const legacyPath = join(userDataPath, "templates", "kafka-connection-templates.json");
    await mkdir(join(userDataPath, "templates"), { recursive: true });
    await writeFile(
      legacyPath,
      JSON.stringify({ ...DEFAULT_CONNECTION_TEMPLATE_DOCUMENT, version: 1 }),
    );
    const before = await readFile(legacyPath);
    const options = {
      platform: "linux" as const,
      safeStorage: new ReversibleSafeStorage(),
      userDataPath,
    };
    const backend = await createElectronKafkaBackend(options);
    const events: HostEvent[] = [];
    backend.subscribe((event) => events.push(event));
    await expect(
      backend.execute({
        command: "recipes.create",
        id: "create",
        payload: trustRecipeInput(),
        version: HOST_PROTOCOL_VERSION,
      }),
    ).resolves.toMatchObject({ ok: true });
    const created = events.filter((event) => event.event === "recipes.changed").at(-1)?.payload;
    await backend.shutdown();
    const restarted = await createElectronKafkaBackend(options);
    const restored: HostEvent[] = [];
    restarted.subscribe((event) => restored.push(event));
    try {
      await expect(
        restarted.execute({
          command: "recipes.list",
          id: "list",
          payload: {},
          version: HOST_PROTOCOL_VERSION,
        }),
      ).resolves.toMatchObject({ ok: true });
      expect(restored.filter((event) => event.event === "recipes.changed").at(-1)?.payload).toEqual(
        created,
      );
      expect(await readFile(legacyPath)).toEqual(before);
    } finally {
      await restarted.shutdown();
    }
  });

  it("never seeds a legacy file while initializing modern starter recipes", async () => {
    const userDataPath = await temporaryUserData();
    const backend = await createElectronKafkaBackend({
      platform: "linux",
      safeStorage: new ReversibleSafeStorage(),
      userDataPath,
    });
    const events: HostEvent[] = [];
    backend.subscribe((event) => events.push(event));
    try {
      await expect(
        backend.execute({
          command: "recipes.list",
          id: "list",
          payload: {},
          version: HOST_PROTOCOL_VERSION,
        }),
      ).resolves.toMatchObject({ ok: true });
      expect(
        events.filter((event) => event.event === "recipes.changed").at(-1)?.payload.recipes,
      ).toHaveLength(3);
      await expect(
        readFile(join(userDataPath, "templates", "kafka-connection-templates.json")),
      ).rejects.toMatchObject({ code: "ENOENT" });
    } finally {
      await backend.shutdown();
    }
  });

  it("isolates corrupt legacy data from modern recipes and fails explicit migration safely", async () => {
    const userDataPath = await temporaryUserData();
    const legacyPath = join(userDataPath, "templates", "kafka-connection-templates.json");
    await mkdir(join(userDataPath, "templates"), { recursive: true });
    await writeFile(legacyPath, "corrupt preserved catalog");
    const backend = await createElectronKafkaBackend({
      platform: "linux",
      safeStorage: new ReversibleSafeStorage(),
      userDataPath,
    });
    try {
      await expect(
        backend.execute({
          command: "recipes.list",
          id: "list",
          payload: {},
          version: HOST_PROTOCOL_VERSION,
        }),
      ).resolves.toMatchObject({ ok: true });
      await expect(
        backend.execute({
          command: "recipes.legacy.preview",
          id: "review",
          payload: {},
          version: HOST_PROTOCOL_VERSION,
        }),
      ).resolves.toMatchObject({ ok: false, error: { code: "TEMPLATE_CORRUPT" } });
      await expect(
        backend.execute({
          command: "recipes.create",
          id: "create",
          payload: trustRecipeInput(),
          version: HOST_PROTOCOL_VERSION,
        }),
      ).resolves.toMatchObject({ ok: true });
      expect(await readFile(legacyPath, "utf8")).toBe("corrupt preserved catalog");
    } finally {
      await backend.shutdown();
    }
  });

  it("rereads a reviewed legacy file and rejects conversion after an external edit", async () => {
    const userDataPath = await temporaryUserData();
    const legacyPath = join(userDataPath, "templates", "kafka-connection-templates.json");
    await mkdir(join(userDataPath, "templates"), { recursive: true });
    const original = { ...DEFAULT_CONNECTION_TEMPLATE_DOCUMENT, version: 1 };
    await writeFile(legacyPath, JSON.stringify(original));
    const backend = await createElectronKafkaBackend({
      platform: "linux",
      safeStorage: new ReversibleSafeStorage(),
      userDataPath,
    });
    try {
      const reviewed = await backend.execute({
        command: "recipes.legacy.preview",
        id: "review",
        payload: {},
        version: HOST_PROTOCOL_VERSION,
      });
      if (!reviewed.ok || !("legacy" in reviewed.result) || reviewed.result.legacy === null)
        throw new Error("Expected migration review");
      const payload = {
        name: "Reviewed conversion",
        kind: "jks" as const,
        materialName: "nsp-25-4",
        passwordName: "nsp-25-11",
        oauthName: null,
        expectedSourceRevision: reviewed.result.legacy.sourceRevision,
      };
      await expect(
        backend.execute({
          command: "recipes.legacy.convert",
          id: "convert",
          payload,
          version: HOST_PROTOCOL_VERSION,
        }),
      ).resolves.toMatchObject({ ok: true });
      expect(JSON.parse(await readFile(legacyPath, "utf8"))).toEqual(original);
      const edited = {
        ...original,
        catalogs: original.catalogs.map((catalog) =>
          catalog.catalog === "truststore-fetch"
            ? {
                ...catalog,
                entries: catalog.entries.map((entry) => ({
                  ...entry,
                  template: `${entry.template} --changed`,
                })),
              }
            : catalog,
        ),
      };
      const editedBytes = JSON.stringify(edited);
      await writeFile(legacyPath, editedBytes);
      await expect(
        backend.execute({
          command: "recipes.legacy.convert",
          id: "stale",
          payload,
          version: HOST_PROTOCOL_VERSION,
        }),
      ).resolves.toMatchObject({ ok: false, error: { code: "VALIDATION" } });
      expect(await readFile(legacyPath, "utf8")).toBe(editedBytes);
    } finally {
      await backend.shutdown();
    }
  });
});
