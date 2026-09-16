import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { afterEach, describe, expect, it } from "vitest";

import { HOST_PROTOCOL_VERSION, type HostEvent } from "../../src/features/kafka/contracts";
import { createElectronKafkaBackend, type ElectronSafeStoragePort } from "../../src/platform/electron/main";
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

function templateEvents(
  events: readonly HostEvent[],
): readonly Extract<HostEvent, { readonly event: "templates.changed" }>[] {
  return events.filter(
    (event): event is Extract<HostEvent, { readonly event: "templates.changed" }> =>
      event.event === "templates.changed",
  );
}

describe("Electron connection-template backend composition", () => {
  it("restores recipe identity independently without changing the legacy template bytes", async () => {
    const userDataPath = await temporaryUserData();
    const options = {
      platform: "linux" as const,
      safeStorage: new ReversibleSafeStorage(),
      userDataPath,
    };
    const backend = await createElectronKafkaBackend(options);
    const events: HostEvent[] = [];
    backend.subscribe((event) => {
      events.push(event);
    });
    await backend.execute({
      command: "templates.list",
      id: "legacy-seed",
      payload: {},
      version: HOST_PROTOCOL_VERSION,
    });
    const legacyPath = join(userDataPath, "templates", "kafka-connection-templates.json");
    const legacyBytes = await readFile(legacyPath);
    await expect(
      backend.execute({
        command: "recipes.create",
        id: "durable-recipe",
        payload: trustRecipeInput(),
        version: HOST_PROTOCOL_VERSION,
      }),
    ).resolves.toMatchObject({ ok: true });
    const created = events.filter((event) => event.event === "recipes.changed").at(-1)?.payload;
    expect(created?.store).toEqual({ durability: "durable", state: "ready" });
    expect(
      created?.recipes.find((recipe) => recipe.name === trustRecipeInput().name),
    ).toMatchObject({ revision: 1 });
    await backend.shutdown();
    const restarted = await createElectronKafkaBackend(options);
    const restored: HostEvent[] = [];
    restarted.subscribe((event) => {
      restored.push(event);
    });
    try {
      await expect(
        restarted.execute({
          command: "recipes.list",
          id: "restore-recipe",
          payload: {},
          version: HOST_PROTOCOL_VERSION,
        }),
      ).resolves.toMatchObject({ ok: true });
      expect(restored.filter((event) => event.event === "recipes.changed").at(-1)?.payload).toEqual(
        created,
      );
      expect(await readFile(legacyPath)).toEqual(legacyBytes);
    } finally {
      await restarted.shutdown();
    }
  });

  it("restores durable entries, order and selection after a complete backend restart", async () => {
    const userDataPath = await temporaryUserData();
    const safeStorage = new ReversibleSafeStorage();
    const firstBackend = await createElectronKafkaBackend({
      platform: "linux",
      safeStorage,
      userDataPath,
    });
    await firstBackend.execute({
      command: "templates.list",
      id: "seed-templates",
      payload: {},
      version: HOST_PROTOCOL_VERSION,
    });
    await firstBackend.execute({
      command: "templates.create",
      id: "create-template",
      payload: {
        catalog: "oauth-endpoint",
        name: "Local aio",
        template: "http://{host}:15000/rest-gateway/rest/api/v1/auth/token",
      },
      version: HOST_PROTOCOL_VERSION,
    });
    await firstBackend.execute({
      command: "templates.select",
      id: "select-template",
      payload: { catalog: "oauth-endpoint", name: "Local aio" },
      version: HOST_PROTOCOL_VERSION,
    });
    await firstBackend.shutdown();

    const secondBackend = await createElectronKafkaBackend({
      platform: "linux",
      safeStorage,
      userDataPath,
    });
    const events: HostEvent[] = [];
    secondBackend.subscribe((event) => {
      events.push(event);
    });
    await secondBackend.execute({
      command: "templates.list",
      id: "list-restored",
      payload: {},
      version: HOST_PROTOCOL_VERSION,
    });

    expect(templateEvents(events).at(-1)).toMatchObject({
      payload: {
        catalogs: [
          { catalog: "truststore-fetch" },
          { catalog: "truststore-password" },
          {
            catalog: "oauth-endpoint",
            entries: [
              { name: "nsp-25-4" },
              {
                name: "Local aio",
                template: "http://{host}:15000/rest-gateway/rest/api/v1/auth/token",
              },
            ],
            selectedName: "Local aio",
          },
        ],
        store: { durability: "durable", state: "ready" },
      },
    });
    expect(
      JSON.parse(
        await readFile(join(userDataPath, "templates", "kafka-connection-templates.json"), "utf8"),
      ),
    ).toMatchObject({ version: 1 });
    await secondBackend.shutdown();
  });

  it("keeps profiles available while corrupt template data fails closed and is preserved", async () => {
    const userDataPath = await temporaryUserData();
    const templatePath = join(userDataPath, "templates", "kafka-connection-templates.json");
    await mkdir(join(templatePath, ".."), { recursive: true });
    const corrupt = '{"version":999,"catalogs":[],"commandOutput":"do not expose"}';
    await writeFile(templatePath, corrupt, { mode: 0o600 });
    const backend = await createElectronKafkaBackend({
      platform: "linux",
      safeStorage: new ReversibleSafeStorage(),
      userDataPath,
    });
    const events: HostEvent[] = [];
    backend.subscribe((event) => {
      events.push(event);
    });

    await expect(
      backend.execute({
        command: "templates.list",
        id: "list-corrupt",
        payload: {},
        version: HOST_PROTOCOL_VERSION,
      }),
    ).resolves.toMatchObject({
      error: { code: "TEMPLATE_CORRUPT" },
      ok: false,
    });
    await expect(
      backend.execute({
        command: "profiles.list",
        id: "list-profiles",
        payload: {},
        version: HOST_PROTOCOL_VERSION,
      }),
    ).resolves.toMatchObject({ ok: true });

    expect(templateEvents(events).at(-1)).toMatchObject({
      payload: {
        catalogs: [
          { entries: [], selectedName: null },
          { entries: [], selectedName: null },
          { entries: [], selectedName: null },
        ],
        store: { durability: "durable", state: "unavailable" },
      },
    });
    expect(events.filter((event) => event.event === "profiles.changed").at(-1)).toMatchObject({
      payload: {
        profiles: [],
        store: {
          durability: "durable",
          protection: "os-protected",
          state: "ready",
        },
      },
    });
    expect(JSON.stringify(events)).not.toMatch(/commandOutput|do not expose/);
    await expect(readFile(templatePath, "utf8")).resolves.toBe(corrupt);
    await backend.shutdown();
  });
});
