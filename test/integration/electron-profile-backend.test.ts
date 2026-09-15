import { mkdtemp, readFile, rm } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";

import { afterEach, describe, expect, it } from "vitest";

import { HOST_PROTOCOL_VERSION, type HostEvent } from "../../src/kafka/contracts";
import { createElectronKafkaBackend, type ElectronSafeStoragePort } from "../../src/main";
import { trustRecipeInput } from "../support/trust-recipe";

class ReversibleSafeStorage implements ElectronSafeStoragePort {
  available = true;
  backend: ReturnType<ElectronSafeStoragePort["getSelectedStorageBackend"]> = "gnome_libsecret";

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

  getSelectedStorageBackend():
    "basic_text" | "gnome_libsecret" | "kwallet" | "kwallet5" | "kwallet6" | "unknown" {
    return this.backend;
  }

  isAsyncEncryptionAvailable(): Promise<boolean> {
    return Promise.resolve(this.available);
  }
}

const temporaryDirectories: string[] = [];

async function temporaryUserData(): Promise<string> {
  const directory = await mkdtemp(join(tmpdir(), "streamskope-electron-profile-"));
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

describe("Electron profile backend composition", () => {
  it("restores an OS-protected profile after a complete backend restart", async () => {
    const userDataPath = await temporaryUserData();
    const safeStorage = new ReversibleSafeStorage();
    const truststore = (
      await readFile(join(process.cwd(), "node_modules/jks-js/examples/assets/truststore.jks"))
    ).toString("base64");
    const firstBackend = await createElectronKafkaBackend({
      platform: "linux",
      safeStorage,
      userDataPath,
    });
    const firstEvents: HostEvent[] = [];
    firstBackend.subscribe((event) => {
      firstEvents.push(event);
    });

    await expect(
      firstBackend.execute({
        command: "recipes.create",
        id: "create-recipe",
        payload: trustRecipeInput(),
        version: HOST_PROTOCOL_VERSION,
      }),
    ).resolves.toMatchObject({ ok: true });
    const recipe = firstEvents
      .filter((event) => event.event === "recipes.changed")
      .at(-1)
      ?.payload.recipes.find((entry) => entry.name === "Certificate file");
    if (!recipe) throw new Error("Expected fixture recipe");

    await expect(
      firstBackend.execute({
        command: "profiles.create",
        id: "create-profile",
        payload: {
          profile: {
            brokers: ["127.0.0.1:19093"],
            name: "Local validation",
            binding: {
              mode: "replace",
              recipeId: recipe.id,
              recipeRevision: recipe.revision,
              overrides: { certificate_path: "/original/ca.pem" },
            },
            oauth: {
              clientId: "admin",
              clientSecret: { mode: "replace", value: "fixture-secret" },
              scope: "kafka",
              tokenEndpoint: "http://127.0.0.1:15000/token",
            },
            trust: {
              kind: "jks",
              label: "truststore.jks",
              material: { mode: "replace", value: truststore },
              password: { mode: "replace", value: "password" },
            },
          },
        },
        version: HOST_PROTOCOL_VERSION,
      }),
    ).resolves.toMatchObject({ ok: true });
    const usage = await firstBackend.execute({
      command: "recipes.usage",
      id: "review-deletion",
      payload: { id: recipe.id, revision: recipe.revision },
      version: HOST_PROTOCOL_VERSION,
    });
    if (!usage.ok || !("usage" in usage.result)) throw new Error("Expected exact usage review");
    expect(usage.result.usage).toHaveLength(1);
    await expect(
      firstBackend.execute({
        command: "recipes.delete",
        id: "delete-recipe",
        payload: {
          id: recipe.id,
          revision: recipe.revision,
          confirmedProfileIds: usage.result.usage.map((profile) => profile.id),
        },
        version: HOST_PROTOCOL_VERSION,
      }),
    ).resolves.toMatchObject({ ok: true });
    await firstBackend.shutdown();

    const secondBackend = await createElectronKafkaBackend({
      platform: "linux",
      safeStorage,
      userDataPath,
    });
    const secondEvents: HostEvent[] = [];
    secondBackend.subscribe((event) => {
      secondEvents.push(event);
    });
    await secondBackend.execute({
      command: "profiles.list",
      id: "list-profiles",
      payload: {},
      version: HOST_PROTOCOL_VERSION,
    });

    expect(secondEvents.filter((event) => event.event === "profiles.changed").at(-1)).toMatchObject(
      {
        payload: {
          profiles: [
            {
              name: "Local validation",
              oauth: { clientSecretPresent: true },
              trust: { materialPresent: true },
            },
          ],
          store: {
            durability: "durable",
            protection: "os-protected",
            state: "ready",
          },
        },
      },
    );
    const restored = secondEvents.filter((event) => event.event === "profiles.changed").at(-1)
      ?.payload.profiles[0];
    if (!restored) throw new Error("Expected restored profile");
    await expect(
      secondBackend.execute({
        command: "profiles.update",
        id: "retained-binding-update",
        version: HOST_PROTOCOL_VERSION,
        payload: {
          profileId: restored.id,
          profile: {
            expectedRevision: 1,
            brokers: restored.brokers,
            name: restored.name,
            binding: {
              mode: "replace",
              recipeId: recipe.id,
              recipeRevision: recipe.revision,
              overrides: { certificate_path: "/updated/ca.pem" },
            },
            trust: {
              kind: "jks",
              label: "truststore.jks",
              material: { mode: "retain" },
              password: { mode: "retain" },
            },
            oauth: {
              clientId: "admin",
              clientSecret: { mode: "retain" },
              scope: "kafka",
              tokenEndpoint: "http://127.0.0.1:15000/token",
            },
          },
        },
      }),
    ).resolves.toMatchObject({ ok: true });
    const serializedEvents = JSON.stringify([...firstEvents, ...secondEvents]);
    expect(serializedEvents).not.toContain("/original/ca.pem");
    expect(serializedEvents).not.toContain("/updated/ca.pem");
    expect(serializedEvents).not.toMatch(/fixture-secret|BEGIN CERTIFICATE/);
    expect(serializedEvents).not.toContain(truststore);
    await secondBackend.shutdown();
  });

  it("exposes an unavailable store and performs no durable write without OS protection", async () => {
    const userDataPath = await temporaryUserData();
    const safeStorage = new ReversibleSafeStorage();
    safeStorage.available = false;
    const backend = await createElectronKafkaBackend({
      platform: "linux",
      safeStorage,
      userDataPath,
    });
    const events: HostEvent[] = [];
    backend.subscribe((event) => {
      events.push(event);
    });

    await expect(
      backend.execute({
        command: "profiles.list",
        id: "list-profiles",
        payload: {},
        version: HOST_PROTOCOL_VERSION,
      }),
    ).resolves.toMatchObject({ ok: true });
    expect(events.filter((event) => event.event === "profiles.changed").at(-1)).toMatchObject({
      payload: {
        profiles: [],
        store: {
          durability: "durable",
          protection: "unavailable",
          state: "unavailable",
        },
      },
    });

    await expect(
      backend.execute({
        command: "profiles.create",
        id: "create-profile",
        payload: {
          profile: {
            brokers: ["127.0.0.1:19093"],
            name: "Unavailable",
            trust: {
              kind: "pem",
              label: "ca.pem",
              material: { mode: "replace", value: "sensitive-trust" },
              password: { mode: "clear" },
            },
          },
        },
        version: HOST_PROTOCOL_VERSION,
      }),
    ).resolves.toMatchObject({
      error: { code: "PROFILE_STORE_UNAVAILABLE", stage: "storage" },
      ok: false,
    });
    await expect(
      readFile(join(userDataPath, "profiles", "kafka-profiles.json")),
    ).rejects.toMatchObject({ code: "ENOENT" });
    await backend.shutdown();
  });
});
