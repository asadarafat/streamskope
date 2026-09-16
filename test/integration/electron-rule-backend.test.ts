import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";

import { afterEach, describe, expect, it } from "vitest";

import {
  HOST_PROTOCOL_VERSION,
  type HostEvent,
  type KafkaRuleDefinition,
} from "../../src/features/kafka/contracts";
import {
  createElectronKafkaBackend,
  type ElectronSafeStoragePort,
} from "../../src/platform/electron/main";

const rule: KafkaRuleDefinition = {
  cooldownMs: 5_000,
  description: "Detect critical orders.",
  enabled: true,
  expression: '$.priority == "high"',
  level: "warn",
  name: "High priority",
  topic: "orders",
};

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
  const directory = await mkdtemp(join(tmpdir(), "streamskope-electron-rule-"));
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

function ruleEvents(
  events: readonly HostEvent[],
): readonly Extract<HostEvent, { readonly event: "rules.changed" }>[] {
  return events.filter(
    (event): event is Extract<HostEvent, { readonly event: "rules.changed" }> =>
      event.event === "rules.changed",
  );
}

describe("Electron rule backend composition", () => {
  it("restores exact rule order and fields after a complete backend restart", async () => {
    const userDataPath = await temporaryUserData();
    const safeStorage = new ReversibleSafeStorage();
    const firstBackend = await createElectronKafkaBackend({
      platform: "linux",
      safeStorage,
      userDataPath,
    });

    await expect(
      firstBackend.execute({
        command: "rules.create",
        id: "create-first",
        payload: { rule },
        version: HOST_PROTOCOL_VERSION,
      }),
    ).resolves.toMatchObject({ ok: true });
    await expect(
      firstBackend.execute({
        command: "rules.create",
        id: "create-second",
        payload: {
          rule: {
            cooldownMs: rule.cooldownMs,
            ...(rule.description === undefined ? {} : { description: rule.description }),
            enabled: false,
            expression: rule.expression,
            level: "error",
            name: "Second",
          },
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
    const events: HostEvent[] = [];
    secondBackend.subscribe((event) => {
      events.push(event);
    });
    await secondBackend.execute({
      command: "rules.list",
      id: "list-restored",
      payload: {},
      version: HOST_PROTOCOL_VERSION,
    });

    expect(ruleEvents(events).at(-1)).toMatchObject({
      payload: {
        rules: [
          rule,
          {
            cooldownMs: rule.cooldownMs,
            description: rule.description,
            enabled: false,
            expression: rule.expression,
            level: "error",
            name: "Second",
          },
        ],
        store: { durability: "durable", state: "ready" },
      },
    });
    expect(
      JSON.parse(await readFile(join(userDataPath, "rules", "kafka-rules.json"), "utf8")),
    ).toEqual({
      rules: [
        rule,
        {
          cooldownMs: rule.cooldownMs,
          description: rule.description,
          enabled: false,
          expression: rule.expression,
          level: "error",
          name: "Second",
        },
      ],
      version: 1,
    });
    await secondBackend.shutdown();
  });

  it("preserves corrupt rule bytes while profiles and templates remain available", async () => {
    const userDataPath = await temporaryUserData();
    const path = join(userDataPath, "rules", "kafka-rules.json");
    await mkdir(dirname(path), { recursive: true });
    const corrupt = '{"version":999,"rules":[],"expression":"must-not-enter-activity-or-events"}';
    await writeFile(path, corrupt, { mode: 0o600 });
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
        command: "rules.list",
        id: "list-corrupt",
        payload: {},
        version: HOST_PROTOCOL_VERSION,
      }),
    ).resolves.toMatchObject({
      error: { code: "RULE_CORRUPT", stage: "storage" },
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
    await expect(
      backend.execute({
        command: "templates.list",
        id: "list-templates",
        payload: {},
        version: HOST_PROTOCOL_VERSION,
      }),
    ).resolves.toMatchObject({ ok: true });

    expect(ruleEvents(events).at(-1)).toMatchObject({
      payload: {
        rules: [],
        store: { durability: "durable", state: "unavailable" },
      },
    });
    expect(events.some((event) => event.event === "profiles.changed")).toBe(true);
    expect(events.some((event) => event.event === "templates.changed")).toBe(true);
    expect(JSON.stringify(events)).not.toContain("must-not-enter-activity-or-events");
    await expect(readFile(path, "utf8")).resolves.toBe(corrupt);
    await backend.shutdown();
  });
});
