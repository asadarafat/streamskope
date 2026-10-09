import { execFile } from "node:child_process";
import { createHash } from "node:crypto";
import {
  chmod,
  link,
  lstat,
  mkdir,
  mkdtemp,
  readFile,
  readdir,
  readlink,
  rm,
  symlink,
  writeFile,
} from "node:fs/promises";
import { tmpdir } from "node:os";
import { basename, dirname, join, resolve } from "node:path";
import { promisify } from "node:util";

import { afterEach, describe, expect, it, vi } from "vitest";

import { KAFKA_OPERATIONAL_PREFERENCE_DEFAULTS } from "../../src/features/kafka/contracts";
import type { PluginManifest, PluginProfileSource } from "../../src/plugins/contracts";
import { inspectBrowserData } from "../../src/platform/node/browser-data-preflight";
import {
  parseBrowserDataInspection,
  type BrowserDataInspection,
} from "../../src/platform/node/browser-data-compatibility";
import { AtomicKafkaProfileFileStore } from "../../src/platform/node/kafka-profile-file-store";
import { AtomicNatsProfileFileStore } from "../../src/platform/node/nats-profile-file-store";
import { AtomicKafkaRuleFileStore } from "../../src/platform/node/kafka-rule-file-store";
import { AtomicKafkaOperationalPreferenceFileStore } from "../../src/platform/node/kafka-operational-preference-file-store";
import { AtomicKafkaTopicConfigurationHistoryFileStore } from "../../src/platform/node/kafka-topic-configuration-history-file-store";
import { AtomicKafkaQueryFileStore } from "../../src/platform/node/kafka-query-file-store";
import { AtomicKafkaTrustRecipeFileStore } from "../../src/platform/node/kafka-trust-recipe-file-store";
import { AtomicObservationFileStore } from "../../src/platform/node/kafka-observation-file-store";
import { openPassphraseVault } from "../../src/platform/node/vault/passphrase-vault";
import { PluginStore } from "../../src/platform/node/plugins/store";
import { encodePluginPackage, pluginPackageSha256 } from "../../src/platform/node/plugins/package";
import { PluginNetworkSettings } from "../../src/platform/node/plugins/network-settings";

const execute = promisify(execFile);
const roots: string[] = [];
const secret = "private-preflight-sentinel";
const timestamp = "2026-10-08T12:00:00.000Z";

afterEach(async () => {
  vi.restoreAllMocks();
  await Promise.all(roots.splice(0).map((root) => rm(root, { recursive: true, force: true })));
});

async function directory(): Promise<string> {
  const root = await mkdtemp(join(tmpdir(), "streamskope-data-inspection-"));
  roots.push(root);
  return root;
}

interface FileSnapshot {
  readonly path: string;
  readonly mode: number;
  readonly uid: number;
  readonly gid: number;
  readonly links: number;
  readonly content: string | null;
}

/** Test-private inventory; never follows symlinks or reads FIFOs. */
async function tree(root: string): Promise<readonly FileSnapshot[]> {
  const result: FileSnapshot[] = [];
  async function visit(path: string, relative: string): Promise<void> {
    const metadata = await lstat(path);
    result.push({
      path: relative,
      mode: metadata.mode,
      uid: metadata.uid,
      gid: metadata.gid,
      links: metadata.nlink,
      content: metadata.isFile()
        ? createHash("sha256")
            .update(await readFile(path))
            .digest("hex")
        : metadata.isSymbolicLink()
          ? await readlink(path)
          : null,
    });
    if (metadata.isDirectory()) {
      for (const name of (await readdir(path)).sort())
        await visit(join(path, name), `${relative}/${name}`);
    }
  }
  try {
    await visit(root, ".");
  } catch (error) {
    if (
      typeof error !== "object" ||
      error === null ||
      !("code" in error) ||
      error.code !== "ENOENT"
    )
      throw error;
  }
  return result;
}

async function check(root: string): Promise<BrowserDataInspection> {
  const before = await tree(root);
  const report = parseBrowserDataInspection(
    await inspectBrowserData(root, { hostRelease: "v0.11.0" }),
  );
  expect(await tree(root)).toEqual(before);
  expect(report.unverified).toEqual([
    "protected-content-authenticity",
    "protected-profile-schema",
    "remote-plugin-resource-cleanup",
    "host-quiescence",
  ]);
  const serialized = JSON.stringify(report);
  for (const privateValue of [
    secret,
    root,
    "broker.private.invalid",
    "Private profile",
    "private-profile",
    "private-execution",
  ])
    expect(serialized).not.toContain(privateValue);
  return report;
}

function row(
  report: BrowserDataInspection,
  kind: string,
): BrowserDataInspection["documents"][number] | undefined {
  return report.documents.find((entry) => entry.kind === kind);
}

async function write(root: string, relative: string, contents: string): Promise<void> {
  const path = join(root, relative);
  await mkdir(dirname(path), { recursive: true, mode: 0o700 });
  await writeFile(path, contents, { mode: 0o600 });
}

async function seed(source?: PluginProfileSource): Promise<string> {
  const root = await directory();
  const vault = await openPassphraseVault({ dataRoot: root, passphrase: secret, mode: "create" });
  try {
    await new AtomicKafkaProfileFileStore(
      vault.paths.kafkaProfiles,
      vault.protector,
      vault.capability,
    ).commit([
      {
        id: "private-profile",
        name: "Private profile",
        brokers: ["broker.private.invalid:9092"],
        transport: "plaintext",
        createdAt: timestamp,
        updatedAt: timestamp,
        ...(source ? { source } : {}),
      },
    ]);
    await new AtomicNatsProfileFileStore(vault.paths.natsProfiles, vault.protector, {
      protection: "passphrase-protected",
    }).save([
      {
        id: "private-nats-profile",
        revision: 1,
        name: "Private profile",
        servers: ["nats://broker.private.invalid:4222"],
        authentication: { mode: "token", token: secret },
        tls: { mode: "plaintext" },
        createdAt: timestamp,
        updatedAt: timestamp,
      },
    ]);
  } finally {
    await vault.lock();
  }
  return root;
}

const pluginManifest: PluginManifest = {
  id: "example.inspection",
  name: "Inspection fixture",
  version: "1.0.0",
  apiVersion: 4,
  backend: "backend.cjs",
  renderer: "renderer.js",
  compatibility: {
    streamskope: { minimum: "0.10.0", maximumExclusive: "1.0.0" },
    target: { system: "fixture", minimum: "1.0.0", maximum: "1.0.0" },
  },
};

async function plugin(
  root: string,
  pending = false,
): Promise<{ store: PluginStore; digest: string; marker: string }> {
  const store = new PluginStore(join(root, "plugins"));
  const marker = join(root, "executed-private-plugin");
  const bytes = encodePluginPackage(
    pluginManifest,
    new Map([
      [
        "backend.cjs",
        Buffer.from(
          `require('node:fs').writeFileSync(${JSON.stringify(marker)}, 'executed'); throw new Error('${secret}');`,
        ),
      ],
      ["renderer.js", Buffer.from(`throw new Error('${secret}');`)],
    ]),
  );
  const digest = pluginPackageSha256(bytes);
  await store.install(bytes, digest);
  if (!pending) await store.activatePending();
  return { store, digest, marker };
}

describe.skipIf(process.platform !== "linux")("read-only browser data preflight", () => {
  it("leaves missing and empty roots absent or empty without creating setup state", async () => {
    const root = await directory();
    const missing = join(root, "not-created");
    expect((await check(missing)).outcome).toBe("eligible");
    expect(await readdir(root)).toEqual([]);
    expect((await check(root)).outcome).toBe("eligible");
    expect(await readdir(root)).toEqual([]);
  });

  it("inspects migrated codec preferences and every exact predecessor backup without writes", async () => {
    const root = await seed();
    const path = join(root, "workbench/kafka-operational-preferences.json");
    await mkdir(dirname(path), { recursive: true });
    const { codecs, ...legacy } = KAFKA_OPERATIONAL_PREFERENCE_DEFAULTS;
    expect(codecs).toEqual({ key: "auto", value: "auto" });
    const original = JSON.stringify({ version: 1, preferences: legacy });
    await writeFile(path, original);
    await new AtomicKafkaOperationalPreferenceFileStore(path).commit({
      ...KAFKA_OPERATIONAL_PREFERENCE_DEFAULTS,
      codecs: { key: "utf8", value: "avro" },
    });
    const before = await readFile(path);
    const report = await check(root);
    expect(report.outcome).toBe("eligible");
    expect(row(report, "preferences")).toMatchObject({
      count: 2,
      formats: [1, 2],
      state: "verified",
    });
    expect(await readFile(path)).toEqual(before);
    expect(await readFile(`${path}.pre-codecs-v1`, "utf8")).toBe(original);
    await writeFile(`${path}.pre-codecs-v1.1`, "malformed", { mode: 0o600 });
    expect(row(await check(root), "preferences")).toMatchObject({
      state: "blocked",
      reason: "unsupported-format",
    });
  });

  it("blocks format 2 preferences whose protection settings are missing without repairing them", async () => {
    const root = await directory();
    const { protection, ...preferences } = KAFKA_OPERATIONAL_PREFERENCE_DEFAULTS;
    expect(protection).toEqual({
      readOnly: false,
      maskKey: false,
      maskHeaders: [],
      valuePaths: [],
    });
    await write(
      root,
      "workbench/kafka-operational-preferences.json",
      JSON.stringify({ version: 2, preferences }),
    );
    const report = await check(root);
    expect(report.outcome).toBe("blocked");
    expect(row(report, "preferences")).toMatchObject({
      state: "blocked",
      reason: "unsupported-format",
    });
  });

  it("accepts real browser-encrypted envelopes and production nonsecret stores without invoking decrypting loaders or transport", async () => {
    const root = await seed();
    await new AtomicKafkaRuleFileStore(join(root, "rules/kafka-rules.json")).commit({ rules: [] });
    await new AtomicKafkaOperationalPreferenceFileStore(
      join(root, "workbench/kafka-operational-preferences.json"),
    ).commit(KAFKA_OPERATIONAL_PREFERENCE_DEFAULTS);
    await new AtomicKafkaTopicConfigurationHistoryFileStore(
      join(root, "history/kafka-topic-configuration-history.json"),
    ).commit({ entries: [] });
    await new AtomicKafkaQueryFileStore(join(root, "queries/kafka-queries.json")).commit([]);
    await new AtomicKafkaTrustRecipeFileStore(
      join(root, "templates/trust-acquisition-recipes.json"),
    ).commit({ version: 1, recipes: [] });
    await new AtomicObservationFileStore(join(root, "history/kafka-observations.json")).commit({
      schemaVersion: 1,
      series: [],
    });
    const kafkaLoad = vi.spyOn(AtomicKafkaProfileFileStore.prototype, "load");
    const natsLoad = vi.spyOn(AtomicNatsProfileFileStore.prototype, "load");
    const network = vi.spyOn(PluginNetworkSettings.prototype, "snapshot");
    const fetch = vi.spyOn(globalThis, "fetch");
    const report = await check(root);
    expect(report.outcome).toBe("eligible");
    expect(row(report, "kafka-profiles")).toMatchObject({
      state: "verified",
      count: 1,
      formats: [3],
    });
    expect(row(report, "nats-profiles")).toMatchObject({
      state: "verified",
      count: 1,
      formats: [1],
    });
    for (const kind of ["rules", "topic-history", "queries", "trust-recipes", "observations"])
      expect(row(report, kind)).toMatchObject({ state: "verified", formats: [1] });
    expect(row(report, "preferences")).toMatchObject({ state: "verified", formats: [2] });
    for (const method of [kafkaLoad, natsLoad, network, fetch])
      expect(method).not.toHaveBeenCalled();
  });

  it.each(["kafka-profiles.json", "nats-profiles.json", "vault.json"])(
    "refuses a future %s format without rewriting any sibling data",
    async (file) => {
      const root = await seed();
      const document = JSON.parse(await readFile(join(root, file), "utf8")) as Record<
        string,
        unknown
      >;
      await write(root, file, JSON.stringify({ ...document, version: 999, sentinel: secret }));
      expect((await check(root)).outcome).toBe("blocked");
    },
  );

  it("refuses protected profiles without their vault and does not create replacement metadata", async () => {
    const root = await seed();
    await rm(join(root, "vault.json"));
    const report = await check(root);
    expect(report.outcome).toBe("blocked");
    expect(report.documents.some((entry) => entry.reason === "vault-required")).toBe(true);
    expect(await readdir(root)).not.toContain("vault.json");
  });

  it.each(["bad-base64", Buffer.from("desktop-protected-bytes").toString("base64")])(
    "refuses unsupported encrypted bytes %s without attempting re-encryption",
    async (protectedValue) => {
      const root = await seed();
      const document = JSON.parse(await readFile(join(root, "nats-profiles.json"), "utf8")) as {
        profiles: Array<Record<string, unknown>>;
      };
      document.profiles[0]!.protectedValue = protectedValue;
      await write(root, "nats-profiles.json", JSON.stringify(document));
      const load = vi.spyOn(AtomicNatsProfileFileStore.prototype, "load");
      expect((await check(root)).outcome).toBe("blocked");
      expect(load).not.toHaveBeenCalled();
    },
  );

  it.each(["symlink", "hardlink", "fifo", "permissive-file"])(
    "rejects a %s before following or repairing private data",
    async (kind) => {
      const root = await seed();
      const outside = await directory();
      const target = join(outside, "outside-private");
      await writeFile(target, secret, { mode: 0o600 });
      const before = await tree(outside);
      const victim = join(root, "kafka-profiles.json");
      if (kind === "permissive-file") await chmod(victim, 0o644);
      else {
        await rm(victim);
        if (kind === "symlink") await symlink(target, victim);
        else if (kind === "hardlink") await link(target, victim);
        else await execute("mkfifo", ["-m", "600", victim]);
      }
      const actualBefore = kind === "hardlink" ? await tree(outside) : before;
      const report = await check(root);
      expect(report.outcome).toBe("blocked");
      expect(row(report, "filesystem")).toMatchObject({
        state: "blocked",
        reason: "unsafe-filesystem",
      });
      expect(await tree(outside)).toEqual(actualBefore);
    },
  );

  it("rejects a symlinked ancestor without traversing its data tree", async () => {
    const base = await directory();
    const actual = await seed();
    const alias = join(base, "linked");
    await symlink(dirname(actual), alias);
    const before = await tree(actual);
    expect((await check(join(alias, basename(actual)))).outcome).toBe("blocked");
    expect(await tree(actual)).toEqual(before);
  });

  it("preserves protected desktop proxy settings without decrypting or configuring a browser transport", async () => {
    const root = await seed();
    const vault = await openPassphraseVault({ dataRoot: root, passphrase: secret, mode: "unlock" });
    const configure = vi.fn((): Promise<void> => Promise.resolve());
    const fetch = vi.fn<typeof globalThis.fetch>();
    const settings = new PluginNetworkSettings({
      path: join(root, "plugins/network.json"),
      protector: vault.protector,
      durableSettings: true,
      changing: (): void => undefined,
      transport: {
        nativeAvailable: true,
        supportedProxyProtocols: ["http", "https"],
        configure,
        fetch,
        close: (): Promise<void> => Promise.resolve(),
      },
    });
    try {
      await settings.update({
        configuration: {
          mode: "custom",
          proxyUrl: "http://broker.private.invalid:3128",
          offline: false,
        },
        credentials: { action: "replace", username: "private-proxy-user", password: secret },
      });
    } finally {
      settings.close();
      await vault.lock();
    }
    configure.mockClear();
    const unprotect = vi.spyOn(vault.protector, "unprotect");
    const report = await check(root);
    expect(report.outcome).toBe("blocked");
    expect(row(report, "plugin-network")).toMatchObject({
      state: "blocked",
      reason: "network-configuration-unsupported",
    });
    expect(configure).not.toHaveBeenCalled();
    expect(fetch).not.toHaveBeenCalled();
    expect(unprotect).not.toHaveBeenCalled();
  });

  it("validates installed package bytes without executing either module", async () => {
    const root = await seed();
    const installed = await plugin(root);
    const report = await check(root);
    expect(report.outcome).toBe("eligible");
    expect(row(report, "plugin-installations")).toMatchObject({ state: "verified", count: 1 });
    expect(await readdir(root)).not.toContain("executed-private-plugin");
    await write(root, `plugins/${pluginManifest.id}/${installed.digest}/backend.cjs`, secret);
    expect((await check(root)).outcome).toBe("blocked");
  });

  it.each(["install", "remove"])(
    "preserves pending plugin %s instead of activating or removing files",
    async (operation) => {
      const root = await seed();
      const installed = await plugin(root, operation === "install");
      if (operation === "remove") await installed.store.remove(pluginManifest.id);
      const report = await check(root);
      expect(report.outcome).toBe("blocked");
      expect(row(report, "plugin-installations")).toMatchObject({
        state: "blocked",
        reason: "plugin-change-pending",
      });
    },
  );

  it("detects orphan recovery ownership and never treats a null journal as remote cleanup evidence", async () => {
    const root = await seed();
    const store = new PluginStore(join(root, "plugins"));
    await store.writeRecoveryState("example.removed", { execution: "private-execution", secret });
    const pending = await check(root);
    expect(pending.outcome).toBe("blocked");
    expect(row(pending, "plugin-recovery")).toMatchObject({
      state: "blocked",
      reason: "plugin-recovery-pending",
    });
    await store.writeRecoveryState("example.removed", null);
    expect((await check(root)).outcome).toBe("eligible");
  });

  it.each(["streamskope.eda", "streamskope.nsp", "example.unknown"])(
    "refuses unproven managed source %s even when its recovery journal is null",
    async (pluginId) => {
      const root = await seed({
        kind: "plugin",
        pluginId,
        version: 1,
        data: { state: "ready", secret },
      });
      await new PluginStore(join(root, "plugins")).writeRecoveryState(pluginId, null);
      const report = await check(root);
      expect(report.outcome).toBe("blocked");
      expect(row(report, "kafka-profiles")).toMatchObject({
        state: "blocked",
        reason: "managed-source-unverified",
      });
    },
  );

  it("inspects security profile metadata and its preserved predecessor without unlocking or rewriting", async () => {
    const root = await seed();
    const vault = await openPassphraseVault({ dataRoot: root, passphrase: secret, mode: "unlock" });
    try {
      const store = new AtomicKafkaProfileFileStore(
        join(root, "kafka-profiles.json"),
        vault.protector,
        vault.capability,
      );
      await store.commit(
        (await store.load()).map((profile) => ({
          ...profile,
          sasl: { mechanism: "SCRAM-SHA-256" as const, username: "fixture", password: secret },
        })),
      );
    } finally {
      await vault.lock();
    }
    const report = await check(root);
    expect(report.outcome, JSON.stringify(report.documents)).toBe("eligible");
    expect(row(report, "kafka-profiles")).toMatchObject({ state: "verified", formats: [4] });
    expect(row(report, "profile-backups")).toMatchObject({ state: "verified", formats: [3] });
    const document = JSON.parse(await readFile(join(root, "kafka-profiles.json"), "utf8")) as {
      rollbackGeneration: string;
    };
    expect(document.rollbackGeneration).toMatch(/^kafka-profiles\.json\.pre-security-v3/u);
    await rm(join(root, document.rollbackGeneration));
    expect(row(await check(root), "kafka-profiles")).toMatchObject({
      state: "blocked",
      reason: "interrupted-state",
    });
  });

  it.each([".pre-upgrade.bak", ".pre-transport-v2", ".pre-security-v3", ".pre-security-v3.99"])(
    "retains unproven managed ownership in a %s backup even when current profiles are plain",
    async (suffix) => {
      const root = await seed({
        kind: "plugin",
        pluginId: "streamskope.eda",
        version: 1,
        data: { state: "ready", secret },
      });
      const current = join(root, "kafka-profiles.json");
      await write(root, `kafka-profiles.json${suffix}`, await readFile(current, "utf8"));
      const vault = await openPassphraseVault({
        dataRoot: root,
        passphrase: secret,
        mode: "unlock",
      });
      try {
        const store = new AtomicKafkaProfileFileStore(current, vault.protector, vault.capability);
        await store.commit((await store.load()).map(({ source: _source, ...profile }) => profile));
      } finally {
        await vault.lock();
      }
      const report = await check(root);
      expect(row(report, "kafka-profiles")).toMatchObject({ state: "verified" });
      expect(row(report, "profile-backups")).toMatchObject({
        state: "blocked",
        reason: "managed-source-unverified",
      });
      expect(report.outcome).toBe("blocked");
    },
  );

  it.each([
    ["queries/kafka-queries.json", JSON.stringify({ schemaVersion: 999, queries: [], secret })],
    ["plugins/.packages/index.json", `{${secret}`],
    ["plugins/.archive-interrupted.tmp", secret],
    [".vault-interrupted.tmp", secret],
    ["unknown-private-file", secret],
  ])("preserves refused corrupt or interrupted state at %s", async (file, contents) => {
    const root = await seed();
    await write(root, file, contents);
    expect((await check(root)).outcome).toBe("blocked");
  });

  it("emits one safe CLI report with truthful exit status and rejects caller-supplied identity arguments", async () => {
    const root = await seed();
    const entry = resolve("src/platform/node/browser-data-preflight-entry.ts");
    async function run(
      args: readonly string[],
    ): Promise<{ stdout: string; stderr: string; code: number }> {
      try {
        const result = await execute(process.execPath, ["--import", "tsx", entry, ...args], {
          timeout: 10_000,
          maxBuffer: 64 * 1024,
        });
        return { ...result, code: 0 };
      } catch (error) {
        const result = error as { stdout?: string; stderr?: string; code?: number };
        return { stdout: result.stdout ?? "", stderr: result.stderr ?? "", code: result.code ?? 1 };
      }
    }
    const before = await tree(root);
    for (const [args, expectedCode] of [
      [[root], 0],
      [[], 2],
      [[root, secret], 2],
      [[`relative-${secret}`], 2],
    ] as const) {
      const result = await run(args);
      expect(result.code, result.stderr).toBe(expectedCode);
      expect(result.stderr).toBe("");
      expect(result.stdout.trim().split("\n")).toHaveLength(1);
      const report = parseBrowserDataInspection(JSON.parse(result.stdout) as unknown);
      expect(report.outcome).toBe(expectedCode === 0 ? "eligible" : "blocked");
      expect(result.stdout).not.toContain(secret);
      expect(result.stdout).not.toContain(root);
    }
    expect(await tree(root)).toEqual(before);
  });
});
