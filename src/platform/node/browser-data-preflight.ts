import { join } from "node:path";

import { NATS_LIMITS } from "../../features/nats/contracts";
import { isPluginCompatibleWithHost, parseReleaseVersion } from "../../plugins/compatibility";
import { STREAMSKOPE_RELEASE } from "../../plugins/host-release";

import {
  BROWSER_DATA_COMPATIBILITY,
  BROWSER_DATA_DOCUMENT_KINDS,
  BROWSER_DATA_INSPECTION_LIMITATIONS,
  parseBrowserDataInspection,
  type BrowserDataDocumentInspection,
  type BrowserDataDocumentKind,
  type BrowserDataInspection,
  type BrowserDataInspectionReason,
} from "./browser-data-compatibility";
import {
  BrowserDataInspectionError,
  inspectBrowserDataFiles,
  type BrowserDataFile,
} from "./browser-data-files";
import { readBoundedFile } from "./bounded-file";
import {
  inspectKafkaProfileEnvelope,
  KAFKA_PROFILE_FILE_MAX_BYTES,
} from "./kafka-profile-file-store";
import { inspectNatsProfileEnvelope } from "./nats-profile-file-store";
import { AtomicKafkaRuleFileStore } from "./kafka-rule-file-store";
import { AtomicKafkaOperationalPreferenceFileStore } from "./kafka-operational-preference-file-store";
import { AtomicKafkaTopicConfigurationHistoryFileStore } from "./kafka-topic-configuration-history-file-store";
import { AtomicKafkaQueryFileStore } from "./kafka-query-file-store";
import { AtomicKafkaTrustRecipeFileStore } from "./kafka-trust-recipe-file-store";
import { AtomicObservationFileStore } from "./kafka-observation-file-store";
import { PluginStore } from "./plugins/store";
import {
  PLUGIN_NETWORK_SETTINGS_MAX_BYTES,
  parsePluginNetworkSettingsDocument,
} from "./plugins/network-settings";
import type { TrustedPluginPublisher } from "./plugins/publishers";
import { inspectPassphraseVault } from "./vault/passphrase-vault";
import { assertVaultValueEnvelope } from "./vault/vault-crypto";

interface InspectionOptions {
  /** Internal qualification fixtures only. The packaged CLI always uses its own release/trust. */
  readonly hostRelease?: string;
  readonly trustedPublishers?: readonly TrustedPluginPublisher[];
}
interface VerifiedDocument {
  readonly count: number;
  readonly formats: readonly number[];
}
const BACKUP = /^kafka-profiles\.json\.(?:pre-upgrade\.bak|pre-transport-v2(?:\.[1-9]\d?)?)$/u;
const RECOVERY = /^plugins\/\.recovery\/([a-z][a-z0-9]*(?:[.-][a-z0-9]+)*)\.json$/u;
const DIRECTORIES = [
  "rules",
  "workbench",
  "history",
  "queries",
  "templates",
  "plugins",
  "plugins/.packages",
  "plugins/.recovery",
];
const STATIC_FILES = [
  "vault.json",
  "vault.lock",
  "setup-code",
  "kafka-profiles.json",
  "nats-profiles.json",
  "rules/kafka-rules.json",
  "workbench/kafka-operational-preferences.json",
  "history/kafka-topic-configuration-history.json",
  "queries/kafka-queries.json",
  "templates/trust-acquisition-recipes.json",
  "history/kafka-observations.json",
  "plugins/state.json",
  "plugins/network.json",
  "plugins/catalog.json",
  "plugins/.packages/index.json",
];
function fail(reason: BrowserDataInspectionReason): never {
  throw new BrowserDataInspectionError(reason);
}
function reason(
  error: unknown,
  fallback: BrowserDataInspectionReason,
): BrowserDataInspectionReason {
  return error instanceof BrowserDataInspectionError ? error.reason : fallback;
}
function rows(): BrowserDataDocumentInspection[] {
  return BROWSER_DATA_DOCUMENT_KINDS.map((kind) => ({
    kind,
    state: "missing",
    count: 0,
    formats: [],
    reason: null,
  }));
}
function report(
  hostRelease: string,
  documents: readonly BrowserDataDocumentInspection[],
): BrowserDataInspection {
  return parseBrowserDataInspection({
    schemaVersion: 1,
    dataContract: BROWSER_DATA_COMPATIBILITY.contract,
    hostRelease,
    documents,
    outcome: documents.some((row) => row.state === "blocked" || row.state === "not-inspected")
      ? "blocked"
      : "eligible",
    unverified: BROWSER_DATA_INSPECTION_LIMITATIONS,
  });
}
/** Safe fallback also used by the standalone CLI; it never serializes an exception. */
export function unavailableBrowserDataInspection(
  failure: BrowserDataInspectionReason = "unavailable",
  hostRelease = STREAMSKOPE_RELEASE,
): BrowserDataInspection {
  return report(
    hostRelease,
    BROWSER_DATA_DOCUMENT_KINDS.map((kind) => ({
      kind,
      state: kind === "filesystem" ? "blocked" : "not-inspected",
      count: 0,
      formats: [],
      reason: failure,
    })),
  );
}
function protectedEnvelope(bytes: Buffer): void {
  try {
    assertVaultValueEnvelope(bytes);
  } catch {
    fail("invalid-protected-envelope");
  }
}

/** Inspect only outer documents and verified package bytes. No key, lease, runtime, or network is opened. */
export async function inspectBrowserData(
  root: string,
  options: InspectionOptions = {},
): Promise<BrowserDataInspection> {
  let hostRelease = STREAMSKOPE_RELEASE;
  try {
    if (options.hostRelease !== undefined) {
      if (!options.hostRelease.startsWith("v")) fail("invalid-request");
      hostRelease = `v${parseReleaseVersion(options.hostRelease.slice(1))}`;
    }
  } catch {
    return unavailableBrowserDataInspection("invalid-request");
  }
  let files: ReadonlyMap<string, BrowserDataFile>;
  try {
    files = await inspectBrowserDataFiles(root);
  } catch (error) {
    return unavailableBrowserDataInspection(reason(error, "unavailable"), hostRelease);
  }
  const documents = rows();
  if (files.size === 0) return report(hostRelease, documents);
  const set = (
    kind: BrowserDataDocumentKind,
    value: Omit<BrowserDataDocumentInspection, "kind">,
  ): void => {
    documents[BROWSER_DATA_DOCUMENT_KINDS.indexOf(kind)] = { kind, ...value };
  };
  const allowed = new Map<string, BrowserDataFile["type"]>([["", "directory"]]);
  for (const path of DIRECTORIES) allowed.set(path, "directory");
  for (const path of STATIC_FILES) allowed.set(path, "file");
  const exists = (path: string): boolean => files.has(path);
  const group = (path: string): boolean =>
    [...files.keys()].some((name) => name.startsWith(`${path}/`));
  const read = (path: string, max: number): Promise<Buffer> => {
    if (files.get(path)?.type !== "file") fail("unsafe-filesystem");
    return readBoundedFile(join(root, path), max, { rejectSymlinks: true });
  };
  const inspect = async (
    kind: BrowserDataDocumentKind,
    present: boolean,
    action: () => Promise<VerifiedDocument>,
    fallback: BrowserDataInspectionReason = "unsupported-format",
  ): Promise<void> => {
    if (!present) return;
    try {
      const result = await action();
      set(kind, { state: "verified", ...result, reason: null });
    } catch (error) {
      set(kind, { state: "blocked", count: 0, formats: [], reason: reason(error, fallback) });
    }
  };
  await inspect("vault", exists("vault.json"), async () => {
    if ((await inspectPassphraseVault(root)) !== "present") fail("unavailable");
    return { count: 1, formats: [1] };
  });
  const kafka = async (path: string): Promise<VerifiedDocument> => {
    const document = inspectKafkaProfileEnvelope(await read(path, KAFKA_PROFILE_FILE_MAX_BYTES));
    for (const profile of document.profiles) protectedEnvelope(profile.protectedBytes);
    if (document.profiles.some((profile) => profile.source !== undefined))
      fail("managed-source-unverified");
    if (
      document.rollbackGeneration !== undefined &&
      (!BACKUP.test(document.rollbackGeneration) || !exists(document.rollbackGeneration))
    )
      fail("interrupted-state");
    return { count: document.profiles.length, formats: [document.version] };
  };
  await inspect("kafka-profiles", exists("kafka-profiles.json"), () =>
    kafka("kafka-profiles.json"),
  );
  await inspect("nats-profiles", exists("nats-profiles.json"), async () => {
    const document = inspectNatsProfileEnvelope(
      await read("nats-profiles.json", NATS_LIMITS.profileFileBytes),
    );
    for (const profile of document.profiles) protectedEnvelope(profile.protectedBytes);
    return { count: document.profiles.length, formats: [1] };
  });
  await inspect("rules", exists("rules/kafka-rules.json"), async () => ({
    count:
      (await new AtomicKafkaRuleFileStore(join(root, "rules/kafka-rules.json")).load())?.rules
        .length ?? 0,
    formats: [1],
  }));
  await inspect("preferences", exists("workbench/kafka-operational-preferences.json"), async () => {
    await new AtomicKafkaOperationalPreferenceFileStore(
      join(root, "workbench/kafka-operational-preferences.json"),
    ).load();
    return { count: 1, formats: [1] };
  });
  await inspect(
    "topic-history",
    exists("history/kafka-topic-configuration-history.json"),
    async () => ({
      count:
        (
          await new AtomicKafkaTopicConfigurationHistoryFileStore(
            join(root, "history/kafka-topic-configuration-history.json"),
          ).load()
        )?.entries.length ?? 0,
      formats: [1],
    }),
  );
  await inspect("queries", exists("queries/kafka-queries.json"), async () => ({
    count: (await new AtomicKafkaQueryFileStore(join(root, "queries/kafka-queries.json")).load())
      .length,
    formats: [1],
  }));
  await inspect("trust-recipes", exists("templates/trust-acquisition-recipes.json"), async () => ({
    count:
      (
        await new AtomicKafkaTrustRecipeFileStore(
          join(root, "templates/trust-acquisition-recipes.json"),
        ).load()
      )?.recipes.length ?? 0,
    formats: [1],
  }));
  await inspect("observations", exists("history/kafka-observations.json"), async () => ({
    count: (
      await new AtomicObservationFileStore(join(root, "history/kafka-observations.json")).load()
    ).series.length,
    formats: [1],
  }));
  // Constructors supply verification authority only; none activate installed modules or repair caches.
  let store: PluginStore;
  try {
    store = new PluginStore(
      join(root, "plugins"),
      options.trustedPublishers === undefined
        ? {}
        : { trustedPublishers: options.trustedPublishers },
    );
  } catch {
    return unavailableBrowserDataInspection("invalid-request", hostRelease);
  }
  await inspect(
    "plugin-installations",
    exists("plugins/state.json") ||
      [...files.keys()].some(
        (name) => /^plugins\/[a-z]/u.test(name) && !STATIC_FILES.includes(name),
      ),
    async () => {
      const installed = await store.inspectInstalled();
      for (const item of installed.packages) {
        const parent = `plugins/${item.id}`;
        const directory = `${parent}/${item.sha256}`;
        allowed.set(parent, "directory");
        allowed.set(directory, "directory");
        for (const name of [
          "package.skope-plugin",
          item.manifest.backend,
          item.manifest.renderer,
          ...(item.manifest.styles === undefined ? [] : [item.manifest.styles]),
          ...(item.manifest.resources ?? []).map((resource) => resource.path),
        ])
          allowed.set(`${directory}/${name}`, "file");
      }
      if (installed.pending) fail("plugin-change-pending");
      if (installed.unresolved) fail("interrupted-state");
      if (
        installed.packages.some(
          (item) => item.referenced && !isPluginCompatibleWithHost(item.manifest, hostRelease),
        )
      )
        fail("plugin-incompatible");
      return { count: installed.packages.length, formats: exists("plugins/state.json") ? [1] : [] };
    },
    "plugin-package-invalid",
  );
  await inspect("plugin-network", exists("plugins/network.json"), async () => {
    const document = parsePluginNetworkSettingsDocument(
      JSON.parse(
        new TextDecoder("utf-8", { fatal: true }).decode(
          await read("plugins/network.json", PLUGIN_NETWORK_SETTINGS_MAX_BYTES),
        ),
      ) as unknown,
    );
    if (document.protectedCredentials !== undefined)
      protectedEnvelope(Buffer.from(document.protectedCredentials, "base64"));
    if (document.configuration.mode === "custom") fail("network-configuration-unsupported");
    return { count: 1, formats: [1] };
  });
  await inspect("plugin-catalog", exists("plugins/catalog.json"), async () => ({
    count: (await store.catalogCache.read())?.entries.length ?? 0,
    formats: [1],
  }));
  await inspect(
    "plugin-package-cache",
    group("plugins/.packages"),
    async () => {
      const cache = await store.packageCache.inspect();
      for (const digest of cache.retainedDigests)
        allowed.set(`plugins/.packages/${digest}.skope-plugin`, "file");
      return {
        count: cache.retainedDigests.length,
        formats: exists("plugins/.packages/index.json") ? [1] : [],
      };
    },
    "plugin-package-invalid",
  );
  await inspect("plugin-recovery", group("plugins/.recovery"), async () => {
    let count = 0;
    let pending = false;
    for (const path of files.keys()) {
      if (!path.startsWith("plugins/.recovery/")) continue;
      const match = RECOVERY.exec(path);
      const id = match?.[1];
      if (id === undefined || id.length > 80) fail("unsupported-format");
      allowed.set(path, "file");
      if ((await store.readRecoveryState(id)) !== null) pending = true;
      count += 1;
    }
    if (pending) fail("plugin-recovery-pending");
    return { count, formats: [1] };
  });
  const backups = [...files.keys()].filter((path) => BACKUP.test(path));
  for (const path of backups) allowed.set(path, "file");
  await inspect("profile-backups", backups.length > 0, async () => {
    const formats = new Set<number>();
    for (const path of backups)
      for (const format of (await kafka(path)).formats) formats.add(format);
    return { count: backups.length, formats: [...formats].sort() };
  });
  await inspect("host-state", exists("vault.lock") || exists("setup-code"), async () => {
    if (exists("vault.lock") && (await read("vault.lock", 0)).length !== 0)
      fail("unsupported-format");
    if (
      exists("setup-code") &&
      !/^[A-Za-z0-9_-]{43}$/u.test((await read("setup-code", 128)).toString("utf8").trim())
    )
      fail("unsupported-format");
    return { count: Number(exists("vault.lock")) + Number(exists("setup-code")), formats: [] };
  });
  if (
    !exists("vault.json") &&
    [...files].some(
      ([path, value]) => value.type === "file" && path !== "setup-code" && path !== "vault.lock",
    )
  )
    set("vault", { state: "blocked", count: 0, formats: [], reason: "vault-required" });
  const unknown = [...files].find(([path, value]) => allowed.get(path) !== value.type);
  set(
    "filesystem",
    unknown === undefined
      ? { state: "verified", count: files.size, formats: [], reason: null }
      : {
          state: "blocked",
          count: files.size,
          formats: [],
          reason: /(?:^|\/)\.[^/]*(?:tmp|install|repair|remove|archive)/u.test(unknown[0])
            ? "interrupted-state"
            : "unrecognized-path",
        },
  );
  return report(hostRelease, documents);
}
