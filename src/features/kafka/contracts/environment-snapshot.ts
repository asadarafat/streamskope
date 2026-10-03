import {
  record,
  exactKeys,
  text,
  declaredValue,
  truth,
  canonicalIsoTimestamp,
} from "./validation-primitives";
export const ENVIRONMENT_CONFIG_KEYS = [
  "cleanup.policy",
  "retention.ms",
  "retention.bytes",
  "segment.ms",
  "min.insync.replicas",
  "max.message.bytes",
  "compression.type",
] as const;
export type EnvironmentConfigKey = (typeof ENVIRONMENT_CONFIG_KEYS)[number];
export interface EnvironmentTopic {
  readonly name: string;
  readonly topicId: string;
  readonly configs: readonly {
    readonly key: EnvironmentConfigKey;
    readonly value: string | null;
    readonly mutable: boolean;
  }[];
}
export interface EnvironmentSnapshot {
  readonly format: "streamskope.topic-config/v1";
  readonly clusterId: string;
  readonly observedAt: string;
  readonly topics: readonly EnvironmentTopic[];
}
export interface EnvironmentDifference {
  readonly topic: string;
  readonly key: EnvironmentConfigKey;
  readonly source: string | null;
  readonly target: string | null;
  readonly supported: boolean;
  readonly reason: string;
}
export interface EnvironmentSelection {
  readonly topic: string;
  readonly key: EnvironmentConfigKey;
}
export interface EnvironmentProfile {
  readonly id: string;
  readonly revision: number;
}
export interface EnvironmentInput {
  readonly source: EnvironmentSnapshot;
  readonly target: EnvironmentSnapshot;
  readonly targetProfile: EnvironmentProfile | null;
  readonly selected: readonly EnvironmentSelection[];
}
export interface EnvironmentReview {
  readonly planId: string;
  readonly expiresAt: string;
  readonly confirmation: string;
  readonly source: EnvironmentSnapshot;
  readonly target: EnvironmentSnapshot;
  readonly changes: readonly EnvironmentDifference[];
}
export interface EnvironmentOutcome {
  readonly results: readonly {
    readonly topic: string;
    readonly state: "acknowledged" | "rejected" | "unknown" | "unsent";
    readonly verified: boolean;
  }[];
  readonly detail: string;
}
export function environmentTopic(value: unknown): string {
  const v = text(value, "topic", 249);
  if (!/^[a-zA-Z0-9._-]+$/u.test(v) || v === "." || v === "..")
    throw new Error("Invalid topic name.");
  return v;
}
function list<T>(value: unknown, max: number, parse: (x: unknown) => T): readonly T[] {
  if (!Array.isArray(value) || value.length > max) throw new Error("Snapshot exceeds its limit.");
  return value.map(parse);
}
export function configValue(key: EnvironmentConfigKey, value: unknown): string {
  const v = text(value, "configuration value", 128);
  if (key === "cleanup.policy") {
    if (!["compact", "delete", "compact,delete", "delete,compact"].includes(v))
      throw new Error("Unsupported cleanup policy.");
    return v.split(",").sort().join(",");
  }
  if (key === "compression.type")
    return declaredValue(
      v,
      ["producer", "uncompressed", "gzip", "snappy", "lz4", "zstd"],
      "compression",
    );
  if (!/^-?\d{1,19}$/u.test(v)) throw new Error("Expected a numeric topic setting.");
  return BigInt(v).toString();
}
export function parseEnvironmentSnapshot(value: unknown): EnvironmentSnapshot {
  const p = record(value, "snapshot");
  exactKeys(p, ["format", "clusterId", "observedAt", "topics"], "snapshot");
  const topics = list(p.topics, 20, (v) => {
    const t = record(v, "topic");
    exactKeys(t, ["name", "topicId", "configs"], "topic");
    const configs = list(t.configs, ENVIRONMENT_CONFIG_KEYS.length, (x) => {
      const c = record(x, "config");
      exactKeys(c, ["key", "value", "mutable"], "config");
      const key = declaredValue(c.key, ENVIRONMENT_CONFIG_KEYS, "key");
      return {
        key,
        value: c.value === null ? null : configValue(key, c.value),
        mutable: truth(c.mutable, "mutable"),
      };
    });
    if (new Set(configs.map((c) => c.key)).size !== configs.length)
      throw new Error("Repeated setting.");
    return {
      name: environmentTopic(t.name),
      topicId: text(t.topicId, "topic identity", 128),
      configs: [...configs].sort((a, b) => a.key.localeCompare(b.key, "en-US")),
    };
  });
  if (!topics.length || new Set(topics.map((t) => t.name)).size !== topics.length)
    throw new Error("Select 1–20 distinct topics.");
  return {
    format: declaredValue(p.format, ["streamskope.topic-config/v1"], "format"),
    clusterId: text(p.clusterId, "clusterId", 128),
    observedAt: canonicalIsoTimestamp(p.observedAt, "observedAt"),
    topics: [...topics].sort((a, b) => a.name.localeCompare(b.name, "en-US")),
  };
}
export function environmentDiff(
  source: EnvironmentSnapshot,
  target: EnvironmentSnapshot,
): readonly EnvironmentDifference[] {
  const rows: EnvironmentDifference[] = [];
  for (const topic of source.topics) {
    const other = target.topics.find((t) => t.name === topic.name);
    for (const config of topic.configs) {
      const actual = other?.configs.find((c) => c.key === config.key);
      if (config.value === actual?.value) continue;
      const supported = !!actual?.mutable && actual.value !== null && config.value !== null;
      rows.push({
        topic: topic.name,
        key: config.key,
        source: config.value,
        target: actual?.value ?? null,
        supported,
        reason: supported
          ? "Existing mutable topic setting"
          : !other
            ? "Missing target topic; creation is unsupported"
            : "Value unavailable or target setting is immutable",
      });
    }
  }
  return rows.sort(
    (a, b) => a.topic.localeCompare(b.topic, "en-US") || a.key.localeCompare(b.key, "en-US"),
  );
}
export function environmentIdentity(snapshot: EnvironmentSnapshot): string {
  return JSON.stringify({ clusterId: snapshot.clusterId, topics: snapshot.topics });
}
export function exportEnvironment(snapshot: EnvironmentSnapshot): string {
  return JSON.stringify(parseEnvironmentSnapshot(snapshot), null, 2) + "\n";
}
export function parseEnvironmentProfile(value: unknown): EnvironmentProfile | null {
  if (value === null) return null;
  const p = record(value, "profile");
  exactKeys(p, ["id", "revision"], "profile");
  if (!Number.isSafeInteger(p.revision) || Number(p.revision) < 1)
    throw new Error("Invalid profile revision.");
  return { id: text(p.id, "id", 128), revision: Number(p.revision) };
}
export function parseEnvironmentInput(value: unknown): EnvironmentInput {
  const p = record(value, "promotion");
  exactKeys(p, ["source", "target", "targetProfile", "selected"], "promotion");
  const selected = list(p.selected, 64, (x) => {
    const c = record(x, "selection");
    exactKeys(c, ["topic", "key"], "selection");
    return {
      topic: environmentTopic(c.topic),
      key: declaredValue(c.key, ENVIRONMENT_CONFIG_KEYS, "key"),
    };
  });
  if (
    !selected.length ||
    new Set(selected.map((s) => `${s.topic}/${s.key}`)).size !== selected.length
  )
    throw new Error("Select 1–64 distinct differences.");
  return {
    source: parseEnvironmentSnapshot(p.source),
    target: parseEnvironmentSnapshot(p.target),
    targetProfile: parseEnvironmentProfile(p.targetProfile),
    selected,
  };
}
export function parseEnvironmentReview(value: unknown): EnvironmentReview {
  const p = record(value, "review");
  exactKeys(p, ["planId", "expiresAt", "confirmation", "source", "target", "changes"], "review");
  const source = parseEnvironmentSnapshot(p.source),
    target = parseEnvironmentSnapshot(p.target);
  const changes = list(p.changes, 64, (x) => {
    const c = record(x, "change");
    exactKeys(c, ["topic", "key", "source", "target", "supported", "reason"], "change");
    const key = declaredValue(c.key, ENVIRONMENT_CONFIG_KEYS, "key");
    return {
      topic: environmentTopic(c.topic),
      key,
      source: c.source === null ? null : configValue(key, c.source),
      target: c.target === null ? null : configValue(key, c.target),
      supported: truth(c.supported, "supported"),
      reason: text(c.reason, "reason", 256),
    };
  });
  return {
    planId: text(p.planId, "planId", 128),
    expiresAt: canonicalIsoTimestamp(p.expiresAt, "expiresAt"),
    confirmation: text(p.confirmation, "confirmation", 256),
    source,
    target,
    changes,
  };
}
export function parseEnvironmentOutcome(value: unknown): EnvironmentOutcome {
  const p = record(value, "outcome");
  exactKeys(p, ["results", "detail"], "outcome");
  return {
    detail: text(p.detail, "detail", 512),
    results: list(p.results, 20, (x) => {
      const r = record(x, "result");
      exactKeys(r, ["topic", "state", "verified"], "result");
      return {
        topic: environmentTopic(r.topic),
        state: declaredValue(r.state, ["acknowledged", "rejected", "unknown", "unsent"], "state"),
        verified: truth(r.verified, "verified"),
      };
    }),
  };
}
