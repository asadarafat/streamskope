import { HostContractValidationError } from "./validation-error";
import {
  exactKeys,
  record,
  text,
  truth,
  declaredValue,
  nonNegativeInteger,
} from "./validation-primitives";
import {
  parseKafkaTopicIdentity,
  parseKafkaTopicName,
  type KafkaTopicIdentity,
} from "./topic-identity";

export const TOPIC_ADMINISTRATION_LIMITS = { partitions: 4096 } as const;
export type TopicAdministrationInput =
  | { readonly kind: "delete"; readonly topic: string }
  | { readonly kind: "expand"; readonly topic: string; readonly partitions: number };
export interface TopicAdministrationSnapshot {
  readonly identity: KafkaTopicIdentity;
  readonly partitions: number;
  readonly replicasSha256: string;
  readonly internal: boolean;
  readonly deleteSupported: boolean;
  readonly deletePermission: "allowed" | "denied" | "unknown";
  readonly expandPermission: "allowed" | "denied" | "unknown";
}
export interface TopicAdministrationReview {
  readonly planId: string;
  readonly connectionName: string;
  readonly expiresAt: string;
  readonly input: TopicAdministrationInput;
  readonly baseline: TopicAdministrationSnapshot;
  readonly confirmation: string;
}
export interface TopicAdministrationOutcome {
  readonly input: TopicAdministrationInput;
  readonly state: "acknowledged" | "rejected" | "unknown" | "unsent";
  readonly verification: "verified" | "different" | "unavailable";
  readonly cleanup: "confirmed" | "unresolved";
  readonly detail: string;
}
function partitionCount(value: unknown): number {
  const count = nonNegativeInteger(value, "partitions");
  if (count < 1 || count > TOPIC_ADMINISTRATION_LIMITS.partitions)
    throw new HostContractValidationError("partitions", "requires 1 through 4096 partitions");
  return count;
}
export function parseTopicAdministrationInput(value: unknown): TopicAdministrationInput {
  const v = record(value, "topicChange");
  const kind = declaredValue(v.kind, ["delete", "expand"] as const, "kind");
  exactKeys(
    v,
    kind === "delete" ? ["kind", "topic"] : ["kind", "topic", "partitions"],
    "topicChange",
  );
  const topic = parseKafkaTopicName(v.topic, "topic");
  if (topic.startsWith("__"))
    throw new HostContractValidationError(
      "topic",
      "reserved internal topic names cannot be changed here",
    );
  return kind === "delete"
    ? { kind, topic }
    : { kind, topic, partitions: partitionCount(v.partitions) };
}
export function parseTopicAdministrationSnapshot(value: unknown): TopicAdministrationSnapshot {
  const v = record(value, "topicBaseline");
  exactKeys(
    v,
    [
      "identity",
      "partitions",
      "replicasSha256",
      "internal",
      "deleteSupported",
      "deletePermission",
      "expandPermission",
    ],
    "topicBaseline",
  );
  const replicasSha256 = text(v.replicasSha256, "replicasSha256", 64);
  if (!/^[0-9a-f]{64}$/u.test(replicasSha256))
    throw new HostContractValidationError(
      "replicasSha256",
      "requires a replica-assignment fingerprint",
    );
  return {
    identity: parseKafkaTopicIdentity(v.identity),
    partitions: partitionCount(v.partitions),
    replicasSha256,
    internal: truth(v.internal, "internal"),
    deleteSupported: truth(v.deleteSupported, "deleteSupported"),
    deletePermission: declaredValue(
      v.deletePermission,
      ["allowed", "denied", "unknown"] as const,
      "deletePermission",
    ),
    expandPermission: declaredValue(
      v.expandPermission,
      ["allowed", "denied", "unknown"] as const,
      "expandPermission",
    ),
  };
}
export function topicAdministrationConfirmation(input: TopicAdministrationInput): string {
  return input.kind === "delete"
    ? `DELETE ${input.topic}`
    : `EXPAND ${input.topic} TO ${input.partitions}`;
}
export function sameTopicAdministrationBaseline(
  a: TopicAdministrationSnapshot,
  b: TopicAdministrationSnapshot,
): boolean {
  return (
    a.identity.clusterId === b.identity.clusterId &&
    a.identity.topicId === b.identity.topicId &&
    a.identity.topic === b.identity.topic &&
    a.partitions === b.partitions &&
    a.replicasSha256 === b.replicasSha256 &&
    a.internal === b.internal &&
    a.deleteSupported === b.deleteSupported &&
    a.deletePermission === b.deletePermission &&
    a.expandPermission === b.expandPermission
  );
}
export function parseTopicAdministrationReview(value: unknown): TopicAdministrationReview {
  const v = record(value, "topicReview");
  exactKeys(
    v,
    ["planId", "connectionName", "expiresAt", "input", "baseline", "confirmation"],
    "topicReview",
  );
  const input = parseTopicAdministrationInput(v.input),
    baseline = parseTopicAdministrationSnapshot(v.baseline);
  const confirmation = text(v.confirmation, "confirmation", 512);
  if (
    input.topic !== baseline.identity.topic ||
    confirmation !== topicAdministrationConfirmation(input)
  )
    throw new HostContractValidationError(
      "topicReview",
      "must identify the reviewed topic and exact change",
    );
  return {
    planId: text(v.planId, "planId", 128),
    connectionName: text(v.connectionName, "connectionName", 256),
    expiresAt: text(v.expiresAt, "expiresAt", 64),
    input,
    baseline,
    confirmation,
  };
}
export function parseTopicAdministrationOutcome(value: unknown): TopicAdministrationOutcome {
  const v = record(value, "topicOutcome");
  exactKeys(v, ["input", "state", "verification", "cleanup", "detail"], "topicOutcome");
  return {
    input: parseTopicAdministrationInput(v.input),
    state: declaredValue(
      v.state,
      ["acknowledged", "rejected", "unknown", "unsent"] as const,
      "state",
    ),
    verification: declaredValue(
      v.verification,
      ["verified", "different", "unavailable"] as const,
      "verification",
    ),
    cleanup: declaredValue(v.cleanup, ["confirmed", "unresolved"] as const, "cleanup"),
    detail: text(v.detail, "detail", 2048),
  };
}
