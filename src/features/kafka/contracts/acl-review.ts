import { kafkaAclIdentity, type KafkaAclBinding } from "./acl-types";
import { parseKafkaAclBinding } from "./acl-validation";
import { HostContractValidationError } from "./validation-error";
import {
  declaredValue,
  exactKeys,
  nonNegativeInteger,
  record,
  text,
} from "./validation-primitives";

export interface TopicAccessInput {
  readonly topic: string;
  readonly principal: string;
  /** The address seen by the broker, not a DNS name or wildcard. */
  readonly host: string;
}
export interface TopicAccessExplanation {
  readonly input: TopicAccessInput;
  readonly effective: "allowed" | "denied" | "unknown";
  readonly aclDecision: "allowed" | "denied" | "broker-default";
  readonly resourceBindings: number;
  readonly matching: readonly KafkaAclBinding[];
  readonly omittedBindings: number;
  readonly policy: {
    readonly brokers: number;
    readonly standardAuthorizer: boolean;
    readonly allowIfNoAcl: boolean | null;
    readonly superuser: "yes" | "no" | "unknown";
  };
  readonly reasons: readonly string[];
}
export interface AclChangeInput {
  readonly action: "create" | "delete";
  readonly acl: KafkaAclBinding;
  readonly access: TopicAccessInput | null;
}
export interface AclChangeReview {
  readonly planId: string;
  readonly connectionName: string;
  readonly expiresAt: string;
  readonly input: AclChangeInput;
  readonly beforePresent: boolean;
  readonly afterPresent: boolean;
  readonly beforeAccess: TopicAccessExplanation | null;
  readonly afterAccess: TopicAccessExplanation | null;
}
export function aclChangeConfirmation(input: AclChangeInput): string {
  return `${input.action} ${kafkaAclIdentity(input.acl)}`;
}
export function parseTopicAccessInput(value: unknown): TopicAccessInput {
  const p = record(value, "access");
  exactKeys(p, ["topic", "principal", "host"], "access");
  const topic = text(p.topic, "access.topic", 249);
  const principal = text(p.principal, "access.principal", 1024);
  const host = text(p.host, "access.host", 64);
  if (!/^[a-zA-Z0-9._-]+$/u.test(topic) || topic === "." || topic === "..")
    throw new HostContractValidationError("access.topic", "requires one concrete topic name");
  if (!/^[^:*\s]+:[^*]+$/u.test(principal))
    throw new HostContractValidationError(
      "access.principal",
      "requires one concrete Kafka principal, such as User:alice",
    );
  // Exact IP spelling matters in Kafka ACLs. Do not normalize or resolve addresses.
  const ipv4 = host.split(".");
  const isV4 =
    ipv4.length === 4 && ipv4.every((v) => /^(0|[1-9]\d{0,2})$/u.test(v) && Number(v) <= 255);
  const isV6 =
    host.includes(":") &&
    /^[a-fA-F0-9:]+$/u.test(host) &&
    ((): boolean => {
      try {
        return new URL(`http://[${host}]/`).hostname.length > 0;
      } catch {
        return false;
      }
    })();
  if (!isV4 && !isV6)
    throw new HostContractValidationError(
      "access.host",
      "requires the exact client IP address seen by Kafka",
    );
  return { topic, principal, host };
}
export function parseAclChangeInput(value: unknown): AclChangeInput {
  const p = record(value, "aclChange");
  exactKeys(p, ["action", "acl", "access"], "aclChange");
  const acl = parseKafkaAclBinding(p.acl, "aclChange.acl");
  const access = p.access === null ? null : parseTopicAccessInput(p.access);
  if (acl.resourceType === "TOPIC" && access === null)
    throw new HostContractValidationError(
      "aclChange.access",
      "requires a representative topic reader for impact review",
    );
  if (
    access !== null &&
    (acl.resourceType !== "TOPIC" ||
      !(acl.patternType === "PREFIXED"
        ? access.topic.startsWith(acl.resourceName)
        : acl.resourceName === "*" || acl.resourceName === access.topic))
  )
    throw new HostContractValidationError(
      "aclChange.access.topic",
      "must be covered by the reviewed topic binding",
    );
  return {
    action: declaredValue(p.action, ["create", "delete"] as const, "aclChange.action"),
    acl,
    access,
  };
}
function flag(value: unknown): boolean {
  if (typeof value !== "boolean") throw new HostContractValidationError("flag", "must be boolean");
  return value;
}
export function parseTopicAccessExplanation(value: unknown): TopicAccessExplanation {
  const p = record(value, "explanation");
  exactKeys(
    p,
    [
      "input",
      "effective",
      "aclDecision",
      "resourceBindings",
      "matching",
      "omittedBindings",
      "policy",
      "reasons",
    ],
    "explanation",
  );
  const policy = record(p.policy, "policy");
  exactKeys(policy, ["brokers", "standardAuthorizer", "allowIfNoAcl", "superuser"], "policy");
  if (
    !Array.isArray(p.matching) ||
    p.matching.length > 50 ||
    !Array.isArray(p.reasons) ||
    p.reasons.length > 16
  )
    throw new HostContractValidationError("explanation", "exceeds bounded evidence");
  return {
    input: parseTopicAccessInput(p.input),
    effective: declaredValue(p.effective, ["allowed", "denied", "unknown"] as const, "effective"),
    aclDecision: declaredValue(
      p.aclDecision,
      ["allowed", "denied", "broker-default"] as const,
      "aclDecision",
    ),
    resourceBindings: nonNegativeInteger(p.resourceBindings, "resourceBindings"),
    matching: p.matching.map((acl) => parseKafkaAclBinding(acl, "matching")),
    omittedBindings: nonNegativeInteger(p.omittedBindings, "omittedBindings"),
    policy: {
      brokers: nonNegativeInteger(policy.brokers, "brokers"),
      standardAuthorizer: flag(policy.standardAuthorizer),
      allowIfNoAcl: policy.allowIfNoAcl === null ? null : flag(policy.allowIfNoAcl),
      superuser: declaredValue(policy.superuser, ["yes", "no", "unknown"] as const, "superuser"),
    },
    reasons: p.reasons.map((reason) => text(reason, "reason", 1024)),
  };
}
export function parseAclChangeReview(value: unknown): AclChangeReview {
  const p = record(value, "aclReview");
  exactKeys(
    p,
    [
      "planId",
      "connectionName",
      "expiresAt",
      "input",
      "beforePresent",
      "afterPresent",
      "beforeAccess",
      "afterAccess",
    ],
    "aclReview",
  );
  return {
    planId: text(p.planId, "planId", 128),
    connectionName: text(p.connectionName, "connectionName", 256),
    expiresAt: text(p.expiresAt, "expiresAt", 64),
    input: parseAclChangeInput(p.input),
    beforePresent: flag(p.beforePresent),
    afterPresent: flag(p.afterPresent),
    beforeAccess: p.beforeAccess === null ? null : parseTopicAccessExplanation(p.beforeAccess),
    afterAccess: p.afterAccess === null ? null : parseTopicAccessExplanation(p.afterAccess),
  };
}
