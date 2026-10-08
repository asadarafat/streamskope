import type { KafkaAclBinding } from "../contracts/acl-types";
import type { TopicAccessInput, TopicAccessExplanation } from "../contracts/acl-review";
import type { KafkaConfigurationEntry } from "../contracts";

import type { KafkaClusterMetadata } from "./types";

const standardAuthorizer = "org.apache.kafka.metadata.authorizer.StandardAuthorizer";
export interface BrokerAccessPolicy {
  readonly id: number;
  readonly authorizer: string | null;
  readonly allowIfNoAcl: boolean | null;
  readonly superUsers: readonly string[] | null;
}
interface AccessPolicyReader {
  describeClusterMetadata(signal?: AbortSignal): Promise<KafkaClusterMetadata>;
  describeBrokerConfiguration(
    brokerId: number,
    signal?: AbortSignal,
  ): Promise<readonly KafkaConfigurationEntry[]>;
}
/** Only observed values count as evidence. Missing/redacted/custom policy is unknown. */
export async function readAccessPolicy(
  connection: AccessPolicyReader,
): Promise<readonly BrokerAccessPolicy[]> {
  const signal = AbortSignal.timeout(15_000);
  try {
    const cluster = await connection.describeClusterMetadata(signal);
    if (cluster.brokers.length === 0 || cluster.brokers.length > 32) return [];
    const result: BrokerAccessPolicy[] = [];
    for (let start = 0; start < cluster.brokers.length; start += 4) {
      result.push(
        ...(await Promise.all(
          cluster.brokers
            .slice(start, start + 4)
            .map(async ({ nodeId }): Promise<BrokerAccessPolicy> => {
              try {
                const configs = await connection.describeBrokerConfiguration(nodeId, signal);
                const get = (name: string): string | null => {
                  const entry = configs.find((c) => c.name === name);
                  return entry && !entry.isSensitive ? entry.value : null;
                };
                const allow = get("allow.everyone.if.no.acl.found"),
                  users = get("super.users");
                return {
                  id: nodeId,
                  authorizer: get("authorizer.class.name"),
                  allowIfNoAcl: allow === "true" ? true : allow === "false" ? false : null,
                  superUsers:
                    users === null
                      ? null
                      : users
                          .split(";")
                          .map((u) => u.trim())
                          .filter(Boolean)
                          .sort(),
                };
              } catch {
                return { id: nodeId, authorizer: null, allowIfNoAcl: null, superUsers: null };
              }
            }),
        )),
      );
    }
    return result.sort((a, b) => a.id - b.id);
  } catch {
    return [];
  }
}
export function topicResourceMatches(acl: KafkaAclBinding, topic: string): boolean {
  return (
    acl.resourceType === "TOPIC" &&
    (acl.patternType === "PREFIXED"
      ? topic.startsWith(acl.resourceName)
      : acl.resourceName === "*" || acl.resourceName === topic)
  );
}
export function explainTopicAccess(
  input: TopicAccessInput,
  inventory: readonly KafkaAclBinding[],
  policies: readonly BrokerAccessPolicy[],
): TopicAccessExplanation {
  const resource = inventory.filter((acl) => topicResourceMatches(acl, input.topic));
  const matching = resource.filter(
    (acl) =>
      (acl.principal === input.principal || acl.principal === "User:*") &&
      (acl.host === input.host || acl.host === "*") &&
      (acl.operation === "READ" || acl.operation === "ALL"),
  );
  const aclDecision = matching.some((acl) => acl.permission === "DENY")
    ? "denied"
    : matching.some((acl) => acl.permission === "ALLOW")
      ? "allowed"
      : resource.length > 0
        ? "denied"
        : "broker-default";
  const decisions = policies.map((policy): TopicAccessExplanation["effective"] => {
    if (policy.authorizer !== standardAuthorizer) return "unknown";
    if (policy.superUsers?.includes(input.principal)) return "allowed";
    const decision =
      aclDecision === "broker-default"
        ? policy.allowIfNoAcl === null
          ? "unknown"
          : policy.allowIfNoAcl
            ? "allowed"
            : "denied"
        : aclDecision;
    return decision === "denied" && policy.superUsers === null ? "unknown" : decision;
  });
  const effective =
    decisions.length > 0 && decisions.every((d) => d === decisions[0]) ? decisions[0]! : "unknown";
  const standard =
    policies.length > 0 && policies.every((p) => p.authorizer === standardAuthorizer);
  const allow =
    policies.length > 0 && policies.every((p) => p.allowIfNoAcl === policies[0]?.allowIfNoAcl)
      ? policies[0]!.allowIfNoAcl
      : null;
  const superuser =
    policies.length > 0 && policies.every((p) => p.superUsers?.includes(input.principal))
      ? "yes"
      : policies.length > 0 &&
          policies.every((p) => p.superUsers !== null && !p.superUsers.includes(input.principal))
        ? "no"
        : "unknown";
  return {
    input,
    effective,
    aclDecision,
    resourceBindings: resource.length,
    matching: matching.slice(0, 50),
    omittedBindings: Math.max(0, matching.length - 50),
    policy: {
      brokers: policies.length,
      standardAuthorizer: standard,
      allowIfNoAcl: allow,
      superuser,
    },
    reasons: [
      aclDecision === "broker-default"
        ? "No topic resource binding matches. The broker's no-ACL policy decides access for non-superusers."
        : aclDecision === "allowed"
          ? "A READ or ALL allow matches the topic, principal and client address; no matching deny was found."
          : matching.some((a) => a.permission === "DENY")
            ? "A matching READ or ALL deny takes precedence over matching allows, except for configured superusers."
            : "Bindings exist for this topic, but none grants this principal READ from this address. The no-ACL default does not apply.",
      ...(effective === "unknown"
        ? [
            "Effective access is unknown because broker policy is missing, differs across brokers, uses a custom authorizer, or may include an unobserved superuser.",
          ]
        : []),
      "This is a snapshot for the exact supplied Kafka principal and broker-observed IP. Login-to-principal mapping, NAT, external policies and authentication are not verified.",
      "Topic READ alone does not authorize consumer-group operations. This is not an exhaustive list of people who can read the topic or a live access test.",
    ],
  };
}
