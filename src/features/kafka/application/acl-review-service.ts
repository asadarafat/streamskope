import { utf8ByteLength } from "../contracts/message-limits";
import { KAFKA_ACL_LIMITS, type KafkaAclBinding } from "../contracts/acl-types";
import { parseKafkaAclBinding } from "../contracts/acl-validation";
import {
  aclChangeConfirmation,
  parseAclChangeInput,
  parseTopicAccessInput,
  type AclChangeInput,
  type AclChangeReview,
  type TopicAccessInput,
  type TopicAccessExplanation,
} from "../contracts/acl-review";
import type { KafkaWriteOutcome } from "../contracts/reviewed-writes";

import { ConnectionPlans, type ReviewContext } from "./connection-plans";
import { explainTopicAccess, readAccessPolicy, type BrokerAccessPolicy } from "./topic-access";

function canonical(acl: KafkaAclBinding): string {
  return JSON.stringify([
    acl.resourceType,
    acl.patternType,
    acl.resourceName,
    acl.principal,
    acl.host,
    acl.operation,
    acl.permission,
  ]);
}
interface Baseline {
  readonly inventory: readonly KafkaAclBinding[];
  readonly policy: readonly BrokerAccessPolicy[];
}
function fingerprint(baseline: Baseline): string {
  return JSON.stringify([baseline.inventory.map(canonical).sort(), baseline.policy]);
}
async function readBaseline(context: ReviewContext): Promise<Baseline> {
  if (!context.connection.listAcls) throw new Error("ACL inventory is unavailable.");
  const inventory = await context.connection.listAcls(AbortSignal.timeout(15_000));
  if (
    inventory.length > KAFKA_ACL_LIMITS.acls ||
    utf8ByteLength(JSON.stringify(inventory)) > 2 * 1024 * 1024
  )
    throw new Error(
      "ACL inventory exceeds the safe review bound; narrow the change outside StreamSkope.",
    );
  // An unsupported binding cannot be silently dropped from a security decision.
  return {
    inventory: inventory.map((acl) => parseKafkaAclBinding(acl, "inventory")),
    policy: await readAccessPolicy(context.connection),
  };
}
export class AclReviewService {
  private readonly plans: ConnectionPlans<
    { input: AclChangeInput; baseline: Baseline },
    KafkaWriteOutcome
  >;
  private active = false;
  private reads = 0;
  constructor(context: () => ReviewContext | null, now = Date.now) {
    this.plans = new ConnectionPlans(context, now);
  }
  private async snapshot(): Promise<{ context: ReviewContext; baseline: Baseline }> {
    const context = this.plans.context();
    if (!context || this.reads >= 2)
      throw new Error("Connect and wait for the active access review.");
    this.reads++;
    try {
      const baseline = await readBaseline(context);
      if (!this.plans.current(context)) throw new Error("Connection changed while reading ACLs.");
      return { context, baseline };
    } finally {
      this.reads--;
    }
  }
  async explain(input: TopicAccessInput): Promise<TopicAccessExplanation> {
    const parsed = parseTopicAccessInput(input),
      { baseline } = await this.snapshot();
    return explainTopicAccess(parsed, baseline.inventory, baseline.policy);
  }
  async review(input: AclChangeInput): Promise<AclChangeReview> {
    const parsed = parseAclChangeInput(input),
      { context, baseline } = await this.snapshot();
    if (parsed.action === "create" ? !context.connection.createAcl : !context.connection.deleteAcl)
      throw new Error("ACL mutation is unavailable.");
    const present = baseline.inventory.some((acl) => canonical(acl) === canonical(parsed.acl));
    const after = baseline.inventory.filter((acl) => canonical(acl) !== canonical(parsed.acl));
    if (parsed.action === "create") after.push(parsed.acl);
    const plan = this.plans.add(context, { input: parsed, baseline });
    return {
      planId: plan.id,
      connectionName: context.connectionName,
      expiresAt: plan.expiresAt,
      input: parsed,
      beforePresent: present,
      afterPresent: parsed.action === "create",
      beforeAccess:
        parsed.access === null
          ? null
          : explainTopicAccess(parsed.access, baseline.inventory, baseline.policy),
      afterAccess:
        parsed.access === null ? null : explainTopicAccess(parsed.access, after, baseline.policy),
    };
  }
  apply(planId: string, confirmation: string): Promise<KafkaWriteOutcome> {
    return this.plans.apply(
      planId,
      ({ input }) => confirmation === aclChangeConfirmation(input),
      async (plan) => {
        if (this.active) throw new Error("Another ACL change is active. Wait and review again.");
        this.active = true;
        const result = (
          state: KafkaWriteOutcome["state"],
          verification: KafkaWriteOutcome["verification"],
          detail: string,
        ): KafkaWriteOutcome => ({ state, verification, detail, receipt: null });
        try {
          const { input, baseline } = plan.value;
          let fresh: Baseline;
          try {
            fresh = await readBaseline(plan.context);
          } catch {
            return result(
              "rejected",
              "not-applicable",
              "ACL inventory or policy could not be rechecked. No mutation was sent; review again.",
            );
          }
          if (!this.plans.current(plan.context) || fingerprint(fresh) !== fingerprint(baseline))
            return result(
              "rejected",
              "not-applicable",
              "ACL inventory, visible broker policy or connection changed. No mutation was sent; review again.",
            );
          const desired = input.action === "create";
          if (fresh.inventory.some((acl) => canonical(acl) === canonical(input.acl)) === desired)
            return result(
              "acknowledged",
              "verified",
              "The exact binding is already in the reviewed desired state. No mutation was sent.",
            );
          try {
            if (desired) await plan.context.connection.createAcl!(input.acl);
            else await plan.context.connection.deleteAcl!(input.acl);
          } catch (error) {
            const denied =
              typeof error === "object" &&
              error !== null &&
              "code" in error &&
              error.code === "AUTHORIZATION_DENIED";
            return result(
              denied ? "rejected" : "unknown",
              "unavailable",
              denied
                ? "Kafka rejected the ACL change. No automatic retry was made."
                : "The ACL mutation was not acknowledged and may have been applied. Inspect the exact binding before another attempt; no automatic retry was made.",
            );
          }
          try {
            const observed = await plan.context.connection.listAcls!();
            if (
              !this.plans.current(plan.context) ||
              observed.some((acl) => canonical(acl) === canonical(input.acl)) !== desired
            )
              throw new Error("Unverified");
            return result(
              "acknowledged",
              "verified",
              "Kafka acknowledged the exact binding change and its desired state was read back. Concurrent policy changes remain possible.",
            );
          } catch {
            return result(
              "acknowledged",
              "unavailable",
              "Kafka acknowledged the exact binding change, but read-back verification is unavailable. Refresh inventory; do not repeat the mutation to retry verification.",
            );
          }
        } finally {
          this.active = false;
        }
      },
    );
  }
}
