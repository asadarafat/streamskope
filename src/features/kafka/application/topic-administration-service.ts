import {
  parseTopicAdministrationInput,
  parseTopicAdministrationSnapshot,
  parseTopicAdministrationOutcome,
  sameTopicAdministrationBaseline,
  topicAdministrationConfirmation,
  type TopicAdministrationInput,
  type TopicAdministrationSnapshot,
  type TopicAdministrationOutcome,
  type TopicAdministrationReview,
} from "../contracts/topic-administration";

import { ConnectionPlans } from "./connection-plans";
import type { TopicAdministrationScope } from "./connection-scope";

export function assertTopicAdministrationAllowed(
  input: TopicAdministrationInput,
  baseline: TopicAdministrationSnapshot,
): void {
  if (baseline.internal) throw new Error("Internal topics cannot be changed here.");
  if (
    input.kind === "delete" &&
    (!baseline.deleteSupported || baseline.deletePermission === "denied")
  )
    throw new Error("Identity-safe topic deletion is unsupported or denied.");
  if (
    input.kind === "expand" &&
    (input.partitions <= baseline.partitions || baseline.expandPermission === "denied")
  )
    throw new Error(
      "Expansion must increase the partition count and requires topic ALTER permission.",
    );
}
export class TopicAdministrationService {
  private readonly plans: ConnectionPlans<
    { input: TopicAdministrationInput; baseline: TopicAdministrationSnapshot },
    TopicAdministrationOutcome,
    TopicAdministrationScope
  >;
  private active = false;
  constructor(context: () => TopicAdministrationScope | null, now = Date.now) {
    this.plans = new ConnectionPlans(context, (scope) => scope.isCurrent(), now);
  }
  async review(input: TopicAdministrationInput): Promise<TopicAdministrationReview> {
    const parsed = parseTopicAdministrationInput(input),
      scope = this.plans.context();
    if (!scope?.snapshot || !scope.tryApply)
      throw new Error("Topic administration is unavailable.");
    const baseline = parseTopicAdministrationSnapshot(await scope.snapshot(parsed.topic));
    if (baseline.identity.topic !== parsed.topic) throw new Error("Topic identity does not match.");
    assertTopicAdministrationAllowed(parsed, baseline);
    const plan = this.plans.add(scope, { input: parsed, baseline });
    return {
      planId: plan.id,
      expiresAt: plan.expiresAt,
      connectionName: scope.connectionName,
      input: parsed,
      baseline,
      confirmation: topicAdministrationConfirmation(parsed),
    };
  }
  apply(id: string, confirmation: string): Promise<TopicAdministrationOutcome> {
    return this.plans.apply(
      id,
      (value) => confirmation === topicAdministrationConfirmation(value.input),
      async (plan) => {
        if (this.active) throw new Error("Another topic change is running. Wait and review again.");
        this.active = true;
        const { input, baseline } = plan.value;
        const unsent = (detail: string): TopicAdministrationOutcome => ({
          input,
          state: "unsent",
          verification: "unavailable",
          cleanup: "confirmed",
          detail,
        });
        try {
          const fresh = parseTopicAdministrationSnapshot(await plan.context.snapshot!(input.topic));
          if (
            !this.plans.current(plan.context) ||
            !sameTopicAdministrationBaseline(baseline, fresh)
          )
            return unsent(
              "The connection, topic identity, partition count, replica assignment or permissions changed. No mutation was sent; review again.",
            );
          assertTopicAdministrationAllowed(input, fresh);
          const dispatched = plan.context.tryApply!(input, baseline);
          if (!dispatched.started)
            return unsent("Connection changed before admission. No mutation was sent.");
          try {
            return parseTopicAdministrationOutcome(await dispatched.result);
          } catch {
            return {
              input,
              state: "unknown",
              verification: "unavailable",
              cleanup: "unresolved",
              detail:
                "The admitted operation returned no usable result. Inspect the topic and reconnect before another attempt; no retry was made.",
            };
          }
        } finally {
          this.active = false;
        }
      },
    );
  }
}
