import {
  ENVIRONMENT_CONFIG_KEYS,
  configValue,
  parseEnvironmentSnapshot,
  parseEnvironmentInput,
  environmentDiff,
  environmentIdentity,
  type EnvironmentSnapshot,
  type EnvironmentInput,
  type EnvironmentProfile,
  type EnvironmentReview,
  type EnvironmentOutcome,
} from "../contracts/environment-snapshot";

import type { KafkaActiveConnection } from "./types";
import { ConnectionPlans, type ReviewContext } from "./connection-plans";
import type { ReplayDestination, ReplayDestinationPort } from "./replay-destination";

export async function captureEnvironment(
  connection: KafkaActiveConnection,
  topics: readonly string[],
  signal: AbortSignal,
): Promise<EnvironmentSnapshot> {
  if (!topics.length || topics.length > 20 || new Set(topics).size !== topics.length)
    throw new Error("Select 1–20 distinct topics.");
  const metadata = await connection.describeClusterMetadata(signal);
  if (!metadata.clusterId || !connection.describeTopicIdentity)
    throw new Error("Stable cluster/topic identities are required.");
  const captured = [];
  for (const name of [...topics].sort()) {
    signal.throwIfAborted();
    const identity = await connection.describeTopicIdentity(name);
    if (
      identity.clusterId !== metadata.clusterId ||
      !identity.topicId ||
      /^0+$/u.test(identity.topicId.replaceAll("-", ""))
    )
      throw new Error("Topic identity unavailable.");
    const entries = await connection.describeTopicConfiguration(name, signal);
    captured.push({
      name,
      topicId: identity.topicId,
      configs: ENVIRONMENT_CONFIG_KEYS.map((key) => {
        const e = entries.find((c) => c.name === key);
        let value: string | null = null;
        try {
          if (e && !e.isSensitive && e.type !== "password" && e.value !== null)
            value = configValue(key, e.value);
        } catch {
          /* Unsupported values are omitted, not exported. */
        }
        return { key, value, mutable: !!e && !e.readOnly && !e.isSensitive && value !== null };
      }),
    });
  }
  signal.throwIfAborted();
  return parseEnvironmentSnapshot({
    format: "streamskope.topic-config/v1",
    clusterId: metadata.clusterId,
    observedAt: new Date().toISOString(),
    topics: captured,
  });
}
interface Plan {
  readonly input: EnvironmentInput;
  readonly review: EnvironmentReview;
}
export class EnvironmentService {
  private readonly plans: ConnectionPlans<Plan, EnvironmentOutcome>;
  private applying = false;
  constructor(
    private readonly context: () => ReviewContext | null,
    private readonly destinations?: ReplayDestinationPort,
    private readonly now = Date.now,
  ) {
    this.plans = new ConnectionPlans(context, now);
  }
  private async target(
    profile: EnvironmentProfile | null,
    owner: ReviewContext,
    signal: AbortSignal,
  ): Promise<ReplayDestination> {
    if (profile) {
      if (!this.destinations) throw new Error("Saved destinations unavailable.");
      return this.destinations.open(profile.id, profile.revision, signal);
    }
    return {
      connection: owner.connection,
      name: owner.connectionName,
      current: () => this.plans.current(owner),
      close: () => Promise.resolve(),
    };
  }
  async capture(
    topics: readonly string[],
    profile: EnvironmentProfile | null,
  ): Promise<EnvironmentSnapshot> {
    const owner = this.context();
    if (!owner) throw new Error("Connect first.");
    const signal = AbortSignal.timeout(30000);
    const target = await this.target(profile, owner, signal);
    try {
      const snapshot = await captureEnvironment(target.connection, topics, signal);
      if (!target.current() || !this.plans.current(owner)) throw new Error("Connection changed.");
      return snapshot;
    } finally {
      await target.close();
    }
  }
  async review(value: EnvironmentInput): Promise<EnvironmentReview> {
    const input = parseEnvironmentInput(value);
    const owner = this.context();
    if (!owner) throw new Error("Connect first.");
    const fresh = await this.capture(
      input.target.topics.map((t) => t.name),
      input.targetProfile,
    );
    if (environmentIdentity(fresh) !== environmentIdentity(input.target))
      throw new Error("Target changed. Capture it again.");
    const diff = environmentDiff(input.source, fresh);
    const changes = input.selected.map((s) => {
      const d = diff.find((d) => d.topic === s.topic && d.key === s.key);
      if (!d?.supported) throw new Error("Unsupported difference selected.");
      return d;
    });
    const review: EnvironmentReview = {
      planId: "pending",
      expiresAt: new Date(this.now() + 120000).toISOString(),
      confirmation: `promote ${changes.length} settings to ${fresh.clusterId}`,
      source: input.source,
      target: fresh,
      changes,
    };
    const plan = this.plans.add(owner, { input, review });
    return { ...review, planId: plan.id, expiresAt: plan.expiresAt };
  }
  apply(id: string, confirmation: string): Promise<EnvironmentOutcome> {
    return this.plans.apply(
      id,
      (v) => v.review.confirmation === confirmation,
      async (plan) => {
        if (this.applying) throw new Error("Another promotion is in progress.");
        this.applying = true;
        const topics = [...new Set(plan.value.review.changes.map((c) => c.topic))];
        const results: EnvironmentOutcome["results"][number][] = [];
        let target: ReplayDestination | undefined;
        const signal = AbortSignal.timeout(60000);
        let dispatching = false;
        let cleanupConfirmed = true;
        try {
          target = await this.target(plan.value.input.targetProfile, plan.context, signal);
          const fresh = await captureEnvironment(
            target.connection,
            plan.value.input.target.topics.map((t) => t.name),
            signal,
          );
          if (environmentIdentity(fresh) !== environmentIdentity(plan.value.input.target))
            throw new Error("Stale target.");
          for (const topic of topics) {
            if (
              !target.current() ||
              !this.plans.current(plan.context) ||
              this.now() >= Date.parse(plan.expiresAt)
            )
              throw new Error("Connection changed or review expired.");
            const before = await captureEnvironment(target.connection, [topic], signal);
            const expected = { ...fresh, topics: fresh.topics.filter((t) => t.name === topic) };
            if (environmentIdentity(before) !== environmentIdentity(expected))
              throw new Error("Topic changed.");
            const changes = plan.value.review.changes
              .filter((c) => c.topic === topic)
              .map((c) => ({ name: c.key, value: c.source!, isSensitive: false }));
            await target.connection.alterTopicConfiguration(topic, changes, true, signal);
            signal.throwIfAborted();
            if (
              !target.current() ||
              !this.plans.current(plan.context) ||
              this.now() >= Date.parse(plan.expiresAt)
            )
              throw new Error("Review changed.");
            const finalSnapshot = await captureEnvironment(target.connection, [topic], signal);
            if (environmentIdentity(finalSnapshot) !== environmentIdentity(expected))
              throw new Error("Topic changed during validation.");
            signal.throwIfAborted();
            if (
              !target.current() ||
              !this.plans.current(plan.context) ||
              this.now() >= Date.parse(plan.expiresAt)
            )
              throw new Error("Review changed.");
            dispatching = true;
            await target.connection.alterTopicConfiguration(topic, changes, false, signal);
            dispatching = false;
            let verified = false;
            try {
              const observed = await captureEnvironment(target.connection, [topic], signal);
              verified =
                observed.topics[0]?.topicId === before.topics[0]?.topicId &&
                changes.every((c) =>
                  observed.topics[0]?.configs.some((v) => v.key === c.name && v.value === c.value),
                );
            } catch {
              /* Keep acknowledgement. */
            }
            results.push({ topic, state: "acknowledged", verified });
            if (!verified) break;
          }
        } catch (error) {
          const denied =
            typeof error === "object" &&
            error !== null &&
            "code" in error &&
            error.code === "AUTHORIZATION_DENIED";
          const topic = topics[results.length];
          if (topic)
            results.push({
              topic,
              state: dispatching && !denied ? "unknown" : "rejected",
              verified: false,
            });
        } finally {
          this.applying = false;
          try {
            await target?.close();
          } catch {
            cleanupConfirmed = false;
          }
        }
        for (const topic of topics.slice(results.length))
          results.push({ topic, state: "unsent", verified: false });
        return {
          results,
          detail:
            (cleanupConfirmed ? "" : "Destination cleanup could not be confirmed. ") +
            "Only selected existing topic settings were attempted. Unverified or unknown results stop subsequent topics. Inspect the destination before a new review; no automatic retry or reconciliation runs.",
        };
      },
    );
  }
}
