import {
  parseOffsetResetInput,
  type OffsetResetInput,
  type OffsetResetReview,
  type OffsetResetOutcome,
  type OffsetResetSnapshot,
  type OffsetResetResult,
} from "../contracts/offset-reset";

import { ConnectionPlans, type ReviewContext } from "./connection-plans";

export class OffsetResetService {
  private readonly plans: ConnectionPlans<
    { input: OffsetResetInput; baseline: OffsetResetSnapshot },
    OffsetResetOutcome
  >;
  private active = false;
  constructor(
    context: () => ReviewContext | null,
    private readonly now = Date.now,
  ) {
    this.plans = new ConnectionPlans(context, now);
  }
  async review(input: OffsetResetInput): Promise<OffsetResetReview> {
    const parsed = parseOffsetResetInput(input);
    const context = this.plans.context();
    if (!context?.connection.offsetResetSnapshot || !context.connection.resetGroupOffset)
      throw new Error("Offset resets are unavailable.");
    const baseline = await context.connection.offsetResetSnapshot(parsed);
    const samples = await context.connection
      .offsetResetExamples?.(parsed)
      .catch(() => ({ examples: [], exampleStatus: "unavailable" as const }));
    const plan = this.plans.add(context, { input: parsed, baseline });
    return {
      planId: plan.id,
      connectionName: context.connectionName,
      expiresAt: plan.expiresAt,
      input: parsed,
      baseline,
      ...(samples ?? { examples: [], exampleStatus: "unavailable" }),
    };
  }
  apply(planId: string, confirmation: string): Promise<OffsetResetOutcome> {
    return this.plans.apply(
      planId,
      (value) => confirmation === value.input.groupId,
      async (plan) => {
        if (this.active) throw new Error("Another reset is running. Wait and review again.");
        this.active = true;
        const started = this.now();
        const { input, baseline } = plan.value;
        const expected = baseline.partitions.map((p) => p.before);
        const results: OffsetResetResult[] = [];
        let detail =
          "Selected offsets were acknowledged and read back. Restart consumers only after reviewing every partition.";
        try {
          for (let index = 0; index < input.targets.length; index++) {
            const target = input.targets[index]!;
            if (!this.plans.current(plan.context) || this.now() - started >= 60_000) {
              detail =
                "Connection changed or the 60-second dispatch bound was reached; remaining partitions were not sent.";
              break;
            }
            let fresh: OffsetResetSnapshot;
            try {
              fresh = await plan.context.connection.offsetResetSnapshot!(input);
            } catch {
              detail =
                "Could not recheck group state, offsets or permissions. Remaining partitions were not sent.";
              break;
            }
            if (
              !this.plans.current(plan.context) ||
              !baseline.inactive ||
              !fresh.inactive ||
              fresh.groupRead === "denied" ||
              fresh.partitions.length !== expected.length ||
              fresh.partitions.some(
                (p, i) =>
                  p.before !== expected[i] ||
                  p.topic !== baseline.partitions[i]?.topic ||
                  p.partition !== baseline.partitions[i]?.partition ||
                  BigInt(p.offset) < BigInt(p.low) ||
                  BigInt(p.offset) > BigInt(p.high),
              )
            ) {
              detail =
                "The group is active, denied or the reviewed offsets/retention bounds changed. Remaining partitions were not sent. Stop consumers and preview again.";
              break;
            }
            let result: OffsetResetResult;
            try {
              result = await plan.context.connection.resetGroupOffset!(input.groupId, target);
            } catch {
              result = { ...target, state: "unknown", observed: null, verified: false };
            }
            results.push(result);
            expected[index] = target.offset;
            if (result.state !== "acknowledged" || !result.verified) {
              detail =
                "Stopped after an unverified or failed result. Inspect committed offsets before another attempt; no automatic retry was made.";
              break;
            }
          }
          return {
            groupId: input.groupId,
            partitions: [
              ...results,
              ...input.targets.slice(results.length).map((target): OffsetResetResult => ({
                ...target,
                state: "unsent",
                observed: null,
                verified: false,
              })),
            ],
            detail,
          };
        } finally {
          this.active = false;
        }
      },
    );
  }
}
