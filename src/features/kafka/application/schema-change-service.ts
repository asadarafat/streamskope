import {
  parseSchemaChangeInput,
  type SchemaChangeInput,
  type SchemaChangeReview,
  type SchemaChangeOutcome,
} from "../contracts/schema-changes";

import type { SchemaRegistryReviewScope } from "./connection-scope";
import { ConnectionPlans } from "./connection-plans";
import { SchemaChangeReviewError } from "./schema-change-errors";
import {
  boundedSchemaSnapshot,
  readSchemaChangeBaseline,
  type ReviewedSchemaRegistryPort,
  type SchemaChangeBaseline,
} from "./schema-change-baseline";

function compatibilityVersions(baseline: SchemaChangeBaseline, subject: string): readonly number[] {
  return baseline.before === null
    ? []
    : baseline.policy.effectiveLevel.endsWith("_TRANSITIVE")
      ? baseline.dependencies
          .filter((schema) => schema.subject === subject)
          .map((schema) => schema.version)
      : [baseline.before.version];
}

export class SchemaChangeService {
  private readonly plans: ConnectionPlans<
    { input: SchemaChangeInput; baseline: SchemaChangeBaseline; compatible: boolean },
    SchemaChangeOutcome,
    SchemaRegistryReviewScope
  >;
  private active = false;
  private reads = 0;
  private epoch = 0;
  private readonly pending = new Set<AbortController>();
  constructor(
    context: () => SchemaRegistryReviewScope | null,
    private readonly port: ReviewedSchemaRegistryPort,
    now = Date.now,
  ) {
    this.plans = new ConnectionPlans(
      () => {
        const scope = context(),
          epoch = this.epoch;
        return scope
          ? { ...scope, isCurrent: (): boolean => epoch === this.epoch && scope.isCurrent() }
          : null;
      },
      (scope) => scope.isCurrent(),
      now,
    );
  }
  invalidate(): void {
    this.epoch++;
    for (const controller of this.pending) controller.abort();
  }
  private async withDeadline<T>(run: (signal: AbortSignal) => Promise<T>): Promise<T> {
    const controller = new AbortController();
    this.pending.add(controller);
    try {
      return await run(AbortSignal.any([controller.signal, AbortSignal.timeout(15000)]));
    } finally {
      this.pending.delete(controller);
    }
  }
  async review(value: SchemaChangeInput): Promise<SchemaChangeReview> {
    const input = parseSchemaChangeInput(value),
      scope = this.plans.context();
    if (!scope || this.reads >= 2)
      throw new Error("Connect and wait for the current Registry review.");
    this.reads++;
    try {
      return await this.withDeadline(async (signal) => {
        const baseline = await readSchemaChangeBaseline(scope, this.port, input, signal);
        const observed =
          baseline.before === null
            ? null
            : { id: baseline.before.id, version: baseline.before.version };
        if (JSON.stringify(observed) !== JSON.stringify(input.expectedWriter))
          throw new SchemaChangeReviewError(
            "Select the latest writer before reviewing evolution; a new subject must be absent.",
          );
        const compatible =
          baseline.before === null
            ? true
            : (
                await scope.read(
                  (context, combined) =>
                    this.port.checkProposedCompatibility(
                      context,
                      input.draft,
                      compatibilityVersions(baseline, input.draft.subject),
                      combined,
                    ),
                  signal,
                )
              ).compatible;
        signal.throwIfAborted();
        const plan = this.plans.add(scope, { input, baseline, compatible });
        return {
          planId: plan.id,
          expiresAt: plan.expiresAt,
          connectionName: scope.connectionName,
          input,
          before: baseline.before,
          policy: baseline.policy,
          compatible,
        };
      });
    } finally {
      this.reads--;
    }
  }
  apply(planId: string, confirmation: string): Promise<SchemaChangeOutcome> {
    return this.plans.apply(
      planId,
      ({ input, compatible }) => confirmation === input.draft.subject && compatible,
      async (plan) => {
        if (this.active) throw new Error("Another schema change is active. Wait and review again.");
        this.active = true;
        const rejected = (detail: string): SchemaChangeOutcome => ({
          state: "rejected",
          verification: "not-applicable",
          id: null,
          observed: null,
          detail,
        });
        try {
          return await this.withDeadline(async (signal) => {
            const { input, baseline } = plan.value;
            try {
              const fresh = await readSchemaChangeBaseline(plan.context, this.port, input, signal);
              if (JSON.stringify(fresh) !== JSON.stringify(baseline))
                return rejected(
                  "Writer, history, references or compatibility policy changed. No registration was sent; review again.",
                );
              if (
                fresh.before !== null &&
                !(
                  await plan.context.read(
                    (context, combined) =>
                      this.port.checkProposedCompatibility(
                        context,
                        input.draft,
                        compatibilityVersions(baseline, input.draft.subject),
                        combined,
                      ),
                    signal,
                  )
                ).compatible
              )
                return rejected(
                  "The Registry rejected compatibility on recheck. No registration was sent.",
                );
              if (
                JSON.stringify(
                  await readSchemaChangeBaseline(plan.context, this.port, input, signal),
                ) !== JSON.stringify(baseline)
              )
                return rejected(
                  "Registry state changed during compatibility checks. No registration was sent; review again.",
                );
              signal.throwIfAborted();
            } catch {
              return rejected(
                "Registry state could not be rechecked. No registration was sent; review again.",
              );
            }
            if (!this.plans.current(plan.context))
              return rejected("The connection changed before dispatch. No registration was sent.");
            let id: number;
            try {
              const dispatch = plan.context.tryDispatch((context) =>
                this.port.register(context, input.draft, signal),
              );
              if (!dispatch.started)
                return rejected(
                  "The connection changed before dispatch. No registration was sent.",
                );
              id = (await dispatch.result).id;
            } catch (error) {
              const status =
                error !== null && typeof error === "object" && "status" in error
                  ? error.status
                  : null;
              const denied = status === 401 || status === 403;
              const rejectedByRegistry =
                denied || status === 400 || status === 409 || status === 422;
              return {
                state: rejectedByRegistry ? "rejected" : "unknown",
                verification: "unavailable",
                id: null,
                observed: null,
                detail: denied
                  ? "Registry authorization denied registration. Check service credentials and write permission; no automatic retry was made."
                  : rejectedByRegistry
                    ? "Registry rejected the schema, references or compatibility. Revise the draft and review again; no automatic retry was made."
                    : "Registration was not acknowledged and may have reached the Registry. Inspect subject versions before another attempt; no automatic retry was made.",
              };
            }
            try {
              const observed = await plan.context.read(
                (context, combined) =>
                  this.port.loadReviewSchema(
                    context,
                    { subject: input.draft.subject, version: "latest" },
                    combined,
                  ),
                signal,
              );
              boundedSchemaSnapshot(observed, 2048);
              if (!this.plans.current(plan.context))
                throw new Error("Connection changed during readback.");
              return {
                state: "acknowledged",
                verification: observed?.id === id ? "verified" : "mismatch",
                id,
                observed,
                detail:
                  observed?.id === id
                    ? "Registry acknowledged registration and its writer ID was read back. Concurrent external changes remain possible."
                    : "Registration was acknowledged, but latest does not match its writer ID. Inspect subject versions; do not repeat the write to refresh it.",
              };
            } catch {
              return {
                state: "acknowledged",
                verification: "unavailable",
                id,
                observed: null,
                detail:
                  "Registration was acknowledged, but readback is unavailable. Refresh the subject; do not repeat the write to refresh it.",
              };
            }
          });
        } finally {
          this.active = false;
        }
      },
    );
  }
}
