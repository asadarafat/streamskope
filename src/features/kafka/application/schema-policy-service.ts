import {
  parseSchemaPolicyInput,
  schemaPolicyAfter,
  type SchemaPolicyInput,
  type SchemaPolicyBaseline,
  type SchemaPolicyReview,
  type SchemaPolicyOutcome,
} from "../contracts/schema-policy";

import type { SchemaRegistryReviewScope } from "./connection-scope";
import { ConnectionPlans } from "./connection-plans";
import { SchemaChangeReviewError } from "./schema-change-errors";
import type { SchemaReviewOperations } from "./schema-review-operations";
import type { ReviewedSchemaPolicyPort } from "./schema-registry-types";

export class SchemaPolicyService {
  private readonly plans: ConnectionPlans<
    { input: SchemaPolicyInput; before: SchemaPolicyBaseline },
    SchemaPolicyOutcome,
    SchemaRegistryReviewScope
  >;
  constructor(
    private readonly port: ReviewedSchemaPolicyPort,
    private readonly operations: SchemaReviewOperations,
    now = Date.now,
  ) {
    this.plans = new ConnectionPlans(
      () => operations.context(),
      (scope) => scope.isCurrent(),
      now,
    );
  }
  private baseline(
    scope: SchemaRegistryReviewScope,
    subject: string,
    signal: AbortSignal,
  ): Promise<SchemaPolicyBaseline> {
    return scope.read(async (context, combined) => {
      const schema = await this.port.loadReviewSchema(
        context,
        { subject, version: "latest" },
        combined,
      );
      if (!schema)
        throw new SchemaChangeReviewError(
          "Select an existing subject's latest writer before changing policy.",
        );
      const policy = await this.port.loadCompatibilityPolicy(context, subject, combined);
      return { writer: { id: schema.id, version: schema.version }, policy };
    }, signal);
  }
  load(subject: string): Promise<SchemaPolicyBaseline> {
    const scope = this.plans.context();
    if (!scope) return Promise.reject(new Error("Connect before reading Registry policy."));
    return this.operations.read((signal) => this.baseline(scope, subject, signal));
  }
  review(value: SchemaPolicyInput): Promise<SchemaPolicyReview> {
    const input = parseSchemaPolicyInput(value),
      scope = this.plans.context();
    if (!scope) return Promise.reject(new Error("Connect before reviewing Registry policy."));
    return this.operations.read(async (signal) => {
      const before = await this.baseline(scope, input.subject, signal);
      if (JSON.stringify(before.writer) !== JSON.stringify(input.expectedWriter))
        throw new SchemaChangeReviewError(
          "The latest writer changed. Refresh the subject before reviewing policy.",
        );
      signal.throwIfAborted();
      const plan = this.plans.add(scope, { input, before });
      return {
        planId: plan.id,
        expiresAt: plan.expiresAt,
        connectionName: scope.connectionName,
        input,
        before,
        after: schemaPolicyAfter(before.policy, input.change),
      };
    });
  }
  apply(planId: string, confirmation: string): Promise<SchemaPolicyOutcome> {
    return this.plans.apply(
      planId,
      ({ input }) => confirmation === input.subject,
      (plan) =>
        this.operations.write(async (signal) => {
          const { input, before } = plan.value,
            after = schemaPolicyAfter(before.policy, input.change);
          const rejected = (detail: string): SchemaPolicyOutcome => ({
            state: "rejected",
            verification: "not-applicable",
            acknowledgedLevel: null,
            observed: null,
            detail,
          });
          try {
            const fresh = await this.baseline(plan.context, input.subject, signal);
            if (JSON.stringify(fresh) !== JSON.stringify(before))
              return rejected(
                "Writer or compatibility policy changed. No policy write was sent; review again.",
              );
            signal.throwIfAborted();
            if (!this.plans.current(plan.context))
              return rejected("The connection changed. No policy write was sent.");
            if (JSON.stringify(fresh.policy) === JSON.stringify(after))
              return {
                state: "unchanged",
                verification: "verified",
                acknowledgedLevel: null,
                observed: fresh.policy,
                detail:
                  "The exact requested override or inheritance is already in effect. No policy write was sent.",
              };
          } catch {
            return rejected(
              "Registry state could not be rechecked. No policy write was sent; review again.",
            );
          }
          let acknowledgedLevel: SchemaPolicyOutcome["acknowledgedLevel"];
          try {
            const dispatch = plan.context.tryDispatch((context) =>
              this.port.changeSubjectCompatibility(context, input.subject, input.change, signal),
            );
            if (!dispatch.started)
              return rejected("The connection changed before dispatch. No policy write was sent.");
            acknowledgedLevel = (await dispatch.result).level;
          } catch (error) {
            const status =
              error !== null && typeof error === "object" && "status" in error
                ? error.status
                : null;
            const denied = status === 401 || status === 403,
              refused =
                denied || status === 400 || status === 404 || status === 409 || status === 422;
            return {
              state: refused ? "rejected" : "unknown",
              verification: "unavailable",
              acknowledgedLevel: null,
              observed: null,
              detail: denied
                ? "Registry authorization denied the policy change. Check service credentials and subject configuration permission; no automatic retry was made."
                : refused
                  ? "Registry rejected the policy change. Refresh the subject and review its supported configuration; no automatic retry was made."
                  : "Policy change was not acknowledged and may have reached the Registry. Read its policy before another attempt; no automatic retry was made.",
            };
          }
          try {
            const observed = await plan.context.read(
              (context, combined) =>
                this.port.loadCompatibilityPolicy(context, input.subject, combined),
              signal,
            );
            if (!this.plans.current(plan.context))
              throw new Error("Connection changed during readback.");
            const acknowledgementMatches =
              input.change.mode === "inherit" || acknowledgedLevel === input.change.level;
            const matches =
              acknowledgementMatches && JSON.stringify(observed) === JSON.stringify(after);
            return {
              state: "acknowledged",
              verification: matches ? "verified" : "mismatch",
              acknowledgedLevel,
              observed,
              detail: matches
                ? "Registry acknowledged the policy change and the requested override or inheritance was read back. External changes remain possible."
                : acknowledgementMatches
                  ? "Policy change was acknowledged, but readback differs from the reviewed policy. Read current policy; do not repeat the write to refresh it."
                  : "Registry acknowledged a different level than requested. Read current policy and inspect the receipt; no automatic retry was made.",
            };
          } catch {
            return {
              state: "acknowledged",
              verification: "unavailable",
              acknowledgedLevel,
              observed: null,
              detail:
                "Policy change was acknowledged, but readback is unavailable. Read current policy; do not repeat the write to refresh it.",
            };
          }
        }),
    );
  }
}
