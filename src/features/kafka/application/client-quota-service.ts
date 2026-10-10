import {
  parseClientQuotaEntity,
  parseClientQuotaInput,
  parseClientQuotaSnapshot,
  parseClientQuotaOutcome,
  sameClientQuotaEntity,
  sameClientQuotaBaseline,
  clientQuotaExpected,
  clientQuotaConfirmation,
  type ClientQuotaEntity,
  type ClientQuotaInput,
  type ClientQuotaSnapshot,
  type ClientQuotaReview,
  type ClientQuotaOutcome,
} from "../contracts/client-quotas";

import { ConnectionPlans } from "./connection-plans";
import type { ClientQuotaScope } from "./connection-scope";

export class ClientQuotaService {
  private readonly plans: ConnectionPlans<
    { input: ClientQuotaInput; baseline: ClientQuotaSnapshot },
    ClientQuotaOutcome,
    ClientQuotaScope
  >;
  private active = false;
  constructor(context: () => ClientQuotaScope | null, now = Date.now) {
    this.plans = new ConnectionPlans(context, (scope) => scope.isCurrent(), now);
  }
  async inspect(entity: ClientQuotaEntity): Promise<ClientQuotaSnapshot> {
    const parsed = parseClientQuotaEntity(entity),
      scope = this.plans.context();
    if (!scope?.snapshot) throw new Error("Quota inspection is unavailable.");
    const result = parseClientQuotaSnapshot(await scope.snapshot(parsed));
    if (!scope.isCurrent() || !sameClientQuotaEntity(parsed, result.entity))
      throw new Error("Quota entity or connection changed.");
    return result;
  }
  async review(input: ClientQuotaInput): Promise<ClientQuotaReview> {
    const parsed = parseClientQuotaInput(input),
      scope = this.plans.context();
    if (!scope?.snapshot || !scope.tryApply) throw new Error("Quota changes are unavailable.");
    const baseline = parseClientQuotaSnapshot(await scope.snapshot(parsed.entity));
    if (!baseline.alterSupported || !sameClientQuotaEntity(parsed.entity, baseline.entity))
      throw new Error("Exact quota changes are unavailable.");
    const expected = clientQuotaExpected(baseline.values, parsed.changes);
    if (JSON.stringify(expected) === JSON.stringify(baseline.values))
      throw new Error("The selected changes already match these explicit quotas.");
    const plan = this.plans.add(scope, { input: parsed, baseline });
    return {
      planId: plan.id,
      expiresAt: plan.expiresAt,
      connectionName: scope.connectionName,
      input: parsed,
      baseline,
      expected,
      confirmation: clientQuotaConfirmation(parsed.entity),
    };
  }
  apply(id: string, confirmation: string): Promise<ClientQuotaOutcome> {
    return this.plans.apply(
      id,
      (v) => confirmation === clientQuotaConfirmation(v.input.entity),
      async (plan) => {
        if (this.active)
          throw new Error("Another quota change is running. Review again afterwards.");
        this.active = true;
        const { input, baseline } = plan.value;
        const unsent = (detail: string): ClientQuotaOutcome => ({
          input,
          state: "unsent",
          verification: "unavailable",
          observed: null,
          cleanup: "confirmed",
          detail,
        });
        try {
          const fresh = parseClientQuotaSnapshot(await plan.context.snapshot!(input.entity));
          if (
            !this.plans.current(plan.context) ||
            !sameClientQuotaBaseline(baseline, fresh) ||
            !fresh.alterSupported
          )
            return unsent(
              "Cluster, exact quotas, API capability or connection changed. No mutation was sent; inspect and review again.",
            );
          const dispatched = plan.context.tryApply!(input, fresh);
          if (!dispatched.started)
            return unsent("The connection changed before admission. No mutation was sent.");
          try {
            const outcome = parseClientQuotaOutcome(await dispatched.result);
            if (JSON.stringify(outcome.input) !== JSON.stringify(input))
              throw new Error("The mutation receipt identifies another quota request.");
            return outcome;
          } catch {
            return {
              input,
              state: "unknown",
              verification: "unavailable",
              observed: null,
              cleanup: "unresolved",
              detail:
                "The admitted quota change returned no usable receipt. Inspect the exact entity and original cleanup before another attempt; no retry was made.",
            };
          }
        } finally {
          this.active = false;
        }
      },
    );
  }
}
