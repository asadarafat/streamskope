import {
  parseGroupAdministrationInput,
  parseGroupAdministrationSnapshot,
  parseGroupAdministrationOutcome,
  groupDeleteConfirmation,
  groupDeletionAllowed,
  sameGroupBaseline,
  type GroupAdministrationReview,
  type GroupAdministrationSnapshot,
  type GroupAdministrationOutcome,
} from "../contracts/group-administration";

import { ConnectionPlans } from "./connection-plans";
import type { GroupAdministrationScope } from "./connection-scope";

export class GroupAdministrationService {
  private readonly plans: ConnectionPlans<
    GroupAdministrationSnapshot,
    GroupAdministrationOutcome,
    GroupAdministrationScope
  >;
  private active = false;
  constructor(context: () => GroupAdministrationScope | null, now = Date.now) {
    this.plans = new ConnectionPlans(context, (scope) => scope.isCurrent(), now);
  }
  async review(input: { readonly groupId: string }): Promise<GroupAdministrationReview> {
    const { groupId } = parseGroupAdministrationInput(input),
      scope = this.plans.context();
    if (!scope?.snapshot || !scope.tryDelete) throw new Error("Group deletion is unavailable.");
    const baseline = parseGroupAdministrationSnapshot(await scope.snapshot(groupId));
    if (baseline.groupId !== groupId || !groupDeletionAllowed(baseline))
      throw new Error("Select a supported inactive group with delete permission.");
    const plan = this.plans.add(scope, baseline);
    return {
      planId: plan.id,
      expiresAt: plan.expiresAt,
      connectionName: scope.connectionName,
      baseline,
      confirmation: groupDeleteConfirmation(groupId),
    };
  }
  apply(id: string, confirmation: string): Promise<GroupAdministrationOutcome> {
    return this.plans.apply(
      id,
      (b) => confirmation === groupDeleteConfirmation(b.groupId),
      async (plan) => {
        if (this.active)
          throw new Error("Another group deletion is running. Wait and review again.");
        this.active = true;
        const unsent = (detail: string): GroupAdministrationOutcome => ({
          groupId: plan.value.groupId,
          state: "unsent",
          verification: "unavailable",
          cleanup: "confirmed",
          detail,
        });
        try {
          const fresh = parseGroupAdministrationSnapshot(
            await plan.context.snapshot!(plan.value.groupId),
          );
          if (
            !this.plans.current(plan.context) ||
            !sameGroupBaseline(plan.value, fresh) ||
            !groupDeletionAllowed(fresh)
          )
            return unsent(
              "The group, offsets, inactivity, permissions or connection changed. No deletion was sent; review again.",
            );
          const dispatched = plan.context.tryDelete!(plan.value);
          if (!dispatched.started)
            return unsent("The connection changed before admission. No deletion was sent.");
          try {
            return parseGroupAdministrationOutcome(await dispatched.result);
          } catch {
            return {
              groupId: plan.value.groupId,
              state: "unknown",
              verification: "unavailable",
              cleanup: "unresolved",
              detail:
                "The admitted deletion returned no usable receipt. Inspect the group and reconnect before another attempt; no retry was made.",
            };
          }
        } finally {
          this.active = false;
        }
      },
    );
  }
}
