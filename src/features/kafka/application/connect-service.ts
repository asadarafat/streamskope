import type {
  ConnectInput,
  ConnectInventory,
  ConnectDetail,
  ConnectValidation,
  ConnectReview,
  ConnectOutcome,
} from "../contracts/connect";
import { connectConfig, parseConnectInput } from "../contracts/connect";

import type { KafkaClusterServiceContext } from "./types";
import { ConnectionPlans } from "./connection-plans";
import type { ConnectReviewScope } from "./connection-scope";
import { ConnectWriteAdmission } from "./connect-write-admission";

export interface ConnectState {
  readonly detail: ConnectDetail;
  readonly config: Readonly<Record<string, string>>;
}
export interface ConnectRelationships {
  readonly type: "source" | "sink" | "unknown";
  readonly reportedTopics: readonly string[] | null;
  readonly configuredTopics: readonly string[];
  readonly regexSubscription: boolean;
}
export interface ConnectPort {
  clusterId?(context: KafkaClusterServiceContext, signal: AbortSignal): Promise<string | null>;
  relationships?(
    context: KafkaClusterServiceContext,
    name: string,
    signal: AbortSignal,
  ): Promise<ConnectRelationships>;
  list(context: KafkaClusterServiceContext, signal: AbortSignal): Promise<ConnectInventory>;
  load(
    context: KafkaClusterServiceContext,
    name: string,
    signal: AbortSignal,
  ): Promise<ConnectState | null>;
  validate(
    context: KafkaClusterServiceContext,
    config: Readonly<Record<string, string>>,
    signal: AbortSignal,
  ): Promise<ConnectValidation>;
  apply(
    context: KafkaClusterServiceContext,
    input: ConnectInput,
    signal: AbortSignal,
  ): Promise<ConnectMutationReceipt>;
}
export interface ConnectMutationReceipt {
  readonly state: ConnectOutcome["state"];
  readonly dispatch: "not-sent" | "attempted";
  readonly cleanup: "confirmed" | "unresolved";
  readonly detail: string;
}
interface PlanValue {
  readonly input: ConnectInput;
  readonly baseline: string;
  readonly confirmation: string;
}
function baseline(state: ConnectState | null): string {
  return JSON.stringify(
    state === null
      ? null
      : {
          config: connectConfig(state.config),
          state: state.detail.state,
          tasks: [...state.detail.tasks]
            .sort((a, b) => a.id - b.id)
            .map(({ id, state }) => ({ id, state })),
        },
  );
}
export class ConnectService {
  private readonly plans: ConnectionPlans<PlanValue, ConnectOutcome, ConnectReviewScope>;
  constructor(
    private readonly context: () => ConnectReviewScope | null,
    private readonly port: ConnectPort,
    private readonly now = Date.now,
    private readonly admission = new ConnectWriteAdmission(),
  ) {
    this.plans = new ConnectionPlans(context, (scope) => scope.isCurrent(), now);
  }
  private active(): ConnectReviewScope {
    const scope = this.context();
    if (!scope || !scope.isCurrent())
      throw new Error("Configure Kafka Connect in the connected profile.");
    return scope;
  }
  async list(): Promise<ConnectInventory> {
    const owner = this.active();
    const result = await owner.read(
      (service, signal) => this.port.list(service, signal),
      AbortSignal.timeout(15000),
    );
    if (!this.plans.current(owner)) throw new Error("Connection changed.");
    return result;
  }
  async load(name: string): Promise<ConnectDetail> {
    const owner = this.active();
    const result = await owner.read(
      (service, signal) => this.port.load(service, name, signal),
      AbortSignal.timeout(15000),
    );
    if (!this.plans.current(owner) || !result)
      throw new Error("Connector missing or connection changed.");
    return result.detail;
  }
  private async prepare(input: ConnectInput): Promise<{
    owner: ConnectReviewScope;
    input: ConnectInput;
    before: ConnectState | null;
    validation: ConnectValidation;
  }> {
    input = parseConnectInput(input);
    const owner = this.active();
    const signal = AbortSignal.timeout(20000);
    const before = await owner.read(
      (service, combined) => this.port.load(service, input.name, combined),
      signal,
    );
    if (input.action === "create" && before) throw new Error("Connector already exists.");
    if (input.action !== "create" && !before) throw new Error("Connector no longer exists.");
    if (input.remove?.some((key) => !before || !Object.hasOwn(before.config, key)))
      throw new Error("A removal field is no longer configured. Refresh and review again.");
    const config: Record<string, string> =
      input.action === "update"
        ? { ...before?.config, ...input.config, name: input.name }
        : { ...input.config, name: input.name };
    for (const key of input.remove ?? []) delete config[key];
    const merged = { ...input, config: connectConfig(config) };
    const validation =
      input.action === "create" || input.action === "update"
        ? await owner.read(
            (service, combined) => this.port.validate(service, config, combined),
            signal,
          )
        : { issues: [] };
    if (!this.plans.current(owner)) throw new Error("Connection changed.");
    return { owner, input: merged, before, validation };
  }
  async validate(input: ConnectInput): Promise<ConnectValidation> {
    return (await this.prepare(input)).validation;
  }
  async review(input: ConnectInput): Promise<ConnectReview> {
    input = parseConnectInput(input);
    if (input.action === "update" && !Object.keys(input.config).length && !input.remove?.length)
      throw new Error("Choose configuration fields to set or remove.");
    const prepared = await this.prepare(input);
    if (prepared.validation.issues.length)
      throw new Error("Resolve validation errors before reviewing.");
    const confirmation = `${input.action} ${input.name}`;
    const plan = this.plans.add(prepared.owner, {
      input: prepared.input,
      baseline: baseline(prepared.before),
      confirmation,
    });
    return {
      planId: plan.id,
      expiresAt: plan.expiresAt,
      name: input.name,
      action: input.action,
      fields: Object.keys(input.config).sort(),
      removedFields: input.remove ?? [],
      connectionName: prepared.owner.connectionName,
      confirmation,
      before: prepared.before?.detail ?? null,
    };
  }
  apply(planId: string, confirmation: string): Promise<ConnectOutcome> {
    return this.plans.apply(
      planId,
      (value) => value.confirmation === confirmation,
      async (plan) => {
        const rejected = (
          detail: string,
          observed: ConnectDetail | null = null,
        ): ConnectOutcome => ({
          state: "rejected",
          dispatch: "not-sent",
          verification: "not-applicable",
          cleanup: plan.context.cleanupUnresolved() ? "unresolved" : "confirmed",
          detail,
          observed,
        });
        const release = this.admission.acquire();
        if (!release)
          return rejected("Another Connect action is in progress. Review again afterwards.");
        try {
          const signal = AbortSignal.timeout(
            Math.max(1, Math.min(20000, Date.parse(plan.expiresAt) - this.now())),
          );
          let fresh: ConnectState | null;
          try {
            fresh = await plan.context.read(
              (service, combined) => this.port.load(service, plan.value.input.name, combined),
              signal,
            );
            if (
              !this.plans.current(plan.context) ||
              this.now() >= Date.parse(plan.expiresAt) ||
              baseline(fresh) !== plan.value.baseline
            )
              return rejected(
                "Connector changed or review expired. Refresh and review again.",
                fresh?.detail ?? null,
              );
            signal.throwIfAborted();
          } catch {
            return rejected("Connect could not be checked; no action was sent.");
          }
          const dispatch = plan.context.tryDispatch((service) =>
            this.port.apply(service, plan.value.input, signal),
          );
          if (!dispatch.started)
            return rejected("Connection changed before dispatch. No action was sent.");
          let receipt: ConnectMutationReceipt;
          try {
            receipt = await dispatch.result;
          } catch {
            return {
              state: "unknown",
              dispatch: "attempted",
              verification: "not-applicable",
              cleanup: "unresolved",
              detail:
                "The original adapter did not return a receipt. Inspect Connect before another action; cleanup has not been confirmed.",
              observed: null,
            };
          }
          let observed: ConnectState | null = null;
          let verification: ConnectOutcome["verification"] =
            receipt.state === "acknowledged" ? "unavailable" : "not-applicable";
          if (
            receipt.state === "acknowledged" &&
            receipt.cleanup === "confirmed" &&
            plan.context.isCurrent()
          ) {
            try {
              observed = await plan.context.read(
                (service, combined) => this.port.load(service, plan.value.input.name, combined),
                signal,
              );
              verification = connectActionObserved(plan.value.input, observed)
                ? "verified"
                : "different";
            } catch {
              /* The actual acknowledgement survives lost or revoked readback. */
            }
          }
          return {
            ...receipt,
            cleanup: plan.context.cleanupUnresolved() ? "unresolved" : receipt.cleanup,
            verification,
            observed: observed?.detail ?? null,
          };
        } finally {
          release();
        }
      },
    );
  }
}
function connectActionObserved(input: ConnectInput, observed: ConnectState | null): boolean {
  if (input.action === "delete") return observed === null;
  if (!observed) return false;
  if (input.action === "create" || input.action === "update")
    return (
      JSON.stringify(connectConfig(observed.config)) === JSON.stringify(connectConfig(input.config))
    );
  const expected =
    input.action === "pause" ? "PAUSED" : input.action === "stop" ? "STOPPED" : "RUNNING";
  return (
    observed.detail.state === expected &&
    observed.detail.tasks.every((task) => task.state === expected)
  );
}
