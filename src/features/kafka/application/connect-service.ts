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
import { ConnectionPlans, type ReviewContext } from "./connection-plans";

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
  ): Promise<void>;
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
  private readonly plans: ConnectionPlans<PlanValue, ConnectOutcome, ReviewContext>;
  private applying = false;
  constructor(
    private readonly context: () => ReviewContext | null,
    private readonly port: ConnectPort,
    private readonly now = Date.now,
  ) {
    this.plans = new ConnectionPlans(
      context,
      (expected) => {
        const actual = context();
        return (
          actual?.connection === expected.connection && actual.generation === expected.generation
        );
      },
      now,
    );
  }
  private active(): { owner: ReviewContext; service: KafkaClusterServiceContext } {
    const owner = this.context();
    const service = owner?.connection.clusterServiceContext?.("connect");
    if (!owner || !service) throw new Error("Configure Kafka Connect in the connected profile.");
    return { owner, service };
  }
  async list(): Promise<ConnectInventory> {
    const { owner, service } = this.active();
    const result = await this.port.list(service, AbortSignal.timeout(15000));
    if (!this.plans.current(owner)) throw new Error("Connection changed.");
    return result;
  }
  async load(name: string): Promise<ConnectDetail> {
    const { owner, service } = this.active();
    const result = await this.port.load(service, name, AbortSignal.timeout(15000));
    if (!this.plans.current(owner) || !result)
      throw new Error("Connector missing or connection changed.");
    return result.detail;
  }
  private async prepare(input: ConnectInput): Promise<{
    owner: ReviewContext;
    input: ConnectInput;
    before: ConnectState | null;
    validation: ConnectValidation;
  }> {
    input = parseConnectInput(input);
    const { owner, service } = this.active();
    const signal = AbortSignal.timeout(20000);
    const before = await this.port.load(service, input.name, signal);
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
        ? await this.port.validate(service, config, signal)
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
      (v) => v.confirmation === confirmation,
      async (plan) => {
        if (this.applying)
          return {
            state: "rejected",
            detail: "Another Connect action is in progress. Review again afterwards.",
            observed: null,
          };
        this.applying = true;
        let dispatched = false;
        try {
          const service = plan.context.connection.clusterServiceContext?.("connect");
          if (!service) throw new Error("Endpoint unavailable.");
          const signal = AbortSignal.timeout(20000);
          const fresh = await this.port.load(service, plan.value.input.name, signal);
          if (
            !this.plans.current(plan.context) ||
            this.now() >= Date.parse(plan.expiresAt) ||
            baseline(fresh) !== plan.value.baseline
          )
            return {
              state: "rejected",
              detail: "Connector changed or review expired. Refresh and review again.",
              observed: fresh?.detail ?? null,
            };
          signal.throwIfAborted();
          dispatched = true;
          await this.port.apply(service, plan.value.input, signal);
          let observed: ConnectDetail | null = null;
          try {
            observed =
              (await this.port.load(service, plan.value.input.name, signal))?.detail ?? null;
          } catch {
            /* Acknowledgement remains valid when read-back fails. */
          }
          return {
            state: "acknowledged",
            detail:
              "Connect accepted the action. State changes are asynchronous; refresh until the expected state is observed. No automatic retry was sent.",
            observed,
          };
        } catch (error) {
          const status =
            typeof error === "object" && error !== null && "status" in error
              ? error.status
              : undefined;
          const denied =
            typeof status === "number" && [400, 401, 403, 404, 409, 422].includes(status);
          return {
            state: !dispatched || denied ? "rejected" : "unknown",
            detail: denied
              ? `Connect rejected the request (HTTP ${String(status)}). Check endpoint permissions and current configuration.`
              : dispatched
                ? "The action outcome is unknown. Refresh Connect and inspect its state before a new review; do not assume it failed."
                : "Connect could not be checked; no action was sent.",
            observed: null,
          };
        } finally {
          this.applying = false;
        }
      },
    );
  }
}
