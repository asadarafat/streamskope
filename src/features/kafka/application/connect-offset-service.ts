import { connectName, connectConfig } from "../contracts/connect";
import {
  parseConnectOffsetsInput,
  parseConnectOffsetsSnapshot,
  type ConnectOffsetsInput,
  type ConnectOffsetsSnapshot,
  type ConnectOffsetsReview,
  type ConnectOffsetsOutcome,
} from "../contracts/connect-offsets";

import type { ConnectReviewScope } from "./connection-scope";
import { ConnectionPlans } from "./connection-plans";
import { ConnectWriteAdmission } from "./connect-write-admission";
import type {
  ConnectOffsetsPort,
  ConnectOffsetState,
  ConnectOffsetRead,
  ConnectOffsetMutation,
} from "./connect-offset-types";
import type { ConnectMutationReceipt } from "./connect-service";

interface Snapshot {
  readonly owner: ConnectReviewScope;
  readonly state: ConnectOffsetState;
  readonly view: ConnectOffsetsSnapshot;
}
interface Plan {
  readonly name: string;
  readonly baseline: string;
  readonly mutation: ConnectOffsetMutation;
  readonly expected: string;
  readonly confirmation: string;
}
function positions(state: ConnectOffsetState): string {
  return JSON.stringify(state.offsets.map(({ partition, offset }) => ({ partition, offset })));
}
function baseline(state: ConnectOffsetState): string {
  return JSON.stringify({
    clusterId: state.clusterId,
    workerVersion: state.workerVersion,
    mapping: state.mapping,
    config: connectConfig(state.connector.config),
    state: state.connector.detail.state,
    tasks: [...state.connector.detail.tasks]
      .sort((a, b) => a.id - b.id)
      .map(({ id, state }) => ({ id, state })),
    positions: positions(state),
  });
}
function stopped(state: ConnectOffsetState): boolean {
  return (
    state.connector.detail.state === "STOPPED" &&
    state.connector.detail.tasks.every((task) => task.state === "STOPPED")
  );
}
const DETAILS = {
  available:
    "Offsets are a point-in-time worker observation. Only supported stopped connectors can change offsets; the API is not an atomic compare-and-set.",
  unsupported:
    "This worker API, connector mapping or broker routing is unsupported. No offset edit is available.",
  denied: "Offset inspection was denied. Check Connect API permissions and profile authentication.",
  missing: "The connector was not found. Refresh connector inventory.",
  unavailable:
    "Offsets or matching broker identity could not be verified. Check endpoint trust, reachability and connection state; no offset edit is available.",
} as const;
export class ConnectOffsetsService {
  private readonly snapshots = new Map<string, Snapshot>();
  private readonly plans: ConnectionPlans<Plan, ConnectOffsetsOutcome, ConnectReviewScope>;
  constructor(
    private readonly scope: () => ConnectReviewScope | null,
    private readonly port: ConnectOffsetsPort,
    private readonly now = Date.now,
    private readonly admission = new ConnectWriteAdmission(),
  ) {
    this.plans = new ConnectionPlans(scope, (owner) => owner.isCurrent(), now);
  }
  private active(): ConnectReviewScope {
    const owner = this.scope();
    if (!owner?.isCurrent()) throw new Error("Configure Connect in the current connection.");
    return owner;
  }
  private async read(
    owner: ConnectReviewScope,
    name: string,
    signal: AbortSignal,
  ): Promise<ConnectOffsetRead> {
    if (!owner.brokerClusterId) return { status: "unavailable" };
    const clusterId = await owner.brokerClusterId(signal);
    if (!clusterId || !owner.isCurrent()) return { status: "unavailable" };
    const state = await owner.read(
      (context, combined) => this.port.inspectOffsets(context, name, combined),
      signal,
    );
    if (
      state.status === "available" &&
      (state.clusterId !== clusterId || state.connector.detail.name !== name)
    )
      return { status: "unavailable" };
    return state;
  }
  private project(
    owner: ConnectReviewScope,
    name: string,
    state: ConnectOffsetRead,
  ): ConnectOffsetsSnapshot {
    for (const [key, item] of this.snapshots)
      if (!item.owner.isCurrent() || Date.parse(item.view.expiresAt!) <= this.now())
        this.snapshots.delete(key);
    const available = state.status === "available",
      id = available ? crypto.randomUUID() : null;
    const view = parseConnectOffsetsSnapshot({
      name,
      connectionName: owner.connectionName,
      status: state.status,
      snapshotId: id,
      expiresAt: available ? new Date(this.now() + 120_000).toISOString() : null,
      clusterId: available ? state.clusterId : null,
      workerVersion: available ? state.workerVersion : null,
      connectorState: available ? state.connector.detail.state : "UNKNOWN",
      mapping: available ? state.mapping : null,
      positions: available
        ? state.offsets.map((item, index) => ({
            partitionRef: crypto.randomUUID(),
            label: state.mapping === "file-source" ? `Source partition ${index + 1}` : item.label,
            position: item.position,
          }))
        : [],
      observedAt: new Date(this.now()).toISOString(),
      detail: DETAILS[state.status],
    });
    if (available) {
      if (this.snapshots.size >= 16) this.snapshots.delete(this.snapshots.keys().next().value!);
      this.snapshots.set(id!, { owner, state: structuredClone(state), view });
    }
    return view;
  }
  async inspect(name: string): Promise<ConnectOffsetsSnapshot> {
    name = connectName(name);
    const owner = this.active();
    let state: ConnectOffsetRead;
    try {
      state = await this.read(owner, name, AbortSignal.timeout(20_000));
    } catch {
      if (!owner.isCurrent()) throw new Error("Original Connect authority is unavailable.");
      state = { status: "unavailable" };
    }
    return this.project(owner, name, state);
  }
  async review(value: ConnectOffsetsInput): Promise<ConnectOffsetsReview> {
    const input = parseConnectOffsetsInput(value),
      snapshot = this.snapshots.get(input.snapshotId);
    if (
      !snapshot ||
      !snapshot.owner.isCurrent() ||
      this.now() >= Date.parse(snapshot.view.expiresAt!)
    )
      throw new Error("Offset snapshot expired or connection changed. Inspect again.");
    const fresh = await this.read(snapshot.owner, snapshot.view.name, AbortSignal.timeout(20_000));
    if (
      fresh.status !== "available" ||
      !stopped(fresh) ||
      baseline(fresh) !== baseline(snapshot.state) ||
      this.now() >= Date.parse(snapshot.view.expiresAt!)
    )
      throw new Error("Stop the connector and inspect unchanged offsets again before review.");
    const index = snapshot.view.positions.findIndex(
      (item) => item.partitionRef === input.partitionRef,
    );
    if (input.action !== "reset" && index < 0) throw new Error("Choose an observed partition.");
    if (!fresh.offsets.length) throw new Error("No observed offsets to change.");
    const selected = index < 0 ? null : fresh.offsets[index]!;
    const mutation: ConnectOffsetMutation = {
      name: snapshot.view.name,
      action: input.action,
      partition: selected?.partition ?? null,
      offset:
        input.action === "set"
          ? { [fresh.mapping === "file-source" ? "position" : "kafka_offset"]: input.position! }
          : null,
    };
    const expected =
      input.action === "reset"
        ? []
        : fresh.offsets.flatMap((item, i) =>
            i !== index
              ? [item]
              : input.action === "remove"
                ? []
                : [{ ...item, offset: mutation.offset!, position: input.position! }],
          );
    const confirmation = `${input.action} OFFSETS ${mutation.name}`;
    const plan = this.plans.add(snapshot.owner, {
      name: mutation.name,
      baseline: baseline(fresh),
      mutation,
      expected: positions({ ...fresh, offsets: expected }),
      confirmation,
    });
    return {
      planId: plan.id,
      expiresAt: plan.expiresAt,
      name: mutation.name,
      connectionName: snapshot.owner.connectionName,
      clusterId: fresh.clusterId,
      mapping: fresh.mapping,
      input,
      changes: snapshot.view.positions.flatMap((item, i) =>
        input.action === "reset" || i === index
          ? [{ label: item.label, before: item.position, after: input.position }]
          : [],
      ),
      confirmation,
    };
  }
  apply(id: string, confirmation: string): Promise<ConnectOffsetsOutcome> {
    return this.plans.apply(
      id,
      (value) => value.confirmation === confirmation,
      async (plan) => {
        const original = { planId: plan.id, confirmation: plan.value.confirmation };
        const refuse = (detail: string): ConnectOffsetsOutcome => ({
          ...original,
          state: "rejected",
          dispatch: "not-sent",
          verification: "not-applicable",
          cleanup: plan.context.cleanupUnresolved() ? "unresolved" : "confirmed",
          observed: null,
          detail,
        });
        const release = this.admission.acquire();
        if (!release)
          return refuse(
            "Another Connect mutation is running. Inspect and review again afterwards.",
          );
        try {
          const signal = AbortSignal.timeout(
            Math.max(1, Math.min(20_000, Date.parse(plan.expiresAt) - this.now())),
          );
          try {
            const fresh = await this.read(plan.context, plan.value.name, signal);
            if (
              fresh.status !== "available" ||
              !stopped(fresh) ||
              baseline(fresh) !== plan.value.baseline ||
              !plan.context.isCurrent() ||
              this.now() >= Date.parse(plan.expiresAt)
            )
              return refuse(
                "Connector, broker identity or offsets changed, or review expired. No action was sent; inspect and review again.",
              );
            signal.throwIfAborted();
          } catch {
            return refuse("Original Connect offsets could not be verified. No action was sent.");
          }
          const attempt = plan.context.tryDispatch((context) =>
            this.port.applyOffsets(context, plan.value.mutation, signal),
          );
          if (!attempt.started)
            return refuse(
              "Original Connect authority was revoked before dispatch. No action was sent.",
            );
          let receipt: ConnectMutationReceipt;
          try {
            receipt = await attempt.result;
          } catch {
            return {
              ...original,
              state: "unknown",
              dispatch: "attempted",
              verification: "not-applicable",
              cleanup: "unresolved",
              observed: null,
              detail:
                "Original offset outcome is unknown and cleanup is unconfirmed. Inspect Connect before any new review; no retry was sent.",
            };
          }
          let observed: ConnectOffsetsSnapshot | null = null,
            verification: ConnectOffsetsOutcome["verification"] =
              receipt.state === "acknowledged" ? "unavailable" : "not-applicable";
          if (
            receipt.state === "acknowledged" &&
            receipt.cleanup === "confirmed" &&
            plan.context.isCurrent()
          ) {
            try {
              const state = await this.read(plan.context, plan.value.name, signal);
              observed = this.project(plan.context, plan.value.name, state);
              if (state.status === "available")
                verification =
                  stopped(state) && positions(state) === plan.value.expected
                    ? "verified"
                    : "different";
            } catch {
              /* Retain actual ACK when readback is unavailable or revoked. */
            }
          }
          return {
            ...original,
            ...receipt,
            verification,
            observed,
            cleanup: plan.context.cleanupUnresolved() ? "unresolved" : receipt.cleanup,
          };
        } finally {
          release();
        }
      },
    );
  }
}
