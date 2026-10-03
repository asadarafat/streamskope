import {
  HOST_PROTOCOL_VERSION,
  KAFKA_ACL_LIMITS,
  kafkaAclIdentity,
  type HostCommand,
  type HostCommandResponse,
  type HostEvent,
  type KafkaAclSnapshot,
} from "../contracts";
import { ConnectionAttemptSupersededError, type KafkaApplicationSession } from "../application";

import {
  failureResponse,
  successResponse,
  translateFacadeFailure,
  type ActivityInput,
} from "./facade-support";

type AclCommand = Extract<HostCommand, { readonly command: `acls.${string}` }>;

interface AclFacadeBindings {
  readonly available: () => boolean;
  readonly nextSequence: () => number;
  readonly now: () => Date;
  readonly publish: (event: HostEvent) => void;
  readonly recordActivity: (input: ActivityInput) => void;
  readonly session: KafkaApplicationSession;
}

const unavailableAclSnapshot: KafkaAclSnapshot = {
  acls: [],
  connectionName: null,
  omittedAcls: 0,
  refreshedAt: null,
  state: "unavailable",
};

export class AclFacadeController {
  private snapshot = unavailableAclSnapshot;
  private generation = 0;

  constructor(private readonly bindings: AclFacadeBindings) {}

  invalidate(): void {
    this.generation += 1;
    this.publish(unavailableAclSnapshot);
  }

  async execute(command: AclCommand, correlationId: string): Promise<HostCommandResponse> {
    const connectionName =
      this.bindings.session.snapshot().connectionName ?? "No active connection";
    const previous = this.snapshot;
    const generation = this.generation;
    let acknowledged = false;
    this.publish({
      ...previous,
      connectionName,
      state: "loading",
    });
    try {
      if (command.command === "acls.create") {
        await this.bindings.session.createAcl(command.payload);
      } else if (command.command === "acls.delete") {
        await this.bindings.session.deleteAcl(command.payload.acl);
      }
      acknowledged = command.command !== "acls.list";
      if (generation !== this.generation) {
        if (acknowledged)
          return this.acknowledged(
            command,
            correlationId,
            "Kafka acknowledged the exact ACL mutation; the connection changed before inventory refresh.",
            true,
          );
        throw new ConnectionAttemptSupersededError();
      }
      const allAcls = await this.bindings.session.listAcls();
      if (generation !== this.generation) throw new ConnectionAttemptSupersededError();
      const matches =
        command.command === "acls.list"
          ? true
          : allAcls.some((acl) => kafkaAclIdentity(acl) === object(command));
      const verified =
        command.command === "acls.list" || (command.command === "acls.create" ? matches : !matches);
      const acls = allAcls.slice(0, KAFKA_ACL_LIMITS.acls);
      this.publish({
        acls,
        connectionName,
        omittedAcls: Math.max(0, allAcls.length - acls.length),
        refreshedAt: this.bindings.now().toISOString(),
        state: acls.length === 0 ? "empty" : "ready",
      });
      this.bindings.recordActivity({
        correlationId,
        detail:
          command.command === "acls.list"
            ? `Loaded ${String(acls.length)} exact Kafka ACL binding${acls.length === 1 ? "" : "s"}.`
            : verified
              ? `${operation(command)} was acknowledged and the exact binding was reconciled with current inventory.`
              : `${operation(command)} was acknowledged, but refreshed inventory does not match the requested state. Inspect the binding before another change.`,
        object: object(command),
        operation: operation(command),
        outcome: "succeeded",
        severity: verified ? "info" : "warning",
      });
      return successResponse(command, correlationId);
    } catch (error) {
      if (acknowledged) {
        const warning =
          "Kafka acknowledged the exact ACL mutation, but inventory refresh is unavailable. Do not repeat the mutation to retry the refresh.";
        if (generation === this.generation)
          this.publish({
            ...previous,
            connectionName,
            state: "stale",
            error: {
              code: "BACKEND_UNAVAILABLE",
              stage: "backend",
              correlationId,
              retryable: false,
              activeStateChanged: false,
              summary: warning,
              recovery: "Refresh ACLs to reconcile the exact binding.",
            },
          });
        return this.acknowledged(command, correlationId, warning, true);
      }
      let translated = translateFacadeFailure(
        error,
        { activeStateChanged: false, connection: undefined, correlationId },
        this.bindings.available(),
      );
      if (command.command !== "acls.list")
        translated = {
          ...translated,
          error: {
            ...translated.error,
            retryable: false,
            recovery: `${translated.error.recovery} The mutation was not acknowledged. Refresh and inspect the exact binding before another attempt.`,
          },
        };
      const cancelled = error instanceof ConnectionAttemptSupersededError;
      if (!cancelled && generation === this.generation) {
        const refreshedAfterMutation =
          command.command !== "acls.list"
            ? await this.refreshAfterFailedMutation(connectionName, translated.error, generation)
            : false;
        if (!refreshedAfterMutation && generation === this.generation) {
          const retain =
            previous.connectionName === connectionName && previous.refreshedAt !== null;
          this.publish(
            retain
              ? { ...previous, error: translated.error, state: "stale" }
              : {
                  acls: [],
                  connectionName,
                  error: translated.error,
                  omittedAcls: 0,
                  refreshedAt: null,
                  state:
                    translated.error.code === "AUTHORIZATION_DENIED"
                      ? "denied"
                      : translated.error.code === "UNSUPPORTED_OPERATION"
                        ? "unsupported"
                        : "failed",
                },
          );
        }
      }
      this.bindings.recordActivity({
        correlationId,
        detail: translated.detail,
        object: object(command),
        operation: operation(command),
        outcome: cancelled ? "cancelled" : "failed",
        severity: cancelled ? "warning" : "error",
      });
      return failureResponse(command, translated.error);
    }
  }

  private acknowledged(
    command: AclCommand,
    correlationId: string,
    detail: string,
    warning: boolean,
  ): HostCommandResponse {
    this.bindings.recordActivity({
      correlationId,
      detail,
      object: object(command),
      operation: operation(command),
      outcome: "succeeded",
      severity: warning ? "warning" : "info",
    });
    return successResponse(command, correlationId);
  }

  private publish(payload: KafkaAclSnapshot): void {
    this.snapshot = payload;
    this.bindings.publish({
      event: "acls.changed",
      payload,
      sequence: this.bindings.nextSequence(),
      version: HOST_PROTOCOL_VERSION,
    });
  }

  private async refreshAfterFailedMutation(
    connectionName: string,
    error: KafkaAclSnapshot["error"],
    generation: number,
  ): Promise<boolean> {
    try {
      const allAcls = await this.bindings.session.listAcls();
      if (generation !== this.generation) return false;
      const acls = allAcls.slice(0, KAFKA_ACL_LIMITS.acls);
      this.publish({
        acls,
        connectionName,
        ...(error === undefined ? {} : { error }),
        omittedAcls: Math.max(0, allAcls.length - acls.length),
        refreshedAt: this.bindings.now().toISOString(),
        state: acls.length === 0 ? "empty" : "ready",
      });
      return true;
    } catch {
      return false;
    }
  }
}

function operation(command: AclCommand): string {
  switch (command.command) {
    case "acls.list":
      return "Refresh ACLs";
    case "acls.create":
      return "Create ACL";
    case "acls.delete":
      return "Delete ACL";
  }
}

function object(command: AclCommand): string {
  if (command.command === "acls.list") return "Kafka ACLs";
  return kafkaAclIdentity(
    command.command === "acls.delete" ? command.payload.acl : command.payload,
  );
}
