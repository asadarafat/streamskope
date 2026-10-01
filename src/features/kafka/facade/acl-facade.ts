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

  constructor(private readonly bindings: AclFacadeBindings) {}

  invalidate(): void {
    this.publish(unavailableAclSnapshot);
  }

  async execute(command: AclCommand, correlationId: string): Promise<HostCommandResponse> {
    const connectionName =
      this.bindings.session.snapshot().connectionName ?? "No active connection";
    const previous = this.snapshot;
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
      const allAcls = await this.bindings.session.listAcls();
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
            : `${operation(command)} was confirmed by a refreshed exact ACL inventory.`,
        object: object(command),
        operation: operation(command),
        outcome: "succeeded",
        severity: "info",
      });
      return successResponse(command, correlationId);
    } catch (error) {
      const translated = translateFacadeFailure(
        error,
        { activeStateChanged: false, connection: undefined, correlationId },
        this.bindings.available(),
      );
      const cancelled = error instanceof ConnectionAttemptSupersededError;
      if (!cancelled) {
        const refreshedAfterDelete =
          command.command === "acls.delete"
            ? await this.refreshAfterFailedDelete(connectionName, translated.error)
            : false;
        if (!refreshedAfterDelete) {
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

  private publish(payload: KafkaAclSnapshot): void {
    this.snapshot = payload;
    this.bindings.publish({
      event: "acls.changed",
      payload,
      sequence: this.bindings.nextSequence(),
      version: HOST_PROTOCOL_VERSION,
    });
  }

  private async refreshAfterFailedDelete(
    connectionName: string,
    error: KafkaAclSnapshot["error"],
  ): Promise<boolean> {
    try {
      const allAcls = await this.bindings.session.listAcls();
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
