import {
  HOST_PROTOCOL_VERSION,
  type HostCommand,
  type HostCommandResponse,
  type HostEvent,
  type KafkaConsumerGroupDetailSnapshot,
  type KafkaConsumerGroupInventorySnapshot,
} from "../contracts";
import { ConnectionAttemptSupersededError, type KafkaApplicationSession } from "../application";

import {
  failureResponse,
  successResponse,
  translateFacadeFailure,
  type ActivityInput,
} from "./facade-support";

export type ConsumerGroupHostCommand = Extract<
  HostCommand,
  { readonly command: "consumerGroups.list" | "consumerGroups.load" }
>;

export interface ConsumerGroupFacadeBindings {
  readonly available: () => boolean;
  readonly nextSequence: () => number;
  readonly now: () => Date;
  readonly publish: (event: HostEvent) => void;
  readonly recordActivity: (input: ActivityInput) => void;
  readonly session: Pick<
    KafkaApplicationSession,
    "describeConsumerGroup" | "listConsumerGroups" | "snapshot"
  >;
}

const unavailableInventory: KafkaConsumerGroupInventorySnapshot = {
  connectionName: null,
  groups: [],
  omittedGroups: 0,
  refreshedAt: null,
  state: "unavailable",
};

const unavailableDetail: KafkaConsumerGroupDetailSnapshot = {
  connectionName: null,
  group: null,
  groupId: null,
  refreshedAt: null,
  state: "unavailable",
};

export class ConsumerGroupFacadeController {
  private detail = unavailableDetail;
  private inventory = unavailableInventory;

  constructor(private readonly bindings: ConsumerGroupFacadeBindings) {}

  execute(command: ConsumerGroupHostCommand, correlationId: string): Promise<HostCommandResponse> {
    return command.command === "consumerGroups.list"
      ? this.list(command, correlationId)
      : this.load(command, correlationId);
  }

  invalidate(): void {
    this.publishInventory(unavailableInventory);
    this.publishDetail(unavailableDetail);
  }

  private async list(
    command: Extract<HostCommand, { readonly command: "consumerGroups.list" }>,
    correlationId: string,
  ): Promise<HostCommandResponse> {
    const connectionName =
      this.bindings.session.snapshot().connectionName ?? "No active connection";
    const previous = this.inventory;
    this.publishInventory({
      connectionName,
      groups: [],
      omittedGroups: 0,
      refreshedAt: null,
      state: "loading",
    });
    try {
      const inventory = await this.bindings.session.listConsumerGroups();
      this.publishInventory({
        connectionName,
        groups: inventory.groups,
        omittedGroups: inventory.omittedGroups,
        refreshedAt: this.bindings.now().toISOString(),
        state: inventory.groups.length === 0 ? "empty" : "ready",
      });
      this.bindings.recordActivity({
        correlationId,
        detail: `Loaded ${String(inventory.groups.length)} Kafka consumer group${
          inventory.groups.length === 1 ? "" : "s"
        }${
          inventory.omittedGroups === 0
            ? "."
            : `; ${String(inventory.omittedGroups)} additional groups were omitted by the safety bound.`
        }`,
        object: connectionName,
        operation: "Refresh consumer groups",
        outcome: "succeeded",
        severity: "info",
      });
      return successResponse(command, correlationId);
    } catch (error) {
      const translated = this.translate(error, correlationId);
      const cancelled = error instanceof ConnectionAttemptSupersededError;
      if (!cancelled) {
        const canRetain =
          previous.connectionName === connectionName && previous.refreshedAt !== null;
        this.publishInventory(
          canRetain
            ? { ...previous, error: translated.error, state: "stale" }
            : {
                connectionName,
                error: translated.error,
                groups: [],
                omittedGroups: 0,
                refreshedAt: null,
                state: translated.error.code === "AUTHORIZATION_DENIED" ? "denied" : "failed",
              },
        );
      }
      this.recordFailure(
        connectionName,
        "Refresh consumer groups",
        correlationId,
        translated.detail,
        cancelled,
      );
      return failureResponse(command, translated.error);
    }
  }

  private async load(
    command: Extract<HostCommand, { readonly command: "consumerGroups.load" }>,
    correlationId: string,
  ): Promise<HostCommandResponse> {
    const connectionName =
      this.bindings.session.snapshot().connectionName ?? "No active connection";
    const { groupId } = command.payload;
    const previous = this.detail;
    this.publishDetail({
      connectionName,
      group: null,
      groupId,
      refreshedAt: null,
      state: "loading",
    });
    try {
      const group = await this.bindings.session.describeConsumerGroup(groupId);
      this.publishDetail({
        connectionName,
        group,
        groupId,
        refreshedAt: this.bindings.now().toISOString(),
        state: "ready",
      });
      this.bindings.recordActivity({
        correlationId,
        detail: `Loaded ${String(group.members.length)} member${
          group.members.length === 1 ? "" : "s"
        } and ${String(group.offsets.length)} committed offset${
          group.offsets.length === 1 ? "" : "s"
        } for the selected consumer group.`,
        object: groupId,
        operation: "Load consumer group",
        outcome: "succeeded",
        severity: "info",
      });
      return successResponse(command, correlationId);
    } catch (error) {
      const translated = this.translate(error, correlationId);
      const cancelled = error instanceof ConnectionAttemptSupersededError;
      if (!cancelled) {
        const canRetain =
          previous.connectionName === connectionName &&
          previous.groupId === groupId &&
          previous.group !== null &&
          previous.refreshedAt !== null;
        this.publishDetail(
          canRetain
            ? { ...previous, error: translated.error, state: "stale" }
            : {
                connectionName,
                error: translated.error,
                group: null,
                groupId,
                refreshedAt: null,
                state:
                  translated.error.code === "AUTHORIZATION_DENIED"
                    ? "denied"
                    : translated.error.code === "CONSUMER_GROUP_NOT_FOUND"
                      ? "not-found"
                      : "failed",
              },
        );
      }
      this.recordFailure(
        groupId,
        "Load consumer group",
        correlationId,
        translated.detail,
        cancelled,
      );
      return failureResponse(command, translated.error);
    }
  }

  private publishInventory(payload: KafkaConsumerGroupInventorySnapshot): void {
    this.inventory = payload;
    this.bindings.publish({
      event: "consumerGroups.changed",
      payload,
      sequence: this.bindings.nextSequence(),
      version: HOST_PROTOCOL_VERSION,
    });
  }

  private publishDetail(payload: KafkaConsumerGroupDetailSnapshot): void {
    this.detail = payload;
    this.bindings.publish({
      event: "consumerGroup.changed",
      payload,
      sequence: this.bindings.nextSequence(),
      version: HOST_PROTOCOL_VERSION,
    });
  }

  private recordFailure(
    object: string,
    operation: string,
    correlationId: string,
    detail: string,
    cancelled: boolean,
  ): void {
    this.bindings.recordActivity({
      correlationId,
      detail,
      object,
      operation,
      outcome: cancelled ? "cancelled" : "failed",
      severity: cancelled ? "warning" : "error",
    });
  }

  private translate(
    error: unknown,
    correlationId: string,
  ): ReturnType<typeof translateFacadeFailure> {
    return translateFacadeFailure(
      error,
      { activeStateChanged: false, connection: undefined, correlationId },
      this.bindings.available(),
    );
  }
}
