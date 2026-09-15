import {
  HOST_PROTOCOL_VERSION,
  REDPANDA_TRANSFORM_LIMITS,
  type HostCommand,
  type HostCommandResponse,
  type HostError,
  type HostEvent,
  type RedpandaTransformDetailSnapshot,
  type RedpandaTransformInventorySnapshot,
  type RedpandaTransformLogsSnapshot,
} from "../contracts";
import type { KafkaApplicationSession, RedpandaTransformPort } from "../application";

import {
  failureResponse,
  isStructuredFailure,
  successResponse,
  type ActivityInput,
} from "./facade-support";

type TransformCommand = Extract<HostCommand, { readonly command: `transforms.${string}` }>;

interface TransformFacadeOptions {
  readonly nextSequence: () => number;
  readonly now: () => Date;
  readonly port?: RedpandaTransformPort;
  readonly publish: (event: HostEvent) => void;
  readonly recordActivity: (activity: ActivityInput) => void;
  readonly session: KafkaApplicationSession;
}

const unavailableInventory: RedpandaTransformInventorySnapshot = {
  connectionName: null,
  endpoint: null,
  omittedTransforms: 0,
  refreshedAt: null,
  state: "unavailable",
  transforms: [],
};
const unavailableDetail: RedpandaTransformDetailSnapshot = {
  connectionName: null,
  endpoint: null,
  refreshedAt: null,
  state: "unavailable",
  transform: null,
  transformName: null,
};
const unavailableLogs: RedpandaTransformLogsSnapshot = {
  connectionName: null,
  logs: [],
  omittedLogs: 0,
  refreshedAt: null,
  state: "unavailable",
  transformName: null,
};

export class TransformFacadeController {
  private controller: AbortController | undefined;
  private detail = unavailableDetail;
  private inventory = unavailableInventory;
  private logs = unavailableLogs;

  constructor(private readonly options: TransformFacadeOptions) {}

  invalidate(): void {
    this.controller?.abort();
    this.controller = undefined;
    this.publishInventory(unavailableInventory);
    this.publishDetail(unavailableDetail);
    this.publishLogs(unavailableLogs);
  }

  async execute(command: TransformCommand, correlationId: string): Promise<HostCommandResponse> {
    const context = this.options.session.clusterServiceContext("redpandaAdmin");
    const connectionName = this.options.session.snapshot().connectionName;
    if (context === null || this.options.port === undefined) {
      this.publishInventory({ ...unavailableInventory, connectionName, state: "not-configured" });
      this.options.recordActivity({
        correlationId,
        detail: "No Redpanda Admin endpoint is configured for the active connection.",
        object: connectionName ?? "Transforms",
        operation: transformOperation(command),
        outcome: "succeeded",
        severity: "warning",
      });
      return successResponse(command, correlationId);
    }
    this.controller?.abort();
    const controller = new AbortController();
    this.controller = controller;
    try {
      switch (command.command) {
        case "transforms.list":
          await this.loadInventory(context, connectionName, controller.signal);
          break;
        case "transforms.load":
          await this.loadDetail(context, connectionName, command.payload.name, controller.signal);
          break;
        case "transforms.logs.load": {
          this.publishLogs({
            connectionName,
            logs: [],
            omittedLogs: 0,
            refreshedAt: null,
            state: "loading",
            transformName: command.payload.name,
          });
          const result = await this.options.session.loadTransformLogs(
            command.payload.name,
            controller.signal,
          );
          controller.signal.throwIfAborted();
          this.publishLogs({
            connectionName,
            logs: result.logs,
            omittedLogs: result.omittedLogs,
            refreshedAt: this.options.now().toISOString(),
            state: result.logs.length === 0 ? "empty" : "ready",
            transformName: command.payload.name,
          });
          break;
        }
        case "transforms.delete":
          await this.options.port.delete(context, command.payload.name, controller.signal);
          await this.loadInventory(context, connectionName, controller.signal);
          this.publishDetail({
            ...unavailableDetail,
            connectionName,
            endpoint: context.baseUrl,
            state: "empty",
          });
          this.publishLogs({ ...unavailableLogs, connectionName, state: "empty" });
          break;
      }
      if (this.controller !== controller) return successResponse(command, correlationId);
      this.options.recordActivity({
        correlationId,
        detail: `${transformOperation(command)} completed with confirmed cluster evidence.`,
        object: transformObject(command),
        operation: transformOperation(command),
        outcome: "succeeded",
        severity: "info",
      });
      return successResponse(command, correlationId);
    } catch (error) {
      if (this.controller !== controller) {
        return failureResponse(
          command,
          transformFailure(error, correlationId, transformObject(command)),
        );
      }
      const failure = transformFailure(error, correlationId, transformObject(command));
      const failureState = transformFailureState(error, failure, command.command);
      switch (command.command) {
        case "transforms.list":
        case "transforms.delete":
          this.publishInventory({
            ...this.inventory,
            error: failure,
            state: this.inventory.refreshedAt === null ? failureState : "stale",
          });
          break;
        case "transforms.load":
          this.publishDetail({
            ...this.detail,
            error: failure,
            state:
              this.detail.refreshedAt === null || this.detail.transformName !== command.payload.name
                ? failureState
                : "stale",
            transformName: command.payload.name,
          });
          break;
        case "transforms.logs.load":
          this.publishLogs({
            ...this.logs,
            error: failure,
            state:
              this.logs.refreshedAt === null || this.logs.transformName !== command.payload.name
                ? failureState
                : "stale",
            transformName: command.payload.name,
          });
          break;
      }
      this.options.recordActivity({
        correlationId,
        detail: `${failure.summary} ${failure.recovery}`,
        object: transformObject(command),
        operation: transformOperation(command),
        outcome: failure.code === "CANCELLED" ? "cancelled" : "failed",
        severity: failure.code === "CANCELLED" ? "warning" : "error",
      });
      return failureResponse(command, failure);
    } finally {
      if (this.controller === controller) this.controller = undefined;
    }
  }

  private async loadInventory(
    context: NonNullable<ReturnType<KafkaApplicationSession["clusterServiceContext"]>>,
    connectionName: string | null,
    signal: AbortSignal,
  ): Promise<void> {
    this.publishInventory({
      ...this.inventory,
      connectionName,
      endpoint: context.baseUrl,
      state: "loading",
    });
    const all = await this.options.port!.list(context, signal);
    signal.throwIfAborted();
    const transforms = all.slice(0, REDPANDA_TRANSFORM_LIMITS.transforms);
    this.publishInventory({
      connectionName,
      endpoint: context.baseUrl,
      omittedTransforms: Math.max(0, all.length - transforms.length),
      refreshedAt: this.options.now().toISOString(),
      state: transforms.length === 0 ? "empty" : "ready",
      transforms,
    });
  }

  private async loadDetail(
    context: NonNullable<ReturnType<KafkaApplicationSession["clusterServiceContext"]>>,
    connectionName: string | null,
    name: string,
    signal: AbortSignal,
  ): Promise<void> {
    this.publishDetail({
      connectionName,
      endpoint: context.baseUrl,
      refreshedAt: null,
      state: "loading",
      transform: null,
      transformName: name,
    });
    const transforms = await this.options.port!.list(context, signal);
    signal.throwIfAborted();
    const transform = transforms.find((candidate) => candidate.name === name) ?? null;
    this.publishDetail({
      connectionName,
      endpoint: context.baseUrl,
      refreshedAt: this.options.now().toISOString(),
      state: transform === null ? "not-found" : "ready",
      transform,
      transformName: name,
    });
  }

  private publishInventory(payload: RedpandaTransformInventorySnapshot): void {
    this.inventory = payload;
    this.options.publish({
      event: "transforms.changed",
      payload,
      sequence: this.options.nextSequence(),
      version: HOST_PROTOCOL_VERSION,
    });
  }
  private publishDetail(payload: RedpandaTransformDetailSnapshot): void {
    this.detail = payload;
    this.options.publish({
      event: "transform.changed",
      payload,
      sequence: this.options.nextSequence(),
      version: HOST_PROTOCOL_VERSION,
    });
  }
  private publishLogs(payload: RedpandaTransformLogsSnapshot): void {
    this.logs = payload;
    this.options.publish({
      event: "transformLogs.changed",
      payload,
      sequence: this.options.nextSequence(),
      version: HOST_PROTOCOL_VERSION,
    });
  }
}

function transformOperation(command: TransformCommand): string {
  switch (command.command) {
    case "transforms.list":
      return "Refresh transforms";
    case "transforms.load":
      return "Load transform";
    case "transforms.logs.load":
      return "Load transform logs";
    case "transforms.delete":
      return "Delete transform";
  }
}
function transformObject(command: TransformCommand): string {
  return command.command === "transforms.list" ? "Redpanda transforms" : command.payload.name;
}
function transformFailure(error: unknown, correlationId: string, target: string): HostError {
  const status =
    error !== null &&
    typeof error === "object" &&
    "status" in error &&
    typeof error.status === "number" &&
    Number.isInteger(error.status)
      ? error.status
      : null;
  if (isStructuredFailure(error)) {
    return {
      activeStateChanged: false,
      code: error.code,
      correlationId,
      recovery: error.recovery,
      retryable: error.retryable,
      stage: error.stage,
      summary: error.message,
      target: error.target ?? target,
    };
  }
  const cancelled =
    error instanceof Error &&
    (error.name === "AbortError" ||
      error.name === "ConnectionAttemptSupersededError" ||
      error.name === "OperationAborted");
  const denied = status === 401 || status === 403;
  return {
    activeStateChanged: false,
    code: cancelled ? "CANCELLED" : denied ? "AUTHORIZATION_DENIED" : "BACKEND_UNAVAILABLE",
    correlationId,
    recovery: cancelled
      ? "Retry when the active connection and transform selection are stable."
      : denied
        ? "Grant the connected identity Redpanda Admin or transform-log read permission, then retry."
        : "Verify the Redpanda Admin endpoint, Kafka log-topic permission, TLS trust and network path, then retry.",
    retryable: cancelled || !denied,
    stage: denied ? "authorization" : "backend",
    summary: cancelled
      ? "The transform operation was cancelled."
      : denied
        ? "Transform access was denied."
        : "The transform operation could not be completed.",
    target,
  };
}

function transformFailureState(
  error: unknown,
  failure: HostError,
  command: TransformCommand["command"],
): RedpandaTransformInventorySnapshot["state"] {
  const status =
    error !== null &&
    typeof error === "object" &&
    "status" in error &&
    typeof error.status === "number" &&
    Number.isInteger(error.status)
      ? error.status
      : null;
  if (failure.code === "AUTHORIZATION_DENIED") return "denied";
  if (command === "transforms.list" && (status === 404 || status === 405 || status === 501)) {
    return "unsupported";
  }
  if (
    error instanceof Error &&
    error.name === "RedpandaTransformResponseError" &&
    status === null
  ) {
    return "invalid-response";
  }
  if (failure.code === "BACKEND_UNAVAILABLE" || failure.code === "TOPIC_NOT_FOUND") {
    return "unavailable";
  }
  return "failed";
}
