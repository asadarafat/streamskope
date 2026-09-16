import {
  HOST_PROTOCOL_VERSION,
  type HostCommand,
  type HostCommandResponse,
  type HostError,
  type HostEvent,
  type SchemaRegistryDetailSnapshot,
  type SchemaRegistryInventorySnapshot,
} from "../contracts";
import type { KafkaApplicationSession, SchemaRegistryPort } from "../application";

import { failureResponse, successResponse, type ActivityInput } from "./facade-support";

type SchemaCommand = Extract<HostCommand, { readonly command: `schemas.${string}` }>;

interface SchemaRegistryFacadeOptions {
  readonly nextSequence: () => number;
  readonly now: () => Date;
  readonly port?: SchemaRegistryPort;
  readonly publish: (event: HostEvent) => void;
  readonly recordActivity: (activity: ActivityInput) => void;
  readonly session: KafkaApplicationSession;
}

function inventoryEvent(payload: SchemaRegistryInventorySnapshot, sequence: number): HostEvent {
  return { event: "schemas.changed", payload, sequence, version: HOST_PROTOCOL_VERSION };
}

function detailEvent(payload: SchemaRegistryDetailSnapshot, sequence: number): HostEvent {
  return { event: "schema.changed", payload, sequence, version: HOST_PROTOCOL_VERSION };
}

function compatibilityEvent(
  payload: Extract<HostEvent, { readonly event: "schemaCompatibility.changed" }>["payload"],
  sequence: number,
): HostEvent {
  return {
    event: "schemaCompatibility.changed",
    payload,
    sequence,
    version: HOST_PROTOCOL_VERSION,
  };
}

function safeFailure(error: unknown, correlationId: string, target: string): HostError {
  const status =
    error !== null &&
    typeof error === "object" &&
    "status" in error &&
    typeof error.status === "number" &&
    Number.isInteger(error.status)
      ? error.status
      : null;
  const denied = status === 401 || status === 403;
  const cancelled =
    error instanceof Error &&
    ["AbortError", "OperationAborted", "ConnectionAttemptSupersededError"].includes(error.name);
  const invalid =
    error instanceof Error && error.name === "SchemaRegistryResponseError" && status === null;
  return {
    activeStateChanged: false,
    code: cancelled
      ? "CANCELLED"
      : denied
        ? "AUTHORIZATION_DENIED"
        : invalid
          ? "INTERNAL"
          : "BACKEND_UNAVAILABLE",
    correlationId,
    recovery: cancelled
      ? "Retry after the current connection or schema selection is stable."
      : denied
        ? "Grant the connected identity the required Schema Registry permission, then retry."
        : invalid
          ? "Verify that the endpoint implements the compatible Schema Registry API."
          : "Verify the Schema Registry endpoint, TLS trust, authentication and network path, then retry.",
    retryable: cancelled || (!denied && !invalid),
    stage: denied ? "authorization" : invalid ? "internal" : "backend",
    summary: cancelled
      ? "The Schema Registry operation was cancelled."
      : denied
        ? "Schema Registry authorization was denied."
        : invalid
          ? "Schema Registry returned an invalid bounded response."
          : "Schema Registry could not complete the request.",
    target,
  };
}

function schemaFailureState(
  error: unknown,
  failure: HostError,
): SchemaRegistryInventorySnapshot["state"] {
  const invalid =
    error instanceof Error &&
    error.name === "SchemaRegistryResponseError" &&
    (!("status" in error) || error.status === null);
  if (failure.code === "AUTHORIZATION_DENIED") return "denied";
  if (invalid) return "invalid-response";
  if (failure.code === "BACKEND_UNAVAILABLE") return "unavailable";
  return "failed";
}

export class SchemaRegistryFacadeController {
  private controller: AbortController | undefined;
  private detail: SchemaRegistryDetailSnapshot = {
    compatibilityLevel: null,
    connectionName: null,
    endpoint: null,
    refreshedAt: null,
    schema: null,
    state: "unavailable",
    subject: null,
    versions: [],
  };
  private inventory: SchemaRegistryInventorySnapshot = {
    connectionName: null,
    endpoint: null,
    omittedSubjects: 0,
    refreshedAt: null,
    state: "unavailable",
    subjects: [],
  };

  constructor(private readonly options: SchemaRegistryFacadeOptions) {}

  invalidate(): void {
    this.controller?.abort();
    this.controller = undefined;
    this.inventory = {
      connectionName: null,
      endpoint: null,
      omittedSubjects: 0,
      refreshedAt: null,
      state: "unavailable",
      subjects: [],
    };
    this.detail = {
      compatibilityLevel: null,
      connectionName: null,
      endpoint: null,
      refreshedAt: null,
      schema: null,
      state: "unavailable",
      subject: null,
      versions: [],
    };
    this.publishInventory();
    this.publishDetail();
  }

  async execute(command: SchemaCommand, correlationId: string): Promise<HostCommandResponse> {
    const context = this.options.session.clusterServiceContext("schemaRegistry");
    const connectionName = this.options.session.snapshot().connectionName;
    if (context === null || this.options.port === undefined) {
      this.inventory = {
        connectionName,
        endpoint: null,
        omittedSubjects: 0,
        refreshedAt: null,
        state: "not-configured",
        subjects: [],
      };
      this.publishInventory();
      this.detail = {
        ...this.detail,
        connectionName,
        endpoint: null,
        state: "not-configured",
      };
      this.publishDetail();
      this.options.recordActivity({
        correlationId,
        detail: "No Schema Registry endpoint is configured for the active connection.",
        object: connectionName ?? "Schema Registry",
        operation: this.operation(command),
        outcome: "succeeded",
        severity: "warning",
      });
      return successResponse(command, correlationId);
    }
    this.controller?.abort();
    const controller = new AbortController();
    this.controller = controller;
    const target = context.baseUrl;
    try {
      switch (command.command) {
        case "schemas.list":
          await this.loadInventory(context, connectionName, controller.signal);
          break;
        case "schemas.load":
          await this.loadDetail(context, connectionName, command.payload, controller.signal);
          break;
        case "schemas.compatibility.check": {
          const result = await this.options.port.checkCompatibility(
            context,
            command.payload,
            controller.signal,
          );
          this.options.publish(
            compatibilityEvent(
              { ...result, subject: command.payload.subject, version: command.payload.version },
              this.options.nextSequence(),
            ),
          );
          break;
        }
        case "schemas.register": {
          const result = await this.options.port.checkCompatibility(
            context,
            command.payload,
            controller.signal,
          );
          this.options.publish(
            compatibilityEvent(
              { ...result, subject: command.payload.subject, version: command.payload.version },
              this.options.nextSequence(),
            ),
          );
          if (!result.compatible) {
            const error: HostError = {
              activeStateChanged: false,
              code: "VALIDATION",
              correlationId,
              recovery: "Revise the schema using the Registry compatibility messages, then retry.",
              retryable: false,
              stage: "validation",
              summary: "Schema Registry reported the proposed version as incompatible.",
              target: command.payload.subject,
            };
            this.recordFailure(command, correlationId, error);
            return failureResponse(command, error);
          }
          const registered = await this.options.port.register(
            context,
            command.payload,
            controller.signal,
          );
          await Promise.all([
            this.loadInventory(context, connectionName, controller.signal),
            this.loadLatestDetail(
              context,
              connectionName,
              command.payload.subject,
              controller.signal,
            ),
          ]);
          this.options.recordActivity({
            correlationId,
            detail: `Schema Registry confirmed registration with ID ${String(registered.id)}.`,
            object: command.payload.subject,
            operation: this.operation(command),
            outcome: "succeeded",
            severity: "info",
          });
          return successResponse(command, correlationId);
        }
        case "schemas.delete":
          await this.options.port.delete(context, command.payload, controller.signal);
          await this.loadInventory(context, connectionName, controller.signal);
          this.detail = {
            compatibilityLevel: null,
            connectionName,
            endpoint: context.baseUrl,
            refreshedAt: this.options.now().toISOString(),
            schema: null,
            state: "empty",
            subject: null,
            versions: [],
          };
          this.publishDetail();
          break;
      }
      this.options.recordActivity({
        correlationId,
        detail: `${this.operation(command)} completed with confirmed Schema Registry data.`,
        object: this.object(command),
        operation: this.operation(command),
        outcome: "succeeded",
        severity: "info",
      });
      return successResponse(command, correlationId);
    } catch (error) {
      if (this.controller !== controller) {
        return failureResponse(
          command,
          safeFailure(
            new DOMException("Obsolete schema request", "AbortError"),
            correlationId,
            target,
          ),
        );
      }
      const failure = safeFailure(error, correlationId, target);
      const failureState = schemaFailureState(error, failure);
      if (command.command === "schemas.list") {
        this.inventory = {
          ...this.inventory,
          error: failure,
          state: this.inventory.refreshedAt === null ? failureState : "stale",
        };
        this.publishInventory();
      } else if (command.command === "schemas.load") {
        this.detail = {
          ...this.detail,
          error: failure,
          state:
            this.detail.refreshedAt === null || this.detail.subject !== command.payload.subject
              ? failureState
              : "stale",
          subject: command.payload.subject,
        };
        this.publishDetail();
      }
      this.recordFailure(command, correlationId, failure);
      return failureResponse(command, failure);
    } finally {
      if (this.controller === controller) {
        this.controller = undefined;
      }
    }
  }

  private async loadInventory(
    context: NonNullable<ReturnType<KafkaApplicationSession["clusterServiceContext"]>>,
    connectionName: string | null,
    signal: AbortSignal,
  ): Promise<void> {
    this.inventory = {
      ...this.inventory,
      connectionName,
      endpoint: context.baseUrl,
      state: "loading",
    };
    this.publishInventory();
    const result = await this.options.port!.listSubjects(context, signal);
    signal.throwIfAborted();
    this.inventory = {
      connectionName,
      endpoint: context.baseUrl,
      omittedSubjects: result.omittedSubjects,
      refreshedAt: this.options.now().toISOString(),
      state: result.subjects.length === 0 ? "empty" : "ready",
      subjects: result.subjects,
    };
    this.publishInventory();
  }

  private async loadDetail(
    context: NonNullable<ReturnType<KafkaApplicationSession["clusterServiceContext"]>>,
    connectionName: string | null,
    identity: Extract<SchemaCommand, { readonly command: "schemas.load" }>["payload"],
    signal: AbortSignal,
  ): Promise<void> {
    this.detail = {
      ...this.detail,
      connectionName,
      endpoint: context.baseUrl,
      state: "loading",
      subject: identity.subject,
    };
    this.publishDetail();
    const result = await this.options.port!.loadSubject(context, identity, signal);
    signal.throwIfAborted();
    this.detail = {
      ...result,
      connectionName,
      endpoint: context.baseUrl,
      refreshedAt: this.options.now().toISOString(),
      state: "ready",
      subject: identity.subject,
    };
    this.publishDetail();
  }

  private async loadLatestDetail(
    context: NonNullable<ReturnType<KafkaApplicationSession["clusterServiceContext"]>>,
    connectionName: string | null,
    subject: string,
    signal: AbortSignal,
  ): Promise<void> {
    const result = await this.options.port!.loadLatestSubject(context, subject, signal);
    signal.throwIfAborted();
    this.detail = {
      ...result,
      connectionName,
      endpoint: context.baseUrl,
      refreshedAt: this.options.now().toISOString(),
      state: "ready",
      subject,
    };
    this.publishDetail();
  }

  private operation(command: SchemaCommand): string {
    switch (command.command) {
      case "schemas.list":
        return "Load schemas";
      case "schemas.load":
        return "Load schema detail";
      case "schemas.compatibility.check":
        return "Check schema compatibility";
      case "schemas.register":
        return "Register schema version";
      case "schemas.delete":
        return "Delete schema";
    }
  }

  private object(command: SchemaCommand): string {
    return command.command === "schemas.list"
      ? "Schema Registry"
      : command.command === "schemas.delete"
        ? command.payload.target.subject
        : command.payload.subject;
  }

  private publishInventory(): void {
    this.options.publish(inventoryEvent(this.inventory, this.options.nextSequence()));
  }

  private publishDetail(): void {
    this.options.publish(detailEvent(this.detail, this.options.nextSequence()));
  }

  private recordFailure(command: SchemaCommand, correlationId: string, error: HostError): void {
    this.options.recordActivity({
      correlationId,
      detail: `${error.summary} ${error.recovery}`,
      object: this.object(command),
      operation: this.operation(command),
      outcome: error.code === "CANCELLED" ? "cancelled" : "failed",
      severity: "error",
    });
  }
}
