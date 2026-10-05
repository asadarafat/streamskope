import { ProviderCommandAdmission } from "../../../platform/providers/operation-ownership";
import {
  NATS_PROTOCOL_VERSION,
  NATS_PROVIDER_EVENT_CODEC,
  NatsContractValidationError,
  parseCorrelatedNatsResponse,
  parseNatsCommand,
  parseNatsEvent,
  type NatsCommand,
  type NatsCommandResponse,
  type NatsEvent,
  type NatsHost,
  type NatsHostError,
  type NatsProfilesSnapshot,
} from "../contracts";
import { natsIdentifier, natsInteger } from "../contracts/validation-primitives";
import type { NatsProfileService } from "../application/profile-service";
import { safeNatsOperationFailure, natsCleanupFailure } from "../application/failure";
import type {
  NatsApplicationSession,
  NatsSessionChange,
  NatsSessionContext,
} from "../application/session";

export interface NatsBackendFacadeOptions {
  readonly createCorrelationId?: () => string;
}

function errorStage(command: NatsCommand): NatsHostError["stage"] {
  switch (command.command) {
    case "profiles.list":
    case "profiles.create":
    case "profiles.update":
    case "profiles.delete":
      return "profiles";
    case "profiles.connect":
    case "connection.disconnect":
      return "connection";
    case "subscription.start":
    case "subscription.stop":
      return "subscription";
  }
}

/** Correlates typed requests and safe events while owning their complete completion barriers. */
export class NatsBackendFacade implements NatsHost {
  private readonly admission = new ProviderCommandAdmission();
  private readonly listeners = new Set<(event: NatsEvent) => void>();
  private readonly createCorrelationId;
  private readonly unsubscribeSession;
  private sequence = 0;
  private available = true;
  private shutdownPromise: Promise<void> | undefined;

  constructor(
    private readonly session: NatsApplicationSession,
    private readonly profiles: NatsProfileService,
    options: NatsBackendFacadeOptions = {},
  ) {
    this.createCorrelationId =
      options.createCorrelationId ?? ((): string => globalThis.crypto.randomUUID());
    this.unsubscribeSession = session.subscribe((change) => this.publishSessionChange(change));
  }

  snapshot(): ReturnType<NatsApplicationSession["snapshot"]> {
    return this.session.snapshot();
  }

  execute<Command extends NatsCommand>(
    submitted: Command,
  ): Promise<NatsCommandResponse<Command["command"]>> {
    const submittedCopy = { ...submitted };
    let command: NatsCommand;
    let context: NatsSessionContext;
    try {
      command = parseNatsCommand(submittedCopy);
      context = {
        operation: command.command,
        correlationId: natsIdentifier(this.createCorrelationId()),
      };
    } catch (error) {
      return Promise.reject(error instanceof Error ? error : new NatsContractValidationError());
    }
    if (!this.admission.accepts(false))
      return Promise.resolve(
        parseCorrelatedNatsResponse(
          this.failure(
            command,
            context,
            {
              code: "unavailable",
              summary: "The NATS provider is unavailable.",
              recovery: "Restart StreamSkope to create a new NATS session.",
            },
            "lifecycle",
          ),
          submittedCopy,
        ),
      );
    return this.admission.track(() =>
      this.run(command, context).then((response) => {
        try {
          return parseCorrelatedNatsResponse(response, submittedCopy);
        } catch {
          // Do not join shutdown here: it waits for this admitted response to settle.
          void this.shutdown().catch(() => undefined);
          return parseCorrelatedNatsResponse(
            this.failure(
              command,
              context,
              {
                code: "unavailable",
                summary: "The NATS provider could not return a valid response.",
                recovery: "Restart StreamSkope to create a new NATS session.",
              },
              "lifecycle",
            ),
            submittedCopy,
          );
        }
      }),
    );
  }

  subscribe(listener: (event: NatsEvent) => void): () => void {
    const subscriber = (event: NatsEvent): void => listener(event);
    this.listeners.add(subscriber);
    try {
      this.deliver(subscriber, this.availability());
    } catch {
      this.listeners.delete(subscriber);
      void this.shutdown().catch(() => undefined);
      throw new NatsContractValidationError();
    }
    return (): void => {
      this.listeners.delete(subscriber);
    };
  }

  /** Selected-provider cleanup confirms the real subscription; connection remains owned. */
  stopStream(): Promise<void> {
    // Join before admission tracking: shutdown already waits for admitted operations.
    if (this.shutdownPromise !== undefined) return this.shutdownPromise;
    let context: NatsSessionContext;
    try {
      context = {
        operation: "subscription.stop",
        correlationId: natsIdentifier(this.createCorrelationId()),
      };
    } catch (error) {
      return Promise.reject(error instanceof Error ? error : new NatsContractValidationError());
    }
    return this.admission.track(() => this.session.stop(context).then(() => undefined));
  }

  shutdown(): Promise<void> {
    if (this.shutdownPromise !== undefined) return this.shutdownPromise;
    this.admission.close();
    this.available = false;
    let complete!: () => void;
    let fail!: (error: unknown) => void;
    const barrier = new Promise<void>((resolve, reject) => {
      complete = resolve;
      fail = reject;
    });
    this.shutdownPromise = barrier;
    // The barrier is visible before availability listeners can synchronously request cleanup.
    let publicationFailures: readonly unknown[] = [];
    try {
      this.publish(this.availability());
    } catch (error) {
      publicationFailures = [error];
    }
    // Cleanup must still run if event encoding or sequence authority has failed.
    this.completeShutdown(publicationFailures).then(complete, fail);
    return barrier;
  }

  private async run(
    command: NatsCommand,
    context: NatsSessionContext,
  ): Promise<NatsCommandResponse> {
    const base = { version: NATS_PROTOCOL_VERSION, id: command.id, ok: true as const };
    const { correlationId } = context;
    try {
      switch (command.command) {
        case "profiles.list": {
          const profiles = await this.profiles.list();
          return {
            ...base,
            command: command.command,
            result: { correlationId, profiles, ...this.session.snapshot() },
          };
        }
        case "profiles.create": {
          const profiles = await this.profiles.create(command.payload.profile);
          this.publishProfiles(profiles, context);
          return { ...base, command: command.command, result: { correlationId, profiles } };
        }
        case "profiles.update": {
          const profiles = await this.profiles.update(
            command.payload.profileId,
            command.payload.expectedRevision,
            command.payload.profile,
          );
          this.publishProfiles(profiles, context);
          return { ...base, command: command.command, result: { correlationId, profiles } };
        }
        case "profiles.delete": {
          const profiles = await this.profiles.delete(
            command.payload.profileId,
            command.payload.expectedRevision,
          );
          this.publishProfiles(profiles, context);
          return { ...base, command: command.command, result: { correlationId, profiles } };
        }
        case "profiles.connect": {
          // Reserve synchronously in session.connect, before its FIFO credential resolution awaits.
          const connection = await this.session.connect(
            command.payload.profileId,
            (signal) =>
              this.profiles.resolve(
                command.payload.profileId,
                command.payload.expectedRevision,
                signal,
              ),
            context,
          );
          return { ...base, command: command.command, result: { correlationId, connection } };
        }
        case "connection.disconnect":
          return {
            ...base,
            command: command.command,
            result: { correlationId, ...(await this.session.disconnect(context)) },
          };
        case "subscription.start":
          return {
            ...base,
            command: command.command,
            result: {
              correlationId,
              subscription: await this.session.start(command.payload.subject, context),
            },
          };
        case "subscription.stop":
          return {
            ...base,
            command: command.command,
            result: { correlationId, subscription: await this.session.stop(context) },
          };
      }
    } catch (error) {
      return this.failure(command, context, safeNatsOperationFailure(error), errorStage(command));
    }
  }

  private failure(
    command: NatsCommand,
    context: NatsSessionContext,
    failure: ReturnType<typeof safeNatsOperationFailure>,
    stage: NatsHostError["stage"],
  ): NatsCommandResponse {
    return {
      version: NATS_PROTOCOL_VERSION,
      id: command.id,
      command: command.command,
      ok: false,
      error: {
        ...failure,
        stage: failure.code === "validation" ? "validation" : stage,
        operation: context.operation,
        correlationId: context.correlationId,
      },
    };
  }
  private publishProfiles(payload: NatsProfilesSnapshot, context: NatsSessionContext): void {
    if (!this.available) return;
    try {
      this.publishParsed({
        version: NATS_PROTOCOL_VERSION,
        sequence: this.nextSequence(),
        event: "profiles.changed",
        payload,
        ...context,
      });
    } catch {
      // Retire a broken presentation contract without rewriting the genuine commit receipt.
      void this.shutdown().catch(() => undefined);
    }
  }
  private publishSessionChange(change: NatsSessionChange): void {
    if (!this.available) return;
    try {
      this.publishParsed({
        version: NATS_PROTOCOL_VERSION,
        sequence: this.nextSequence(),
        event: change.event,
        payload: change.payload,
        ...change.context,
      });
    } catch {
      // A malformed internal event retires this provider and confirms all actual owned cleanup.
      void this.shutdown().catch(() => undefined);
    }
  }
  private publishParsed(wire: unknown): void {
    this.publish(parseNatsEvent(wire));
  }
  private availability(): NatsEvent {
    return parseNatsEvent(
      NATS_PROVIDER_EVENT_CODEC.availability(
        this.nextSequence(),
        this.available ? "ready" : "unavailable",
        this.available ? undefined : "Restart StreamSkope to create a new NATS session.",
      ),
    );
  }
  private nextSequence(): number {
    this.sequence = natsInteger(this.sequence + 1);
    return this.sequence;
  }
  private publish(event: NatsEvent): void {
    for (const listener of [...this.listeners])
      if (this.listeners.has(listener)) this.deliver(listener, event);
  }
  private deliver(listener: (event: NatsEvent) => void, event: NatsEvent): void {
    try {
      listener(event);
    } catch {
      /* One failing presentation observer cannot revoke another consumer. */
    }
  }
  private async completeShutdown(publicationFailures: readonly unknown[]): Promise<void> {
    const outcomes = await Promise.allSettled([
      Promise.resolve().then(() => this.session.shutdown()),
      this.admission.idle(),
      Promise.resolve().then(() => this.unsubscribeSession()),
    ]);
    this.listeners.clear();
    const failures = [
      ...publicationFailures,
      ...outcomes.flatMap((outcome) =>
        outcome.status === "rejected" ? [outcome.reason as unknown] : [],
      ),
    ];
    if (failures.length > 0)
      throw natsCleanupFailure(
        new AggregateError(failures, "NATS provider shutdown failed.", { cause: failures[0] }),
      );
  }
}
