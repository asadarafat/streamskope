import { ProviderCommandAdmission } from "../../../platform/providers/operation-ownership";
import {
  parseNatsSubject,
  type NatsCommandName,
  type NatsConnectionSnapshot,
  type NatsRecordsBatch,
  type NatsSafeFailure,
  type NatsSubscriptionSnapshot,
} from "../contracts";

import type { NatsEngine } from "./engine-port";
import type { NatsResolvedProfile } from "./profile-types";
import {
  NatsOperationError,
  natsCancelled,
  natsCleanupFailure,
  safeNatsOperationFailure,
} from "./failure";
import { emptyNatsCounters, NatsRecordBuffer } from "./record-buffer";

export interface NatsSessionContext {
  readonly operation: NatsCommandName;
  readonly correlationId: string;
}
export type NatsSessionChange =
  | {
      readonly event: "connection.state";
      readonly payload: NatsConnectionSnapshot;
      readonly context: NatsSessionContext;
    }
  | {
      readonly event: "subscription.changed";
      readonly payload: NatsSubscriptionSnapshot;
      readonly context: NatsSessionContext;
    }
  | {
      readonly event: "records.batch";
      readonly payload: NatsRecordsBatch;
      readonly context: NatsSessionContext;
    };

/** Owns user intentions and presentation; the engine owns actual broker resources. */
export class NatsApplicationSession {
  private readonly work = new ProviderCommandAdmission();
  private readonly listeners = new Set<(change: NatsSessionChange) => void>();
  private connection: NatsConnectionSnapshot = { state: "disconnected", profile: null };
  private subscription: NatsSubscriptionSnapshot = {
    state: "idle",
    generation: null,
    subject: null,
    counters: emptyNatsCounters(),
  };
  private connectionIntent = 0;
  private subscriptionIntent = 0;
  private readonly profileReservations = new Map<number, string>();
  private connectionController: AbortController | undefined;
  private subscriptionController: AbortController | undefined;
  private connectionSetup: Promise<NatsConnectionSnapshot> | undefined;
  private subscriptionSetup: Promise<NatsSubscriptionSnapshot> | undefined;
  private records: NatsRecordBuffer | undefined;
  private cleanupFailed = false;
  private closing = false;
  private shutdownPromise: Promise<void> | undefined;
  constructor(private readonly engine: NatsEngine) {}

  snapshot(): {
    readonly connection: NatsConnectionSnapshot;
    readonly subscription: NatsSubscriptionSnapshot;
  } {
    return { connection: this.connection, subscription: this.subscriptionSnapshot() };
  }
  isProfileInUse(id: string): boolean {
    return [...this.profileReservations.values()].includes(id);
  }
  subscribe(listener: (change: NatsSessionChange) => void): () => void {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }
  connect(
    profileId: string,
    resolve: (signal: AbortSignal) => Promise<NatsResolvedProfile>,
    context: NatsSessionContext,
  ): Promise<NatsConnectionSnapshot> {
    this.assertAvailable();
    const intent = ++this.connectionIntent;
    this.disconnectPromise = undefined;
    this.connectionController?.abort();
    const controller = new AbortController();
    this.connectionController = controller;
    const priorReservations = [...this.profileReservations.keys()];
    this.profileReservations.set(intent, profileId);
    this.revokeSubscription();
    this.connection = { state: "connecting", profile: null };
    const operation = this.work.track(() =>
      Promise.resolve().then(async () => {
        try {
          this.assertConnection(intent, controller.signal);
          await this.engine.disconnect();
          for (const reservation of priorReservations) this.profileReservations.delete(reservation);
          this.assertConnection(intent, controller.signal);
          this.finishStoppedSubscription();
          this.emitSubscription(context);
          const profile = await resolve(controller.signal);
          this.assertConnection(intent, controller.signal);
          this.connection = { state: "connecting", profile: profile.identity };
          this.emitConnection(context);
          await this.engine.connect(profile.connection, {
            signal: controller.signal,
            onConnectionLoss: (failure) => this.connectionLost(intent, failure, context),
          });
          this.assertConnection(intent, controller.signal);
          const confirmed: NatsConnectionSnapshot = {
            state: "connected",
            profile: profile.identity,
          };
          this.connection = confirmed;
          this.emitConnection(context);
          return confirmed;
        } catch (error) {
          if (intent === this.connectionIntent && !this.closing) {
            const failure = this.connection.failure ?? safeNatsOperationFailure(error);
            this.connection = { state: "failed", profile: this.connection.profile, failure };
            this.emitConnection(context);
            try {
              await this.engine.disconnect();
              for (const reservation of [...priorReservations, intent])
                this.profileReservations.delete(reservation);
            } catch (cause) {
              this.cleanupFailed = true;
              if (intent === this.connectionIntent) {
                this.connection = {
                  state: "failed",
                  profile: this.connection.profile,
                  failure: natsCleanupFailure(cause).failure,
                };
                this.emitConnection(context);
              }
              throw natsCleanupFailure(cause);
            }
            throw new NatsOperationError(failure, { cause: error });
          }
          throw error;
        }
      }),
    );
    this.connectionSetup = operation;
    this.emitSubscription(context);
    this.emitConnection(context);
    return operation;
  }

  start(subject: string, context: NatsSessionContext): Promise<NatsSubscriptionSnapshot> {
    this.assertAvailable();
    parseNatsSubject(subject);
    if (this.connection.state !== "connected")
      return Promise.reject(
        new NatsOperationError({
          code: "not-connected",
          summary: "Connect a NATS profile before starting a subscription.",
        }),
      );
    this.stopPromise = undefined;
    const connectionIntent = this.connectionIntent;
    const intent = ++this.subscriptionIntent;
    this.subscriptionController?.abort();
    this.records?.close();
    const controller = new AbortController();
    this.subscriptionController = controller;
    const generation = globalThis.crypto.randomUUID();
    const buffer = new NatsRecordBuffer(
      generation,
      (batch) => {
        if (this.currentSubscription(intent, connectionIntent))
          this.emit({ event: "records.batch", payload: batch, context });
      },
      (error) => this.subscriptionFailed(intent, connectionIntent, error, context),
    );
    this.records = buffer;
    this.subscription = { state: "loading", generation, subject, counters: buffer.counters() };
    const operation = this.work.track(() =>
      Promise.resolve().then(async () => {
        try {
          await this.engine.stopSubscription();
          this.assertSubscription(intent, connectionIntent, controller.signal);
          await this.engine.startSubscription(subject, {
            signal: controller.signal,
            onMessage: (receipt) => {
              if (!this.currentSubscription(intent, connectionIntent) || controller.signal.aborted)
                return;
              try {
                buffer.accept(receipt);
              } catch (error) {
                this.subscriptionFailed(intent, connectionIntent, error, context);
              }
            },
            onFailure: (failure) =>
              this.subscriptionFailed(
                intent,
                connectionIntent,
                new NatsOperationError(failure),
                context,
              ),
          });
          this.assertSubscription(intent, connectionIntent, controller.signal);
          if (this.subscription.state === "failed")
            throw new NatsOperationError(
              this.subscription.failure ?? {
                code: "connection",
                summary: "The NATS subscription failed during setup.",
              },
            );
          this.subscription = {
            state: "streaming",
            generation,
            subject,
            counters: buffer.counters(),
          };
          const confirmed = this.subscriptionSnapshot();
          this.emitSubscription(context);
          buffer.startPublishing();
          return confirmed;
        } catch (error) {
          if (this.currentSubscription(intent, connectionIntent)) {
            buffer.close();
            const incoming = safeNatsOperationFailure(error);
            const failure =
              incoming.code === "cleanup" ? incoming : (this.subscription.failure ?? incoming);
            this.cleanupFailed ||= failure.code === "cleanup";
            this.subscription = {
              state: "failed",
              generation,
              subject,
              counters: buffer.counters(),
              failure,
            };
            this.emitSubscription(context);
            throw new NatsOperationError(failure, { cause: error });
          }
          throw error;
        }
      }),
    );
    this.subscriptionSetup = operation;
    this.emitSubscription(context);
    return operation;
  }

  private stopPromise: Promise<NatsSubscriptionSnapshot> | undefined;
  stop(context: NatsSessionContext): Promise<NatsSubscriptionSnapshot> {
    if (this.shutdownPromise !== undefined)
      return this.shutdownPromise.then(() => this.subscriptionSnapshot());
    if (this.stopPromise !== undefined) return this.stopPromise;
    const intent = ++this.subscriptionIntent;
    this.subscriptionController?.abort();
    this.records?.close();
    const pending = this.subscriptionSetup;
    const snapshot = this.subscriptionSnapshot();
    const stopped: NatsSubscriptionSnapshot = {
      state: snapshot.generation === null ? "idle" : "stopped",
      generation: snapshot.generation,
      subject: snapshot.subject,
      counters: snapshot.counters,
    };
    this.subscription = { ...stopped, state: stopped.generation === null ? "idle" : "stopping" };
    const operation = this.work.track(() =>
      Promise.resolve().then(async () => {
        try {
          await this.engine.stopSubscription();
          if (pending !== undefined) await Promise.allSettled([pending]);
          if (intent === this.subscriptionIntent && !this.closing) {
            this.subscription = stopped;
            this.emitSubscription(context);
          }
          return stopped;
        } catch (error) {
          if (intent === this.subscriptionIntent && !this.closing) {
            this.cleanupFailed = true;
            this.subscription = {
              ...stopped,
              state: "failed",
              failure: natsCleanupFailure(error).failure,
            };
            this.emitSubscription(context);
          }
          throw natsCleanupFailure(error);
        }
      }),
    );
    this.stopPromise = operation;
    this.emitSubscription(context);
    return operation;
  }
  private disconnectPromise:
    | Promise<{
        readonly connection: NatsConnectionSnapshot;
        readonly subscription: NatsSubscriptionSnapshot;
      }>
    | undefined;
  disconnect(context: NatsSessionContext): Promise<{
    readonly connection: NatsConnectionSnapshot;
    readonly subscription: NatsSubscriptionSnapshot;
  }> {
    if (this.shutdownPromise !== undefined) return this.shutdownPromise.then(() => this.snapshot());
    if (this.disconnectPromise !== undefined) return this.disconnectPromise;
    const intent = ++this.connectionIntent;
    this.connectionController?.abort();
    this.revokeSubscription();
    const pending = [this.connectionSetup, this.subscriptionSetup].filter(
      (value) => value !== undefined,
    );
    const reservations = [...this.profileReservations.keys()];
    const priorSubscription = this.subscriptionSnapshot();
    this.connection = { state: "disconnecting", profile: this.connection.profile };
    const operation = this.work.track(() =>
      Promise.resolve().then(async () => {
        try {
          await this.engine.disconnect();
          await Promise.allSettled(pending);
          for (const reservation of reservations) this.profileReservations.delete(reservation);
          const subscription: NatsSubscriptionSnapshot = {
            state: priorSubscription.generation === null ? "idle" : "stopped",
            generation: priorSubscription.generation,
            subject: priorSubscription.subject,
            counters: priorSubscription.counters,
          };
          const connection: NatsConnectionSnapshot = { state: "disconnected", profile: null };
          if (intent === this.connectionIntent && !this.closing) {
            this.subscription = subscription;
            this.cleanupFailed = false;
            this.connection = connection;
            this.emitSubscription(context);
            this.emitConnection(context);
          }
          return { connection, subscription };
        } catch (error) {
          if (intent === this.connectionIntent && !this.closing) {
            this.cleanupFailed = true;
            this.connection = {
              state: "failed",
              profile: this.connection.profile,
              failure: natsCleanupFailure(error).failure,
            };
            this.emitConnection(context);
          }
          throw natsCleanupFailure(error);
        }
      }),
    );
    this.disconnectPromise = operation;
    operation.then(
      () => {
        if (this.disconnectPromise === operation) this.disconnectPromise = undefined;
      },
      () => undefined,
    );
    this.emitSubscription(context);
    this.emitConnection(context);
    return operation;
  }
  shutdown(): Promise<void> {
    if (this.shutdownPromise !== undefined) return this.shutdownPromise;
    this.closing = true;
    this.work.close();
    this.connectionIntent += 1;
    this.connectionController?.abort();
    this.revokeSubscription();
    const operation = Promise.resolve().then(async () => {
      const results = await Promise.allSettled([this.engine.shutdown(), this.work.idle()]);
      this.listeners.clear();
      if (results.every((result) => result.status === "fulfilled"))
        this.profileReservations.clear();
      const failures = results.flatMap((result) =>
        result.status === "rejected" ? [result.reason as unknown] : [],
      );
      if (failures.length > 0)
        throw natsCleanupFailure(
          new AggregateError(failures, "NATS shutdown failed.", { cause: failures[0] }),
        );
    });
    this.shutdownPromise = operation;
    return operation;
  }
  private subscriptionSnapshot(): NatsSubscriptionSnapshot {
    return {
      ...this.subscription,
      counters: this.records?.counters() ?? this.subscription.counters,
    };
  }
  private revokeSubscription(): void {
    this.subscriptionIntent += 1;
    this.subscriptionController?.abort();
    this.records?.close();
    this.stopPromise = undefined;
    if (this.subscription.generation !== null && this.subscription.state !== "stopped")
      this.subscription = { ...this.subscriptionSnapshot(), state: "stopping" };
  }
  private finishStoppedSubscription(): void {
    if (this.subscription.state !== "stopping") return;
    const snapshot = this.subscriptionSnapshot();
    this.subscription = {
      state: "stopped",
      generation: snapshot.generation,
      subject: snapshot.subject,
      counters: snapshot.counters,
    };
  }
  private currentSubscription(intent: number, connectionIntent: number): boolean {
    return (
      !this.closing &&
      intent === this.subscriptionIntent &&
      connectionIntent === this.connectionIntent
    );
  }
  private assertAvailable(): void {
    if (this.closing)
      throw new NatsOperationError({
        code: "unavailable",
        summary: "The NATS provider is shutting down.",
      });
    if (this.cleanupFailed) throw natsCleanupFailure();
  }
  private assertConnection(intent: number, signal: AbortSignal): void {
    if (this.closing || intent !== this.connectionIntent || signal.aborted) throw natsCancelled();
  }
  private assertSubscription(intent: number, connectionIntent: number, signal: AbortSignal): void {
    if (!this.currentSubscription(intent, connectionIntent) || signal.aborted)
      throw natsCancelled();
  }
  private connectionLost(
    intent: number,
    failure: NatsSafeFailure,
    context: NatsSessionContext,
  ): void {
    if (this.closing || intent !== this.connectionIntent) return;
    this.connectionController?.abort();
    this.revokeSubscription();
    this.connection = { state: "failed", profile: this.connection.profile, failure };
    if (this.subscription.generation !== null && this.subscription.state !== "stopped")
      this.subscription = {
        ...this.subscriptionSnapshot(),
        state: "failed",
        failure: this.subscription.failure ?? failure,
      };
    const reservations = [...this.profileReservations.keys()];
    this.emitSubscription(context);
    this.emitConnection(context);
    void this.work
      .track(() => this.engine.disconnect())
      .then(
        () => {
          for (const reservation of reservations) this.profileReservations.delete(reservation);
        },
        (cause: unknown) => {
          this.cleanupFailed = true;
          if (!this.closing && intent === this.connectionIntent) {
            this.connection = {
              state: "failed",
              profile: this.connection.profile,
              failure: natsCleanupFailure(cause).failure,
            };
            this.emitConnection(context);
          }
        },
      );
  }
  private subscriptionFailed(
    intent: number,
    connectionIntent: number,
    error: unknown,
    context: NatsSessionContext,
  ): void {
    if (!this.currentSubscription(intent, connectionIntent) || this.subscription.state === "failed")
      return;
    this.subscriptionController?.abort();
    this.records?.close();
    this.subscription = {
      ...this.subscriptionSnapshot(),
      state: "failed",
      failure: safeNatsOperationFailure(error),
    };
    this.emitSubscription(context);
    void this.work
      .track(() => this.engine.stopSubscription())
      .catch((cause: unknown) => {
        if (this.currentSubscription(intent, connectionIntent)) {
          this.cleanupFailed = true;
          this.subscription = {
            ...this.subscriptionSnapshot(),
            state: "failed",
            failure: natsCleanupFailure(cause).failure,
          };
          this.emitSubscription(context);
        }
      });
  }
  private emitConnection(context: NatsSessionContext): void {
    this.emit({ event: "connection.state", payload: this.connection, context });
  }
  private emitSubscription(context: NatsSessionContext): void {
    this.emit({ event: "subscription.changed", payload: this.subscriptionSnapshot(), context });
  }
  private emit(change: NatsSessionChange): void {
    if (this.closing) return;
    for (const listener of this.listeners) {
      try {
        listener(change);
      } catch {
        /* A retired presentation observer cannot throw into the broker callback. */
      }
    }
  }
}
