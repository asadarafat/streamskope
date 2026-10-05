import { connect as sdkConnect, type NodeConnectionOptions } from "@nats-io/transport-node";

import type { NatsEngine, NatsMessageReceipt } from "../application/engine-port";
import { parseNatsConnectionInput } from "../application/profile-record-validation";
import type { NatsConnectionInput } from "../application/profile-types";
import { parseNatsSubject, type NatsSafeFailure } from "../contracts";

import {
  natsEngineFailure,
  normalizeNatsEngineFailure,
  safeNatsFailure,
  subscriptionClosureFailure,
  NatsEngineFailure,
} from "./failure";
import { copyNatsMessage } from "./message-copy";
import { waitForNatsOperation } from "./operation-wait";
import type {
  NatsSdkConnect,
  NatsSdkConnection,
  NatsSdkMessage,
  NatsSdkSubscription,
} from "./sdk-types";

type ConnectOptions = Parameters<NatsEngine["connect"]>[1];
type SubscriptionOptions = Parameters<NatsEngine["startSubscription"]>[1];

interface ConnectionOwner {
  readonly connection: NatsSdkConnection;
  readonly onLoss: (failure: NatsSafeFailure) => void;
  authority: boolean;
  lossReported: boolean;
  closeWork?: Promise<void>;
  closedMonitor?: Promise<void>;
  statusMonitor?: Promise<void>;
}

interface ConnectAttempt {
  readonly controller: AbortController;
  authority: boolean;
  work?: Promise<void>;
  cleanupFailure?: unknown;
  owner?: ConnectionOwner;
}

interface SubscriptionOwner {
  readonly connection: ConnectionOwner;
  readonly controller: AbortController;
  readonly options: SubscriptionOptions;
  authority: boolean;
  failure?: NatsEngineFailure;
  subscription?: NatsSdkSubscription;
  setupWork?: Promise<void>;
  closedMonitor?: Promise<void>;
  stopWork?: Promise<void>;
}

interface CleanupSnapshot {
  readonly attempt?: ConnectAttempt;
  readonly subscription?: SubscriptionOwner;
  readonly connections: readonly ConnectionOwner[];
}

export interface NatsEngineOptions {
  readonly connect?: NatsSdkConnect;
  readonly operationTimeoutMs?: number;
  readonly now?: () => Date;
}

function observed(work: Promise<void>): void {
  void work.catch(() => undefined);
}

/** One owner for actual SDK work, copied callbacks and confirmed subscription cleanup. */
export class StreamSkopeNatsEngine implements NatsEngine {
  private readonly sdk: NatsSdkConnect;
  private readonly timeoutMs: number;
  private readonly now: () => Date;
  private readonly connections = new Set<ConnectionOwner>();
  private attempt: ConnectAttempt | undefined;
  private connection: ConnectionOwner | undefined;
  private subscription: SubscriptionOwner | undefined;
  private ownershipRevision = 0;
  private disconnectRevision = -1;
  private disconnectWork: Promise<void> | undefined;
  private shutdownWork: Promise<void> | undefined;
  private shuttingDown = false;

  constructor(options: NatsEngineOptions = {}) {
    this.sdk = options.connect ?? sdkConnect;
    this.timeoutMs = options.operationTimeoutMs ?? 5_000;
    this.now = options.now ?? ((): Date => new Date());
    if (!Number.isSafeInteger(this.timeoutMs) || this.timeoutMs <= 0 || this.timeoutMs > 60_000)
      throw natsEngineFailure("validation");
  }

  connect(input: NatsConnectionInput, options: ConnectOptions): Promise<void> {
    if (this.shuttingDown) return Promise.reject(natsEngineFailure("unavailable"));
    let validated: NatsConnectionInput;
    try {
      validated = parseNatsConnectionInput(input);
      options.signal?.throwIfAborted();
      this.nextOwnership();
    } catch (error) {
      return Promise.reject(
        natsEngineFailure(options.signal?.aborted === true ? "cancelled" : "validation", error),
      );
    }
    const previous = this.retireCurrent();
    const previousDisconnect = this.disconnectWork;
    const attempt: ConnectAttempt = { controller: new AbortController(), authority: true };
    this.attempt = attempt;
    const onAbort = (): void => this.revokeAttempt(attempt);
    options.signal?.addEventListener("abort", onAbort, { once: true });
    const work = Promise.resolve().then(async (): Promise<void> => {
      await this.cleanup(previous);
      await previousDisconnect;
      this.assertAttempt(attempt);
      this.disconnectWork = undefined;
      // Register the real SDK work in this attempt's barrier before invoking it.
      const connection = await Promise.resolve().then(() => this.sdk(this.sdkOptions(validated)));
      const owner = this.ownConnection(connection, options.onConnectionLoss, false);
      attempt.owner = owner;
      if (!attempt.authority || this.attempt !== attempt || this.shuttingDown) {
        try {
          await this.closeConnection(owner);
        } catch (error) {
          attempt.cleanupFailure = error;
          throw natsEngineFailure("cleanup", error);
        }
        throw natsEngineFailure("cancelled");
      }
      // The SDK upgrades server-required TLS even with tls:null. A plaintext
      // profile must not report that differently configured transport as ready.
      if (validated.tls.mode === "plaintext" && connection.info?.tls_required === true) {
        await this.closeConnection(owner);
        throw natsEngineFailure("validation");
      }
      owner.authority = true;
      this.connection = owner;
      if (connection.isClosed()) {
        this.reportLoss(owner, natsEngineFailure("connection").failure);
        await this.closeConnection(owner);
        throw natsEngineFailure("connection");
      }
      if (!attempt.authority || this.attempt !== attempt || this.shuttingDown) {
        await this.closeConnection(owner);
        throw natsEngineFailure("cancelled");
      }
    });
    attempt.work = work;
    observed(work);
    const waiting = waitForNatsOperation(work, this.timeoutMs + 1_000, attempt.controller.signal);
    return waiting
      .catch((error: unknown): never => {
        this.revokeAttempt(attempt);
        throw normalizeNatsEngineFailure(error);
      })
      .finally(() => {
        options.signal?.removeEventListener("abort", onAbort);
      });
  }

  startSubscription(subject: string, options: SubscriptionOptions): Promise<void> {
    if (this.shuttingDown) return Promise.reject(natsEngineFailure("unavailable"));
    let validated: string;
    try {
      validated = parseNatsSubject(subject);
      options.signal?.throwIfAborted();
      this.nextOwnership();
    } catch (error) {
      return Promise.reject(
        natsEngineFailure(options.signal?.aborted === true ? "cancelled" : "validation", error),
      );
    }
    const connection = this.connection;
    if (connection?.authority !== true || connection.connection.isClosed())
      return Promise.reject(natsEngineFailure("not-connected"));
    const previous = this.subscription;
    if (previous !== undefined) this.revokeSubscription(previous);
    const owner: SubscriptionOwner = {
      connection,
      controller: new AbortController(),
      options,
      authority: true,
    };
    this.subscription = owner;
    const onAbort = (): void => {
      this.revokeSubscription(owner);
      observed(this.stopOwnedSubscription(owner));
    };
    options.signal?.addEventListener("abort", onAbort, { once: true });
    const work = Promise.resolve().then(async (): Promise<void> => {
      if (previous !== undefined) await this.stopOwnedSubscription(previous);
      this.assertSubscription(owner);
      const subscription = connection.connection.subscribe(validated, {
        callback: (error, message): void => this.receive(owner, error, message),
      });
      owner.subscription = subscription;
      owner.closedMonitor = subscription.closed.then((error): void => {
        if (!owner.authority) return;
        this.failSubscription(
          owner,
          subscriptionClosureFailure(error) ?? natsEngineFailure("connection"),
        );
      });
      observed(owner.closedMonitor);
      this.assertSubscription(owner);
      await waitForNatsOperation(
        Promise.resolve().then(() => connection.connection.flush()),
        this.timeoutMs,
        owner.controller.signal,
      );
      this.assertSubscription(owner);
      if (subscription.isClosed()) throw owner.failure ?? natsEngineFailure("connection");
    });
    owner.setupWork = work;
    observed(work);
    return work
      .catch((error: unknown): never => {
        const incoming = normalizeNatsEngineFailure(error);
        const failure =
          incoming.failure.code === "cleanup" ? incoming : (owner.failure ?? incoming);
        if (failure.failure.code !== "cancelled") this.failSubscription(owner, failure);
        else this.revokeSubscription(owner);
        observed(this.stopOwnedSubscription(owner));
        throw failure;
      })
      .finally(() => options.signal?.removeEventListener("abort", onAbort));
  }

  stopSubscription(): Promise<void> {
    const owner = this.subscription;
    if (owner === undefined) return Promise.resolve();
    this.revokeSubscription(owner);
    return this.stopOwnedSubscription(owner);
  }

  disconnect(): Promise<void> {
    if (this.disconnectWork !== undefined && this.disconnectRevision === this.ownershipRevision)
      return this.disconnectWork;
    const previous = this.disconnectWork;
    const snapshot = this.retireCurrent();
    this.disconnectRevision = this.ownershipRevision;
    const work = Promise.resolve().then(async (): Promise<void> => {
      const results = await Promise.allSettled([
        this.cleanup(snapshot),
        ...(previous === undefined ? [] : [previous]),
      ]);
      this.assertCleanup(results);
    });
    this.disconnectWork = work;
    observed(work);
    return work;
  }

  shutdown(): Promise<void> {
    if (this.shutdownWork !== undefined) return this.shutdownWork;
    this.shuttingDown = true;
    // Publish the permanent barrier before any owner can synchronously reenter.
    const work = Promise.resolve().then(() => this.disconnect());
    this.shutdownWork = work;
    observed(work);
    return work;
  }

  private sdkOptions(input: NatsConnectionInput): NodeConnectionOptions {
    return {
      servers: [...input.servers],
      timeout: this.timeoutMs,
      reconnect: false,
      waitOnFirstConnect: false,
      ignoreClusterUpdates: true,
      noRandomize: true,
      debug: false,
      tls:
        input.tls.mode === "plaintext"
          ? null
          : {
              rejectUnauthorized: true,
              ...(input.tls.caPem === undefined ? {} : { ca: input.tls.caPem }),
            },
      ...(input.authentication.mode === "none" ? {} : { token: input.authentication.token }),
    };
  }

  private nextOwnership(): void {
    if (this.ownershipRevision === Number.MAX_SAFE_INTEGER) throw natsEngineFailure("unavailable");
    this.ownershipRevision += 1;
  }

  private assertAttempt(attempt: ConnectAttempt): void {
    if (!attempt.authority || this.attempt !== attempt || this.shuttingDown)
      throw natsEngineFailure("cancelled");
  }

  private revokeAttempt(attempt: ConnectAttempt): void {
    attempt.authority = false;
    attempt.controller.abort();
    if (attempt.owner !== undefined) {
      attempt.owner.authority = false;
      if (this.connection === attempt.owner) this.connection = undefined;
      observed(this.closeConnection(attempt.owner));
    }
  }

  private assertSubscription(owner: SubscriptionOwner): void {
    if (owner.failure !== undefined) throw owner.failure;
    if (!owner.authority || this.subscription !== owner || this.shuttingDown)
      throw natsEngineFailure("cancelled");
    if (!owner.connection.authority || this.connection !== owner.connection)
      throw natsEngineFailure("not-connected");
  }

  private revokeSubscription(owner: SubscriptionOwner): void {
    owner.authority = false;
    owner.controller.abort();
  }

  private receive(owner: SubscriptionOwner, error: Error | null, message: NatsSdkMessage): void {
    if (!owner.authority || this.subscription !== owner || !owner.connection.authority) return;
    if (error !== null) {
      this.failSubscription(owner, normalizeNatsEngineFailure(error));
      return;
    }
    try {
      const receipt: NatsMessageReceipt = copyNatsMessage(message, this.now());
      owner.options.onMessage(receipt);
    } catch (failure) {
      this.failSubscription(owner, natsEngineFailure("unavailable", failure));
    }
  }

  private failSubscription(owner: SubscriptionOwner, failure: NatsEngineFailure): void {
    if (owner.failure !== undefined || !owner.authority) return;
    owner.failure = failure;
    this.revokeSubscription(owner);
    try {
      owner.options.onFailure({ ...failure.failure });
    } catch {
      // Presentation observers cannot throw into the SDK reader or skip cleanup.
    }
    observed(this.stopOwnedSubscription(owner));
  }

  private stopOwnedSubscription(owner: SubscriptionOwner): Promise<void> {
    if (owner.stopWork !== undefined) return owner.stopWork;
    this.revokeSubscription(owner);
    const work = Promise.resolve().then(async (): Promise<void> => {
      await owner.setupWork?.catch((error: unknown): void => {
        const failure = normalizeNatsEngineFailure(error);
        if (failure.failure.code === "cleanup") throw failure;
      });
      const subscription = owner.subscription;
      if (subscription === undefined) return;
      const connection = owner.connection;
      try {
        subscription.unsubscribe();
        const closed = await waitForNatsOperation(subscription.closed, this.timeoutMs);
        const failure = subscriptionClosureFailure(closed);
        if (failure !== undefined && failure.failure.code !== "permission") throw failure;
        if (!connection.authority || connection.connection.isClosed()) {
          await this.closeConnection(connection);
          return;
        }
        await waitForNatsOperation(
          Promise.resolve().then(() => connection.connection.flush()),
          this.timeoutMs,
        );
      } catch (failure) {
        this.reportLoss(connection, safeNatsFailure(failure));
        await this.closeConnection(connection);
      }
      await owner.closedMonitor;
    });
    owner.stopWork = work.catch((error: unknown): never => {
      throw natsEngineFailure("cleanup", error);
    });
    observed(owner.stopWork);
    return owner.stopWork;
  }

  private ownConnection(
    connection: NatsSdkConnection,
    onLoss: (failure: NatsSafeFailure) => void,
    authority: boolean,
  ): ConnectionOwner {
    const owner: ConnectionOwner = { connection, onLoss, authority, lossReported: false };
    this.connections.add(owner);
    owner.closedMonitor = connection.closed().then((error): void => {
      if (owner.authority) this.reportLoss(owner, safeNatsFailure(error));
    });
    // A new public SDK status iterator created after closure is never ended by that closure.
    if (!connection.isClosed()) owner.statusMonitor = this.monitorStatus(owner);
    observed(owner.closedMonitor);
    if (owner.statusMonitor !== undefined) observed(owner.statusMonitor);
    return owner;
  }

  private async monitorStatus(owner: ConnectionOwner): Promise<void> {
    try {
      for await (const status of owner.connection.status()) {
        if (!owner.authority) continue;
        if (
          status.type === "disconnect" ||
          status.type === "close" ||
          status.type === "staleConnection"
        )
          this.reportLoss(owner, natsEngineFailure("connection").failure);
        else if (status.type === "error" && status.error instanceof Error) {
          const failure = normalizeNatsEngineFailure(status.error);
          if (failure.failure.code === "permission") continue;
          if (failure.failure.code === "authentication") this.reportLoss(owner, failure.failure);
        }
      }
    } catch (error) {
      if (owner.authority) this.reportLoss(owner, safeNatsFailure(error));
    }
  }

  private reportLoss(owner: ConnectionOwner, failure: NatsSafeFailure): void {
    if (!owner.authority || owner.lossReported) return;
    owner.authority = false;
    owner.lossReported = true;
    if (this.connection === owner) this.connection = undefined;
    const subscription = this.subscription;
    if (subscription?.connection === owner)
      this.failSubscription(subscription, new NatsEngineFailure(failure));
    try {
      owner.onLoss({ ...failure });
    } catch {
      // Observer failure does not change the owned connection cleanup barrier.
    }
    observed(this.closeConnection(owner));
  }

  private closeConnection(owner: ConnectionOwner): Promise<void> {
    if (owner.closeWork !== undefined) return owner.closeWork;
    owner.authority = false;
    const work = Promise.resolve().then(async (): Promise<void> => {
      const results = await Promise.allSettled([
        Promise.resolve().then(() => owner.connection.close()),
        Promise.resolve().then(async (): Promise<void> => {
          await owner.connection.closed();
          await owner.closedMonitor;
          await owner.statusMonitor;
        }),
      ]);
      this.assertCleanup(results);
      this.connections.delete(owner);
    });
    owner.closeWork = work;
    observed(work);
    return work;
  }

  private retireCurrent(): CleanupSnapshot {
    const attempt = this.attempt;
    const subscription = this.subscription;
    if (attempt !== undefined) this.revokeAttempt(attempt);
    if (subscription !== undefined) this.revokeSubscription(subscription);
    for (const owner of this.connections) owner.authority = false;
    this.attempt = undefined;
    this.connection = undefined;
    return {
      ...(attempt === undefined ? {} : { attempt }),
      ...(subscription === undefined ? {} : { subscription }),
      connections: [...this.connections],
    };
  }

  private async cleanup(snapshot: CleanupSnapshot): Promise<void> {
    const results = await Promise.allSettled([
      ...(snapshot.subscription === undefined
        ? []
        : [this.stopOwnedSubscription(snapshot.subscription)]),
      ...snapshot.connections.map((owner) => this.closeConnection(owner)),
      ...(snapshot.attempt === undefined
        ? []
        : [
            Promise.resolve().then(async (): Promise<void> => {
              await snapshot.attempt?.work?.catch(() => undefined);
              if (snapshot.attempt?.cleanupFailure !== undefined)
                throw natsEngineFailure("cleanup", snapshot.attempt.cleanupFailure);
            }),
          ]),
    ]);
    this.assertCleanup(results);
  }

  private assertCleanup(results: readonly PromiseSettledResult<unknown>[]): void {
    const failures = results.flatMap((result) =>
      result.status === "rejected" ? [result.reason as unknown] : [],
    );
    if (failures.length > 0)
      throw natsEngineFailure(
        "cleanup",
        new AggregateError(failures, "NATS owned cleanup failed.", { cause: failures[0] }),
      );
  }
}
