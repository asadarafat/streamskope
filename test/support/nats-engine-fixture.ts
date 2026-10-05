import type { ServerInfo, Status } from "@nats-io/transport-node";

import type {
  NatsSdkConnection,
  NatsSdkMessage,
  NatsSdkSubscription,
} from "../../src/features/nats/engine/sdk-types";

export interface Deferred<Value> {
  readonly promise: Promise<Value>;
  readonly resolve: (value: Value) => void;
  readonly reject: (reason: unknown) => void;
}

export function natsDeferred<Value>(): Deferred<Value> {
  let resolve!: (value: Value) => void;
  let reject!: (reason: unknown) => void;
  const promise = new Promise<Value>((complete, fail) => {
    resolve = complete;
    reject = fail;
  });
  return { promise, resolve, reject };
}

class StatusFeed implements AsyncIterable<Status> {
  private readonly pending: Status[] = [];
  private notification = natsDeferred<void>();
  private done = false;

  push(status: Status): void {
    this.pending.push(status);
    this.notification.resolve();
  }

  close(): void {
    this.done = true;
    this.notification.resolve();
  }

  async *[Symbol.asyncIterator](): AsyncGenerator<Status> {
    while (!this.done || this.pending.length > 0) {
      if (this.pending.length === 0) {
        await this.notification.promise;
        this.notification = natsDeferred<void>();
      }
      while (this.pending.length > 0) {
        const status = this.pending.shift();
        if (status !== undefined) yield status;
      }
    }
  }
}

export class NatsSubscriptionFixture implements NatsSdkSubscription {
  readonly closure = natsDeferred<void | Error>();
  readonly closed = this.closure.promise;
  unsubscribeCalls = 0;
  unsubscribeError: Error | undefined;
  private done = false;

  constructor(
    readonly subject: string,
    private readonly callback: (error: Error | null, message: NatsSdkMessage) => void,
  ) {}

  emit(message: NatsSdkMessage): void {
    // Intentionally allow stale fixture callbacks after close to prove fencing.
    this.callback(null, message);
  }

  fail(error: Error, placeholder: NatsSdkMessage): void {
    this.callback(error, placeholder);
    this.finish(error);
  }

  finish(error?: Error): void {
    this.done = true;
    this.closure.resolve(error);
  }

  unsubscribe(): void {
    this.unsubscribeCalls += 1;
    if (this.unsubscribeError !== undefined) throw this.unsubscribeError;
    this.finish();
  }

  isClosed(): boolean {
    return this.done;
  }
}

/** Supported SDK subset only; real socket/TLS qualification is a separate fixture. */
export class NatsConnectionFixture implements NatsSdkConnection {
  info?: Pick<ServerInfo, "tls_required">;
  readonly subscriptions: NatsSubscriptionFixture[] = [];
  readonly closure = natsDeferred<void | Error>();
  private readonly statuses = new StatusFeed();
  private done = false;
  flushCalls = 0;
  closeCalls = 0;
  statusEnded = false;
  flushOperation: (() => Promise<void>) | undefined;
  closeOperation: (() => Promise<void>) | undefined;
  onIsClosed: (() => void) | undefined;
  onSubscribe: ((subscription: NatsSubscriptionFixture) => void) | undefined;

  subscribe(
    subject: string,
    options: { readonly callback: (error: Error | null, message: NatsSdkMessage) => void },
  ): NatsSdkSubscription {
    const subscription = new NatsSubscriptionFixture(subject, options.callback);
    this.subscriptions.push(subscription);
    this.onSubscribe?.(subscription);
    return subscription;
  }

  flush(): Promise<void> {
    this.flushCalls += 1;
    return this.flushOperation?.() ?? Promise.resolve();
  }

  async close(): Promise<void> {
    this.closeCalls += 1;
    await this.closeOperation?.();
    this.finishClose();
  }

  finishClose(error?: Error): void {
    this.done = true;
    for (const subscription of this.subscriptions) subscription.finish(error);
    this.closure.resolve(error);
    this.statuses.close();
  }

  closed(): Promise<void | Error> {
    return this.closure.promise;
  }

  isClosed(): boolean {
    this.onIsClosed?.();
    return this.done;
  }

  pushStatus(status: Status): void {
    this.statuses.push(status);
  }

  async *status(): AsyncGenerator<Status> {
    try {
      yield* this.statuses;
    } finally {
      this.statusEnded = true;
    }
  }
}

export function natsMessage(data = "fixture", subject = "qualification.events"): NatsSdkMessage {
  return { subject, reply: "", data: new TextEncoder().encode(data) };
}
