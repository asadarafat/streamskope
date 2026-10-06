import type { NatsContainerlabFixtureIntent, NatsContainerlabFixtureRecord } from "./ownership";

/** Owns daemon resources; the lifecycle owns locking, private journals and protocol readiness. */
export interface NatsPersistentRuntime {
  start(
    intent: NatsContainerlabFixtureIntent,
    saveProgress: (intent: NatsContainerlabFixtureIntent) => Promise<void>,
    signal?: AbortSignal,
  ): Promise<NatsContainerlabFixtureRecord>;
  status(record: NatsContainerlabFixtureRecord): Promise<string>;
  resume(record: NatsContainerlabFixtureRecord, signal?: AbortSignal): Promise<void>;
  /** Resolves only after every owned server, network and volume is confirmed absent. */
  stop(record: NatsContainerlabFixtureRecord): Promise<void>;
  /** Never treats one empty listing as proof that an unsettled create cannot arrive later. */
  recover(intent: NatsContainerlabFixtureIntent): Promise<void>;
}
