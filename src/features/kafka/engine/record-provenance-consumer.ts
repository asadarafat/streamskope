import {
  Consumer,
  connectionsApiChannel,
  protocolAPIsByName,
  type CallbackWithPromise,
  type FetchOptions,
  type fetchV17,
} from "@platformatic/kafka";

import {
  parseKafkaRecordProvenance,
  type KafkaRecordProvenance,
} from "../contracts/record-locator";
import { KafkaRecordLocatorError } from "../application/record-locator-errors";

type FetchResponse = fetchV17.FetchResponse;
type BufferedFetchOptions = FetchOptions<Buffer, Buffer, Buffer, Buffer>;
interface ReaderIdentity {
  readonly clusterId: string;
  readonly topicId: string;
  readonly topic: string;
}
interface WireIdentity {
  readonly version: number;
  readonly topics: readonly string[];
}

/** Fence each public fetch before the SDK buffers its records. No per-record metadata calls. */
export class RecordProvenanceConsumer extends Consumer<Buffer, Buffer, Buffer, Buffer> {
  private identity: ReaderIdentity | undefined;
  private verified = false;
  private unavailable = false;
  private closedProvenance = false;
  private readonly wireIdentities = new WeakMap<object, WireIdentity>();
  private readonly diagnostics: Parameters<typeof connectionsApiChannel.subscribe>[0] = {
    start: (): void => undefined,
    end: (): void => undefined,
    asyncEnd: (): void => undefined,
    error: (): void => undefined,
    asyncStart: (context): void => {
      // This public diagnostic runs before the SDK rewrites legacy topic names to cached UUIDs.
      // It cannot throw: all verification errors return through the owned fetch callback.
      if (
        this.closedProvenance ||
        context.connection.ownerId !== this.instanceId ||
        context.apiKey !== protocolAPIsByName.Fetch ||
        typeof context.apiVersion !== "number"
      )
        return;
      const value = context.result;
      if (value === null || typeof value !== "object" || !("responses" in value)) return;
      const topics = value.responses;
      if (!Array.isArray(topics) || topics.length > 2_048) return;
      const identities: string[] = [];
      for (const topic of topics as readonly unknown[]) {
        if (
          topic === null ||
          typeof topic !== "object" ||
          !("topicId" in topic) ||
          typeof topic.topicId !== "string" ||
          topic.topicId.length > 256
        )
          return;
        identities.push(topic.topicId);
      }
      this.wireIdentities.set(value, { version: context.apiVersion, topics: identities });
    },
  };

  bindRecordIdentity(identity: ReaderIdentity | undefined): void {
    try {
      if (identity !== undefined)
        parseKafkaRecordProvenance({
          clusterId: identity.clusterId,
          topicId: identity.topicId,
          leaderEpoch: 0,
        });
      this.identity = identity === undefined ? undefined : { ...identity };
    } catch {
      this.identity = undefined;
    }
    connectionsApiChannel.subscribe(this.diagnostics);
  }

  releaseRecordProvenance(): void {
    this.closedProvenance = true;
    connectionsApiChannel.unsubscribe(this.diagnostics);
  }

  recordProvenance(leaderEpoch: number): KafkaRecordProvenance | undefined {
    const identity = this.identity;
    if (
      identity === undefined ||
      !this.verified ||
      this.unavailable ||
      this.closedProvenance ||
      !Number.isSafeInteger(leaderEpoch) ||
      leaderEpoch < 0 ||
      leaderEpoch > 2_147_483_647 ||
      this.currentMetadata?.id !== identity.clusterId ||
      this.currentMetadata.topics.get(identity.topic)?.id !== identity.topicId
    )
      return undefined;
    return { clusterId: identity.clusterId, topicId: identity.topicId, leaderEpoch };
  }

  override fetch(options: BufferedFetchOptions, callback: CallbackWithPromise<FetchResponse>): void;
  override fetch(options: BufferedFetchOptions): Promise<FetchResponse>;
  override fetch(
    options: BufferedFetchOptions,
    callback?: CallbackWithPromise<FetchResponse>,
  ): Promise<FetchResponse> | void {
    if (callback === undefined)
      return new Promise<FetchResponse>((resolve, reject) => {
        this.fetch(options, (error, result): void => {
          if (error !== null) reject(error);
          else if (result === undefined) reject(new KafkaRecordLocatorError("unavailable"));
          else resolve(result);
        });
      });
    const identity = this.identity;
    if (
      identity !== undefined &&
      options.topics.some((topic) => topic.topicId !== identity.topicId)
    ) {
      callback(new KafkaRecordLocatorError("resource-replaced"));
      return;
    }
    super.fetch(options, (error, result): void => {
      if (error !== null || result === undefined) {
        callback(error, result);
        return;
      }
      let problem: Error | undefined;
      try {
        this.verifyFetch(result);
      } catch (failure) {
        problem = failure instanceof Error ? failure : new KafkaRecordLocatorError("unavailable");
      }
      if (problem !== undefined) callback(problem);
      else callback(null, result);
    });
  }

  private verifyFetch(result: FetchResponse): void {
    const wire = this.wireIdentities.get(result);
    this.wireIdentities.delete(result);
    const identity = this.identity;
    if (this.closedProvenance) throw new KafkaRecordLocatorError("revoked");
    if (identity === undefined || wire === undefined || wire.version <= 12) {
      // A later modern response must never lend identity to an earlier unverified buffered row.
      this.unavailable = true;
      return;
    }
    if (
      this.currentMetadata?.id !== identity.clusterId ||
      this.currentMetadata.topics.get(identity.topic)?.id !== identity.topicId ||
      wire.topics.some((topic) => topic !== identity.topicId) ||
      result.responses.some((topic) => topic.topicId !== identity.topicId)
    )
      throw new KafkaRecordLocatorError("resource-replaced");
    this.verified = true;
  }
}
