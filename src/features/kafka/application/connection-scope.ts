import type {
  KafkaConsumerGroupDetails,
  KafkaFetchRequest,
  KafkaWriteInput,
  KafkaWriteOutcome,
} from "../contracts";
import type { ObservationGroupHealth, TopicHealth } from "../contracts/observations";

import { ObservationOperationError, observationAborted } from "./observation-errors";
import type { KafkaActiveConnection, KafkaMessageStream } from "./types";

export interface ConnectionScopeContext {
  readonly connection: KafkaActiveConnection;
  readonly generation: number;
  readonly connectionName: string;
}

export type WriteDispatch =
  | { readonly started: false }
  | { readonly started: true; readonly result: Promise<KafkaWriteOutcome> };

/** Authorizes one reviewed attempt without exposing the adapter or its lifecycle. */
export interface ReviewedWriteScope {
  readonly connectionName: string;
  isCurrent(): boolean;
  readonly reviewWrite?: (input: KafkaWriteInput) => Promise<void>;
  readonly tryDispatchWrite?: (input: KafkaWriteInput) => WriteDispatch;
}

export interface ObservationRecordReader {
  openMessageStream(request: KafkaFetchRequest, signal: AbortSignal): Promise<KafkaMessageStream>;
}

/** Read authority is fenced against connection changes; the sampler owns its reader. */
export interface ObservationScope {
  readonly connectionIdentity: object;
  assertCurrent(signal: AbortSignal): void;
  readonly observeTopicHealth?: (topic: string, signal: AbortSignal) => Promise<TopicHealth>;
  readonly observeConsumerGroup?: (
    groupId: string,
    topic: string,
    partitions: readonly number[],
    signal: AbortSignal,
  ) => Promise<ObservationGroupHealth>;
  readonly describeConsumerGroup?: (
    groupId: string,
    signal: AbortSignal,
  ) => Promise<KafkaConsumerGroupDetails>;
  withRecordReader<T>(
    signal: AbortSignal,
    run: (reader: ObservationRecordReader) => Promise<T>,
  ): Promise<T>;
}

/** Session-owned factories hide connection identity and generation from feature services. */
export class KafkaConnectionScopes {
  private readonly identities = new WeakMap<KafkaActiveConnection, object>();

  constructor(private readonly context: () => ConnectionScopeContext | null) {}

  reviewedWrite(): ReviewedWriteScope | null {
    const active = this.context();
    if (active === null) return null;
    const context = { ...active };
    const { connection } = context;
    return {
      connectionName: context.connectionName,
      isCurrent: (): boolean => this.current(context),
      ...(connection.reviewWrite === undefined
        ? {}
        : {
            reviewWrite: async (input: KafkaWriteInput): Promise<void> => {
              this.assertReviewedCurrent(context);
              await connection.reviewWrite!(input);
              this.assertReviewedCurrent(context);
            },
          }),
      ...(connection.applyWrite === undefined
        ? {}
        : {
            tryDispatchWrite: (input: KafkaWriteInput): WriteDispatch => {
              if (!this.current(context)) return { started: false };
              // No await separates admission from dispatch. A started write keeps its receipt.
              return { started: true, result: connection.applyWrite!(input) };
            },
          }),
    };
  }

  observation(): ObservationScope | null {
    const active = this.context();
    if (active === null) return null;
    const context = { ...active };
    const { connection } = context;
    let identity = this.identities.get(connection);
    if (identity === undefined) {
      identity = {};
      this.identities.set(connection, identity);
    }
    const assertCurrent = (signal: AbortSignal): void => {
      if (signal.aborted) throw observationAborted(signal);
      if (!this.current(context))
        throw new ObservationOperationError(
          "OBSERVATION_DISCONNECTED",
          "The connection changed during observation.",
          "Capture again using the current connection.",
          true,
        );
    };
    const read = async <T>(signal: AbortSignal, run: () => Promise<T>): Promise<T> => {
      assertCurrent(signal);
      const result = await run();
      assertCurrent(signal);
      return result;
    };
    return {
      connectionIdentity: identity,
      assertCurrent,
      ...(connection.observeTopicHealth === undefined
        ? {}
        : {
            observeTopicHealth: (topic: string, signal: AbortSignal): Promise<TopicHealth> =>
              read(signal, () => connection.observeTopicHealth!(topic, signal)),
          }),
      ...(connection.observeConsumerGroup === undefined
        ? {}
        : {
            observeConsumerGroup: (
              groupId: string,
              topic: string,
              partitions: readonly number[],
              signal: AbortSignal,
            ): Promise<ObservationGroupHealth> =>
              read(signal, () =>
                connection.observeConsumerGroup!(groupId, topic, partitions, signal),
              ),
          }),
      ...(connection.describeConsumerGroup === undefined
        ? {}
        : {
            describeConsumerGroup: (
              groupId: string,
              signal: AbortSignal,
            ): Promise<KafkaConsumerGroupDetails> =>
              read(signal, () => connection.describeConsumerGroup!(groupId, signal)),
          }),
      withRecordReader: <T>(
        signal: AbortSignal,
        run: (reader: ObservationRecordReader) => Promise<T>,
      ): Promise<T> =>
        read(signal, () =>
          run({
            openMessageStream: (
              request: KafkaFetchRequest,
              readerSignal: AbortSignal,
            ): Promise<KafkaMessageStream> => {
              assertCurrent(readerSignal);
              // The sampler must receive a late-opened reader so it can close it itself.
              return connection.openMessageStream(request, readerSignal);
            },
          }),
        ),
    };
  }

  private current(expected: ConnectionScopeContext): boolean {
    const current = this.context();
    return (
      current?.connection === expected.connection && current.generation === expected.generation
    );
  }

  private assertReviewedCurrent(expected: ConnectionScopeContext): void {
    if (!this.current(expected))
      throw new Error("The connection changed. Review the destination again.");
  }
}
