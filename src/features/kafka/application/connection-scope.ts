import type { KafkaRecordLocator } from "../contracts/record-locator";
import type {
  KafkaAclBinding,
  KafkaConfigurationEntry,
  KafkaConsumerGroupDetails,
  KafkaFetchRequest,
  KafkaWriteInput,
  KafkaWriteOutcome,
} from "../contracts";
import type { ObservationGroupHealth, TopicHealth } from "../contracts/observations";
import type {
  OffsetResetInput,
  OffsetResetResult,
  OffsetResetReview,
  OffsetResetSnapshot,
  OffsetResetTarget,
} from "../contracts/offset-reset";
import type { KafkaWriteDestination } from "../contracts/reviewed-writes";

import { ObservationOperationError, observationAborted } from "./observation-errors";
import type { KafkaReadCheckpoint } from "./read-checkpoint";
import type { KafkaActiveConnection, KafkaClusterMetadata, KafkaMessageStream } from "./types";

export interface ConnectionScopeContext {
  readonly connection: KafkaActiveConnection;
  readonly generation: number;
  readonly connectionName: string;
}

export type MutationDispatch<T> =
  { readonly started: false } | { readonly started: true; readonly result: Promise<T> };

export type WriteDispatch = MutationDispatch<KafkaWriteOutcome>;

export interface ReviewAuthority {
  readonly connectionName: string;
  isCurrent(): boolean;
}

/** Authorizes one reviewed attempt without exposing the adapter or its lifecycle. */
export interface ReviewedWriteScope extends ReviewAuthority {
  readonly reviewWrite?: (input: KafkaWriteInput) => Promise<KafkaWriteDestination | void>;
  readonly tryDispatchWrite?: (input: KafkaWriteInput) => WriteDispatch;
}

export interface AclReviewScope extends ReviewAuthority {
  readonly describeClusterMetadata: (signal?: AbortSignal) => Promise<KafkaClusterMetadata>;
  readonly describeBrokerConfiguration: (
    brokerId: number,
    signal?: AbortSignal,
  ) => Promise<readonly KafkaConfigurationEntry[]>;
  readonly listAcls?: (signal?: AbortSignal) => Promise<readonly KafkaAclBinding[]>;
  readonly tryCreateAcl?: (binding: KafkaAclBinding) => MutationDispatch<void>;
  readonly tryDeleteAcl?: (binding: KafkaAclBinding) => MutationDispatch<void>;
}

export interface OffsetResetScope extends ReviewAuthority {
  readonly offsetResetSnapshot?: (input: OffsetResetInput) => Promise<OffsetResetSnapshot>;
  readonly offsetResetExamples?: (
    input: OffsetResetInput,
  ) => Promise<Pick<OffsetResetReview, "examples" | "exampleStatus">>;
  readonly tryResetGroupOffset?: (
    groupId: string,
    target: OffsetResetTarget,
  ) => MutationDispatch<OffsetResetResult>;
}

export interface ObservationRecordReader {
  openMessageStream(request: KafkaFetchRequest, signal: AbortSignal): Promise<KafkaMessageStream>;
}

/** One captured connection authorizes finite reads; the caller owns every opened reader. */
export interface RecordReadScope extends ReviewAuthority {
  openMessageStream(
    request: KafkaFetchRequest,
    signal: AbortSignal,
    checkpoint?: KafkaReadCheckpoint,
    expectedLocator?: KafkaRecordLocator,
  ): Promise<KafkaMessageStream>;
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

  recordRead(): RecordReadScope | null {
    const active = this.context();
    if (active === null) return null;
    const context = { ...active };
    return {
      connectionName: context.connectionName,
      isCurrent: (): boolean => this.current(context),
      openMessageStream: async (
        request,
        signal,
        checkpoint,
        expectedLocator,
      ): Promise<KafkaMessageStream> => {
        signal.throwIfAborted();
        this.assertReviewedCurrent(context);
        // Do not reject a late-opened reader here: its owner must receive and close it.
        return context.connection.openMessageStream(request, signal, checkpoint, expectedLocator);
      },
    };
  }

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
            reviewWrite: (input: KafkaWriteInput): Promise<KafkaWriteDestination | void> =>
              this.readReviewed(context, () => connection.reviewWrite!(input)),
          }),
      ...(connection.applyWrite === undefined
        ? {}
        : {
            tryDispatchWrite: (input: KafkaWriteInput): WriteDispatch =>
              this.dispatch(context, () => connection.applyWrite!(input)),
          }),
    };
  }

  aclReview(): AclReviewScope | null {
    const active = this.context();
    if (active === null) return null;
    const context = { ...active };
    const { connection } = context;
    return {
      connectionName: context.connectionName,
      isCurrent: (): boolean => this.current(context),
      describeClusterMetadata: (signal?: AbortSignal): Promise<KafkaClusterMetadata> =>
        this.readReviewed(context, () => connection.describeClusterMetadata(signal)),
      describeBrokerConfiguration: (
        brokerId: number,
        signal?: AbortSignal,
      ): Promise<readonly KafkaConfigurationEntry[]> =>
        this.readReviewed(context, () => connection.describeBrokerConfiguration(brokerId, signal)),
      ...(connection.listAcls === undefined
        ? {}
        : {
            listAcls: (signal?: AbortSignal): Promise<readonly KafkaAclBinding[]> =>
              this.readReviewed(context, () => connection.listAcls!(signal)),
          }),
      ...(connection.createAcl === undefined
        ? {}
        : {
            tryCreateAcl: (binding: KafkaAclBinding): MutationDispatch<void> =>
              this.dispatch(context, () => connection.createAcl!(binding)),
          }),
      ...(connection.deleteAcl === undefined
        ? {}
        : {
            tryDeleteAcl: (binding: KafkaAclBinding): MutationDispatch<void> =>
              this.dispatch(context, () => connection.deleteAcl!(binding)),
          }),
    };
  }

  offsetReset(): OffsetResetScope | null {
    const active = this.context();
    if (active === null) return null;
    const context = { ...active };
    const { connection } = context;
    return {
      connectionName: context.connectionName,
      isCurrent: (): boolean => this.current(context),
      ...(connection.offsetResetSnapshot === undefined
        ? {}
        : {
            offsetResetSnapshot: (input: OffsetResetInput): Promise<OffsetResetSnapshot> =>
              this.readReviewed(context, () => connection.offsetResetSnapshot!(input)),
          }),
      ...(connection.offsetResetExamples === undefined
        ? {}
        : {
            offsetResetExamples: (
              input: OffsetResetInput,
            ): Promise<Pick<OffsetResetReview, "examples" | "exampleStatus">> =>
              this.readReviewed(context, () => connection.offsetResetExamples!(input)),
          }),
      ...(connection.resetGroupOffset === undefined
        ? {}
        : {
            tryResetGroupOffset: (
              groupId: string,
              target: OffsetResetTarget,
            ): MutationDispatch<OffsetResetResult> =>
              this.dispatch(context, () => connection.resetGroupOffset!(groupId, target)),
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

  private async readReviewed<T>(
    context: ConnectionScopeContext,
    read: () => Promise<T>,
  ): Promise<T> {
    this.assertReviewedCurrent(context);
    const result = await read();
    this.assertReviewedCurrent(context);
    return result;
  }

  private dispatch<T>(
    context: ConnectionScopeContext,
    send: () => Promise<T>,
  ): MutationDispatch<T> {
    if (!this.current(context)) return { started: false };
    // Admission and adapter invocation share this turn. Once admitted, preserve its outcome.
    try {
      return { started: true, result: send() };
    } catch (error) {
      // A synchronous adapter failure does not establish that nothing reached the broker.
      // eslint-disable-next-line @typescript-eslint/prefer-promise-reject-errors -- Preserve the admitted adapter failure unchanged.
      return { started: true, result: Promise.reject(error) };
    }
  }
}
