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
import type {
  TopicAdministrationInput,
  TopicAdministrationSnapshot,
  TopicAdministrationOutcome,
} from "../contracts/topic-administration";

import { ObservationOperationError, observationAborted } from "./observation-errors";
import type { KafkaReadCheckpoint } from "./read-checkpoint";
import type {
  KafkaActiveConnection,
  KafkaClusterMetadata,
  KafkaMessageStream,
  KafkaClusterServiceContext,
} from "./types";

export interface ClientQuotaScope extends ReviewAuthority {
  readonly snapshot?: (
    entity: import("../contracts/client-quotas").ClientQuotaEntity,
  ) => Promise<import("../contracts/client-quotas").ClientQuotaSnapshot>;
  readonly tryApply?: (
    input: import("../contracts/client-quotas").ClientQuotaInput,
    baseline: import("../contracts/client-quotas").ClientQuotaSnapshot,
  ) => MutationDispatch<import("../contracts/client-quotas").ClientQuotaOutcome>;
}
export interface GroupAdministrationScope extends ReviewAuthority {
  readonly snapshot?: (
    groupId: string,
  ) => Promise<import("../contracts/group-administration").GroupAdministrationSnapshot>;
  readonly tryDelete?: (
    baseline: import("../contracts/group-administration").GroupAdministrationSnapshot,
  ) => MutationDispatch<import("../contracts/group-administration").GroupAdministrationOutcome>;
}
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

/** Captured service authority; successful mutations survive connection revocation. */
export interface ClusterServiceReviewScope extends ReviewAuthority {
  cleanupUnresolved(): boolean;
  read<T>(
    run: (context: KafkaClusterServiceContext, signal: AbortSignal) => Promise<T>,
    signal: AbortSignal,
  ): Promise<T>;
  tryDispatch<T>(run: (context: KafkaClusterServiceContext) => Promise<T>): MutationDispatch<T>;
}

export type SchemaRegistryReviewScope = Omit<ClusterServiceReviewScope, "cleanupUnresolved">;
export type ConnectReviewScope = ClusterServiceReviewScope;

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
  readonly resolveOffsetReset?: (
    input: import("../contracts/offset-reset").OffsetResetSelectionInput,
  ) => Promise<OffsetResetInput>;
  readonly offsetResetSnapshot?: (input: OffsetResetInput) => Promise<OffsetResetSnapshot>;
  readonly offsetResetExamples?: (
    input: OffsetResetInput,
  ) => Promise<Pick<OffsetResetReview, "examples" | "exampleStatus">>;
  readonly tryResetGroupOffset?: (
    groupId: string,
    target: OffsetResetTarget,
    baseline?: OffsetResetSnapshot,
  ) => MutationDispatch<OffsetResetResult>;
}

export interface TopicAdministrationScope extends ReviewAuthority {
  readonly snapshot?: (topic: string) => Promise<TopicAdministrationSnapshot>;
  readonly tryApply?: (
    input: TopicAdministrationInput,
    baseline: TopicAdministrationSnapshot,
  ) => MutationDispatch<TopicAdministrationOutcome>;
}

/** Metadata-only authority for associating local notes with a real broker resource. */
export interface TopicCatalogScope extends ReviewAuthority {
  readonly describeTopicIdentity?: (topic: string) => Promise<KafkaWriteDestination>;
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

  schemaRegistry(): SchemaRegistryReviewScope | null {
    return this.clusterService("schemaRegistry");
  }

  connect(): ConnectReviewScope | null {
    return this.clusterService("connect");
  }

  private clusterService(name: "schemaRegistry" | "connect"): ClusterServiceReviewScope | null {
    const active = this.context();
    if (active === null) return null;
    const context = { ...active };
    const service = context.connection.clusterServiceContext?.(name);
    if (!service) return null;
    const current = (): boolean =>
      this.current(context) &&
      !service.signal?.aborted &&
      service.requestOwner?.available !== false;
    return {
      connectionName: context.connectionName,
      isCurrent: current,
      cleanupUnresolved: (): boolean => service.requestOwner?.cleanupUnresolved === true,
      read: async <T>(
        run: (value: KafkaClusterServiceContext, signal: AbortSignal) => Promise<T>,
        signal: AbortSignal,
      ): Promise<T> => {
        const combined =
          service.signal === undefined ? signal : AbortSignal.any([signal, service.signal]);
        combined.throwIfAborted();
        if (!current()) throw new Error("Original service authority is unavailable.");
        const result = await this.readReviewed(context, () => run(service, combined));
        combined.throwIfAborted();
        if (!current()) throw new Error("Original service authority is unavailable.");
        return result;
      },
      tryDispatch: <T>(
        run: (value: KafkaClusterServiceContext) => Promise<T>,
      ): MutationDispatch<T> =>
        current() ? this.dispatch(context, () => run(service)) : { started: false },
    };
  }

  topicCatalog(): TopicCatalogScope | null {
    const active = this.context();
    if (active === null) return null;
    const context = { ...active };
    const { connection } = context;
    return {
      connectionName: context.connectionName,
      isCurrent: (): boolean => this.current(context),
      ...(connection.describeTopicIdentity === undefined
        ? {}
        : {
            describeTopicIdentity: (topic: string): Promise<KafkaWriteDestination> =>
              this.readReviewed(context, () => connection.describeTopicIdentity!(topic)),
          }),
    };
  }

  topicAdministration(): TopicAdministrationScope | null {
    const active = this.context();
    if (active === null) return null;
    const context = { ...active },
      { connection } = context;
    return {
      connectionName: context.connectionName,
      isCurrent: (): boolean => this.current(context),
      ...(connection.topicAdministrationSnapshot === undefined
        ? {}
        : {
            snapshot: (topic: string): Promise<TopicAdministrationSnapshot> =>
              this.readReviewed(context, () => connection.topicAdministrationSnapshot!(topic)),
          }),
      ...(connection.applyTopicAdministration === undefined
        ? {}
        : {
            tryApply: (
              input: TopicAdministrationInput,
              baseline: TopicAdministrationSnapshot,
            ): MutationDispatch<TopicAdministrationOutcome> =>
              this.dispatch(context, () => connection.applyTopicAdministration!(input, baseline)),
          }),
    };
  }

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

  clientQuotas(): ClientQuotaScope | null {
    const active = this.context();
    if (!active) return null;
    const context = { ...active },
      { connection } = context;
    return {
      connectionName: context.connectionName,
      isCurrent: () => this.current(context),
      ...(connection.clientQuotaSnapshot === undefined
        ? {}
        : {
            snapshot: (entity: import("../contracts/client-quotas").ClientQuotaEntity) =>
              this.readReviewed(context, () => connection.clientQuotaSnapshot!(entity)),
          }),
      ...(connection.applyClientQuotas === undefined
        ? {}
        : {
            tryApply: (
              input: import("../contracts/client-quotas").ClientQuotaInput,
              baseline: import("../contracts/client-quotas").ClientQuotaSnapshot,
            ) => this.dispatch(context, () => connection.applyClientQuotas!(input, baseline)),
          }),
    };
  }
  groupAdministration(): GroupAdministrationScope | null {
    const active = this.context();
    if (!active) return null;
    const context = { ...active },
      { connection } = context;
    return {
      connectionName: context.connectionName,
      isCurrent: () => this.current(context),
      ...(connection.groupAdministrationSnapshot === undefined
        ? {}
        : {
            snapshot: (groupId: string) =>
              this.readReviewed(context, () => connection.groupAdministrationSnapshot!(groupId)),
          }),
      ...(connection.deleteConsumerGroup === undefined
        ? {}
        : {
            tryDelete: (
              baseline: import("../contracts/group-administration").GroupAdministrationSnapshot,
            ) => this.dispatch(context, () => connection.deleteConsumerGroup!(baseline)),
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
      ...(connection.resolveOffsetReset === undefined
        ? {}
        : {
            resolveOffsetReset: (
              input: import("../contracts/offset-reset").OffsetResetSelectionInput,
            ): Promise<OffsetResetInput> =>
              this.readReviewed(context, () => connection.resolveOffsetReset!(input)),
          }),
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
              baseline?: OffsetResetSnapshot,
            ): MutationDispatch<OffsetResetResult> =>
              this.dispatch(context, () =>
                baseline === undefined
                  ? connection.resetGroupOffset!(groupId, target)
                  : connection.resetGroupOffset!(groupId, target, baseline),
              ),
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
