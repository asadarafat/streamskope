import { randomUUID } from "node:crypto";

import { Admin, Consumer, AclOperations, findErrorBy } from "@platformatic/kafka";

import {
  parseOffsetResetInput,
  type OffsetResetInput,
  type OffsetResetSelectionInput,
  type OffsetResetTarget,
  type OffsetResetSnapshot,
  type OffsetResetResult,
  type OffsetResetReview,
  type OffsetResetExample,
} from "../contracts/offset-reset";
import type { KafkaMessage } from "../contracts";

import { requireConsumerGroupProtocol } from "./platformatic-group-protocol";
import { platformaticClientOptions } from "./platformatic-options";
import { OwnedKafkaResources } from "./owned-kafka-resources";
import type { KafkaClientInput, KafkaRawMessage } from "./types";

export class PlatformaticOffsetReset {
  private readonly resources: OwnedKafkaResources;
  constructor(
    private readonly input: KafkaClientInput,
    private readonly lifetime: AbortSignal,
    private readonly prepare?: (
      message: KafkaRawMessage,
      signal: AbortSignal,
    ) => Promise<KafkaMessage>,
  ) {
    this.resources = new OwnedKafkaResources(lifetime);
  }

  private admin<T>(run: (admin: Admin) => Promise<T>): Promise<{ value: T; cleaned: boolean }> {
    return this.resources.run(
      () => new Admin(platformaticClientOptions(this.input, "streamskope-offset-review")),
      run,
    );
  }
  private async read<T>(run: (admin: Admin) => Promise<T>): Promise<T> {
    const { value, cleaned } = await this.admin(run);
    if (!cleaned) throw new Error("Original offset review resources did not close cleanly.");
    this.lifetime.throwIfAborted();
    return value;
  }
  close(): Promise<void> {
    return this.resources.close();
  }

  async resolve(input: OffsetResetSelectionInput): Promise<OffsetResetInput> {
    return this.read(async (admin) => {
      await requireConsumerGroupProtocol(admin, input.groupId);
      const timestamp =
        input.position.kind === "timestamp"
          ? BigInt(input.position.timestampMs)
          : input.position.kind === "earliest"
            ? -2n
            : -1n;
      const values = await admin.listOffsets({
        topics: [...new Set(input.partitions.map((p) => p.topic))].map((name) => ({
          name,
          partitions: input.partitions
            .filter((p) => p.topic === name)
            .map((p) => ({ partitionIndex: p.partition, timestamp })),
        })),
      });
      return parseOffsetResetInput({
        groupId: input.groupId,
        targets: input.partitions.map((p) => {
          const offset = values
            .find((t) => t.name === p.topic)
            ?.partitions.find((t) => t.partitionIndex === p.partition)?.offset;
          if (offset === undefined || offset < 0n)
            throw new Error(
              "No retained offset matches the selected time or partition. Choose another position and preview again.",
            );
          return { ...p, offset: offset.toString() };
        }),
      });
    });
  }
  async snapshot(input: OffsetResetInput): Promise<OffsetResetSnapshot> {
    const parsed = parseOffsetResetInput(input);
    return this.read((admin) => this.inspect(admin, parsed));
  }
  private async inspect(admin: Admin, parsed: OffsetResetInput): Promise<OffsetResetSnapshot> {
    await requireConsumerGroupProtocol(admin, parsed.groupId);
    const topics = [...new Set(parsed.targets.map((t) => t.topic))];
    const lookup = (timestamp: bigint): Parameters<Admin["listOffsets"]>[0] => ({
      topics: topics.map((name) => ({
        name,
        partitions: parsed.targets
          .filter((t) => t.topic === name)
          .map((t) => ({ partitionIndex: t.partition, timestamp })),
      })),
    });
    const [metadata, groups, offsets, low, high] = await Promise.all([
      admin.metadata({ topics, forceUpdate: true, autocreateTopics: false }),
      admin.describeGroups({ groups: [parsed.groupId], includeAuthorizedOperations: true }),
      admin.listConsumerGroupOffsets({ groups: [parsed.groupId], requireStable: false }),
      admin.listOffsets(lookup(-2n)),
      admin.listOffsets(lookup(-1n)),
    ]);
    const group = groups.get(parsed.groupId);
    if (!group || group.error || !metadata.id)
      throw new Error("Consumer group or cluster identity could not be described.");
    const permission = group.authorizedOperations;
    return {
      clusterId: metadata.id,
      topics: topics.map((topic) => {
        const topicId = metadata.topics.get(topic)?.id;
        if (!topicId || topicId === "00000000-0000-0000-0000-000000000000")
          throw new Error(
            "Topic UUID unavailable; offset mutation requires positive identity evidence.",
          );
        return { topic, topicId };
      }),
      inactive: ["Empty", "EMPTY"].includes(group.state) && group.members.size === 0,
      state: group.state,
      groupRead:
        permission < 0
          ? "unknown"
          : (permission & (1 << AclOperations.READ)) !== 0
            ? "allowed"
            : "denied",
      partitions: parsed.targets.map((target) => {
        const find = (values: typeof low): bigint | undefined =>
          values
            .flatMap((t) => (t.name === target.topic ? t.partitions : []))
            .find((p) => p.partitionIndex === target.partition)?.offset;
        const earliest = find(low),
          end = find(high);
        if (earliest === undefined || end === undefined || earliest < 0n || end < earliest)
          throw new Error("Partition bounds unavailable.");
        const before = offsets
          .find((g) => g.groupId === parsed.groupId)
          ?.topics.find((t) => t.name === target.topic)
          ?.partitions.find((p) => p.partitionIndex === target.partition)?.committedOffset;
        const position = BigInt(target.offset),
          previous = before === undefined || before < 0n ? null : before;
        const upper = previous === null ? end : previous < end ? previous : end;
        return {
          ...target,
          before: previous?.toString() ?? null,
          low: earliest.toString(),
          high: end.toString(),
          replayUpperBound: (upper > position ? upper - position : 0n).toString(),
        };
      }),
    };
  }
  async examples(
    input: OffsetResetInput,
  ): Promise<Pick<OffsetResetReview, "examples" | "exampleStatus">> {
    const signal = AbortSignal.any([this.lifetime, AbortSignal.timeout(5_000)]);
    const { value, cleaned } = await this.resources.run(
      () => {
        const consumer = new Consumer<Buffer, Buffer, Buffer, Buffer>({
          ...platformaticClientOptions(
            { ...this.input, operationTimeoutMs: Math.min(5_000, this.input.operationTimeoutMs) },
            "streamskope-reset-examples",
          ),
          groupId: `streamskope-reset-preview-${randomUUID()}`,
          autocommit: false,
          autocreateTopics: false,
        });
        return { consumer, close: (): Promise<void> => consumer.close(true) };
      },
      async ({ consumer }): Promise<Pick<OffsetResetReview, "examples" | "exampleStatus">> => {
        const examples: OffsetResetExample[] = [];
        let unavailable = !this.prepare;
        if (this.prepare)
          try {
            for (const target of input.targets.slice(0, 3)) {
              signal.throwIfAborted();
              const stream = await consumer.consume({
                topics: [target.topic],
                mode: "manual",
                offsets: [
                  {
                    topic: target.topic,
                    partition: target.partition,
                    offset: BigInt(target.offset),
                  },
                ],
                autocommit: false,
                maxFetches: 2,
                maxBytes: 65_536,
                maxBytesPerPartition: 65_536,
                maxWaitTime: 100,
                highWaterMark: 1,
              });
              const stop = (): void => {
                stream.destroy();
              };
              signal.addEventListener("abort", stop, { once: true });
              try {
                signal.throwIfAborted();
                for await (const raw of stream) {
                  if (
                    raw.topic !== target.topic ||
                    raw.partition !== target.partition ||
                    raw.offset < BigInt(target.offset)
                  )
                    continue;
                  const message = await this.prepare(raw, signal);
                  examples.push({
                    topic: target.topic,
                    partition: target.partition,
                    offset: raw.offset.toString(),
                    key: message.key?.slice(0, 512) ?? null,
                    value: message.payload?.slice(0, 512) ?? null,
                  });
                  break;
                }
                signal.throwIfAborted();
              } finally {
                signal.removeEventListener("abort", stop);
                stream.destroy();
              }
            }
          } catch {
            unavailable = true;
          }
        return {
          examples,
          exampleStatus: unavailable ? "unavailable" : examples.length ? "sampled" : "empty",
        };
      },
    );
    if (!cleaned)
      throw new Error(
        "Original offset preview resources did not close cleanly; cleanup remains unresolved.",
      );
    return value;
  }
  async apply(
    groupId: string,
    target: OffsetResetTarget,
    baseline?: OffsetResetSnapshot,
  ): Promise<OffsetResetResult> {
    if (!baseline || !this.resources.available)
      return {
        ...target,
        state: "unsent",
        observed: null,
        verified: false,
        cleanup: this.resources.cleanupUnresolved ? "unresolved" : "confirmed",
      };
    const { value, cleaned } = await this.admin(async (admin): Promise<OffsetResetResult> => {
      let result: OffsetResetResult = {
        ...target,
        state: "unknown",
        observed: null,
        verified: false,
        cleanup: "confirmed",
      };
      let dispatched = false;
      try {
        const fresh = await this.inspect(admin, { groupId, targets: [target] });
        const before = baseline.partitions.find(
            (p) => p.topic === target.topic && p.partition === target.partition,
          ),
          position = fresh.partitions[0]!;
        if (
          this.lifetime.aborted ||
          !before ||
          !fresh.inactive ||
          fresh.groupRead === "denied" ||
          fresh.clusterId !== baseline.clusterId ||
          fresh.topics[0]?.topicId !==
            baseline.topics.find((t) => t.topic === target.topic)?.topicId ||
          position.before !== before.before ||
          BigInt(target.offset) < BigInt(position.low) ||
          BigInt(target.offset) > BigInt(position.high)
        )
          return { ...result, state: "unsent" };
        dispatched = true;
        // Admission owns the actual reply; cancellation cannot replace an acknowledgement.
        await admin.alterConsumerGroupOffsets({
          groupId,
          topics: [
            {
              name: target.topic,
              partitionOffsets: [{ partition: target.partition, offset: BigInt(target.offset) }],
            },
          ],
        });
        result = { ...result, state: "acknowledged" };
        if (this.lifetime.aborted) return result;
        const groups = await admin.listConsumerGroupOffsets({
          groups: [groupId],
          requireStable: false,
        });
        const observed = groups
          .find((g) => g.groupId === groupId)
          ?.topics.find((t) => t.name === target.topic)
          ?.partitions.find((p) => p.partitionIndex === target.partition)?.committedOffset;
        result = {
          ...result,
          observed: observed === undefined || observed < 0n ? null : observed.toString(),
          verified: observed?.toString() === target.offset,
        };
      } catch (error) {
        if (result.state !== "acknowledged")
          result = {
            ...result,
            state: !dispatched
              ? "unsent"
              : [
                    "GROUP_AUTHORIZATION_FAILED",
                    "TOPIC_AUTHORIZATION_FAILED",
                    "NON_EMPTY_GROUP",
                    "UNKNOWN_TOPIC_OR_PARTITION",
                    "ILLEGAL_GENERATION",
                    "UNKNOWN_MEMBER_ID",
                  ].some((id) =>
                    findErrorBy(error instanceof Error ? error : undefined, "apiId", id),
                  )
                ? "rejected"
                : "unknown",
          };
      }
      return result;
    });
    return { ...value, cleanup: cleaned ? "confirmed" : "unresolved" };
  }
}
