import { randomUUID } from "node:crypto";

import { Admin, Consumer, AclOperations, findErrorBy } from "@platformatic/kafka";

import {
  parseOffsetResetInput,
  type OffsetResetInput,
  type OffsetResetTarget,
  type OffsetResetSnapshot,
  type OffsetResetResult,
  type OffsetResetReview,
  type OffsetResetExample,
} from "../contracts/offset-reset";

import { platformaticClientOptions } from "./platformatic-options";
import type { KafkaClientInput } from "./types";

function bounded<T>(operation: Promise<T>, signal: AbortSignal): Promise<T> {
  return new Promise((resolve, reject) => {
    const abort = (): void => reject(new Error("Offset operation interrupted."));
    signal.addEventListener("abort", abort, { once: true });
    if (signal.aborted) abort();
    operation
      .then(resolve, reject)
      .finally(() => signal.removeEventListener("abort", abort))
      .catch(() => undefined);
  });
}
export class PlatformaticOffsetReset {
  constructor(
    private readonly input: KafkaClientInput,
    private readonly lifetime: AbortSignal,
  ) {}
  private async admin<T>(run: (admin: Admin, signal: AbortSignal) => Promise<T>): Promise<T> {
    const admin = new Admin(platformaticClientOptions(this.input, "streamskope-offset-review"));
    const signal = AbortSignal.any([
      this.lifetime,
      AbortSignal.timeout(this.input.operationTimeoutMs),
    ]);
    try {
      signal.throwIfAborted();
      return await bounded(run(admin, signal), signal);
    } finally {
      await admin.close();
    }
  }
  async snapshot(input: OffsetResetInput): Promise<OffsetResetSnapshot> {
    const parsed = parseOffsetResetInput(input);
    return this.admin(async (admin) => {
      const topics = [...new Set(parsed.targets.map((t) => t.topic))];
      const lookup = (timestamp: bigint): Parameters<Admin["listOffsets"]>[0] => ({
        topics: topics.map((name) => ({
          name,
          partitions: parsed.targets
            .filter((t) => t.topic === name)
            .map((t) => ({ partitionIndex: t.partition, timestamp })),
        })),
      });
      const [groups, offsets, low, high] = await Promise.all([
        admin.describeGroups({ groups: [parsed.groupId], includeAuthorizedOperations: true }),
        admin.listConsumerGroupOffsets({ groups: [parsed.groupId], requireStable: false }),
        admin.listOffsets(lookup(-2n)),
        admin.listOffsets(lookup(-1n)),
      ]);
      const group = groups.get(parsed.groupId);
      if (!group) throw new Error("Consumer group could not be described.");
      const permission = group.authorizedOperations;
      return {
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
          const position = BigInt(target.offset);
          const previous = before === undefined || before < 0n ? null : before;
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
    });
  }
  async examples(
    input: OffsetResetInput,
  ): Promise<Pick<OffsetResetReview, "examples" | "exampleStatus">> {
    const examples: OffsetResetExample[] = [];
    const signal = AbortSignal.any([this.lifetime, AbortSignal.timeout(5_000)]);
    const consumer = new Consumer<Buffer, Buffer, Buffer, Buffer>({
      ...platformaticClientOptions(this.input, "streamskope-reset-examples"),
      groupId: `streamskope-reset-preview-${randomUUID()}`,
      autocommit: false,
      autocreateTopics: false,
    });
    let unavailable = false;
    try {
      for (const target of input.targets.slice(0, 3)) {
        signal.throwIfAborted();
        const stream = await bounded(
          consumer.consume({
            topics: [target.topic],
            mode: "manual",
            offsets: [
              { topic: target.topic, partition: target.partition, offset: BigInt(target.offset) },
            ],
            autocommit: false,
            maxFetches: 2,
            maxBytes: 65_536,
            maxBytesPerPartition: 65_536,
            maxWaitTime: 100,
            highWaterMark: 1,
          }),
          signal,
        );
        try {
          await bounded(
            (async (): Promise<void> => {
              for await (const message of stream) {
                if (
                  message.partition !== target.partition ||
                  message.offset < BigInt(target.offset)
                )
                  continue;
                examples.push({
                  topic: target.topic,
                  partition: target.partition,
                  offset: message.offset.toString(),
                  key: message.key?.subarray(0, 192).toString("base64") ?? null,
                  value: message.value?.subarray(0, 192).toString("base64") ?? null,
                });
                break;
              }
            })(),
            signal,
          );
        } finally {
          stream.destroy();
        }
      }
    } catch {
      unavailable = true;
    } finally {
      consumer.close(true);
    }
    return {
      examples,
      exampleStatus: unavailable ? "unavailable" : examples.length ? "sampled" : "empty",
    };
  }
  async apply(groupId: string, target: OffsetResetTarget): Promise<OffsetResetResult> {
    let result: OffsetResetResult = {
      ...target,
      state: "unknown",
      observed: null,
      verified: false,
    };
    let dispatched = false;
    try {
      await this.admin(async (admin, signal) => {
        signal.throwIfAborted();
        dispatched = true;
        await bounded(
          admin.alterConsumerGroupOffsets({
            groupId,
            topics: [
              {
                name: target.topic,
                partitionOffsets: [{ partition: target.partition, offset: BigInt(target.offset) }],
              },
            ],
          }),
          signal,
        );
        result = { ...result, state: "acknowledged" };
        const groups = await bounded(
          admin.listConsumerGroupOffsets({ groups: [groupId], requireStable: false }),
          signal,
        );
        const observed = groups
          .find((g) => g.groupId === groupId)
          ?.topics.find((t) => t.name === target.topic)
          ?.partitions.find((p) => p.partitionIndex === target.partition)?.committedOffset;
        result = {
          ...result,
          observed: observed === undefined || observed < 0n ? null : observed.toString(),
          verified: observed?.toString() === target.offset,
        };
      });
    } catch (error) {
      if (
        result.state !== "acknowledged" &&
        (!dispatched ||
          [
            "GROUP_AUTHORIZATION_FAILED",
            "TOPIC_AUTHORIZATION_FAILED",
            "NON_EMPTY_GROUP",
            "UNKNOWN_TOPIC_OR_PARTITION",
            "ILLEGAL_GENERATION",
            "UNKNOWN_MEMBER_ID",
          ].some((id) => findErrorBy(error instanceof Error ? error : undefined, "apiId", id)))
      )
        result = { ...result, state: "rejected" };
    }
    return result;
  }
}
