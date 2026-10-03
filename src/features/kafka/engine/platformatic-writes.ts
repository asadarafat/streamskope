import { randomUUID } from "node:crypto";

import { Admin, Consumer, Producer, findErrorBy, type ClusterMetadata } from "@platformatic/kafka";

import { parseKafkaWriteInput, type KafkaWriteInput, type KafkaWriteOutcome } from "../contracts";

import { platformaticClientOptions } from "./platformatic-options";
import type { KafkaClientInput } from "./types";

function abortable<T>(operation: Promise<T>, signal: AbortSignal): Promise<T> {
  return new Promise((resolve, reject) => {
    const abort = (): void => reject(new Error("Kafka write was interrupted."));
    if (signal.aborted) abort();
    else signal.addEventListener("abort", abort, { once: true });
    operation
      .then(resolve, reject)
      .finally(() => signal.removeEventListener("abort", abort))
      .catch(() => undefined);
  });
}

function validateDestination(
  input: KafkaWriteInput,
  metadata: ClusterMetadata,
  exists: boolean,
): void {
  const topic = metadata.topics.get(input.topic);
  if (input.kind === "record") {
    if (topic === undefined || input.partition >= topic.partitionsCount)
      throw new Error(
        "The destination topic or partition is not available. Refresh topics and review again.",
      );
  } else {
    if (exists) throw new Error("This topic already exists. No existing topic will be changed.");
    if (input.replicationFactor > metadata.brokers.size)
      throw new Error("Replication factor exceeds the available broker count.");
  }
}

/** Owns the short-lived clients; no automatic topic creation or send retries. */
export class PlatformaticReviewedWrites {
  constructor(
    private readonly input: KafkaClientInput,
    private readonly lifetime: AbortSignal,
  ) {}

  async review(
    input: KafkaWriteInput,
  ): Promise<import("../contracts/reviewed-writes").KafkaWriteDestination> {
    const parsed = parseKafkaWriteInput(input);
    const admin = new Admin(platformaticClientOptions(this.input, "streamskope-write-review"));
    const signal = AbortSignal.any([
      this.lifetime,
      AbortSignal.timeout(this.input.operationTimeoutMs),
    ]);
    try {
      signal.throwIfAborted();
      return await this.validate(admin, parsed, signal);
    } finally {
      await admin.close();
    }
  }

  async apply(input: KafkaWriteInput): Promise<KafkaWriteOutcome> {
    const parsed = parseKafkaWriteInput(input);
    const signal = AbortSignal.any([
      this.lifetime,
      AbortSignal.timeout(this.input.operationTimeoutMs),
    ]);
    const admin = new Admin(platformaticClientOptions(this.input, "streamskope-write"));
    let producer: Producer<Buffer | null, Buffer | null, Buffer, Buffer | null> | undefined;
    let sent = false;
    let outcome: KafkaWriteOutcome;
    try {
      signal.throwIfAborted();
      await this.validate(admin, parsed, signal);
      signal.throwIfAborted();
      if (parsed.kind === "topic") {
        sent = true;
        await abortable(
          admin.createTopics({
            topics: [
              {
                topic: parsed.topic,
                partitions: parsed.partitions,
                replicas: parsed.replicationFactor,
              },
            ],
            configs: parsed.configs.map(({ name, value }) => ({ name, value })),
          }),
          signal,
        );
        outcome = {
          state: "acknowledged",
          detail: "Kafka acknowledged topic creation.",
          receipt: null,
          verification: "unavailable",
        };
        try {
          const actual = (
            await abortable(
              admin.metadata({
                topics: [parsed.topic],
                forceUpdate: true,
                autocreateTopics: false,
              }),
              signal,
            )
          ).topics.get(parsed.topic);
          if (
            actual?.partitionsCount === parsed.partitions &&
            actual.partitions.every(
              (partition) => partition.replicas.length === parsed.replicationFactor,
            )
          )
            outcome = {
              ...outcome,
              verification: "verified",
              detail: "Kafka acknowledged creation; partition and replica counts were read back.",
            };
        } catch {
          /* The acknowledgement survives a failed metadata refresh. */
        }
      } else {
        producer = new Producer({
          ...platformaticClientOptions(this.input, "streamskope-produce"),
          autocreateTopics: false,
          repeatOnStaleMetadata: false,
        });
        const decode = (value: string | null): Buffer | null =>
          value === null ? null : Buffer.from(value, "base64");
        // Distinct Buffer keys preserve repeated header names and their order.
        const headers = new Map(
          parsed.record.headers.map((entry) => [
            Buffer.from(entry.key, "base64"),
            decode(entry.value),
          ]),
        );
        sent = true;
        const result = await abortable(
          producer.send({
            acks: -1,
            autocreateTopics: false,
            repeatOnStaleMetadata: false,
            messages: [
              {
                topic: parsed.topic,
                ...(parsed.timestamp === undefined ? {} : { timestamp: BigInt(parsed.timestamp) }),
                partition: parsed.partition,
                key: decode(parsed.record.key),
                value: decode(parsed.record.value),
                headers,
              },
            ],
          }),
          signal,
        );
        const receipt = result.offsets?.find(
          (item) => item.topic === parsed.topic && item.partition === parsed.partition,
        );
        outcome =
          receipt === undefined || receipt.offset < 0n
            ? {
                state: "unknown",
                detail:
                  "Kafka returned no usable offset. Inspect the destination before another attempt.",
                receipt: null,
                verification: "unavailable",
              }
            : {
                state: "acknowledged",
                detail:
                  "Kafka acknowledged the record. Read-back verification is unavailable; do not resend it to retry verification.",
                receipt: {
                  topic: receipt.topic,
                  partition: receipt.partition,
                  offset: receipt.offset.toString(),
                },
                verification: "unavailable",
              };
        if (receipt !== undefined && receipt.offset >= 0n) {
          try {
            if (await this.verifyRecord(parsed, receipt.offset, signal))
              outcome = {
                ...outcome,
                detail:
                  "Kafka acknowledged the record and exact key, value and ordered header bytes were read back at its offset.",
                verification: "verified",
              };
          } catch {
            /* Read-back permission/transport failure cannot undo a broker acknowledgement. */
          }
        }
      }
    } catch (error) {
      const negativeAcknowledgement = [
        "TOPIC_AUTHORIZATION_FAILED",
        "CLUSTER_AUTHORIZATION_FAILED",
        "INVALID_TOPIC_EXCEPTION",
        "INVALID_CONFIG",
        "INVALID_REPLICATION_FACTOR",
        "INVALID_PARTITIONS",
        "TOPIC_ALREADY_EXISTS",
        "MESSAGE_TOO_LARGE",
        "RECORD_LIST_TOO_LARGE",
        "INVALID_RECORD",
        "NOT_ENOUGH_REPLICAS",
      ].some((id) => findErrorBy(error instanceof Error ? error : undefined, "apiId", id) !== null);
      const rejected = !sent || negativeAcknowledgement;
      outcome = {
        state: rejected ? "rejected" : "unknown",
        detail: rejected
          ? "Kafka did not accept this operation. Check the destination, permissions and settings before reviewing again."
          : "The write may have reached Kafka. Inspect the destination before creating another review; no automatic retry was attempted.",
        receipt: null,
        verification: "not-applicable",
      };
    } finally {
      const cleanup = await Promise.allSettled([
        admin.close(),
        ...(producer === undefined ? [] : [Promise.resolve().then(() => producer!.close(true))]),
      ]);
      if (cleanup.some((result) => result.status === "rejected")) {
        // Cleanup failure never replaces an acknowledgement or causes a resend.
        outcome = {
          ...outcome!,
          detail: `${outcome!.detail} Reconnect: a temporary client did not close cleanly.`,
        };
      }
    }
    return outcome;
  }

  private async validate(
    admin: Admin,
    input: KafkaWriteInput,
    signal: AbortSignal,
  ): Promise<import("../contracts/reviewed-writes").KafkaWriteDestination> {
    const exists = (await abortable(admin.listTopics(), signal)).includes(input.topic);
    const metadata = await abortable(
      admin.metadata({
        topics: input.kind === "record" && exists ? [input.topic] : [],
        forceUpdate: true,
        autocreateTopics: false,
      }),
      signal,
    );
    validateDestination(input, metadata, exists);
    return {
      clusterId: metadata.id,
      topicId: metadata.topics.get(input.topic)?.id ?? "",
      partitions: metadata.topics.get(input.topic)?.partitionsCount ?? 0,
    };
  }

  private async verifyRecord(
    input: Extract<KafkaWriteInput, { kind: "record" }>,
    offset: bigint,
    signal: AbortSignal,
  ): Promise<boolean> {
    signal.throwIfAborted();
    const consumer = new Consumer<Buffer, Buffer, Buffer, Buffer>({
      ...platformaticClientOptions(this.input, "streamskope-write-readback"),
      groupId: `streamskope-readback-${randomUUID()}`,
      autocommit: false,
    });
    try {
      const stream = await abortable(
        consumer.consume({
          topics: [input.topic],
          mode: "manual",
          offsets: [{ topic: input.topic, partition: input.partition, offset }],
          autocommit: false,
          maxFetches: 4,
          maxBytes: 131_072,
          maxBytesPerPartition: 131_072,
          maxWaitTime: 100,
          highWaterMark: 1,
        }),
        signal,
      );
      const verify = async (): Promise<boolean> => {
        for await (const message of stream) {
          if (message.partition !== input.partition || message.offset !== offset) continue;
          const encode = (value: Buffer | null | undefined): string | null =>
            value == null ? null : value.toString("base64");
          return (
            encode(message.key) === input.record.key &&
            encode(message.value) === input.record.value &&
            JSON.stringify(
              message.headerEntries.map(([key, value]) => ({
                key: key.toString("base64"),
                value: encode(value),
              })),
            ) === JSON.stringify(input.record.headers)
          );
        }
        return false;
      };
      try {
        return await abortable(verify(), signal);
      } finally {
        stream.destroy();
      }
    } finally {
      consumer.close(true);
    }
  }
}
