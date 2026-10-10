import { createHash } from "node:crypto";

import {
  Admin,
  AclOperations,
  metadataV10,
  deleteTopicsV6,
  findErrorBy,
  type Connection,
} from "@platformatic/kafka";

import {
  parseTopicAdministrationInput,
  parseTopicAdministrationSnapshot,
  sameTopicAdministrationBaseline,
  type TopicAdministrationInput,
  type TopicAdministrationSnapshot,
  type TopicAdministrationOutcome,
} from "../contracts/topic-administration";
import { parseKafkaTopicName } from "../contracts/topic-identity";
import type { MutationDispatch } from "../application/connection-scope";

import type { KafkaClientInput } from "./types";
import { platformaticClientOptions } from "./platformatic-options";
import { OwnedKafkaResources } from "./owned-kafka-resources";

export interface TopicAdministrationClient {
  snapshot(topic: string): Promise<TopicAdministrationSnapshot>;
  tryChange(
    input: TopicAdministrationInput,
    baseline: TopicAdministrationSnapshot,
    signal: AbortSignal,
  ): MutationDispatch<void>;
  deletionVisible(topicId: string): Promise<boolean>;
  close(): Promise<void>;
}
function permission(bits: number, operation: number): "allowed" | "denied" | "unknown" {
  return bits < 0 ? "unknown" : (bits & (1 << operation)) !== 0 ? "allowed" : "denied";
}
/** Public SDK wire APIs let deletion address the reviewed UUID, never a reused name. */
class KafkaTopicAdministrationClient implements TopicAdministrationClient {
  private readonly admin: Admin;
  private controller: Connection | undefined;
  private apis: Awaited<ReturnType<Admin["listApis"]>> = [];
  constructor(private readonly input: KafkaClientInput) {
    this.admin = new Admin({
      ...platformaticClientOptions(input, "streamskope-topic-administration"),
      timeout: input.operationTimeoutMs,
    });
  }
  close(): Promise<void> {
    return this.admin.close();
  }
  private supports(key: number, version: number): boolean {
    return this.apis.some(
      (a) => a.apiKey === key && a.minVersion <= version && a.maxVersion >= version,
    );
  }
  private async prepare(): Promise<{ clusterId: string; controller: Connection }> {
    const metadata = await this.admin.metadata({
      topics: [],
      forceUpdate: true,
      autocreateTopics: false,
    });
    this.apis = await this.admin.listApis();
    if (!this.supports(3, 10))
      throw new Error("Topic UUID metadata is unsupported by this broker.");
    this.controller = (await this.admin.connectToBrokers([metadata.controllerId])).get(
      metadata.controllerId,
    );
    if (!this.controller) throw new Error("The current controller could not be reached.");
    return { clusterId: metadata.id, controller: this.controller };
  }
  async snapshot(topic: string): Promise<TopicAdministrationSnapshot> {
    const { controller } = await this.prepare();
    const metadata = await metadataV10.api.async(controller, [topic], false, true, false);
    const target = metadata.topics.find((t) => t.name === topic);
    if (!target || target.errorCode !== 0 || !target.partitions.length)
      throw new Error("The topic cannot be described.");
    const assignments = [...target.partitions].sort((a, b) => a.partitionIndex - b.partitionIndex);
    if (
      assignments.some(
        (p, i) => p.errorCode !== 0 || p.partitionIndex !== i || !p.replicaNodes.length,
      )
    )
      throw new Error("The complete partition assignment cannot be read.");
    return parseTopicAdministrationSnapshot({
      identity: { clusterId: metadata.clusterId, topicId: target.topicId, topic },
      partitions: assignments.length,
      replicasSha256: createHash("sha256")
        .update(JSON.stringify(assignments.map((p) => [p.partitionIndex, p.replicaNodes])))
        .digest("hex"),
      internal: target.isInternal,
      deleteSupported: this.supports(20, 6),
      deletePermission: permission(target.topicAuthorizedOperations, AclOperations.DELETE),
      expandPermission: permission(target.topicAuthorizedOperations, AclOperations.ALTER),
    });
  }
  tryChange(
    input: TopicAdministrationInput,
    baseline: TopicAdministrationSnapshot,
    signal: AbortSignal,
  ): MutationDispatch<void> {
    if (signal.aborted || !this.controller) return { started: false };
    if (input.kind === "delete") {
      if (!this.supports(20, 6)) return { started: false };
      const result = deleteTopicsV6.api
        .async(
          this.controller,
          [{ name: null, topicId: baseline.identity.topicId }],
          this.input.operationTimeoutMs,
        )
        .then((response) => {
          if (
            response.responses.length !== 1 ||
            response.responses[0]?.topicId !== baseline.identity.topicId ||
            response.responses[0].errorCode !== 0
          )
            throw new Error("Kafka did not return the requested topic UUID acknowledgement.");
        });
      return { started: true, result };
    }
    return {
      started: true,
      result: this.admin.createPartitions({
        topics: [{ name: input.topic, count: input.partitions }],
        validateOnly: false,
      }),
    };
  }
  async deletionVisible(topicId: string): Promise<boolean> {
    if (!this.controller) return false;
    try {
      const response = await metadataV10.api.async(
        this.controller,
        [{ name: null, topicId }],
        false,
        false,
        false,
      );
      // Missing UUID is not inferred from an empty name inventory or permission failure.
      return (
        response.topics.length === 1 &&
        response.topics[0]?.topicId === topicId &&
        response.topics[0].errorCode === 100
      );
    } catch (error) {
      return (
        findErrorBy(error instanceof Error ? error : undefined, "apiId", "UNKNOWN_TOPIC_ID") !==
        null
      );
    }
  }
}

/** Own every admitted client until completion and confirmed close, including late receipts. */
export class PlatformaticTopicAdministration {
  private readonly resources: OwnedKafkaResources;
  constructor(
    private readonly input: KafkaClientInput,
    private readonly lifetime: AbortSignal,
    private readonly createClient: (input: KafkaClientInput) => TopicAdministrationClient = (
      input,
    ) => new KafkaTopicAdministrationClient(input),
  ) {
    this.resources = new OwnedKafkaResources(lifetime);
  }

  private owned<T>(
    run: (client: TopicAdministrationClient) => Promise<T>,
  ): Promise<{ value: T; cleaned: boolean }> {
    return this.resources.run(() => this.createClient(this.input), run);
  }
  async snapshot(topic: string): Promise<TopicAdministrationSnapshot> {
    parseKafkaTopicName(topic, "topic");
    const result = await this.owned(async (client) => {
      const snapshot = await client.snapshot(topic);
      this.lifetime.throwIfAborted();
      return snapshot;
    });
    if (!result.cleaned)
      throw new Error("Topic inspection cleanup is unresolved. Reconnect before continuing.");
    return result.value;
  }
  async apply(
    input: TopicAdministrationInput,
    baseline: TopicAdministrationSnapshot,
  ): Promise<TopicAdministrationOutcome> {
    const parsed = parseTopicAdministrationInput(input),
      expected = parseTopicAdministrationSnapshot(baseline);
    const result = await this.owned(
      async (client): Promise<Omit<TopicAdministrationOutcome, "cleanup">> => {
        let dispatched = false;
        const unsent = (detail: string): Omit<TopicAdministrationOutcome, "cleanup"> => ({
          input: parsed,
          state: "unsent",
          verification: "unavailable",
          detail,
        });
        try {
          const fresh = await client.snapshot(parsed.topic);
          if (
            this.lifetime.aborted ||
            !sameTopicAdministrationBaseline(expected, fresh) ||
            fresh.internal ||
            (parsed.kind === "delete"
              ? !fresh.deleteSupported || fresh.deletePermission === "denied"
              : parsed.partitions <= fresh.partitions || fresh.expandPermission === "denied")
          )
            return unsent(
              "The reviewed topic identity, partition assignment, permissions or connection changed before dispatch. Review again.",
            );
          const operation = client.tryChange(parsed, fresh, this.lifetime);
          if (!operation.started)
            return unsent(
              "The connection was revoked or identity-safe deletion is unsupported. No change was sent.",
            );
          dispatched = true;
          await operation.result;
          let verification: TopicAdministrationOutcome["verification"] = "unavailable";
          if (!this.lifetime.aborted) {
            try {
              if (parsed.kind === "delete")
                verification = (await client.deletionVisible(fresh.identity.topicId))
                  ? "verified"
                  : "different";
              else {
                const actual = await client.snapshot(parsed.topic);
                verification =
                  actual.identity.clusterId === fresh.identity.clusterId &&
                  actual.identity.topicId === fresh.identity.topicId &&
                  actual.partitions === parsed.partitions
                    ? "verified"
                    : "different";
              }
            } catch {
              /* A broker acknowledgement survives failed independent readback. */
            }
          }
          return {
            input: parsed,
            state: "acknowledged",
            verification,
            detail:
              verification === "verified"
                ? "Kafka acknowledged the change; its exact topic identity and requested result were read back."
                : "Kafka acknowledged the change. Readback is different or unavailable; refresh without repeating the mutation.",
          };
        } catch (error) {
          const rejected = [
            "TOPIC_AUTHORIZATION_FAILED",
            "CLUSTER_AUTHORIZATION_FAILED",
            "INVALID_PARTITIONS",
            "INVALID_REPLICA_ASSIGNMENT",
            "INVALID_REQUEST",
            "UNKNOWN_TOPIC_ID",
            "UNKNOWN_TOPIC_OR_PARTITION",
            "TOPIC_DELETION_DISABLED",
          ].some(
            (id) => findErrorBy(error instanceof Error ? error : undefined, "apiId", id) !== null,
          );
          return {
            input: parsed,
            state: !dispatched ? "unsent" : rejected ? "rejected" : "unknown",
            verification: "unavailable",
            detail: !dispatched
              ? "Topic revalidation failed before dispatch; inspect permissions and review again."
              : rejected
                ? "Kafka rejected the change. Inspect permissions and the current resource before reviewing again."
                : "The change may have reached Kafka. Inspect its exact topic identity before another attempt; no automatic retry was made.",
          };
        }
      },
    );
    return {
      ...result.value,
      cleanup: result.cleaned ? "confirmed" : "unresolved",
      detail: result.cleaned
        ? result.value.detail
        : `${result.value.detail} Original client cleanup remains unresolved; reconnect before further changes.`,
    };
  }
  close(): Promise<void> {
    return this.resources.close();
  }
}
