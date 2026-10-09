import { HostContractValidationError } from "./validation-error";
import { exactKeys, record, text } from "./validation-primitives";

export interface KafkaTopicIdentity {
  readonly clusterId: string;
  readonly topicId: string;
  /** Display hint; stable identity is the cluster and topic UUID. */
  readonly topic: string;
}

export function parseKafkaClusterId(value: unknown, path: string): string {
  const clusterId = text(value, path, 256);
  if (!/^[A-Za-z0-9_.-]+$/u.test(clusterId) || /^0+$/u.test(clusterId.replaceAll("-", "")))
    throw new HostContractValidationError(path, "requires a stable cluster ID");
  return clusterId;
}
export function parseKafkaTopicId(value: unknown, path: string): string {
  const topicId = text(value, path, 36);
  if (
    !/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/u.test(topicId) ||
    /^0+$/u.test(topicId.replaceAll("-", ""))
  )
    throw new HostContractValidationError(path, "requires a nonzero Kafka topic UUID");
  return topicId;
}
export function parseKafkaTopicName(value: unknown, path: string): string {
  const topic = text(value, path, 249);
  if (!/^[A-Za-z0-9._-]+$/u.test(topic) || topic === "." || topic === "..")
    throw new HostContractValidationError(path, "requires a valid Kafka topic name");
  return topic;
}
export function parseKafkaTopicIdentity(value: unknown, path = "identity"): KafkaTopicIdentity {
  const input = record(value, path);
  exactKeys(input, ["clusterId", "topicId", "topic"], path);
  return {
    clusterId: parseKafkaClusterId(input.clusterId, `${path}.clusterId`),
    topicId: parseKafkaTopicId(input.topicId, `${path}.topicId`),
    topic: parseKafkaTopicName(input.topic, `${path}.topic`),
  };
}
export function sameKafkaTopicIdentity(a: KafkaTopicIdentity, b: KafkaTopicIdentity): boolean {
  return a.clusterId === b.clusterId && a.topicId === b.topicId;
}
