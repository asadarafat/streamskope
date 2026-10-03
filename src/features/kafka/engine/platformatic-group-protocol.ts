import type { GroupBase } from "@platformatic/kafka";

import { KafkaEngineFailure } from "./failure";

/** The client's DescribeGroups decoder assumes Kafka's consumer subscription format. */
export async function requireConsumerGroupProtocol(
  admin: { listGroups(): Promise<Map<string, GroupBase>> },
  groupId: string,
): Promise<void> {
  const group = (await admin.listGroups()).get(groupId);
  if (!group) throw new Error(`Kafka did not return consumer group ${groupId}.`);
  // Empty groups can retain committed offsets while reporting no protocol.
  if (group.protocolType !== "consumer" && group.protocolType !== "")
    throw new KafkaEngineFailure({
      code: "UNSUPPORTED_OPERATION",
      stage: "kafka",
      retryable: false,
      summary:
        "This group uses an unsupported coordination protocol; it is not a Kafka consumer group.",
      recovery:
        "Select a consumer-protocol group. Connect worker coordination is not a consumer subscription.",
    });
}
