import type { KafkaConsumerGroupDetails } from "../../src/features/kafka/contracts";
import type {
  KafkaAdminFactory,
  KafkaAdminInput,
  KafkaAdminPort,
} from "../../src/features/kafka/engine/types";

export class RecordingAdmin implements KafkaAdminPort {
  closeCalls = 0;
  consumerGroupDetailResult: (() => Promise<KafkaConsumerGroupDetails>) | undefined;

  constructor(
    private readonly listTopicsResult: readonly string[] | (() => Promise<readonly string[]>),
  ) {}

  alterTopicConfiguration(): Promise<void> {
    return Promise.reject(new Error("No topic-configuration operation was configured."));
  }

  close(): Promise<void> {
    this.closeCalls += 1;
    return Promise.resolve();
  }

  describeBrokerConfiguration(): never {
    throw new Error("No broker-configuration operation was configured.");
  }

  describeClusterMetadata(): never {
    throw new Error("No cluster-metadata operation was configured.");
  }

  describeConsumerGroup(): Promise<KafkaConsumerGroupDetails> {
    if (this.consumerGroupDetailResult === undefined) {
      return Promise.reject(new Error("No consumer-group detail operation was configured."));
    }
    return this.consumerGroupDetailResult();
  }

  listConsumerGroups(): Promise<{ readonly groups: []; readonly omittedGroups: 0 }> {
    return Promise.resolve({ groups: [], omittedGroups: 0 });
  }

  listTopics(): Promise<readonly string[]> {
    return typeof this.listTopicsResult === "function"
      ? this.listTopicsResult()
      : Promise.resolve(this.listTopicsResult);
  }

  describeTopicConfiguration(): Promise<readonly never[]> {
    return Promise.reject(new Error("No topic-configuration operation was configured."));
  }
}

export class RecordingAdminFactory implements KafkaAdminFactory {
  readonly inputs: KafkaAdminInput[] = [];

  constructor(private readonly admin: RecordingAdmin) {}

  create(input: KafkaAdminInput): KafkaAdminPort {
    this.inputs.push(input);
    return this.admin;
  }
}
