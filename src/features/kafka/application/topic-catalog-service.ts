import {
  parseKafkaTopicAnnotation,
  type KafkaTopicAnnotation,
  type KafkaTopicAnnotationSnapshot,
} from "../contracts/topic-catalog";
import {
  parseKafkaTopicIdentity,
  parseKafkaTopicName,
  sameKafkaTopicIdentity,
} from "../contracts/topic-identity";

import type { TopicCatalogScope } from "./connection-scope";
import { KafkaQueryLibraryError, type KafkaQueryLibrary } from "./query-library";

/** Resolves notes against one captured broker identity; all persistence has one library owner. */
export class TopicCatalogService {
  constructor(
    private readonly library: KafkaQueryLibrary,
    private readonly scope: () => TopicCatalogScope | null,
  ) {}

  async load(topic: string): Promise<KafkaTopicAnnotationSnapshot> {
    const { scope, identity } = await this.resolve(topic);
    const snapshot = await this.library.getTopic(identity);
    this.assertCurrent(scope);
    return snapshot;
  }

  async put(
    annotation: KafkaTopicAnnotation,
    expected: KafkaTopicAnnotation | null,
  ): Promise<KafkaTopicAnnotationSnapshot> {
    const validated = parseKafkaTopicAnnotation(annotation);
    const { scope, identity } = await this.resolve(validated.identity.topic);
    if (!sameKafkaTopicIdentity(identity, validated.identity))
      throw new KafkaQueryLibraryError(
        "This topic belongs to a different cluster or was recreated. Refresh Local topic notes and review its current identity. Existing notes were preserved.",
      );
    return this.library.putTopic(validated, expected, () => this.assertCurrent(scope));
  }

  private assertCurrent(scope: TopicCatalogScope): void {
    if (!scope.isCurrent())
      throw new KafkaQueryLibraryError(
        "The connection changed. Refresh Local topic notes before saving; the draft was not committed.",
      );
  }

  private async resolve(
    topic: string,
  ): Promise<{ scope: TopicCatalogScope; identity: KafkaTopicAnnotation["identity"] }> {
    const name = parseKafkaTopicName(topic, "topic");
    const scope = this.scope();
    if (scope?.describeTopicIdentity === undefined)
      throw new KafkaQueryLibraryError(
        "Connect to Kafka with access to stable cluster and topic metadata before editing local notes. Stored notes remain available for review and removal.",
      );
    try {
      const metadata = await scope.describeTopicIdentity(name);
      this.assertCurrent(scope);
      return {
        scope,
        identity: parseKafkaTopicIdentity({
          clusterId: metadata.clusterId,
          topicId: metadata.topicId,
          topic: name,
        }),
      };
    } catch (error) {
      if (error instanceof KafkaQueryLibraryError) throw error;
      throw new KafkaQueryLibraryError(
        "The current topic identity could not be verified. Check the connection, topic existence and Describe permissions, then refresh Local topic notes. Nothing was saved.",
      );
    }
  }
}
