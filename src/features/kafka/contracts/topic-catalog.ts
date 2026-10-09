import { parseKafkaRunbookUrl } from "./operational-preference-validation";
import {
  parseKafkaTopicIdentity,
  sameKafkaTopicIdentity,
  type KafkaTopicIdentity,
} from "./topic-identity";
import { HostContractValidationError } from "./validation-error";
import {
  boundedText,
  boundedUtf8Text,
  declaredValue,
  exactKeys,
  record,
  text,
} from "./validation-primitives";

export const KAFKA_TOPIC_CATALOG_LIMITS = Object.freeze({
  topics: 256,
  descriptionBytes: 4_096,
  ownerCharacters: 128,
  labels: 16,
  labelCharacters: 64,
  links: 8,
  linkTitleCharacters: 128,
  linkUrlCharacters: 2_048,
});
export interface KafkaTopicCatalogLink {
  readonly title: string;
  readonly url: string;
}
export interface KafkaTopicAnnotation {
  readonly identity: KafkaTopicIdentity;
  readonly description: string;
  readonly owner: string;
  readonly labels: readonly string[];
  readonly links: readonly KafkaTopicCatalogLink[];
}
export interface KafkaTopicAnnotationSnapshot {
  readonly durability: "session" | "durable";
  readonly identity: KafkaTopicIdentity;
  readonly annotation: KafkaTopicAnnotation | null;
}
export interface KafkaTopicCatalogSnapshot {
  readonly durability: "session" | "durable";
  readonly topics: readonly KafkaTopicAnnotation[];
}
function title(value: unknown, path: string, maximum: number): string {
  const parsed = text(value, path, maximum).trim();
  if (!parsed) throw new HostContractValidationError(path, "must not be blank");
  return parsed;
}
function array(value: unknown, path: string, maximum: number): readonly unknown[] {
  if (!Array.isArray(value) || value.length > maximum)
    throw new HostContractValidationError(path, `must contain at most ${String(maximum)} entries`);
  return value;
}
export function parseKafkaTopicAnnotation(
  value: unknown,
  path = "annotation",
): KafkaTopicAnnotation {
  const input = record(value, path);
  exactKeys(input, ["identity", "description", "owner", "labels", "links"], path);
  const labels = array(input.labels, `${path}.labels`, KAFKA_TOPIC_CATALOG_LIMITS.labels).map(
    (value, index) =>
      title(value, `${path}.labels[${String(index)}]`, KAFKA_TOPIC_CATALOG_LIMITS.labelCharacters),
  );
  const links = array(input.links, `${path}.links`, KAFKA_TOPIC_CATALOG_LIMITS.links).map(
    (value, index): KafkaTopicCatalogLink => {
      const field = `${path}.links[${String(index)}]`;
      const link = record(value, field);
      exactKeys(link, ["title", "url"], field);
      const url = text(link.url, `${field}.url`, KAFKA_TOPIC_CATALOG_LIMITS.linkUrlCharacters);
      if (
        [...url].some(
          (character) => character.charCodeAt(0) < 32 || character.charCodeAt(0) === 127,
        )
      )
        throw new HostContractValidationError(
          `${field}.url`,
          "must not contain control characters",
        );
      return {
        title: title(link.title, `${field}.title`, KAFKA_TOPIC_CATALOG_LIMITS.linkTitleCharacters),
        url: parseKafkaRunbookUrl(url, `${field}.url`),
      };
    },
  );
  if (new Set(labels.map((label) => label.toLowerCase())).size !== labels.length)
    throw new HostContractValidationError(`${path}.labels`, "labels must be unique");
  if (new Set(links.map((link) => link.title.toLowerCase())).size !== links.length)
    throw new HostContractValidationError(`${path}.links`, "link titles must be unique");
  return {
    identity: parseKafkaTopicIdentity(input.identity, `${path}.identity`),
    description: boundedUtf8Text(
      input.description,
      `${path}.description`,
      KAFKA_TOPIC_CATALOG_LIMITS.descriptionBytes,
    ),
    owner: boundedText(
      input.owner,
      `${path}.owner`,
      KAFKA_TOPIC_CATALOG_LIMITS.ownerCharacters,
    ).trim(),
    labels,
    links,
  };
}
export function parseKafkaTopicCatalogEntries(
  value: unknown,
  path = "topics",
): readonly KafkaTopicAnnotation[] {
  const topics = array(value, path, KAFKA_TOPIC_CATALOG_LIMITS.topics).map((entry, index) =>
    parseKafkaTopicAnnotation(entry, `${path}[${String(index)}]`),
  );
  const identities = topics.map(({ identity }) => `${identity.clusterId}/${identity.topicId}`);
  if (new Set(identities).size !== topics.length)
    throw new HostContractValidationError(path, "topic resource identities must be unique");
  return topics;
}
export function parseKafkaTopicAnnotationSnapshot(value: unknown): KafkaTopicAnnotationSnapshot {
  const input = record(value, "catalog");
  exactKeys(input, ["durability", "identity", "annotation"], "catalog");
  const identity = parseKafkaTopicIdentity(input.identity);
  const annotation = input.annotation === null ? null : parseKafkaTopicAnnotation(input.annotation);
  if (annotation !== null && !sameKafkaTopicIdentity(identity, annotation.identity))
    throw new HostContractValidationError(
      "catalog.annotation",
      "must match the selected topic identity",
    );
  return {
    durability: declaredValue(input.durability, ["session", "durable"], "catalog.durability"),
    identity,
    annotation,
  };
}
export function parseKafkaTopicCatalogSnapshot(value: unknown): KafkaTopicCatalogSnapshot {
  const input = record(value, "catalog");
  exactKeys(input, ["durability", "topics"], "catalog");
  return {
    durability: declaredValue(input.durability, ["session", "durable"], "catalog.durability"),
    topics: parseKafkaTopicCatalogEntries(input.topics),
  };
}
