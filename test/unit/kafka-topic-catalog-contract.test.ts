import { describe, expect, it } from "vitest";

import {
  parseKafkaTopicAnnotation,
  parseKafkaTopicAnnotationSnapshot,
  parseKafkaTopicCatalogSnapshot,
} from "../../src/features/kafka/contracts/topic-catalog";
import { sameKafkaTopicIdentity } from "../../src/features/kafka/contracts/topic-identity";
import {
  inspectKafkaQueryLibraryDocument,
  parseKafkaQueryLibraryDocument,
  serializeKafkaQueryLibraryDocument,
} from "../../src/features/kafka/contracts/query-library";

const identity = {
  clusterId: "production-eu",
  topicId: "27c1c482-b9e0-43f2-abd0-ae257fd6a6df",
  topic: "orders",
};
const annotation = {
  identity,
  description: "Investigate delivery\nRunbook below.",
  owner: "Platform",
  labels: ["critical"],
  links: [{ title: "Runbook", url: "https://example.com/orders" }],
};

describe("local topic catalog contracts", () => {
  it("preserves prose, trims labels and ownership, and allows an explicit empty note", () => {
    expect(
      parseKafkaTopicAnnotation({
        ...annotation,
        owner: " Platform ",
        labels: [" critical "],
        links: [{ title: " Runbook ", url: "https://example.com/orders" }],
      }),
    ).toEqual(annotation);
    expect(
      parseKafkaTopicAnnotation({ identity, description: "", owner: "", labels: [], links: [] }),
    ).toEqual({ identity, description: "", owner: "", labels: [], links: [] });
    expect(
      parseKafkaTopicAnnotation({ ...annotation, description: "界".repeat(1365) + "a" })
        .description,
    ).toHaveLength(1366);
    expect(() =>
      parseKafkaTopicAnnotation({ ...annotation, description: "界".repeat(1365) + "ab" }),
    ).toThrow();
  });
  it.each([
    { owner: "a".repeat(129) },
    { labels: ["Critical", "critical"] },
    { labels: [" "] },
    { labels: ["a".repeat(65)] },
    { labels: Array.from({ length: 17 }, (_, i) => String(i)) },
    {
      links: [
        { title: "Runbook", url: "https://example.com" },
        { title: "runbook", url: "https://example.org" },
      ],
    },
    { links: [{ title: " ", url: "https://example.com" }] },
    { links: [{ title: "a".repeat(129), url: "https://example.com" }] },
    {
      links: Array.from({ length: 9 }, (_, i) => ({
        title: String(i),
        url: "https://example.com",
      })),
    },
    { password: "hidden" },
    { record: { value: "hidden" } },
    { identity: { ...identity, username: "hidden" } },
    { links: [{ title: "runbook", url: "https://example.com", secret: "hidden" }] },
  ])("rejects unbounded, ambiguous or undeclared metadata %j", (change) => {
    expect(() => parseKafkaTopicAnnotation({ ...annotation, ...change })).toThrow();
  });
  it.each([
    "http://example.com",
    "javascript:alert(1)",
    "file:///etc/passwd",
    "https://user:password@example.com",
    "https://example.com/\nsecret",
    "https://example.com/\u007f",
    `https://example.com/${"a".repeat(2048)}`,
  ])("rejects unsafe links %s", (url) => {
    expect(() =>
      parseKafkaTopicAnnotation({ ...annotation, links: [{ title: "Runbook", url }] }),
    ).toThrow();
  });
  it("keys notes by cluster and immutable topic ID, independent of display name", () => {
    expect(sameKafkaTopicIdentity(identity, { ...identity, topic: "renamed" })).toBe(true);
    for (const identityChange of [
      { clusterId: "production-us" },
      { topicId: "11111111-1111-1111-1111-111111111111" },
    ]) {
      const other = { ...annotation, identity: { ...identity, ...identityChange } };
      expect(sameKafkaTopicIdentity(identity, other.identity)).toBe(false);
      expect(
        parseKafkaTopicCatalogSnapshot({ durability: "durable", topics: [annotation, other] })
          .topics,
      ).toHaveLength(2);
    }
    expect(() =>
      parseKafkaTopicCatalogSnapshot({
        durability: "durable",
        topics: [annotation, { ...annotation, identity: { ...identity, topic: "renamed" } }],
      }),
    ).toThrow(/unique/u);
    expect(() =>
      parseKafkaTopicAnnotationSnapshot({
        durability: "durable",
        identity: { ...identity, clusterId: "other" },
        annotation,
      }),
    ).toThrow(/match/u);
  });
  it("inspects actual predecessor formats without rewriting and writes catalog siblings in format4", () => {
    for (const version of [1, 2, 3]) {
      const input = { schemaVersion: version, queries: [] };
      const bytes = JSON.stringify(input);
      expect(inspectKafkaQueryLibraryDocument(input)).toEqual({ ...input, topics: [] });
      expect(parseKafkaQueryLibraryDocument(input)).toEqual({
        schemaVersion: 4,
        queries: [],
        topics: [],
      });
      expect(JSON.stringify(input)).toBe(bytes);
      expect(() => inspectKafkaQueryLibraryDocument({ ...input, topics: [] })).toThrow();
    }
    const current = { schemaVersion: 4, queries: [], topics: [annotation] };
    expect(JSON.parse(serializeKafkaQueryLibraryDocument(current))).toEqual(current);
    expect(parseKafkaQueryLibraryDocument(current)).toEqual(current);
    expect(() => inspectKafkaQueryLibraryDocument({ ...current, schemaVersion: 5 })).toThrow();
  });
});
