import { expect, it, vi } from "vitest";

import {
  createKafkaQueryLink,
  serializeKafkaQuery,
} from "../../src/features/kafka/contracts/query-transfer";
import {
  createKafkaPortableView,
  KAFKA_VIEW_TRANSFER_LIMITS,
  parseKafkaInvestigationTransfer,
  parseKafkaPortableView,
  serializeKafkaPortableView,
  type KafkaPortableView,
} from "../../src/features/kafka/contracts/view-transfer";
import { portableViewSettings } from "../../src/features/kafka/ui/investigation-view-settings";

const position = {
  schemaVersion: 1,
  clusterId: "incident-cluster",
  topicId: "4b914431-8917-44aa-ac51-982639d70b7e",
  leaderEpoch: 7,
  topic: "orders",
  partition: 2,
  offset: "9007199254740993",
} as const;
const query = {
  schemaVersion: 1,
  request: { topic: "orders", mode: "earliest", maxMessages: 25 },
} as const;
const view: KafkaPortableView = {
  kind: "streamskope.kafka-view",
  schemaVersion: 1,
  suggestedName: "Failure investigation ☕",
  configuration: query,
  view: {
    schemaVersion: 1,
    destination: { kind: "topic", workspace: "messages" },
    messages: {
      visibleColumns: ["timestamp", "preview", "offset"],
      columnWidths: [{ column: "preview", pixels: 420 }],
      inspectorWidth: 400,
      filtersOpen: true,
    },
  },
  records: {
    selected: position,
    comparison: { ...position, offset: "9007199254740992" },
    bookmarks: [
      { name: "Failure", locator: position },
      {
        name: "Downstream",
        locator: {
          ...position,
          topic: "payments",
          topicId: "bb914431-8917-44aa-ac51-982639d70b7e",
          offset: "42",
        },
      },
    ],
  },
};

it("parses a hand-written descriptor and preserves reviewed identity, bounds and layout", () => {
  const parsed = parseKafkaInvestigationTransfer(JSON.stringify(view));
  expect(parsed).toEqual({ kind: "view", view });
  if (parsed.kind !== "view") throw new Error("Expected portable view");
  expect(parsed.view.records.selected?.offset).toBe("9007199254740993");
  expect(parsed.view.records.comparison?.leaderEpoch).toBe(7);
  expect(parsed.view.records.bookmarks[1]?.locator.topic).toBe("payments");
  expect(parseKafkaInvestigationTransfer(serializeKafkaPortableView(parsed.view))).toEqual(parsed);
});

it("permits a group-only view with unloaded references and no fabricated topic query", () => {
  const group = {
    ...view,
    configuration: null,
    view: { ...view.view, destination: { kind: "consumer-group", groupId: "payments-worker" } },
  };
  expect(parseKafkaPortableView(group)).toEqual(group);
  expect(() => parseKafkaPortableView({ ...view, configuration: null })).toThrow(/topic view/);
});

it("exports explicit fields without source-local IDs or attached private content", () => {
  const source = {
    id: "source-view-id",
    profileId: "source-profile-id",
    password: "fixture-only-secret",
    payload: "fixture-only-record",
    configuration: query,
    view: view.view,
    records: {
      ...view.records,
      bookmarks: view.records.bookmarks.map((bookmark, i) => ({
        ...bookmark,
        id: `source-bookmark-${String(i)}`,
      })),
    },
  };
  const exported = serializeKafkaPortableView(createKafkaPortableView(source, view.suggestedName));
  expect(JSON.parse(exported)).toEqual(view);
  expect(exported).not.toMatch(
    /source-view-id|source-profile-id|source-bookmark|fixture-only|password|payload/,
  );
});

it("allocates no local IDs while reviewing, and fresh transient IDs only when opening", () => {
  const createId = vi
    .fn()
    .mockReturnValueOnce("receiver-a")
    .mockReturnValueOnce("receiver-b")
    .mockReturnValueOnce("receiver-c")
    .mockReturnValueOnce("receiver-d");
  const parsed = parseKafkaPortableView(view);
  expect(createId).not.toHaveBeenCalled();
  const first = portableViewSettings(parsed, createId);
  const second = portableViewSettings(parsed, createId);
  expect(first.records.bookmarks.map((item) => item.id)).toEqual(["receiver-a", "receiver-b"]);
  expect(second.records.bookmarks.map((item) => item.id)).toEqual(["receiver-c", "receiver-d"]);
  expect(first.records.selected).toEqual(position);
  expect(first.records.comparison).toEqual(view.records.comparison);
  expect(createKafkaPortableView(first, view.suggestedName)).toEqual(view);
  expect(parsed.records.bookmarks[0]).not.toHaveProperty("id");
});

it.each([
  { ...view, kind: "unknown-view" },
  { ...view, schemaVersion: 2 },
  { ...view, suggestedName: " " },
  { ...view, profileId: "secret-source-id" },
  { ...view, payload: "secret-payload" },
  { ...view, catalog: { owner: "secret-owner" } },
  { ...view, records: { ...view.records, requestId: "secret-request" } },
  { ...view, records: { ...view.records, selected: { ...position, password: "secret-password" } } },
  {
    ...view,
    records: {
      ...view.records,
      bookmarks: [{ id: "secret-bookmark-id", name: "Failure", locator: position }],
    },
  },
  {
    ...view,
    records: {
      ...view.records,
      bookmarks: [
        { name: "Failure", locator: position },
        { name: "failure", locator: { ...position, offset: "1" } },
      ],
    },
  },
  {
    ...view,
    records: {
      ...view.records,
      bookmarks: [
        { name: "Failure", locator: position },
        { name: "Duplicate", locator: position },
      ],
    },
  },
  {
    ...view,
    records: { ...view.records, comparison: { ...position, clusterId: "another-cluster" } },
  },
  { ...view, records: { ...view.records, selected: { ...position, offset: "01" } } },
  { ...view, view: { ...view.view, protection: { mode: "off" } } },
])("rejects nonportable fields and invalid identities without echoing input (%#)", (input) => {
  try {
    parseKafkaInvestigationTransfer(JSON.stringify(input));
    throw new Error("Invalid fixture unexpectedly accepted");
  } catch (error) {
    expect(String(error)).toContain("invalid or unsupported import");
    expect(String(error)).not.toMatch(/secret-|another-cluster/);
  }
});

it("bounds raw UTF-8 bytes before parse and accepts exactly the document limit", () => {
  const raw = JSON.stringify(view);
  const remaining = KAFKA_VIEW_TRANSFER_LIMITS.documentBytes - new TextEncoder().encode(raw).length;
  expect(parseKafkaInvestigationTransfer(raw + " ".repeat(remaining))).toEqual({
    kind: "view",
    view,
  });
  expect(() => parseKafkaInvestigationTransfer(raw + " ".repeat(remaining + 1))).toThrow();
  expect(() => parseKafkaInvestigationTransfer("{" + "☕".repeat(44_000) + "}")).toThrow();
});

it("keeps legacy query JSON and link formats and their original size bound", () => {
  for (const input of [
    serializeKafkaQuery(query),
    createKafkaQueryLink(query),
    new URL(createKafkaQueryLink(query)).hash,
  ])
    expect(parseKafkaInvestigationTransfer(input)).toEqual({ kind: "query", query });
  expect(() =>
    parseKafkaInvestigationTransfer(JSON.stringify(query) + " ".repeat(32_768)),
  ).toThrow();
  expect(() =>
    parseKafkaInvestigationTransfer("https://user:secret@example.test/#query=e30"),
  ).toThrow();
});

it("accepts every maximum bookmark without truncating escaped names or locators", () => {
  const maximum = {
    ...view,
    suggestedName: "☕".repeat(128),
    records: {
      ...view.records,
      bookmarks: Array.from({ length: 32 }, (_, i) => ({
        name: `${String(i)}${"\\".repeat(124)}`,
        locator: { ...position, topic: "t".repeat(249), offset: String(i) },
      })),
    },
  };
  const exported = serializeKafkaPortableView(maximum);
  expect(new TextEncoder().encode(exported).length).toBeLessThanOrEqual(131_072);
  expect(parseKafkaInvestigationTransfer(exported)).toEqual({ kind: "view", view: maximum });
});
