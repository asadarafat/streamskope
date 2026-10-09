import { describe, expect, it } from "vitest";

import {
  HOST_PROTOCOL_VERSION,
  createDefaultKafkaInvestigationView,
  inspectKafkaQueryLibraryDocument,
  parseHostCommand,
  parseKafkaInvestigationView,
  parseKafkaQueryLibraryDocument,
  parseKafkaQueryLibrarySnapshot,
  parseKafkaQueryTransfer,
  parseKafkaSavedView,
  serializeKafkaQuery,
  serializeKafkaQueryLibraryDocument,
} from "../../src/features/kafka/contracts";

const query = {
  schemaVersion: 1,
  request: { topic: "orders", mode: "earliest", maxMessages: 100 },
} as const;
const defaultView = {
  schemaVersion: 1,
  destination: { kind: "topic", workspace: "messages" },
  messages: {
    visibleColumns: ["timestamp", "key", "preview", "partition", "offset", "rules"],
    columnWidths: [],
    inspectorWidth: 320,
    filtersOpen: false,
  },
} as const;
const legacy = { id: "incident", name: "Incident", profileId: "production", configuration: query };
const saved = { ...legacy, view: defaultView };

describe("versioned investigation view contracts", () => {
  it("converts the literal legacy disk shape purely and reports its real format", () => {
    const document = { schemaVersion: 1, queries: [legacy] };
    const original = JSON.stringify(document);
    expect(inspectKafkaQueryLibraryDocument(document)).toEqual({
      schemaVersion: 1,
      queries: [saved],
    });
    expect(parseKafkaQueryLibraryDocument(document)).toEqual({
      schemaVersion: 2,
      queries: [saved],
    });
    expect(JSON.stringify(document)).toBe(original);
    expect(createDefaultKafkaInvestigationView()).toEqual(defaultView);
  });

  it("compacts only default disk descriptors without weakening wire entries or snapshots", () => {
    const stored = serializeKafkaQueryLibraryDocument([saved]);
    expect(JSON.parse(stored)).toEqual({ schemaVersion: 2, queries: [legacy] });
    expect(inspectKafkaQueryLibraryDocument(JSON.parse(stored))).toEqual({
      schemaVersion: 2,
      queries: [saved],
    });
    expect(() => parseKafkaSavedView(legacy)).toThrow();
    expect(() =>
      parseKafkaQueryLibrarySnapshot({ durability: "durable", queries: [legacy] }),
    ).toThrow();
    const command = {
      command: "queries.put",
      id: "save-view",
      version: HOST_PROTOCOL_VERSION,
      payload: { query: legacy },
    };
    expect(() => parseHostCommand(command)).toThrow(/view/u);
    expect(parseHostCommand({ ...command, payload: { query: saved } })).toEqual({
      ...command,
      payload: { query: saved },
    });
    expect(
      parseKafkaQueryLibrarySnapshot({ durability: "durable", queries: [saved] }).queries,
    ).toEqual([saved]);
  });

  it.each(["messages", "monitor", "latency", "rules", "configuration"])(
    "preserves topic task %s without duplicating its resource name",
    (workspace) => {
      const entry = parseKafkaSavedView({
        ...saved,
        view: { ...defaultView, destination: { kind: "topic", workspace } },
      });
      expect(entry.configuration?.request.topic).toBe("orders");
      expect(entry.view.destination).toEqual({ kind: "topic", workspace });
      expect(
        parseKafkaQueryLibraryDocument(JSON.parse(serializeKafkaQueryLibraryDocument([entry])))
          .queries,
      ).toEqual([entry]);
    },
  );

  it("preserves a standalone group or its optional background investigation without a fabricated topic", () => {
    for (const configuration of [null, query]) {
      const group = {
        ...saved,
        configuration,
        view: {
          ...defaultView,
          destination: { kind: "consumer-group", groupId: "orders-workers" },
        },
      };
      const entry = parseKafkaSavedView(group);
      expect(entry).toEqual(group);
      const serialized: unknown = JSON.parse(serializeKafkaQueryLibraryDocument([entry]));
      expect(serialized).toMatchObject({
        queries: [{ view: { destination: { groupId: "orders-workers" } } }],
      });
      expect(parseKafkaQueryLibraryDocument(serialized).queries).toEqual([group]);
    }
    expect(() => parseKafkaSavedView({ ...saved, configuration: null })).toThrow();
    expect(() =>
      inspectKafkaQueryLibraryDocument({
        schemaVersion: 2,
        queries: [{ ...legacy, configuration: null }],
      }),
    ).toThrow();
  });

  it("normalizes column order, preserves bounded user layout and returns fresh defaults", () => {
    const view = parseKafkaInvestigationView({
      ...defaultView,
      messages: {
        visibleColumns: ["offset", "key"],
        columnWidths: [
          { column: "offset", pixels: 50 },
          { column: "key", pixels: 960 },
        ],
        inspectorWidth: 560,
        filtersOpen: true,
      },
    });
    expect(view.messages).toEqual({
      visibleColumns: ["key", "offset"],
      columnWidths: [
        { column: "key", pixels: 960 },
        { column: "offset", pixels: 50 },
      ],
      inspectorWidth: 560,
      filtersOpen: true,
    });
    const first = createDefaultKafkaInvestigationView();
    const second = createDefaultKafkaInvestigationView();
    expect(first.messages).not.toBe(second.messages);
    expect(first.messages.visibleColumns).not.toBe(second.messages.visibleColumns);
  });

  it.each([
    { visibleColumns: [] },
    { visibleColumns: ["key", "key"] },
    { visibleColumns: ["password"] },
    { visibleColumns: ["key", "offset", "preview", "timestamp", "rules", "partition", "key"] },
    { columnWidths: [{ column: "key", pixels: 91 }] },
    { columnWidths: [{ column: "key", pixels: 961 }] },
    { columnWidths: [{ column: "key", pixels: 100.5 }] },
    { columnWidths: [{ column: "key", pixels: Number.NaN }] },
    { columnWidths: [{ column: "key", pixels: Number.POSITIVE_INFINITY }] },
    {
      columnWidths: [
        { column: "key", pixels: 100 },
        { column: "key", pixels: 101 },
      ],
    },
    { columnWidths: [{ column: "unknown", pixels: 100 }] },
    { columnWidths: [{ column: "key", pixels: 100, callback: "unexpected" }] },
    { inspectorWidth: 291 },
    { inspectorWidth: 561 },
    { inspectorWidth: 320.5 },
    { filtersOpen: "true" },
    { records: [] },
  ])("rejects invalid layout or undeclared presentation data %#", (override) => {
    expect(() =>
      parseKafkaInvestigationView({
        ...defaultView,
        messages: { ...defaultView.messages, ...override },
      }),
    ).toThrow();
  });

  it.each([
    { kind: "topic", workspace: "unknown" },
    { kind: "topic", workspace: "messages", topic: "other" },
    { kind: "consumer-group", groupId: "" },
    { kind: "consumer-group", groupId: "x".repeat(513) },
    { kind: "consumer-group", groupId: "workers", autoRefresh: true },
    { kind: "connector", name: "worker" },
  ])("rejects unknown or ambiguous destinations %#", (destination) => {
    expect(() => parseKafkaInvestigationView({ ...defaultView, destination })).toThrow();
  });

  it.each([
    { schemaVersion: 3, queries: [saved] },
    { schemaVersion: 1, queries: [saved] },
    { schemaVersion: 1, queries: [{ ...legacy, configuration: null }] },
    { schemaVersion: 2, queries: [saved, { ...saved, name: "Other" }] },
    { schemaVersion: 2, queries: [saved, { ...saved, id: "other", name: "  INCIDENT  " }] },
    {
      schemaVersion: 2,
      queries: Array.from({ length: 101 }, (_, index) => ({
        ...saved,
        id: String(index),
        name: String(index),
      })),
    },
  ])("rejects unsupported, duplicated or oversized library shapes %#", (document) => {
    expect(() => inspectKafkaQueryLibraryDocument(document)).toThrow();
  });

  it.each([
    "password",
    "records",
    "codec",
    "protection",
    "continuation",
    "coverage",
    "jobId",
    "selectedRecord",
    "comparison",
  ])("cannot smuggle %s into a stored view", (field) => {
    expect(() => parseKafkaSavedView({ ...saved, [field]: "unexpected" })).toThrow();
    expect(() =>
      parseKafkaSavedView({ ...saved, view: { ...defaultView, [field]: "unexpected" } }),
    ).toThrow();
  });

  it("leaves portable query schema 1 independent from local views and profile identity", () => {
    const text = serializeKafkaQuery(query);
    expect(parseKafkaQueryTransfer(text)).toEqual(query);
    expect(JSON.parse(text)).toEqual(query);
    expect(() => parseKafkaQueryTransfer(JSON.stringify(saved))).toThrow();
    expect(() =>
      parseKafkaQueryTransfer(JSON.stringify({ ...query, view: defaultView })),
    ).toThrow();
  });
});
