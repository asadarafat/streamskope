import { describe, expect, it } from "vitest";

import {
  parseBrowserDataInspection,
  reviewedLegacyBrowserDataContract,
} from "../../src/platform/node/browser-data-compatibility";

const kinds = [
  "filesystem",
  "vault",
  "kafka-profiles",
  "nats-profiles",
  "rules",
  "preferences",
  "topic-history",
  "queries",
  "trust-recipes",
  "observations",
  "plugin-installations",
  "plugin-network",
  "plugin-catalog",
  "plugin-package-cache",
  "plugin-recovery",
  "profile-backups",
  "host-state",
];
function evidence(): Record<string, unknown> {
  return {
    schemaVersion: 1,
    dataContract: "streamskope-browser-data-v1",
    hostRelease: "v0.11.0",
    outcome: "eligible",
    documents: kinds.map((kind) => ({
      kind,
      state: "missing",
      count: 0,
      formats: [],
      reason: null,
    })),
    unverified: [
      "protected-content-authenticity",
      "protected-profile-schema",
      "remote-plugin-resource-cleanup",
      "host-quiescence",
    ],
  };
}
function documents(report: Record<string, unknown>): Array<Record<string, unknown>> {
  return report.documents as Array<Record<string, unknown>>;
}
describe("closed browser data evidence", () => {
  it("retains explicit inspection limits and refuses a claimed authority field", () => {
    expect(parseBrowserDataInspection(evidence()).unverified).toHaveLength(4);
    expect(() =>
      parseBrowserDataInspection({ ...evidence(), sourceRevision: "caller-supplied" }),
    ).toThrow();
  });
  it.each(["omitted", "duplicate", "reordered", "unknown", "missing-limitation"])(
    "refuses %s coverage",
    (scenario) => {
      const report = evidence();
      const entries = documents(report);
      if (scenario === "omitted") entries.pop();
      if (scenario === "duplicate") entries[1] = entries[0]!;
      if (scenario === "reordered") [entries[0], entries[1]] = [entries[1]!, entries[0]!];
      if (scenario === "unknown") entries[0]!.kind = "caller-operation";
      if (scenario === "missing-limitation") report.unverified = ["host-quiescence"];
      expect(() => parseBrowserDataInspection(report)).toThrow();
    },
  );
  it.each([
    { state: "blocked", reason: "unavailable" },
    { state: "not-inspected", reason: "unavailable" },
    { state: "verified", reason: "private error text" },
    { count: 8193 },
    { count: -1 },
    { count: 0.5 },
    { state: "verified", formats: [2] },
    { state: "verified", formats: [1, 1] },
    { state: "verified", reason: null, extra: "private profile" },
  ])("refuses false eligibility or unbounded row metadata %j", (change) => {
    const report = evidence();
    Object.assign(documents(report)[1]!, change);
    expect(() => parseBrowserDataInspection(report)).toThrow();
  });
  it("recognizes codec preference format 2 without accepting future formats", () => {
    const report = evidence();
    Object.assign(documents(report)[5]!, { state: "verified", count: 2, formats: [1, 2] });
    expect(parseBrowserDataInspection(report).outcome).toBe("eligible");
    documents(report)[5]!.formats = [3];
    expect(() => parseBrowserDataInspection(report)).toThrow();
  });
  it("recognizes actual legacy/current view formats without accepting future formats", () => {
    const report = evidence();
    Object.assign(documents(report)[7]!, { state: "verified", count: 1, formats: [1, 2, 3, 4] });
    expect(parseBrowserDataInspection(report).documents[7]?.formats).toEqual([1, 2, 3, 4]);
    documents(report)[7]!.formats = [5];
    expect(() => parseBrowserDataInspection(report)).toThrow();
  });
  it("accepts current security envelopes but rejects later unknown formats", () => {
    const report = evidence();
    Object.assign(documents(report)[2]!, { state: "verified", count: 1, formats: [4] });
    expect(parseBrowserDataInspection(report).outcome).toBe("eligible");
    documents(report)[2]!.formats = [5];
    expect(() => parseBrowserDataInspection(report)).toThrow();
  });
  it("accepts explicit failure and ordered supported historical Kafka envelopes", () => {
    const report = evidence();
    report.outcome = "blocked";
    Object.assign(documents(report)[2]!, { state: "blocked", reason: "managed-source-unverified" });
    Object.assign(documents(report)[15]!, { state: "verified", count: 4, formats: [1, 2, 3, 4] });
    expect(parseBrowserDataInspection(report).outcome).toBe("blocked");
  });
});

describe("reviewed published predecessor identity", () => {
  const predecessor = {
    version: "0.10.3",
    sourceRevision: "089980705afaffc1a6135a9347eb1bff68b27206",
    registryReference:
      "ghcr.io/asadarafat/streamskope:0.10.3@sha256:74995d8654ad9c967abe631e177ee4739ce7d0267070c03e40b9f62c3dba2057",
    architecture: "arm64" as const,
    imageId: "sha256:deb3afffbb1196488e97993e420083fb47d9058e497a111861f372b3a1a63eaf",
  };
  it("binds the reviewed contract to the exact published image and source, not a semver range", () => {
    expect(reviewedLegacyBrowserDataContract(predecessor)).toBe("streamskope-browser-data-v1");
    for (const change of [
      { version: "0.10.4" },
      { sourceRevision: "0".repeat(40) },
      { registryReference: predecessor.registryReference.replace("0.10.3@", "latest@") },
      { imageId: `sha256:${"a".repeat(64)}` },
      { architecture: "amd64" as const },
    ])
      expect(reviewedLegacyBrowserDataContract({ ...predecessor, ...change })).toBeUndefined();
  });
});
