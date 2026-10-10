import { expect, it } from "vitest";

import {
  HOST_PROTOCOL_VERSION,
  parseHostCommand,
  parseHostCommandResponse,
} from "../../src/features/kafka/contracts";

it.each([
  [
    "records.repair.review",
    { jobId: "parent", targetProfile: { id: "current-profile", revision: 2 } },
  ],
  [
    "records.repair.reconcile",
    { jobId: "parent", targetProfile: null, recordIndex: 0, offset: "9007199254740993" },
  ],
  [
    "records.repair.archive",
    { jobId: "parent", confirmation: "parent", chain: [{ id: "parent", revision: 2 }] },
  ],
] as const)(
  "accepts the paired closed %s command without allowing credentials or private origin snapshots",
  (command, payload) => {
    const input = { command, payload, id: "request", version: HOST_PROTOCOL_VERSION };
    expect(parseHostCommand(input)).toEqual(input);
    expect(() =>
      parseHostCommand({ ...input, payload: { ...payload, password: "private" } }),
    ).toThrow();
    expect(() =>
      parseHostCommand({ ...input, payload: { ...payload, origin: { records: [] } } }),
    ).toThrow();
    expect(() => parseHostCommand({ ...input, version: 65 })).toThrow();
  },
);
it("rejects invalid positions, duplicate archive rows and unknown observation states", () => {
  const base = { id: "request", version: HOST_PROTOCOL_VERSION };
  for (const patch of [
    { recordIndex: -1 },
    { recordIndex: 50 },
    { recordIndex: 1.5 },
    { offset: "01" },
    { offset: "-1" },
  ])
    expect(() =>
      parseHostCommand({
        ...base,
        command: "records.repair.reconcile",
        payload: { jobId: "parent", targetProfile: null, recordIndex: 0, offset: "7", ...patch },
      }),
    ).toThrow();
  expect(() =>
    parseHostCommand({
      ...base,
      command: "records.repair.archive",
      payload: {
        jobId: "parent",
        confirmation: "parent",
        chain: [
          { id: "parent", revision: 2 },
          { id: "parent", revision: 2 },
        ],
      },
    }),
  ).toThrow();
  const response = {
    ...base,
    command: "records.repair.reconcile",
    ok: true,
    result: {
      correlationId: "observation",
      finding: {
        id: "finding",
        recordIndex: 0,
        offset: "7",
        observedAt: "2026-10-10T00:00:00Z",
        state: "equivalent",
        cleanup: "complete",
      },
    },
  };
  expect(parseHostCommandResponse(response)).toEqual(response);
  expect(() =>
    parseHostCommandResponse({
      ...response,
      result: {
        ...response.result,
        finding: { ...response.result.finding, state: "acknowledged" },
      },
    }),
  ).toThrow();
  expect(() =>
    parseHostCommandResponse({
      ...response,
      result: { ...response.result, finding: { ...response.result.finding, original: "private" } },
    }),
  ).toThrow();
});
