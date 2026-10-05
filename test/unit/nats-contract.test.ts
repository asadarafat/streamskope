import { describe, expect, it } from "vitest";

import {
  NATS_LIMITS,
  NATS_PROTOCOL_VERSION,
  NATS_PROVIDER_EVENT_CODEC,
  parseCorrelatedNatsResponse,
  parseNatsCommand,
  parseNatsCopiedMessage,
  parseNatsEvent,
  parseNatsProfileCreateInput,
  parseNatsProfileUpdateInput,
  parseNatsProfilesSnapshot,
  parseNatsConnectionSnapshot,
  parseNatsSubscriptionSnapshot,
  parseNatsRecord,
  parseNatsServers,
  parseNatsSubject,
  type NatsCommand,
  type NatsEvent,
  type NatsRecord,
  type NatsSubscriptionCounters,
} from "../../src/features/nats/contracts";

const time = "2026-10-05T13:00:00.000Z";
const counters: NatsSubscriptionCounters = {
  receivedRecords: 1,
  applicationOmittedRecords: 0,
  publishedRecords: 1,
  queuedRecords: 0,
  queuedBytes: 0,
  transportOmittedRecords: 0,
};
const copied = {
  subject: "qualification.one",
  headers: [{ name: "X-Correlation", values: ["one", "two"] }],
  headersTruncated: false,
  payload: { encoding: "utf8", data: "hello" },
  payloadBytes: 5,
  preview: "hello",
  receivedAt: time,
  timestampProvenance: "host-received",
} as const;
const record: NatsRecord = { ...copied, id: "generation-1.1", generation: "generation-1" };
const profile = {
  name: "Test NATS",
  servers: ["nats://localhost:4222"],
  authentication: { mode: "token", token: { mode: "replace", value: "private-token" } },
  tls: {
    mode: "tls",
    caPem: {
      mode: "replace",
      value: "-----BEGIN CERTIFICATE-----\r\nZGVy\r\n-----END CERTIFICATE-----\r\n",
    },
  },
} as const;
function batch(records: readonly NatsRecord[] = [record]): NatsEvent {
  return {
    version: NATS_PROTOCOL_VERSION,
    sequence: 2,
    event: "records.batch",
    operation: "subscription.start",
    correlationId: "correlation-1",
    payload: { generation: "generation-1", records, counters },
  };
}

describe("Core NATS public contract", () => {
  it("requires exact nonnegative snapshot authorities independently of profile and transport revisions", () => {
    const states: readonly [(value: unknown) => unknown, Record<string, unknown>][] = [
      [
        parseNatsProfilesSnapshot,
        {
          revision: 7,
          capability: { durability: "session", protection: "memory", state: "ready" },
          profiles: [],
        },
      ],
      [parseNatsConnectionSnapshot, { revision: 8, state: "disconnected", profile: null }],
      [
        parseNatsSubscriptionSnapshot,
        {
          revision: 9,
          state: "idle",
          generation: null,
          subject: null,
          counters: { ...counters, receivedRecords: 0, publishedRecords: 0 },
        },
      ],
    ];
    for (const [parse, snapshot] of states) {
      expect(parse(snapshot)).toEqual(snapshot);
      for (const revision of [undefined, -1, 1.5, Infinity, Number.MAX_SAFE_INTEGER + 1])
        expect(() => parse({ ...snapshot, revision })).toThrow();
      const absent = { ...snapshot };
      delete absent.revision;
      expect(() => parse(absent)).toThrow();
    }
    const current = NATS_PROVIDER_EVENT_CODEC.availability(1, "ready");
    expect(() => parseNatsEvent({ ...current, version: 1 })).toThrow();
  });
  it("uses its own protocol and correlated command-specific responses", () => {
    const command: NatsCommand = {
      id: "request-1",
      version: NATS_PROTOCOL_VERSION,
      command: "subscription.start",
      payload: { subject: "qualification.*" },
    };
    const response = {
      id: command.id,
      version: NATS_PROTOCOL_VERSION,
      command: command.command,
      ok: true,
      result: {
        correlationId: "correlation-1",
        subscription: {
          revision: 2,
          state: "streaming",
          generation: "generation-1",
          subject: "qualification.*",
          counters,
        },
      },
    };
    expect(parseCorrelatedNatsResponse(response, command)).toEqual(response);
    expect(() =>
      parseCorrelatedNatsResponse({ ...response, id: "other-request" }, command),
    ).toThrow();
    expect(() =>
      parseCorrelatedNatsResponse({ ...response, command: "subscription.stop" }, command),
    ).toThrow();
    expect(() => parseNatsCommand({ ...command, version: 49 })).toThrow();
    expect(() => parseNatsCommand({ ...command, version: 1 })).toThrow();
    expect(() => parseCorrelatedNatsResponse({ ...response, version: 1 }, command)).toThrow();
  });

  it("rehydrates profiles, connection and subscription together", () => {
    const command: NatsCommand = {
      id: "request-1",
      version: NATS_PROTOCOL_VERSION,
      command: "profiles.list",
      payload: {},
    };
    const result = {
      correlationId: "correlation-1",
      profiles: {
        revision: 0,
        capability: { durability: "session", protection: "memory", state: "ready" },
        profiles: [],
      },
      connection: { revision: 0, state: "disconnected", profile: null },
      subscription: {
        revision: 0,
        state: "idle",
        generation: null,
        subject: null,
        counters: { ...counters, receivedRecords: 0, publishedRecords: 0 },
      },
    };
    expect(
      parseCorrelatedNatsResponse(
        {
          id: command.id,
          version: NATS_PROTOCOL_VERSION,
          command: command.command,
          ok: true,
          result,
        },
        command,
      ).ok,
    ).toBe(true);
    const incomplete = { ...result, subscription: undefined };
    expect(() =>
      parseCorrelatedNatsResponse(
        {
          id: command.id,
          version: NATS_PROTOCOL_VERSION,
          command: command.command,
          ok: true,
          result: incomplete,
        },
        command,
      ),
    ).toThrow();
  });

  it("represents pending profile resolution without inventing validated connection metadata", () => {
    const event = {
      version: NATS_PROTOCOL_VERSION,
      sequence: 1,
      event: "connection.state",
      operation: "profiles.connect",
      correlationId: "correlation-1",
      payload: { revision: 1, state: "connecting", profile: null },
    };
    expect(parseNatsEvent(event)).toEqual(event);
    expect(() =>
      parseNatsEvent({
        ...event,
        payload: { ...event.payload, state: "connected", profile: null },
      }),
    ).toThrow();
  });

  it("rejects undeclared commands, payload fields and unsafe revision values", () => {
    const base = {
      id: "request-1",
      version: NATS_PROTOCOL_VERSION,
      command: "profiles.connect",
      payload: { profileId: "profile-1", expectedRevision: 1 },
    };
    expect(parseNatsCommand(base)).toEqual(base);
    for (const payload of [
      { ...base.payload, token: "hidden" },
      { ...base.payload, expectedRevision: 0 },
      { ...base.payload, expectedRevision: Number.MAX_SAFE_INTEGER + 1 },
    ])
      expect(() => parseNatsCommand({ ...base, payload })).toThrow();
    expect(() => parseNatsCommand({ ...base, command: "messages.start" })).toThrow();
    expect(() => parseNatsCommand({ ...base, id: "request-1\n" })).toThrow();
    expect(() =>
      parseNatsCommand({ ...base, command: "subscription.stop", payload: { disconnected: true } }),
    ).toThrow();
  });

  it.each([
    "nats://admin:secret@localhost:4222",
    "nats://localhost:4222?token=secret",
    "nats://localhost:4222#secret",
    "nats://localhost:4222/file",
    "https://localhost:4222",
    "tls://localhost:4222",
  ])("rejects hidden credentials or unsupported clear-transport URL %s", (server) => {
    expect(() => parseNatsServers([server], "plaintext")).toThrow();
  });

  it("normalizes default ports and rejects duplicate endpoint aliases", () => {
    expect(parseNatsServers(["nats://localhost"], "plaintext")).toEqual(["nats://localhost:4222"]);
    expect(() =>
      parseNatsServers(["nats://localhost", "nats://localhost:4222/"], "plaintext"),
    ).toThrow();
  });

  it("canonicalizes PEM without returning secrets in validation errors", () => {
    const parsed = parseNatsProfileCreateInput(profile);
    expect(parsed.tls).toEqual({
      mode: "tls",
      caPem: {
        mode: "replace",
        value: "-----BEGIN CERTIFICATE-----\nZGVy\n-----END CERTIFICATE-----\n",
      },
    });
    let error: unknown;
    try {
      parseNatsProfileCreateInput({
        ...profile,
        authentication: { mode: "token", token: { mode: "replace", value: "private-secret\n" } },
      });
    } catch (failure) {
      error = failure;
    }
    expect(error).toBeInstanceOf(Error);
    expect(String(error)).not.toContain("private-secret");
    expect(() =>
      parseNatsProfileCreateInput({
        ...profile,
        tls: { mode: "tls", caPem: { mode: "replace", value: "/private/certificate.pem" } },
      }),
    ).toThrow();
  });

  it("requires deliberate retain/replace/clear secret modes", () => {
    const retain = {
      ...profile,
      authentication: { mode: "token", token: { mode: "retain" } },
      tls: { mode: "tls", caPem: { mode: "clear" } },
    };
    expect(parseNatsProfileUpdateInput(retain).authentication).toEqual(retain.authentication);
    expect(() => parseNatsProfileCreateInput(retain)).toThrow();
    expect(() =>
      parseNatsProfileUpdateInput({
        ...profile,
        authentication: { mode: "token", token: { mode: "replace", value: "" } },
      }),
    ).toThrow();
    expect(() =>
      parseNatsProfileUpdateInput({
        ...profile,
        authentication: { mode: "token", token: { mode: "clear" } },
      }),
    ).toThrow();
  });

  it.each(["qualification.*", "qualification.>", ">", "*"])(
    "accepts live subject filter %s",
    (subject) => {
      expect(parseNatsSubject(subject)).toBe(subject);
    },
  );
  it.each([
    "qualification..one",
    "qualification.>.one",
    "qualification.one*",
    "qualification one",
    "",
    "a.\u0000",
  ])("rejects malformed subject filter %s", (subject) => {
    expect(() => parseNatsSubject(subject)).toThrow();
  });

  it("preserves ordered duplicate header values and owns copies", () => {
    const source = { ...copied, headers: [{ name: "X-Correlation", values: ["one", "two"] }] };
    const parsed = parseNatsCopiedMessage(source);
    source.headers[0]!.values[0] = "changed";
    expect(parsed.headers[0]!.values).toEqual(["one", "two"]);
    expect(() => parseNatsCopiedMessage({ ...source, subject: "qualification.*" })).toThrow();
    expect(() => parseNatsCopiedMessage({ ...source, reply: "reply.>" })).toThrow();
  });

  it("distinguishes empty UTF-8 from exact binary bytes and validates byte counts", () => {
    expect(
      parseNatsRecord({
        ...record,
        payload: { encoding: "utf8", data: "" },
        payloadBytes: 0,
        preview: "",
      }).payload.data,
    ).toBe("");
    expect(
      parseNatsRecord({
        ...record,
        payload: { encoding: "base64", data: "/wA=" },
        payloadBytes: 2,
        preview: "[binary payload: 2 bytes]",
      }).payload,
    ).toEqual({ encoding: "base64", data: "/wA=" });
    expect(() =>
      parseNatsRecord({
        ...record,
        payload: { encoding: "base64", data: "Zh==" },
        payloadBytes: 1,
      }),
    ).toThrow();
    expect(() => parseNatsRecord({ ...record, payloadBytes: 4 })).toThrow();
    expect(() => parseNatsRecord({ ...record, offset: "17" })).toThrow();
    expect(
      parseNatsRecord({
        ...record,
        payload: { encoding: "utf8", data: "\uFEFFA" },
        payloadBytes: 4,
      }).payload.data,
    ).toBe("\uFEFFA");
  });

  it("bounds aggregate headers and reports header truncation as a separate fact", () => {
    const headers = [
      { name: "X", values: Array.from({ length: NATS_LIMITS.headerValues }, () => "") },
      { name: "Y", values: ["overflow"] },
    ];
    expect(() => parseNatsCopiedMessage({ ...copied, headers })).toThrow();
    expect(parseNatsCopiedMessage({ ...copied, headersTruncated: true }).headersTruncated).toBe(
      true,
    );
  });

  it("rejects records crossing generation and the complete encoded event budget", () => {
    expect(parseNatsEvent(batch())).toEqual(batch());
    expect(() => parseNatsEvent(batch([{ ...record, generation: "old-generation" }]))).toThrow();
    const expensive = {
      ...record,
      payload: { encoding: "utf8" as const, data: "\u0000".repeat(NATS_LIMITS.payloadBytes) },
      payloadBytes: NATS_LIMITS.payloadBytes,
    };
    expect(parseNatsRecord(expensive).payloadBytes).toBe(NATS_LIMITS.payloadBytes);
    expect(() => parseNatsEvent(batch([expensive]))).toThrow();
  });

  it("keeps platform availability free of fake command context", () => {
    const event = NATS_PROVIDER_EVENT_CODEC.availability(7, "unavailable");
    expect(parseNatsEvent(event)).toEqual(event);
    expect(NATS_PROVIDER_EVENT_CODEC.isAvailability(event)).toBe(true);
    expect(() => parseNatsEvent({ ...event, correlationId: "invented" })).toThrow();
    expect(() => parseNatsEvent({ ...batch(), sequence: Number.MAX_SAFE_INTEGER + 1 })).toThrow();
  });

  it("rejects unsafe failure fields and counter accounting contradictions", () => {
    const event = {
      version: NATS_PROTOCOL_VERSION,
      sequence: 1,
      event: "subscription.changed",
      operation: "subscription.start",
      correlationId: "correlation-1",
      payload: {
        revision: 3,
        state: "failed",
        subject: "qualification.*",
        generation: "generation-1",
        counters,
        failure: { code: "permission", summary: "Subscription is denied.", cause: "secret" },
      },
    };
    expect(() => parseNatsEvent(event)).toThrow();
    expect(() =>
      parseNatsEvent({
        ...batch(),
        payload: { ...batch().payload, counters: { ...counters, applicationOmittedRecords: 1 } },
      }),
    ).toThrow();
    expect(() =>
      parseNatsEvent({
        ...batch(),
        payload: {
          ...batch().payload,
          counters: {
            ...counters,
            receivedRecords: Number.MAX_SAFE_INTEGER,
            publishedRecords: Number.MAX_SAFE_INTEGER,
            applicationOmittedRecords: 1,
          },
        },
      }),
    ).toThrow();
  });
});
