import { expect, it, vi, type Mock } from "vitest";

import {
  ConnectService,
  type ConnectPort,
  type ConnectState,
} from "../../src/features/kafka/application/connect-service";
import { RecordingActiveConnection } from "../support/kafka-backend-facade-fixture";
import {
  HOST_PROTOCOL_VERSION,
  parseHostCommand,
  parseHostCommandResponse,
  parseCorrelatedHostResponse,
} from "../../src/features/kafka/contracts";
import {
  CONNECT_PROTECTED_VALUE,
  parseConnectInput,
  parseConnectReview,
  type ConnectInput,
} from "../../src/features/kafka/contracts/connect";
import { connectDlqContext } from "../../src/features/kafka/contracts/connect-dlq";
const input: ConnectInput = { name: "orders", action: "update", config: { "tasks.max": "2" } };
interface Fixture {
  readonly service: ConnectService;
  readonly port: { [K in keyof ConnectPort]: Mock<NonNullable<ConnectPort[K]>> };
  setState(v: ConnectState | null): void;
  getState(): ConnectState;
  disconnect(): void;
  expire(): void;
}
function fixture(): Fixture {
  const connection = Object.assign(new RecordingActiveConnection(), {
    clusterServiceContext: () => ({
      baseUrl: "https://connect.example",
      authorization: (): Promise<undefined> => Promise.resolve(undefined),
    }),
  });
  let context: {
    connection: typeof connection;
    connectionName: string;
    generation: number;
  } | null = { connection, connectionName: "Test", generation: 1 };
  let now = 1000;
  let state: ConnectState | null = {
    config: { name: "orders", "connector.class": "FileSink", password: "secret", "tasks.max": "1" },
    detail: {
      name: "orders",
      state: "RUNNING",
      tasks: [{ id: 0, state: "RUNNING", failure: "" }],
      config: { password: "[protected]" },
      dlq: null,
      observedAt: new Date(now).toISOString(),
    },
  };
  const port: Fixture["port"] = {
    list: vi
      .fn<ConnectPort["list"]>()
      .mockResolvedValue({ names: ["orders"], plugins: ["FileSink"] }),
    load: vi.fn<ConnectPort["load"]>().mockImplementation(() => Promise.resolve(state)),
    validate: vi.fn<ConnectPort["validate"]>().mockResolvedValue({ issues: [] }),
    apply: vi.fn<ConnectPort["apply"]>().mockResolvedValue(undefined),
  };
  return {
    service: new ConnectService(
      () => context,
      port,
      () => now,
    ),
    port,
    setState: (v: ConnectState | null): void => {
      state = v;
    },
    getState: () => state!,
    disconnect: (): void => {
      context = null;
    },
    expire: (): void => {
      now += 120001;
    },
  };
}
it("preserves omitted host secrets, makes validation non-mutating and returns only bounded review fields", async () => {
  const f = fixture();
  expect(await f.service.validate(input)).toEqual({ issues: [] });
  expect(f.port.apply).not.toHaveBeenCalled();
  const review = await f.service.review(input);
  expect(JSON.stringify(review)).not.toContain("secret");
  expect(review.fields).toEqual(["tasks.max"]);
  const a = await f.service.apply(review.planId, review.confirmation);
  expect(a.state).toBe("acknowledged");
  expect(f.port.apply).toHaveBeenCalledWith(
    expect.anything(),
    { ...input, remove: [], config: { ...f.getState().config, "tasks.max": "2" } },
    expect.any(AbortSignal),
  );
  expect(await f.service.apply(review.planId, review.confirmation)).toEqual(a);
  expect(f.port.apply).toHaveBeenCalledTimes(1);
});
it("rejects changed config, changed task state, expiration, disconnection and incorrect confirmation before mutation", async () => {
  for (const mode of ["config", "tasks", "expire", "disconnect", "confirm"]) {
    const f = fixture();
    const p = await f.service.review(input);
    if (mode === "config")
      f.setState({ ...f.getState(), config: { ...f.getState().config, "tasks.max": "3" } });
    if (mode === "tasks")
      f.setState({
        ...f.getState(),
        detail: { ...f.getState().detail, tasks: [{ id: 0, state: "FAILED", failure: "" }] },
      });
    if (mode === "expire") f.expire();
    if (mode === "disconnect") f.disconnect();
    if (mode === "config" || mode === "tasks")
      expect((await f.service.apply(p.planId, p.confirmation)).state).toBe("rejected");
    else
      await expect(
        f.service.apply(p.planId, mode === "confirm" ? "yes" : p.confirmation),
      ).rejects.toThrow();
    expect(f.port.apply).not.toHaveBeenCalled();
  }
});
it("keeps dispatch timeout unknown, authorization denied rejected and acknowledgement after failed read-back", async () => {
  for (const mode of ["timeout", "denied", "readback"]) {
    const f = fixture();
    const p = await f.service.review(input);
    vi.mocked(f.port.apply).mockImplementation(() => {
      if (mode === "timeout") return Promise.reject(new Error("private token"));
      if (mode === "denied")
        return Promise.reject(Object.assign(new Error("private token"), { status: 403 }));
      vi.mocked(f.port.load).mockRejectedValue(new Error("private token"));
      return Promise.resolve();
    });
    const result = await f.service.apply(p.planId, p.confirmation);
    expect(result.state).toBe(
      mode === "timeout" ? "unknown" : mode === "denied" ? "rejected" : "acknowledged",
    );
    expect(JSON.stringify(result)).not.toContain("private token");
    expect(await f.service.apply(p.planId, p.confirmation)).toEqual(result);
    expect(f.port.apply).toHaveBeenCalledTimes(1);
  }
});
it("requires clean validation and refuses create over an existing connector", async () => {
  const f = fixture();
  vi.mocked(f.port.validate).mockResolvedValue({
    issues: [{ field: "topics", message: "Required" }],
  });
  await expect(f.service.review(input)).rejects.toThrow();
  await expect(f.service.review({ ...input, action: "create" })).rejects.toThrow();
  expect(f.port.apply).not.toHaveBeenCalled();
});
it("strictly validates Connect commands and typed replies", async () => {
  const f = fixture();
  const review = await f.service.review(input);
  expect(
    parseHostCommand({
      version: HOST_PROTOCOL_VERSION,
      id: "a",
      command: "connect.review",
      payload: input,
    }).payload,
  ).toEqual({ ...input, remove: [] });
  expect(() =>
    parseHostCommand({
      version: HOST_PROTOCOL_VERSION,
      id: "a",
      command: "connect.apply",
      payload: { planId: "a", confirmation: "yes", config: {} },
    }),
  ).toThrow();
  expect(
    parseHostCommandResponse({
      version: HOST_PROTOCOL_VERSION,
      id: "a",
      command: "connect.review",
      ok: true,
      result: { correlationId: "c", review },
    }),
  ).toMatchObject({ result: { review } });
  expect(() =>
    parseHostCommand({
      version: HOST_PROTOCOL_VERSION,
      id: "a",
      command: "connect.review",
      payload: { ...input, config: { x: 123 } },
    }),
  ).toThrow();
});
it("recognizes supported DLQ context without bypassing protected or truncated headers", () => {
  const headers = {
    "__connect.errors.topic": "orders",
    "__connect.errors.partition": "0",
    "__connect.errors.offset": "42",
    "__connect.errors.connector.name": "sink",
  };
  expect(connectDlqContext({ headers, truncated: false })).toMatchObject({
    topic: "orders",
    partition: "0",
    offset: "42",
    connector: "sink",
  });
  expect(connectDlqContext({ headers, truncated: true })).toBeNull();
  expect(
    connectDlqContext({
      headers: { ...headers, "__connect.errors.offset": "[MASKED]" },
      truncated: false,
    }),
  ).toBeNull();
});

it("rejects a Connect OAuth endpoint without protected token configuration at the host boundary", () => {
  expect(() =>
    parseHostCommand({
      id: "connect",
      version: HOST_PROTOCOL_VERSION,
      command: "connection.connect",
      payload: {
        name: "Test",
        brokers: ["localhost:9092"],
        tls: { enabled: false },
        services: { connect: { baseUrl: "https://connect.example", authentication: "oauth" } },
      },
    }),
  ).toThrow();
});

it("removes only explicit keys while preserving actual omitted secrets and a frozen review", async () => {
  const f = fixture(),
    update = { ...input, remove: ["password"] };
  const p = await f.service.review(update);
  expect(p).toMatchObject({
    fields: ["tasks.max"],
    removedFields: ["password"],
    connectionName: "Test",
  });
  expect(JSON.stringify(p)).not.toContain("secret");
  update.remove.push("tasks.max");
  expect((await f.service.apply(p.planId, p.confirmation)).state).toBe("acknowledged");
  expect(f.port.apply.mock.calls[0]![1].config).toEqual({
    name: "orders",
    "connector.class": "FileSink",
    "tasks.max": "2",
  });
});
it("compares complete canonical configuration and task state independently of key ordering", async () => {
  const f = fixture(),
    p = await f.service.review(input),
    old = f.getState();
  f.setState({
    ...old,
    config: Object.fromEntries(Object.entries(old.config).reverse()),
    detail: {
      ...old.detail,
      observedAt: "later",
      tasks: old.detail.tasks.map((t) => ({ ...t, failure: "a changed diagnostic" })),
    },
  });
  expect((await f.service.apply(p.planId, p.confirmation)).state).toBe("acknowledged");
  expect(f.port.apply).toHaveBeenCalledTimes(1);
});
it.each([
  { remove: ["tasks.max", "tasks.max"] },
  { remove: ["tasks.max"] },
  { remove: ["name"] },
  { config: { password: CONNECT_PROTECTED_VALUE } },
  { config: { name: "foreign" } },
  { action: "pause", config: { password: "replacement" } },
  { action: "create", remove: ["password"] },
  { remove: "password" },
  { remove: Array.from({ length: 201 }, (_, i) => `field${i}`) },
])(
  "refuses ambiguous or display-only configuration input before remote access: %j",
  async (delta) => {
    const f = fixture();
    await expect(f.service.review({ ...input, ...delta } as ConnectInput)).rejects.toThrow();
    expect(f.port.load).not.toHaveBeenCalled();
    expect(f.port.apply).not.toHaveBeenCalled();
  },
);
it("refuses absent removals, empty updates and oversized merged host configuration", async () => {
  const f = fixture();
  await expect(f.service.review({ ...input, remove: ["absent"] })).rejects.toThrow();
  await expect(f.service.review({ ...input, config: {} })).rejects.toThrow();
  f.setState({
    ...f.getState(),
    config: Object.fromEntries(Array.from({ length: 200 }, (_, i) => [`f${i}`, "value"])),
  });
  await expect(f.service.review(input)).rejects.toThrow();
  expect(f.port.apply).not.toHaveBeenCalled();
});
it("correlates connector and exact set/removal keys and refuses inconsistent reviews", async () => {
  const f = fixture(),
    payload = { ...input, remove: ["password"] },
    review = await f.service.review(payload);
  const command = {
    command: "connect.review" as const,
    id: "correlated",
    version: HOST_PROTOCOL_VERSION,
    payload,
  };
  const reply = {
    command: command.command,
    id: command.id,
    version: command.version,
    ok: true,
    result: { correlationId: "c", review },
  };
  expect(parseCorrelatedHostResponse(reply, command)).toMatchObject({ result: { review } });
  for (const delta of [
    {
      name: "foreign",
      confirmation: "update foreign",
      before: { ...review.before, name: "foreign" },
    },
    { fields: ["password"], removedFields: [] },
    { removedFields: [] },
    { action: "delete", confirmation: "delete orders", removedFields: [] },
  ])
    expect(() =>
      parseCorrelatedHostResponse(
        { ...reply, result: { ...reply.result, review: { ...review, ...delta } } },
        command,
      ),
    ).toThrow();
  expect(() => parseConnectReview({ ...review, fields: ["password"] })).toThrow();
  expect(() => parseConnectReview({ ...review, confirmation: "yes" })).toThrow();
  expect(parseConnectInput(input).remove).toEqual([]);
});
