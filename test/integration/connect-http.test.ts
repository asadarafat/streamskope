import { expect, it, vi } from "vitest";

import { ConnectHttpAdapter } from "../../src/features/kafka/engine/connect-http";
import { OwnedKafkaResources } from "../../src/features/kafka/engine/owned-kafka-resources";
import type {
  BoundedJsonHttpPort,
  OwnedJsonHttpPort,
} from "../../src/features/kafka/engine/bounded-json-http";
function transport(request: BoundedJsonHttpPort["request"]): OwnedJsonHttpPort {
  return {
    request,
    open: (
      input,
    ): import("../../src/features/kafka/engine/owned-http-request").OwnedHttpRequest => ({
      response: request(input),
      closed: Promise.resolve(),
      close: (): Promise<void> => Promise.resolve(),
      dispatched: (): boolean => true,
    }),
  };
}
const context = {
  requestOwner: new OwnedKafkaResources(new AbortController().signal),
  baseUrl: "https://connect.example/prefix",
  caPem: "ca",
  authorization: (): Promise<string> => Promise.resolve("Bearer protected"),
};
it("uses profile TLS/OAuth and JSON requests, redacts arbitrary config and all raw error traces", async () => {
  const request = vi
    .fn<BoundedJsonHttpPort["request"]>()
    .mockResolvedValueOnce({
      status: 200,
      body: { name: "orders", "connector.class": "Sink", password: "secret", custom: "secret" },
    })
    .mockResolvedValueOnce({
      status: 200,
      body: {
        connector: { state: "RUNNING" },
        tasks: [{ id: 0, state: "FAILED", trace: "DataException: password=secret" }],
      },
    });
  const a = new ConnectHttpAdapter(transport(request));
  const result = await a.load(context, "orders", AbortSignal.timeout(1000));
  expect(JSON.stringify(result?.detail)).not.toContain("secret");
  expect(result?.config.password).toBe("secret");
  expect(result?.detail.tasks[0]?.failure).toContain("conversion");
  expect(request).toHaveBeenCalledWith(
    expect.objectContaining({
      authorization: "Bearer protected",
      caPem: "ca",
      url: "https://connect.example/prefix/connectors/orders/config",
      contentType: "application/json",
    }),
  );
});
it("returns field failures without echoing values or remote diagnostics; malformed validation fails closed", async () => {
  const request = vi.fn<BoundedJsonHttpPort["request"]>().mockResolvedValue({
    status: 200,
    body: {
      error_count: 1,
      configs: [
        { definition: { name: "password" }, value: { value: "secret", errors: ["bad secret"] } },
      ],
    },
  });
  const a = new ConnectHttpAdapter(transport(request));
  const result = await a.validate(
    context,
    { "connector.class": "Sink" },
    AbortSignal.timeout(1000),
  );
  expect(result.issues[0]?.field).toBe("password");
  expect(JSON.stringify(result)).not.toContain("secret");
  request.mockResolvedValue({ status: 200, body: { configs: [] } });
  await expect(
    a.validate(context, { "connector.class": "Sink" }, AbortSignal.timeout(1000)),
  ).rejects.toThrow();
});
it("dispatches only the documented failed-task restart endpoint and never retries rejected writes", async () => {
  const request = vi
    .fn<BoundedJsonHttpPort["request"]>()
    .mockResolvedValue({ status: 403, body: { message: "secret" } });
  const a = new ConnectHttpAdapter(transport(request));
  const outcome = await a.apply(
    context,
    { name: "orders", action: "restart-failed", config: {} },
    AbortSignal.timeout(1000),
  );
  expect(outcome).toMatchObject({
    state: "rejected",
    dispatch: "attempted",
    cleanup: "confirmed",
  });
  expect(outcome.detail).toContain("HTTP 403");
  expect(request).toHaveBeenCalledTimes(1);
  expect(request).toHaveBeenCalledWith(
    expect.objectContaining({
      method: "POST",
      url: "https://connect.example/prefix/connectors/orders/restart?includeTasks=true&onlyFailed=true",
    }),
  );
});
it.each([false, true])(
  "reads the supported offset mapping and dispatches one bounded PATCH or reset (source=%s)",
  async (source) => {
    let body: unknown = {
      offsets: [
        {
          partition: source
            ? { filename: "/host-only/source" }
            : { kafka_topic: "orders", kafka_partition: 0 },
          offset: source ? { position: 12 } : { kafka_offset: 12 },
        },
      ],
    };
    const request = vi.fn<BoundedJsonHttpPort["request"]>().mockImplementation((input) => {
      if (input.method !== "GET") return Promise.resolve({ status: 202, body: undefined });
      if (input.url.endsWith("/config"))
        return Promise.resolve({
          status: 200,
          body: {
            name: "orders",
            "connector.class": source
              ? "org.apache.kafka.connect.file.FileStreamSourceConnector"
              : "org.apache.kafka.connect.file.FileStreamSinkConnector",
            file: "/host-only/source",
          },
        });
      if (input.url.endsWith("/status"))
        return Promise.resolve({
          status: 200,
          body: { name: "orders", connector: { state: "STOPPED" }, tasks: [] },
        });
      if (input.url.endsWith("/offsets")) return Promise.resolve({ status: 200, body });
      return Promise.resolve({
        status: 200,
        body: { version: "4.3.1", kafka_cluster_id: "cluster-A" },
      });
    });
    const adapter = new ConnectHttpAdapter(transport(request)),
      state = await adapter.inspectOffsets(context, "orders", AbortSignal.timeout(1000));
    expect(state).toMatchObject({
      status: "available",
      mapping: source ? "file-source" : "kafka-sink",
      offsets: [{ position: 12 }],
    });
    if (state.status !== "available") throw new Error("Missing supported mapping");
    const before = request.mock.calls.length;
    expect(
      await adapter.applyOffsets(
        context,
        { name: "orders", action: "remove", partition: state.offsets[0]!.partition, offset: null },
        AbortSignal.timeout(1000),
      ),
    ).toMatchObject({ state: "acknowledged", cleanup: "confirmed" });
    expect(request.mock.calls.length - before).toBe(1);
    expect(request.mock.lastCall?.[0]).toMatchObject({
      method: "PATCH",
      url: "https://connect.example/prefix/connectors/orders/offsets",
      body: { offsets: [{ partition: state.offsets[0]!.partition, offset: null }] },
      authorization: "Bearer protected",
      caPem: "ca",
    });
    await adapter.applyOffsets(
      context,
      { name: "orders", action: "reset", partition: null, offset: null },
      AbortSignal.timeout(1000),
    );
    expect(request.mock.lastCall?.[0]).toMatchObject({
      method: "DELETE",
      url: "https://connect.example/prefix/connectors/orders/offsets",
    });
    body = { offsets: [] };
    expect(
      await adapter.inspectOffsets(context, "orders", AbortSignal.timeout(1000)),
    ).toMatchObject({ status: "available", offsets: [] });
  },
);
it.each([401, 403, 404, 405, 501, 500])(
  "distinguishes offset inspection HTTP%s from an empty offset set",
  async (status) => {
    const adapter = new ConnectHttpAdapter(
      transport(
        vi
          .fn<BoundedJsonHttpPort["request"]>()
          .mockResolvedValue({ status, body: { secret: "withheld" } }),
      ),
    );
    expect(await adapter.inspectOffsets(context, "orders", AbortSignal.timeout(1000))).toEqual({
      status: [401, 403].includes(status)
        ? "denied"
        : [404, 405, 501].includes(status)
          ? "unsupported"
          : "unavailable",
    });
  },
);
it.each(["duplicate", "unsafe", "extra", "unknown", "routing", "foreign-source"])(
  "refuses %s offset mappings without a mutation",
  async (mode) => {
    const source = mode === "foreign-source",
      entry = {
        partition: source
          ? { filename: "/foreign-source" }
          : { kafka_topic: "orders", kafka_partition: 0 },
        offset: source
          ? { position: 4 }
          : { kafka_offset: mode === "unsafe" ? Number.MAX_SAFE_INTEGER + 1 : 4 },
      };
    const request = vi.fn<BoundedJsonHttpPort["request"]>().mockImplementation((input) =>
      Promise.resolve({
        status: 200,
        body: input.url.endsWith("/config")
          ? {
              name: "orders",
              "connector.class":
                mode === "unknown"
                  ? "custom.SourceConnector"
                  : source
                    ? "org.apache.kafka.connect.file.FileStreamSourceConnector"
                    : "org.apache.kafka.connect.file.FileStreamSinkConnector",
              file: "/host-only/source",
              ...(mode === "routing"
                ? { "consumer.override.bootstrap.servers": "different-cluster" }
                : {}),
            }
          : input.url.endsWith("/status")
            ? { name: "orders", connector: { state: "STOPPED" }, tasks: [] }
            : input.url.endsWith("/offsets")
              ? {
                  offsets:
                    mode === "duplicate"
                      ? [entry, entry]
                      : [
                          {
                            ...entry,
                            ...(mode === "extra" ? { unrecognized: "partition-guess" } : {}),
                          },
                        ],
                }
              : { version: "4.3.1", kafka_cluster_id: "cluster-A" },
      }),
    );
    expect(
      await new ConnectHttpAdapter(transport(request)).inspectOffsets(
        context,
        "orders",
        AbortSignal.timeout(1000),
      ),
    ).toEqual({ status: "unsupported" });
    expect(request.mock.calls.every(([input]) => input.method === "GET")).toBe(true);
  },
);
