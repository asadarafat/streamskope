import { expect, it, vi } from "vitest";

import { ConnectHttpAdapter, ConnectHttpError } from "../../src/features/kafka/engine/connect-http";
import type { BoundedJsonHttpPort } from "../../src/features/kafka/engine/bounded-json-http";
const context = {
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
  const a = new ConnectHttpAdapter({ request });
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
  const a = new ConnectHttpAdapter({ request });
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
  const a = new ConnectHttpAdapter({ request });
  await expect(
    a.apply(
      context,
      { name: "orders", action: "restart-failed", config: {} },
      AbortSignal.timeout(1000),
    ),
  ).rejects.toEqual(new ConnectHttpError(403));
  expect(request).toHaveBeenCalledTimes(1);
  expect(request).toHaveBeenCalledWith(
    expect.objectContaining({
      method: "POST",
      url: "https://connect.example/prefix/connectors/orders/restart?includeTasks=true&onlyFailed=true",
    }),
  );
});
