import { createServer, type Server } from "node:http";

import { afterEach, expect, it } from "vitest";

import { EXTERNAL_METRICS } from "../../src/features/kafka/contracts/external-metrics";
import { PrometheusMetricsAdapter } from "../../src/features/kafka/engine/prometheus-metrics";
import { NodeBoundedJsonHttp } from "../../src/features/kafka/engine/bounded-json-http";
import { OwnedKafkaResources } from "../../src/features/kafka/engine/owned-kafka-resources";
import type { KafkaClusterServiceContext } from "../../src/features/kafka/application/types";
import { createHttpsTrustFixture } from "../support/https-trust-fixture";

const closers: (() => Promise<void>)[] = [];
afterEach(async () => {
  await Promise.all(closers.splice(0).map((close) => close()));
});
function context(
  baseUrl: string,
  authorization = "Bearer private-fixture",
  extra: Partial<KafkaClusterServiceContext> = {},
): KafkaClusterServiceContext {
  const signal = new AbortController().signal;
  return {
    baseUrl,
    signal,
    requestOwner: new OwnedKafkaResources(signal),
    authorization: () => Promise.resolve(authorization),
    ...extra,
  };
}
it("uses independent trusted HTTPS and client identity, rejects credentials and certificate failures without disclosing secrets", async () => {
  const client = await createHttpsTrustFixture((_request, response) => response.end());
  closers.push(() => client.close());
  const fixture = await createHttpsTrustFixture(
    (request, response) => {
      response.setHeader("content-type", "application/json");
      if (request.headers.authorization !== "Bearer private-fixture") {
        response.writeHead(401);
        response.end(JSON.stringify({ error: "private-password-from-remote" }));
        return;
      }
      const url = new URL(request.url!, "https://fixture");
      const query = url.searchParams.get("query")!,
        stamp = query.startsWith("timestamp(");
      response.end(
        JSON.stringify({
          status: "success",
          data: {
            resultType: "vector",
            result: [
              {
                metric: {
                  ...(stamp
                    ? {}
                    : { __name__: EXTERNAL_METRICS.find((m) => query.includes(m.name))!.name }),
                  job: "kafka",
                  instance: "broker:9404",
                },
                value: [
                  Number(url.searchParams.get("time")),
                  stamp ? String(Date.now() / 1000) : "0",
                ],
              },
            ],
          },
        }),
      );
    },
    "valid",
    client.caPem,
  );
  closers.push(() => fixture.close());
  const adapter = new PrometheusMetricsAdapter(new NodeBoundedJsonHttp());
  const input = { endpoint: fixture.origin, job: "kafka", instance: "broker:9404" };
  const clientIdentity = {
    kind: "pem" as const,
    certificatePem: client.caPem,
    privateKeyPem: client.privateKeyPem,
  };
  const secure = { caPem: fixture.caPem, clientIdentity };
  const snapshot = await adapter.capture(
    context(fixture.origin, undefined, secure),
    input,
    AbortSignal.timeout(5000),
  );
  expect(snapshot.metrics.map((m) => m.value)).toEqual([0, 0]);
  await expect.poll(() => fixture.sockets.size).toBe(0);
  for (const [extra, authorization, code] of [
    [secure, "Bearer wrong-password", "HTTPS_AUTHENTICATION"],
    [{ clientIdentity }, "Bearer private-fixture", "TLS_TRUST"],
    [{ caPem: fixture.caPem }, "Bearer private-fixture", "TLS_TRUST"],
  ] as const) {
    const error = await adapter
      .capture(context(fixture.origin, authorization, extra), input, AbortSignal.timeout(5000))
      .catch((cause: unknown) => cause);
    expect(error).toMatchObject({ code });
    const publicError = error as { message: string; recovery: string };
    expect(publicError.message + publicError.recovery).not.toMatch(
      /wrong-password|private-password-from-remote|private-fixture/,
    );
    await expect.poll(() => fixture.sockets.size).toBe(0);
  }
});
it("joins every admitted HTTP socket after cancellation before allowing original owner disposal", async () => {
  const server: Server = createServer((_request, response) => {
    response.writeHead(200, { "content-type": "application/json" });
    response.flushHeaders();
  });
  const sockets = new Set<object>();
  server.on("connection", (socket) => {
    sockets.add(socket);
    socket.once("close", () => sockets.delete(socket));
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  closers.push(
    () =>
      new Promise<void>((resolve) => {
        server.closeAllConnections();
        server.close(() => resolve());
      }),
  );
  const address = server.address();
  if (!address || typeof address === "string") throw new Error("Fixture port unavailable");
  const original = context(`http://127.0.0.1:${address.port}`),
    controller = new AbortController();
  const operation = new PrometheusMetricsAdapter(new NodeBoundedJsonHttp()).capture(
    original,
    { endpoint: original.baseUrl, job: "kafka", instance: "broker:9404" },
    controller.signal,
  );
  const rejected = expect(operation).rejects.toMatchObject({ code: "CANCELLED" });
  await expect.poll(() => sockets.size).toBe(4);
  controller.abort();
  await rejected;
  await expect.poll(() => sockets.size).toBe(0);
  expect(original.requestOwner!.cleanupUnresolved).toBe(false);
});
