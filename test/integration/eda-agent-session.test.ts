import { afterEach, expect, it } from "vitest";

import { EdaApiClient } from "../../plugins/eda/backend/eda-api-client";
import { sourceFromResource } from "../../plugins/eda/backend/eda-capture-source";
import { createHttpsTrustFixture } from "../support/https-trust-fixture";

const closeables: Array<{ close(): Promise<void> }> = [];

afterEach(async () => {
  await Promise.allSettled(closeables.splice(0).map((item) => item.close()));
});

it("serializes only the capture wire contract when given a richer discovered source", async () => {
  const submitted: unknown[] = [];
  const fixture = await createHttpsTrustFixture((request, response) => {
    if (request.url?.endsWith("/protocol/openid-connect/token")) {
      response.end(JSON.stringify({ access_token: "fixture-access" }));
      return;
    }
    if (
      request.method !== "POST" ||
      request.url !== "/core/httpproxy/v1/streamskope-capture/v1/sessions"
    ) {
      response.writeHead(404).end();
      return;
    }
    expect(request.headers.authorization).toBe("Bearer fixture-access");
    const chunks: Buffer[] = [];
    request.on("data", (chunk: Buffer) => chunks.push(chunk));
    request.on("end", () => {
      submitted.push(JSON.parse(Buffer.concat(chunks).toString("utf8")) as unknown);
      response.writeHead(201).end(JSON.stringify({ id: "capture-wire-test", phase: "Pending" }));
    });
  });
  closeables.push(fixture);
  const discovered = sourceFromResource(
    {
      metadata: { name: "interfaces", namespace: "eda-system" },
      spec: { brokers: "existing.example.test:9092", exports: [{ topic: "interfaces" }] },
    },
    "kafka.eda.nokia.com/v1",
    "Producer",
    "eda-system",
  );
  expect(discovered).toMatchObject({
    brokers: ["existing.example.test:9092"],
    topics: ["interfaces"],
  });
  if (discovered === undefined) throw new Error("The fixture producer was not discovered");
  const client = new EdaApiClient(
    { baseUrl: fixture.origin, username: "operator", password: "fixture-password" },
    { caPem: fixture.caPem, clientSecret: "fixture-client-secret" },
  );
  const input = {
    id: "capture-wire-test",
    leaseSeconds: 900,
    localPort: 19092,
    source: { ...discovered, extra: "source-private-value" },
    extra: "request-private-value",
  };
  await expect(client.createCaptureSession(input)).resolves.toMatchObject({ phase: "Pending" });
  expect(submitted).toEqual([
    {
      id: "capture-wire-test",
      leaseSeconds: 900,
      localPort: 19092,
      source: {
        apiVersion: "kafka.eda.nokia.com/v1",
        kind: "Producer",
        name: "interfaces",
        namespace: "eda-system",
      },
    },
  ]);
});

it("creates, inspects, renews, and removes one EDA agent capture through the authenticated proxy", async () => {
  const sessionId = "7ac7d717-8ef0-4a55-bd33-5b723e6b6918";
  const operations: string[] = [];
  const fixture = await createHttpsTrustFixture((request, response) => {
    const path = request.url ?? "";
    if (path.endsWith("/realms/master/protocol/openid-connect/token")) {
      response.end('{"access_token":"admin-access"}');
      return;
    }
    if (path.endsWith("/admin/realms/eda/clients")) {
      response.end('[{"clientId":"eda","id":"eda-uuid"}]');
      return;
    }
    if (path.endsWith("/admin/realms/eda/clients/eda-uuid/client-secret")) {
      response.end('{"value":"eda-client-secret"}');
      return;
    }
    if (path.endsWith("/realms/eda/protocol/openid-connect/token")) {
      response.end('{"access_token":"eda-access"}');
      return;
    }
    expect(request.headers.authorization).toBe("Bearer eda-access");
    operations.push(`${request.method} ${path}`);
    if (request.method === "POST") {
      const chunks: Buffer[] = [];
      request.on("data", (chunk: Buffer) => chunks.push(chunk));
      request.on("end", () => {
        expect(JSON.parse(Buffer.concat(chunks).toString("utf8"))).toMatchObject({
          id: sessionId,
          leaseSeconds: 900,
          localPort: 19092,
          source: { kind: "ClusterProducer", name: "alarms" },
        });
        response.writeHead(201).end(JSON.stringify({ id: sessionId, phase: "Pending" }));
      });
      return;
    }
    if (request.method === "PATCH") {
      response.end(JSON.stringify({ id: sessionId, phase: "Ready" }));
      return;
    }
    if (request.method === "DELETE") {
      response.writeHead(204).end();
      return;
    }
    response.end(JSON.stringify({ id: sessionId, phase: "Ready" }));
  });
  closeables.push(fixture);
  const client = new EdaApiClient(
    { baseUrl: fixture.origin, username: "admin", password: "secret" },
    { caPem: fixture.caPem },
  );
  const input = {
    id: sessionId,
    leaseSeconds: 900,
    localPort: 19092,
    source: {
      apiVersion: "kafka.eda.nokia.com/v1" as const,
      kind: "ClusterProducer" as const,
      name: "alarms",
      namespace: "eda-system",
    },
  };
  await expect(client.createCaptureSession(input)).resolves.toMatchObject({ phase: "Pending" });
  await expect(client.getCaptureSession(sessionId)).resolves.toMatchObject({ phase: "Ready" });
  await expect(client.renewCaptureSession(sessionId, 900)).resolves.toMatchObject({
    phase: "Ready",
  });
  await client.removeCaptureSession(sessionId);
  expect(operations).toEqual([
    "POST /core/httpproxy/v1/streamskope-capture/v1/sessions",
    `GET /core/httpproxy/v1/streamskope-capture/v1/sessions/${sessionId}`,
    `PATCH /core/httpproxy/v1/streamskope-capture/v1/sessions/${sessionId}/lease`,
    `DELETE /core/httpproxy/v1/streamskope-capture/v1/sessions/${sessionId}`,
  ]);
});
