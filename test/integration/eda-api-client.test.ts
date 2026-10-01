import { readFile } from "node:fs/promises";

import { afterEach, expect, it } from "vitest";

import { EdaApiClient } from "../../plugins/eda/backend/eda-api-client";
import { createHttpsTrustFixture } from "../support/https-trust-fixture";

const closeables: Array<{ close(): Promise<void> }> = [];

afterEach(async () => {
  await Promise.allSettled(closeables.splice(0).map(async (closeable) => closeable.close()));
});

it("discovers the EDA client secret, authenticates, and manages Kafka exporters", async () => {
  const created: unknown[] = [];
  const fixture = await createHttpsTrustFixture((request, response) => {
    const path = request.url ?? "";
    if (path.endsWith("/realms/master/protocol/openid-connect/token")) {
      response.setHeader("content-type", "application/json");
      response.end('{"access_token":"admin-access"}');
      return;
    }
    if (path.endsWith("/admin/realms/eda/clients")) {
      expect(request.headers.authorization).toBe("Bearer admin-access");
      response.setHeader("content-type", "application/json");
      response.end('[{"clientId":"eda","id":"eda-uuid"}]');
      return;
    }
    if (path.endsWith("/admin/realms/eda/clients/eda-uuid/client-secret")) {
      response.setHeader("content-type", "application/json");
      response.end('{"value":"eda-client-secret"}');
      return;
    }
    if (path.endsWith("/realms/eda/protocol/openid-connect/token")) {
      response.setHeader("content-type", "application/json");
      response.end('{"access_token":"eda-access"}');
      return;
    }
    if (request.headers.authorization !== "Bearer eda-access") {
      response.writeHead(401);
      response.end();
      return;
    }
    if (request.method === "GET" && path === "/apps/kafka.eda.nokia.com/v1/clusterproducers") {
      response.setHeader("content-type", "application/json");
      response.end(
        JSON.stringify({
          items: [
            {
              apiVersion: "kafka.eda.nokia.com/v1",
              kind: "ClusterProducer",
              metadata: { name: "existing-export", namespace: "eda-system" },
              spec: { brokers: "old:9092", exports: [{ topic: "interfaces" }] },
            },
          ],
        }),
      );
      return;
    }
    if (request.method === "GET" && path === "/apps/kafka.eda.nokia.com/v1/producers") {
      response.setHeader("content-type", "application/json");
      response.end('{"items":[]}');
      return;
    }
    if (
      request.method === "GET" &&
      path === "/apps/kafka.eda.nokia.com/v1/clusterproducers/existing-export"
    ) {
      response.setHeader("content-type", "application/json");
      response.end(
        JSON.stringify({
          apiVersion: "kafka.eda.nokia.com/v1",
          kind: "ClusterProducer",
          metadata: { name: "existing-export", namespace: "eda-system" },
          spec: { brokers: "old:9092", exports: [{ topic: "interfaces" }] },
        }),
      );
      return;
    }
    if (
      request.method === "DELETE" &&
      path === "/apps/kafka.eda.nokia.com/v1/clusterproducers/streamskope-capture"
    ) {
      response.writeHead(404);
      response.end();
      return;
    }
    if (request.method === "POST" && path === "/apps/kafka.eda.nokia.com/v1/clusterproducers") {
      const chunks: Buffer[] = [];
      request.on("data", (chunk: Buffer) => chunks.push(chunk));
      request.on("end", () => {
        created.push(JSON.parse(Buffer.concat(chunks).toString("utf8")) as unknown);
        response.setHeader("content-type", "application/json");
        response.end("{}");
      });
      return;
    }
    response.writeHead(404);
    response.end();
  });
  closeables.push(fixture);
  const client = new EdaApiClient(
    { baseUrl: fixture.origin, password: "admin-password", username: "admin" },
    { caPem: fixture.caPem },
  );

  await expect(client.listProducers()).resolves.toHaveLength(1);
  const source = await client.getProducer({
    apiVersion: "kafka.eda.nokia.com/v1",
    kind: "ClusterProducer",
    name: "existing-export",
    namespace: "eda-system",
  });
  await client.replaceCaptureExporter("ClusterProducer", "eda-system", source);

  expect(created).toEqual([source]);
});

it("detects and explicitly installs the StreamSkope capture application", async () => {
  let installed = false;
  let healthChecks = 0;
  const submitted: unknown[] = [];
  const operations: string[] = [];
  const fixture = await createHttpsTrustFixture((request, response) => {
    const path = request.url ?? "";
    if (path.endsWith("/realms/master/protocol/openid-connect/token")) {
      response.setHeader("content-type", "application/json");
      response.end('{"access_token":"admin-access"}');
      return;
    }
    if (path.endsWith("/admin/realms/eda/clients")) {
      response.setHeader("content-type", "application/json");
      response.end('[{"clientId":"eda","id":"eda-uuid"}]');
      return;
    }
    if (path.endsWith("/admin/realms/eda/clients/eda-uuid/client-secret")) {
      response.setHeader("content-type", "application/json");
      response.end('{"value":"eda-client-secret"}');
      return;
    }
    if (path.endsWith("/realms/eda/protocol/openid-connect/token")) {
      response.setHeader("content-type", "application/json");
      response.end('{"access_token":"eda-access"}');
      return;
    }
    if (request.headers.authorization !== "Bearer eda-access") {
      response.writeHead(401).end();
      return;
    }
    if (request.method === "GET" && path === "/apps/capture.streamskope.io/v1alpha1") {
      response.writeHead(installed ? 200 : 404, { "content-type": "application/json" });
      response.end(installed ? "{}" : '{"message":"not installed"}');
      return;
    }
    if (request.method === "GET" && path === "/core/httpproxy/v1/streamskope-capture/healthz") {
      if (installed) healthChecks += 1;
      response.writeHead(!installed ? 404 : healthChecks === 1 ? 502 : 200, {
        "content-type": "application/json",
      });
      response.end(
        installed
          ? healthChecks === 1
            ? '{"message":"upstream unavailable during rollout"}'
            : `{"status":"ready","version":"${healthChecks < 3 ? "v26.8.1" : "v26.8.2"}"}`
          : "{}",
      );
      return;
    }
    if (
      request.method === "GET" &&
      path === "/apps/appstore.eda.nokia.com/v1/signingkeys/streamskope-capture"
    ) {
      operations.push("key:get");
      response.writeHead(404).end();
      return;
    }
    if (request.method === "POST" && path === "/apps/appstore.eda.nokia.com/v1/signingkeys") {
      const chunks: Buffer[] = [];
      request.on("data", (chunk: Buffer) => chunks.push(chunk));
      request.on("end", () => {
        operations.push("key:create");
        submitted.push(JSON.parse(Buffer.concat(chunks).toString("utf8")) as unknown);
        response.writeHead(201).end("{}");
      });
      return;
    }
    if (
      request.method === "GET" &&
      path === "/apps/appstore.eda.nokia.com/v1/catalogs/streamskope"
    ) {
      operations.push("catalog:get");
      response.writeHead(404, { "content-type": "application/json" });
      response.end('{"message":"not found"}');
      return;
    }
    if (request.method === "POST" && path === "/apps/appstore.eda.nokia.com/v1/catalogs") {
      const chunks: Buffer[] = [];
      request.on("data", (chunk: Buffer) => chunks.push(chunk));
      request.on("end", () => {
        operations.push("catalog:create");
        submitted.push(JSON.parse(Buffer.concat(chunks).toString("utf8")) as unknown);
        response.writeHead(201, { "content-type": "application/json" });
        response.end("{}");
      });
      return;
    }
    if (
      request.method === "POST" &&
      path === "/workflows/v1/appstore.eda.nokia.com/v1/appinstallers"
    ) {
      const chunks: Buffer[] = [];
      request.on("data", (chunk: Buffer) => chunks.push(chunk));
      request.on("end", () => {
        operations.push("installer:create");
        submitted.push(JSON.parse(Buffer.concat(chunks).toString("utf8")) as unknown);
        installed = true;
        response.writeHead(201, { "content-type": "application/json" });
        response.end('{"status":{"result":"Succeeded"}}');
      });
      return;
    }
    response.writeHead(404).end();
  });
  closeables.push(fixture);
  const client = new EdaApiClient(
    { baseUrl: fixture.origin, password: "admin-password", username: "admin" },
    { caPem: fixture.caPem },
  );

  await expect(client.captureApplicationStatus()).resolves.toMatchObject({ state: "missing" });
  await expect(client.installCaptureApplication()).resolves.toMatchObject({ state: "installed" });
  expect(healthChecks).toBe(3);
  expect(operations).toEqual([
    "catalog:get",
    "catalog:create",
    "key:get",
    "key:create",
    "installer:create",
  ]);
  expect(submitted).toHaveLength(3);
  expect(submitted[0]).toEqual({
    apiVersion: "appstore.eda.nokia.com/v1",
    kind: "Catalog",
    metadata: { name: "streamskope" },
    spec: {
      remoteType: "git",
      remoteURL: "https://github.com/asadarafat/streamskope.git",
      skipTLSVerify: false,
      title: "StreamSkope",
    },
  });
  expect(submitted[1]).toEqual({
    apiVersion: "appstore.eda.nokia.com/v1",
    kind: "SigningKey",
    metadata: { name: "streamskope-capture" },
    spec: {
      publicKeys: [
        {
          key: await readFile(
            new URL(
              "../../vendors/streamskope/apps/capture/signing/streamskope-eda.pub",
              import.meta.url,
            ),
            "utf8",
          ),
          title: "StreamSkope Capture",
        },
      ],
    },
  });
  expect(submitted[2]).toMatchObject({
    apiVersion: "appstore.eda.nokia.com/v1",
    kind: "AppInstaller",
    metadata: { namespace: "eda-system" },
    spec: {
      apps: [
        {
          appId: "capture.streamskope.io",
          catalog: "streamskope",
          version: { type: "semver", value: "v26.8.2" },
        },
      ],
      autoProcessRequirements: ["strict"],
      operation: "install",
    },
  });
  expect(JSON.stringify(submitted)).not.toContain("admin-password");
});

it("rejects an independently bumped capture agent version", async () => {
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
    if (path === "/apps/capture.streamskope.io/v1alpha1") {
      response.end("{}");
      return;
    }
    if (path === "/core/httpproxy/v1/streamskope-capture/healthz") {
      response.end('{"status":"ready","version":"v26.8.3"}');
      return;
    }
    response.writeHead(404).end();
  });
  closeables.push(fixture);
  const client = new EdaApiClient(
    { baseUrl: fixture.origin, password: "admin-password", username: "admin" },
    { caPem: fixture.caPem },
  );
  await expect(client.captureApplicationStatus()).resolves.toMatchObject({ state: "missing" });
});

it("refuses to overwrite a conflicting StreamSkope catalog or submit an installer", async () => {
  let installerSubmissions = 0;
  const fixture = await createHttpsTrustFixture((request, response) => {
    const path = request.url ?? "";
    if (path.endsWith("/realms/master/protocol/openid-connect/token")) {
      response.setHeader("content-type", "application/json");
      response.end('{"access_token":"admin-access"}');
      return;
    }
    if (path.endsWith("/admin/realms/eda/clients")) {
      response.setHeader("content-type", "application/json");
      response.end('[{"clientId":"eda","id":"eda-uuid"}]');
      return;
    }
    if (path.endsWith("/admin/realms/eda/clients/eda-uuid/client-secret")) {
      response.setHeader("content-type", "application/json");
      response.end('{"value":"eda-client-secret"}');
      return;
    }
    if (path.endsWith("/realms/eda/protocol/openid-connect/token")) {
      response.setHeader("content-type", "application/json");
      response.end('{"access_token":"eda-access"}');
      return;
    }
    if (
      request.method === "GET" &&
      path === "/apps/appstore.eda.nokia.com/v1/catalogs/streamskope"
    ) {
      response.writeHead(200, { "content-type": "application/json" });
      response.end(
        JSON.stringify({
          apiVersion: "appstore.eda.nokia.com/v1",
          kind: "Catalog",
          metadata: { name: "streamskope" },
          spec: { remoteURL: "https://example.invalid/another-catalog.git", skipTLSVerify: false },
        }),
      );
      return;
    }
    if (
      request.method === "POST" &&
      path === "/workflows/v1/appstore.eda.nokia.com/v1/appinstallers"
    ) {
      installerSubmissions += 1;
    }
    response.writeHead(404, { "content-type": "application/json" });
    response.end('{"message":"not found"}');
  });
  closeables.push(fixture);
  const client = new EdaApiClient(
    { baseUrl: fixture.origin, password: "admin-password", username: "admin" },
    { caPem: fixture.caPem },
  );

  await expect(client.installCaptureApplication()).rejects.toMatchObject({
    code: "BACKEND_UNAVAILABLE",
    message: "The existing StreamSkope catalog points to a different source.",
    retryable: false,
  });
  expect(installerSubmissions).toBe(0);
});

it("reports the EDA catalog resource error before submitting an installer", async () => {
  let installerSubmissions = 0;
  const fixture = await createHttpsTrustFixture((request, response) => {
    const path = request.url ?? "";
    if (path.endsWith("/realms/master/protocol/openid-connect/token")) {
      response.setHeader("content-type", "application/json");
      response.end('{"access_token":"admin-access"}');
      return;
    }
    if (path.endsWith("/admin/realms/eda/clients")) {
      response.setHeader("content-type", "application/json");
      response.end('[{"clientId":"eda","id":"eda-uuid"}]');
      return;
    }
    if (path.endsWith("/admin/realms/eda/clients/eda-uuid/client-secret")) {
      response.setHeader("content-type", "application/json");
      response.end('{"value":"eda-client-secret"}');
      return;
    }
    if (path.endsWith("/realms/eda/protocol/openid-connect/token")) {
      response.setHeader("content-type", "application/json");
      response.end('{"access_token":"eda-access"}');
      return;
    }
    if (request.headers.authorization !== "Bearer eda-access") {
      response.writeHead(401).end();
      return;
    }
    if (
      request.method === "GET" &&
      path === "/apps/appstore.eda.nokia.com/v1/catalogs/streamskope"
    ) {
      response.writeHead(200, { "content-type": "application/json" });
      response.end(
        JSON.stringify({
          apiVersion: "appstore.eda.nokia.com/v1",
          kind: "Catalog",
          metadata: { name: "streamskope" },
          spec: {
            remoteType: "git",
            remoteURL: "https://github.com/asadarafat/streamskope.git",
            skipTLSVerify: false,
          },
          status: {
            operational: false,
            error:
              "could not fetch catalog streamskope: cannot open directory: /vendors/streamskope/apps/capture",
          },
        }),
      );
      return;
    }
    if (
      request.method === "POST" &&
      path === "/workflows/v1/appstore.eda.nokia.com/v1/appinstallers"
    ) {
      installerSubmissions += 1;
    }
    response.writeHead(404, { "content-type": "application/json" });
    response.end('{"message":"not found"}');
  });
  closeables.push(fixture);
  const client = new EdaApiClient(
    { baseUrl: fixture.origin, password: "admin-password", username: "admin" },
    { caPem: fixture.caPem },
  );

  await expect(client.installCaptureApplication()).rejects.toMatchObject({
    code: "BACKEND_UNAVAILABLE",
    message: "EDA cannot read the published StreamSkope Capture catalog directory.",
    recovery:
      "Verify the published version tag and that vendors/streamskope/apps/capture is a physical directory with no catalog navigation symlink, then refresh the EDA catalog.",
    retryable: true,
  });
  expect(installerSubmissions).toBe(0);
});

it("reports an unreadable StreamSkope catalog directory without waiting for readiness timeout", async () => {
  const publicKey = await readFile(
    new URL("../../vendors/streamskope/apps/capture/signing/streamskope-eda.pub", import.meta.url),
    "utf8",
  );
  const fixture = await createHttpsTrustFixture((request, response) => {
    const path = request.url ?? "";
    if (path.endsWith("/realms/master/protocol/openid-connect/token")) {
      response.setHeader("content-type", "application/json");
      response.end('{"access_token":"admin-access"}');
      return;
    }
    if (path.endsWith("/admin/realms/eda/clients")) {
      response.setHeader("content-type", "application/json");
      response.end('[{"clientId":"eda","id":"eda-uuid"}]');
      return;
    }
    if (path.endsWith("/admin/realms/eda/clients/eda-uuid/client-secret")) {
      response.setHeader("content-type", "application/json");
      response.end('{"value":"eda-client-secret"}');
      return;
    }
    if (path.endsWith("/realms/eda/protocol/openid-connect/token")) {
      response.setHeader("content-type", "application/json");
      response.end('{"access_token":"eda-access"}');
      return;
    }
    if (request.headers.authorization !== "Bearer eda-access") {
      response.writeHead(401).end();
      return;
    }
    if (
      request.method === "GET" &&
      path === "/apps/appstore.eda.nokia.com/v1/catalogs/streamskope"
    ) {
      response.writeHead(200, { "content-type": "application/json" });
      response.end(
        JSON.stringify({
          apiVersion: "appstore.eda.nokia.com/v1",
          kind: "Catalog",
          metadata: { name: "streamskope" },
          spec: {
            remoteType: "git",
            remoteURL: "https://github.com/asadarafat/streamskope.git",
            skipTLSVerify: false,
          },
        }),
      );
      return;
    }
    if (
      request.method === "GET" &&
      path === "/apps/appstore.eda.nokia.com/v1/signingkeys/streamskope-capture"
    ) {
      response.writeHead(200, { "content-type": "application/json" });
      response.end(
        JSON.stringify({
          kind: "SigningKey",
          metadata: { name: "streamskope-capture" },
          spec: {
            publicKeys: [{ key: publicKey, title: "StreamSkope Capture" }],
          },
        }),
      );
      return;
    }
    if (
      request.method === "POST" &&
      path === "/workflows/v1/appstore.eda.nokia.com/v1/appinstallers"
    ) {
      response.writeHead(201, { "content-type": "application/json" });
      response.end(
        JSON.stringify({
          metadata: { name: "streamskope-capture-fixture" },
          status: {
            result: "Failed",
            error:
              "error fetching app data: failed to get manifests from asc pod through GRPC: rpc error: code = Unknown desc = could not fetch catalog streamskope: cannot open directory: /vendors/streamskope/apps/capture",
          },
        }),
      );
      return;
    }
    response.writeHead(404, { "content-type": "application/json" });
    response.end('{"message":"not found"}');
  });
  closeables.push(fixture);
  const client = new EdaApiClient(
    { baseUrl: fixture.origin, password: "admin-password", username: "admin" },
    { caPem: fixture.caPem },
    AbortSignal.timeout(250),
  );

  await expect(client.installCaptureApplication()).rejects.toMatchObject({
    code: "BACKEND_UNAVAILABLE",
    message: "EDA cannot read the published StreamSkope Capture catalog directory.",
    recovery:
      "Verify the published version tag and that vendors/streamskope/apps/capture is a physical directory with no catalog navigation symlink, then refresh the EDA catalog.",
    retryable: true,
  });
});
