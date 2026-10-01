import { afterEach, describe, expect, it, vi } from "vitest";

import { EdaCaptureBackend } from "../../plugins/eda/backend";
import { EdaAgentCapture } from "../../plugins/eda/backend/eda-agent-capture";
import { EDA_PROTOCOL_VERSION, parseEdaCaptureResponse } from "../../plugins/eda/contracts";
import type { EdaCaptureDeployInput } from "../../plugins/eda/contracts";
import { parsePluginJson } from "../../src/plugins/validation";
import { edaBackendHost } from "../support/eda-backend";
import { createHttpsTrustFixture } from "../support/https-trust-fixture";

const closeables: { close(): Promise<void> }[] = [];
afterEach(async () => {
  await Promise.all(closeables.splice(0).map((value) => value.close()));
});

async function fixture(version: unknown): Promise<{
  readonly edaApi: EdaCaptureDeployInput["edaApi"];
  readonly operations: readonly string[];
}> {
  const operations: string[] = [];
  const server = await createHttpsTrustFixture((request, response) => {
    const path = request.url ?? "";
    response.setHeader("content-type", "application/json");
    if (path.endsWith("/protocol/openid-connect/token")) {
      response.end(JSON.stringify({ access_token: "fixture-access" }));
      return;
    }
    if (path.endsWith("/admin/realms/eda/clients")) {
      response.end(JSON.stringify([{ clientId: "eda", id: "fixture-client" }]));
      return;
    }
    if (path.endsWith("/client-secret")) {
      response.end(JSON.stringify({ value: "fixture-client-secret" }));
      return;
    }
    expect(request.headers.authorization).toBe("Bearer fixture-access");
    operations.push(`${request.method} ${path}`);
    if (path === "/core/about/version") response.end(JSON.stringify({ eda: { version } }));
    else if (
      path === "/apps/kafka.eda.nokia.com/v1/clusterproducers" ||
      path === "/apps/kafka.eda.nokia.com/v1/producers"
    )
      response.end(JSON.stringify({ items: [] }));
    else if (path === "/apps/capture.streamskope.io/v1alpha1") response.end("{}");
    else if (path === "/core/httpproxy/v1/streamskope-capture/healthz")
      response.end(JSON.stringify({ status: "ready", version: "v26.8.2" }));
    else if (request.method === "GET") response.writeHead(404).end("{}");
    else response.writeHead(201).end("{}");
  });
  closeables.push(server);
  return {
    edaApi: {
      baseUrl: server.origin,
      username: "operator",
      password: "operator-secret",
      verifyTls: false,
    },
    operations,
  };
}

describe("installed EDA plugin cluster version gate", () => {
  it("reads the authenticated running version before discovery and accepts a matching production build", async () => {
    const { edaApi, operations } = await fixture("26.8.2-2609301234-gabcdef0");
    const capture = new EdaAgentCapture(() => Promise.reject(new Error("Unexpected Kafka access")));
    closeables.push(capture);
    await expect(capture.inspect({ edaApi })).resolves.toMatchObject({ sources: [] });
    expect(operations[0]).toBe("GET /core/about/version");
    expect(operations.slice(1).sort()).toEqual([
      "GET /apps/kafka.eda.nokia.com/v1/clusterproducers",
      "GET /apps/kafka.eda.nokia.com/v1/producers",
    ]);
  });

  it("checks the exact running version before installing the cluster application", async () => {
    const { edaApi, operations } = await fixture("v26.8.2");
    const capture = new EdaAgentCapture(() => Promise.reject(new Error("Unexpected Kafka access")));
    closeables.push(capture);
    await expect(
      capture.installApplication({
        edaApi,
        authorization: { username: "platform-admin", password: "administrator-secret" },
      }),
    ).resolves.toMatchObject({ state: "installed", version: "v26.8.2" });
    expect(operations[0]).toBe("GET /core/about/version");
    expect(operations).toContain("POST /workflows/v1/appstore.eda.nokia.com/v1/appinstallers");
    expect(operations.at(-1)).toBe("GET /core/httpproxy/v1/streamskope-capture/healthz");
  });

  it.each([
    "edaCapture.inspect",
    "edaCapture.application.status",
    "edaCapture.application.install",
    "edaCapture.deploy",
  ] as const)(
    "blocks %s on a different EDA patch without accessing or mutating capture resources",
    async (method) => {
      const { edaApi, operations } = await fixture("v26.8.3-2609301234-gabcdef0");
      const probe = vi.fn(() => Promise.resolve([]));
      const capture = new EdaAgentCapture(probe);
      closeables.push(capture);
      const activity: unknown[] = [];
      const backend = new EdaCaptureBackend(
        edaBackendHost({
          recordActivity: (entry) => {
            activity.push(entry);
          },
        }),
        capture,
      );
      const input =
        method === "edaCapture.deploy"
          ? {
              edaApi,
              context: "eda-agent",
              imageDelivery: "configured",
              localPort: 19092,
              source: {
                apiVersion: "kafka.eda.nokia.com/v1",
                kind: "Producer",
                namespace: "eda-system",
                name: "interfaces",
              },
            }
          : method === "edaCapture.application.install"
            ? {
                edaApi,
                authorization: { username: "platform-admin", password: "administrator-secret" },
              }
            : { edaApi };
      const response = parseEdaCaptureResponse(
        await backend.execute({
          method,
          input: parsePluginJson(input),
          requestId: "mismatch",
          correlationId: "version-gate",
        }),
      );
      expect(response).toMatchObject({
        version: EDA_PROTOCOL_VERSION,
        ok: false,
        error: { code: "VALIDATION", stage: "validation", retryable: false },
      });
      if (response.ok) throw new Error("Version mismatch unexpectedly passed");
      expect(response.error.summary).toContain("v26.8.2");
      expect(response.error.summary).toContain("v26.8.3");
      expect(response.error.recovery).toContain("Preferences > Plugins");
      expect(JSON.stringify(response)).not.toMatch(
        /operator-secret|administrator-secret|fixture-client-secret/u,
      );
      // Activity storage receives explicit sensitive values for redaction; its public detail must remain safe.
      expect(activity.map((entry) => (entry as { detail: string }).detail).join(" ")).not.toMatch(
        /operator-secret|administrator-secret|fixture-client-secret/u,
      );
      expect(operations).toEqual(["GET /core/about/version"]);
      expect(capture.status()).toMatchObject({ state: "idle", tunnel: "closed" });
      expect(capture.status().source).toBeUndefined();
      expect(probe).not.toHaveBeenCalled();
    },
  );

  it("fails closed when the API cannot provide a valid product version", async () => {
    const { edaApi, operations } = await fixture(undefined);
    const capture = new EdaAgentCapture(() => Promise.reject(new Error("Unexpected Kafka access")));
    closeables.push(capture);
    await expect(capture.inspect({ edaApi })).rejects.toThrow(/invalid product version/u);
    expect(operations).toEqual(["GET /core/about/version"]);
  });
});
