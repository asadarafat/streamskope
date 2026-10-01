import { createHash, randomUUID } from "node:crypto";
import type { IncomingMessage, ServerResponse } from "node:http";

import { afterEach, expect, it } from "vitest";

import {
  NspApiClient,
  NspCleanupError,
  parseNspVersion,
  validateTrustMaterial,
} from "../../plugins/nsp/backend/api-client";
import { NSP_WORKFLOW_DEFINITION, NSP_WORKFLOW_NAME } from "../../plugins/nsp/backend/workflow";
import { createHttpsTrustFixture } from "../support/https-trust-fixture";

const API = "/wfm/api/v1";
const workflowId = "6b6017a8-f6f7-4f70-a198-89e6fed8241a";
const closes: Array<() => Promise<void>> = [];
afterEach(async () => {
  await Promise.allSettled(closes.splice(0).map((close) => close()));
});

function material(): Record<string, unknown> {
  // The wire validator checks envelope/integrity; the profile trust parser checks certificates.
  const bytes = Buffer.from("feedfeed0000000200000000", "hex");
  return {
    store_type: "JKS",
    encoding: "base64",
    size_bytes: bytes.length,
    certificate_count: 1,
    sha256: createHash("sha256").update(bytes).digest("hex"),
    truststore_base64: bytes.toString("base64"),
    truststore_password: "wire-secret",
  };
}

interface FixtureOptions {
  readonly versionResponse?: unknown;
  readonly versionStatus?: number;
  readonly existingExecutionId?: string;
  readonly definition?: string;
  readonly loseCreateResponse?: boolean;
  readonly denyDelete?: boolean;
  readonly running?: boolean;
  readonly badMaterial?: boolean;
}
interface NspFixture {
  readonly client: (signal?: AbortSignal) => NspApiClient;
  readonly methods: string[];
  readonly bodies: unknown[];
  readonly execution: Record<string, unknown> | undefined;
  readonly revoked: boolean;
}
async function fixture(options: FixtureOptions = {}): Promise<NspFixture> {
  let definition: string | undefined = options.definition;
  let published = definition !== undefined;
  let execution: Record<string, unknown> | undefined =
    options.existingExecutionId === undefined
      ? undefined
      : {
          id: options.existingExecutionId,
          workflow_name: NSP_WORKFLOW_NAME,
          description: `streamskope.nsp:${options.existingExecutionId}`,
          state: "SUCCESS",
          output: { result: material() },
        };
  const methods: string[] = [];
  const bodies: unknown[] = [];
  let revoked = false;
  async function handle(request: IncomingMessage, response: ServerResponse): Promise<void> {
    const chunks: Buffer[] = [];
    for await (const chunk of request) chunks.push(Buffer.from(chunk as Buffer));
    const text = Buffer.concat(chunks).toString();
    const path = new URL(request.url ?? "/", "https://fixture").pathname;
    methods.push(`${request.method} ${path}`);
    const reply = (value: unknown, status = 200): void => {
      response.writeHead(status, { "content-type": "application/json" });
      response.end(JSON.stringify(value));
    };
    if (path.endsWith("/auth/token")) {
      expect(request.headers.authorization).toBe(
        `Basic ${Buffer.from("operator:api-secret").toString("base64")}`,
      );
      reply({ access_token: "fixture-token" });
      return;
    }
    if (path.endsWith("/auth/revocation")) {
      expect(request.headers.authorization).toBe(
        `Basic ${Buffer.from("operator:api-secret").toString("base64")}`,
      );
      expect(request.headers["content-type"]).toBe("application/x-www-form-urlencoded");
      expect(new URLSearchParams(text).get("token")).toBe("fixture-token");
      expect(new URLSearchParams(text).get("token_type_hint")).toBe("client_credentials");
      revoked = true;
      response.end();
      return;
    }
    expect(request.headers.authorization).toBe("Bearer fixture-token");
    if (path === "/sdn/api/v4/system/version") {
      reply(
        "versionResponse" in options
          ? options.versionResponse
          : { response: { status: 0, data: "NSP-CN-26.4.0-rel.200" } },
        options.versionStatus ?? 200,
      );
      return;
    }
    const body = text ? (JSON.parse(text) as Record<string, unknown>) : undefined;
    if (body) bodies.push(body);
    if (path === `${API}/workflow` && request.method === "POST") {
      definition = String(body?.yaml);
      reply({ response: { data: [] } });
      return;
    }
    if (path.endsWith("/status")) {
      published = true;
      reply({});
      return;
    }
    if (path.startsWith(`${API}/workflow/`)) {
      if (!definition) {
        reply({}, 404);
        return;
      }
      reply({
        response: {
          data: {
            id: workflowId,
            name: NSP_WORKFLOW_NAME,
            definition,
            details: { status: published ? "PUBLISHED" : "DRAFT", local_definition: "" },
          },
        },
      });
      return;
    }
    if (path === `${API}/execution` && request.method === "POST") {
      execution = {
        id: body?.id,
        workflow_name: NSP_WORKFLOW_NAME,
        description: body?.description,
        state: options.running ? "RUNNING" : "SUCCESS",
        output: { result: { ...material(), ...(options.badMaterial ? { sha256: "bad" } : {}) } },
      };
      if (options.loseCreateResponse) {
        request.socket.destroy();
        return;
      }
      reply({ response: { data: [execution] } });
      return;
    }
    if (path === `${API}/execution`) {
      reply({
        response: {
          next: "None",
          totalRows: execution ? 1 : 0,
          data: execution ? [execution] : [],
        },
      });
      return;
    }
    if (path.startsWith(`${API}/execution/`)) {
      if (!execution) {
        reply({}, 404);
        return;
      }
      if (request.method === "DELETE") {
        if (options.denyDelete) {
          reply({ secret: "wire-secret" }, 403);
          return;
        }
        execution = undefined;
        reply({ response: { data: true } });
        return;
      }
      if (request.method === "PUT") execution.state = body?.state;
      reply({ response: { data: execution } });
      return;
    }
    reply({}, 404);
  }
  const server = await createHttpsTrustFixture((req, res) => {
    void handle(req, res);
  });
  closes.push(() => server.close());
  const client = (signal?: AbortSignal): NspApiClient =>
    new NspApiClient(
      {
        apiUrl: server.origin,
        username: "operator",
        password: "api-secret",
        verifyCertificate: true,
      },
      { caPem: server.caPem, pollIntervalMs: 1, requestTimeoutMs: 1000, executionTimeoutMs: 100 },
      signal,
    );
  return {
    client,
    methods,
    bodies,
    get execution(): Record<string, unknown> | undefined {
      return execution;
    },
    get revoked(): boolean {
      return revoked;
    },
  };
}

it("creates and publishes one helper, reuses it, retrieves trust, deletes history and revokes its token", async () => {
  const f = await fixture();
  const client = f.client();
  const requestId = randomUUID();
  expect((await client.ensureWorkflow()).id).toBe(workflowId);
  expect((await client.ensureWorkflow()).id).toBe(workflowId);
  const registered: string[] = [];
  expect(
    await client.retrieveTrust(requestId, {
      onExecution: (id) => {
        registered.push(id);
        return Promise.resolve();
      },
    }),
  ).toMatchObject({ truststorePassword: "wire-secret", certificateCount: 1 });
  expect(registered).toEqual([requestId]);
  expect(f.execution).toBeUndefined();
  await client.cleanupExecution(requestId, registered[0]);
  await client.close();
  await client.close();
  expect(f.revoked).toBe(true);
  expect(f.methods.filter((method) => method === `POST ${API}/workflow`)).toHaveLength(1);
  expect(f.methods.filter((method) => method === `POST ${API}/execution`)).toHaveLength(1);
  expect(f.methods.filter((method) => method === "GET /sdn/api/v4/system/version")).toHaveLength(1);
  expect(f.methods.indexOf("GET /sdn/api/v4/system/version")).toBeLessThan(
    f.methods.indexOf(`GET ${API}/workflow/${NSP_WORKFLOW_NAME}`),
  );
  expect(JSON.stringify(f.bodies)).not.toContain("wire-secret");
});

it("reads the authoritative NSP product and build without creating a workflow, then revokes access", async () => {
  const f = await fixture();
  const client = f.client();
  expect(await client.readVersion()).toEqual({
    raw: "NSP-CN-26.4.0-rel.200",
    product: "26.4.0",
    build: 200,
  });
  await client.close();
  expect(f.methods).toEqual([
    "POST /rest-gateway/rest/api/v1/auth/token",
    "GET /sdn/api/v4/system/version",
    "POST /rest-gateway/rest/api/v1/auth/revocation",
  ]);
  expect(f.revoked).toBe(true);
});

it.each(["26.3.0", "26.4.1", "26.8.0"])(
  "refuses helper creation, adoption and execution on unsupported NSP %s",
  async (version) => {
    const f = await fixture({
      definition: NSP_WORKFLOW_DEFINITION,
      versionResponse: { response: { status: 0, data: `NSP-CN-${version}-rel.200` } },
    });
    const client = f.client();
    const checking = client.ensureWorkflow();
    await expect(checking).rejects.toMatchObject({ code: "VERSION" });
    await expect(checking).rejects.toThrow("26.4.0 through 26.4.0");
    await expect(client.retrieveTrust(randomUUID())).rejects.toMatchObject({ code: "VERSION" });
    expect(f.methods.some((method) => method.includes(API))).toBe(false);
    await client.close();
    expect(f.revoked).toBe(true);
  },
);

it.each([
  { versionResponse: { response: { status: 0, data: "unrecognized-secret-response" } } },
  { versionResponse: { response: { status: 1, data: "NSP-CN-26.4.0-rel.200" } } },
  { versionStatus: 403 },
  { versionStatus: 404 },
])("fails closed before workflow access when version cannot be verified: %j", async (options) => {
  const f = await fixture(options);
  const client = f.client();
  const checking = client.ensureWorkflow();
  await expect(checking).rejects.toMatchObject({ code: "VERSION" });
  await expect(checking).rejects.toThrow("26.4.0 through 26.4.0");
  expect(f.methods.some((method) => method.includes(API))).toBe(false);
  await client.close();
  expect(f.revoked).toBe(true);
});

it("can clean an owned execution when the product version endpoint is unavailable", async () => {
  const requestId = randomUUID();
  const f = await fixture({ versionStatus: 404, existingExecutionId: requestId });
  const client = f.client();
  await expect(client.cleanupExecution(requestId)).resolves.toBeUndefined();
  expect(f.methods).toContain(`DELETE ${API}/execution/${requestId}`);
  expect(f.execution).toBeUndefined();
  expect(f.methods).not.toContain("GET /sdn/api/v4/system/version");
  expect(f.methods.some((method) => method.includes(`${API}/workflow`))).toBe(false);
  await client.close();
  expect(f.revoked).toBe(true);
});

it.each([
  undefined,
  [],
  { response: [] },
  { response: { status: "0", data: "NSP-CN-26.4.0-rel.200" } },
  { response: { data: "NSP-CN-26.4.0-rel.200" } },
  { response: { status: 0, data: 26.4 } },
  { response: { status: 0, data: ["NSP-CN-26.4.0-rel.200"] } },
  ...[
    "26.4.0",
    "NSP-CN-26.4-rel.200",
    "NSP-CN-026.4.0-rel.200",
    "NSP-CN-26.4.0-rel.0200",
    "NSP-CN-26.4.0-rel.200\n",
    "prefix-NSP-CN-26.4.0-rel.200",
    "NSP-CN-26.4.0-rel.200-extra",
    "NSP-CN-9007199254740992.4.0-rel.200",
    "NSP-CN-26.4.0-rel.9007199254740992",
    "x".repeat(129),
  ].map((data) => ({ response: { status: 0, data } })),
])(
  "strictly rejects malformed product version wrapper %j without echoing response data",
  (value) => {
    expect(() => parseNspVersion(value)).toThrow(/recognized product version/u);
    try {
      parseNspVersion(value);
    } catch (error) {
      expect(String(error)).not.toContain("unrecognized-secret-response");
    }
  },
);

it("refuses a foreign workflow occupying the helper name without mutation", async () => {
  const f = await fixture({ definition: "operator-owned workflow" });
  const client = f.client();
  await expect(client.ensureWorkflow()).rejects.toMatchObject({ code: "CONFLICT" });
  expect(f.methods.some((method) => method.startsWith("PUT "))).toBe(false);
  expect(f.methods.some((method) => method === `POST ${API}/execution`)).toBe(false);
  await client.close();
});

it("reconciles an accepted execution whose POST response was lost without executing twice", async () => {
  const f = await fixture({ loseCreateResponse: true });
  const client = f.client();
  await expect(client.retrieveTrust(randomUUID())).resolves.toMatchObject({ certificateCount: 1 });
  expect(f.methods.filter((method) => method === `POST ${API}/execution`)).toHaveLength(1);
  expect(f.execution).toBeUndefined();
  await client.close();
});

it("cancels a running execution and cleans it despite the caller signal being aborted", async () => {
  const f = await fixture({ running: true });
  const abort = new AbortController();
  const client = f.client(abort.signal);
  await expect(
    client.retrieveTrust(randomUUID(), {
      onExecution: () => {
        abort.abort();
        return Promise.resolve();
      },
    }),
  ).rejects.toMatchObject({ code: "CANCELLED" });
  expect(f.bodies).toContainEqual({ state: "CANCELLED" });
  expect(f.execution).toBeUndefined();
  await client.close();
  expect(f.revoked).toBe(true);
});

it("preserves the request identity when history cleanup fails and never returns trust", async () => {
  const f = await fixture({ denyDelete: true });
  const client = f.client();
  const requestId = randomUUID();
  await expect(client.retrieveTrust(requestId)).rejects.toMatchObject({
    name: "NspCleanupError",
    requestId,
  });
  expect(f.execution).toBeDefined();
  try {
    await client.cleanupExecution(requestId);
  } catch (error) {
    expect(error).toBeInstanceOf(NspCleanupError);
    expect(String(error)).not.toContain("wire-secret");
  }
  await client.close();
});

it("refuses cleanup of an execution with a different request marker", async () => {
  const f = await fixture({ denyDelete: true });
  const client = f.client();
  await expect(client.retrieveTrust(randomUUID())).rejects.toBeInstanceOf(NspCleanupError);
  const deletes = f.methods.filter((method) => method.startsWith("DELETE ")).length;
  await expect(
    client.cleanupExecution(randomUUID(), String(f.execution?.id)),
  ).rejects.toBeInstanceOf(NspCleanupError);
  expect(f.methods.filter((method) => method.startsWith("DELETE "))).toHaveLength(deletes);
  await client.close();
});

it("cleans execution history when returned truststore integrity fails", async () => {
  const f = await fixture({ badMaterial: true });
  const client = f.client();
  await expect(client.retrieveTrust(randomUUID())).rejects.toMatchObject({ code: "WORKFLOW" });
  expect(f.execution).toBeUndefined();
  await client.close();
});

it("rejects malformed material and never includes secret output in validation errors", () => {
  for (const change of [
    { truststore_base64: "not base64" },
    { size_bytes: 999 },
    { store_type: "PKCS12" },
    { certificate_count: 0 },
    { sha256: "wire-secret" },
  ]) {
    expect(() => validateTrustMaterial({ ...material(), ...change })).toThrow();
    try {
      validateTrustMaterial({ ...material(), ...change });
    } catch (error) {
      expect(String(error)).not.toContain("wire-secret");
    }
  }
  expect(NSP_WORKFLOW_DEFINITION).not.toContain(".keypass");
});
