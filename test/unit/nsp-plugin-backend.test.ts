import { randomUUID } from "node:crypto";

import { expect, it, vi } from "vitest";

import { NspCaptureBackend, type NspApiPort } from "../../plugins/nsp/backend";
import {
  NspApiError,
  type NspExecutionCallbacks,
  type NspTrustMaterial,
} from "../../plugins/nsp/backend/api-client";
import { parseNspResult, toPluginProfileSource } from "../../plugins/nsp/contracts";
import {
  HOST_PROTOCOL_VERSION,
  type HostCommand,
  type ProfileSummary,
  type ProfileTlsSummary,
} from "../../src/features/kafka/contracts";
import { translateFacadeFailure } from "../../src/features/kafka/facade/facade-support";
import type { PluginBackendHost } from "../../src/plugins/api";
import type { JsonValue } from "../../src/plugins/contracts";
import { testHostExecute } from "../support/host-response";

const credentials = {
  apiUrl: "https://nsp.example.test",
  username: "operator",
  password: "private-api-secret",
  verifyCertificate: true,
};
const source = {
  apiUrl: credentials.apiUrl,
  brokers: ["nsp.example.test:9192"],
  workflowName: "streamskopeNspCaptureV1",
  authentication: "tls" as const,
};
const material = {
  truststoreBase64: "private-truststore-bytes",
  truststorePassword: "private-tls-secret",
  sha256: "a".repeat(64),
  certificateCount: 2,
};
const saved = (id = "saved-profile"): ProfileTlsSummary => ({
  id,
  revision: 2,
  name: "NSP example",
  brokers: source.brokers,
  active: false,
  transport: "tls",
  trust: { kind: "jks", label: "NSP", materialPresent: true, passwordPresent: true },
  createdAt: "2026-01-01T00:00:00.000Z",
  updatedAt: "2026-01-01T00:00:00.000Z",
  source: toPluginProfileSource(source),
});
const request = (
  method = "nspCapture.connect",
  input: JsonValue = credentials,
): { method: string; input: JsonValue; requestId: string; correlationId: string } => ({
  method,
  input,
  requestId: randomUUID(),
  correlationId: randomUUID(),
});

function fixture(initialState: JsonValue = null): {
  backend: NspCaptureBackend;
  disconnect: ReturnType<typeof vi.fn<() => Promise<void>>>;
  host: PluginBackendHost;
  api: NspApiPort;
  state: { value: JsonValue };
  profiles: ProfileSummary[];
  commands: HostCommand[];
  events: JsonValue[];
} {
  const state = { value: initialState };
  const disconnect = vi.fn(() => Promise.resolve());
  const profiles: ProfileSummary[] = [];
  const commands: HostCommand[] = [];
  const events: JsonValue[] = [];
  const host: PluginBackendHost = {
    recoveryState: {
      read: () => Promise.resolve(state.value),
      write: (value) => {
        state.value = value;
        return Promise.resolve();
      },
    },
    profiles: () => Promise.resolve(profiles),
    connectionActive: () => false,
    deleteProfile: vi.fn(() => Promise.resolve()),
    disconnectOwnedConnection: disconnect,
    probeTopics: () => Promise.resolve([]),
    recordActivity: vi.fn(),
    publish: (_name, event) => {
      events.push(event);
    },
    failure: (error, context) =>
      translateFacadeFailure(
        error,
        { ...context, activeStateChanged: false, connection: undefined },
        true,
      ).error,
    execute: testHostExecute((command) => {
      commands.push(command);
      if (command.command === "profiles.create") profiles.push(saved());
      return Promise.resolve({
        command: command.command,
        id: command.id,
        version: HOST_PROTOCOL_VERSION,
        ok: true,
        result: {
          correlationId: command.id,
          ...(["profiles.create", "profiles.update"].includes(command.command)
            ? { profileId: "saved-profile" }
            : {}),
        },
      });
    }),
  };
  const api: NspApiPort = {
    authenticate: vi.fn(() => Promise.resolve()),
    readVersion: vi.fn(() =>
      Promise.resolve({ raw: "NSP-CN-26.4.0-rel.200", product: "26.4.0", build: 200 }),
    ),
    ensureWorkflow: vi.fn(() =>
      Promise.resolve({ id: randomUUID(), name: source.workflowName, fingerprint: "a".repeat(64) }),
    ),
    retrieveTrust: vi.fn(
      async (_id: string, callbacks?: NspExecutionCallbacks): Promise<NspTrustMaterial> => {
        await callbacks?.onExecution?.(randomUUID());
        return material;
      },
    ),
    cleanupExecution: vi.fn(() => Promise.resolve()),
    close: vi.fn(() => Promise.resolve()),
  };
  return {
    backend: new NspCaptureBackend(host, () => api),
    disconnect,
    host,
    api,
    state,
    profiles,
    commands,
    events,
  };
}

it("qualifies and saves host-only trust, then reuses the same profile on retry", async () => {
  const f = fixture();
  const first = await f.backend.execute(request());
  const second = await f.backend.execute(request());
  expect(first).toEqual({ ok: true, profileId: "saved-profile" });
  expect(second).toEqual(first);
  expect(f.commands.map((command) => command.command)).toEqual([
    "profiles.test",
    "profiles.create",
    "profiles.test",
    "profiles.update",
  ]);
  expect(f.commands[3]).toMatchObject({
    payload: { profileId: "saved-profile", profile: { expectedRevision: 2 } },
  });
  expect(f.profiles).toHaveLength(1);
  expect(f.api.readVersion).toHaveBeenCalledTimes(2);
  expect(f.state.value).toBeNull();
  for (const secret of [
    credentials.password,
    material.truststoreBase64,
    material.truststorePassword,
  ]) {
    expect(JSON.stringify([first, second, f.events])).not.toContain(secret);
  }
});

it("refreshes an existing profile through update qualification while retaining independent protected credentials", async () => {
  const f = fixture();
  const identity = { certificatePresent: true, privateKeyPresent: true, passphrasePresent: true };
  f.profiles.push({
    ...saved(),
    sasl: { mechanism: "SCRAM-SHA-512", username: "existing-kafka-user", passwordPresent: true },
    clientIdentity: identity,
    services: {
      schemaRegistry: {
        baseUrl: "https://schema.example.test",
        authentication: "basic",
        basic: { username: "schema-user", passwordPresent: true },
        trust: {
          mode: "custom",
          kind: "pem",
          label: "schema.pem",
          materialPresent: true,
          passwordPresent: false,
        },
        clientIdentity: identity,
      },
      connect: {
        baseUrl: "https://connect.example.test",
        authentication: "bearer",
        bearerPresent: true,
        trust: { mode: "system" },
      },
    },
  });
  expect(await f.backend.execute(request())).toEqual({ ok: true, profileId: "saved-profile" });
  const retained = {
    sasl: {
      mechanism: "SCRAM-SHA-512",
      username: "existing-kafka-user",
      password: { mode: "retain" },
    },
    clientIdentity: {
      certificatePem: { mode: "retain" },
      privateKeyPem: { mode: "retain" },
      passphrase: { mode: "retain" },
    },
    services: {
      schemaRegistry: {
        basic: { username: "schema-user", password: { mode: "retain" } },
        trust: { material: { mode: "retain" } },
        clientIdentity: { privateKeyPem: { mode: "retain" } },
      },
      connect: { bearer: { mode: "retain" }, trust: { mode: "system" } },
    },
  };
  expect(f.commands[0]).toMatchObject({
    command: "profiles.test",
    payload: {
      mode: "update",
      profileId: "saved-profile",
      profile: { expectedRevision: 2, ...retained },
    },
  });
  expect(f.commands[1]).toMatchObject({
    command: "profiles.update",
    payload: { profileId: "saved-profile", profile: retained },
  });
});

it("starts automatic refresh with existing OAuth when services reuse the broker token", async () => {
  const f = fixture();
  f.profiles.push({
    ...saved(),
    oauth: {
      clientId: "operator",
      clientSecretPresent: true,
      tokenEndpoint: `${credentials.apiUrl}/rest-gateway/rest/api/v1/auth/token`,
      scope: "",
    },
    services: {
      schemaRegistry: { baseUrl: "https://schema.example.test", authentication: "oauth" },
    },
  });
  expect(await f.backend.execute(request())).toEqual({ ok: true, profileId: "saved-profile" });
  expect(f.commands[0]).toMatchObject({
    command: "profiles.test",
    payload: {
      mode: "update",
      profile: {
        oauth: {
          clientId: "operator",
          clientSecret: { mode: "replace", value: credentials.password },
        },
        services: { schemaRegistry: { authentication: "oauth" } },
      },
    },
  });
  expect(f.commands.map((command) => command.command)).toEqual([
    "profiles.test",
    "profiles.update",
  ]);
});

it.each([false, true])(
  "rejects an unsupported target before remote mutation or profile refresh (existing=%s)",
  async (existing) => {
    const f = fixture();
    if (existing) f.profiles.push(saved());
    vi.mocked(f.api.readVersion).mockResolvedValue({
      raw: "NSP-CN-26.8.0-rel.10",
      product: "26.8.0",
      build: 10,
    });
    const result = parseNspResult(await f.backend.execute(request()));
    expect(result).toMatchObject({
      ok: false,
      error: { code: "VALIDATION" },
    });
    if (!result.ok) {
      expect(result.error.summary).toContain("26.4.0 through 26.4.0");
      expect(result.error.recovery).toContain("Pending execution cleanup remains available");
    }
    expect(f.api.ensureWorkflow).not.toHaveBeenCalled();
    expect(f.api.retrieveTrust).not.toHaveBeenCalled();
    expect(f.commands).toHaveLength(0);
    expect(f.state.value).toBeNull();
    expect(f.api.close).toHaveBeenCalledOnce();
  },
);

it("reconciles pending cleanup before rejecting a newly unsupported target", async () => {
  const recovery = {
    version: 1,
    apiUrl: credentials.apiUrl,
    username: credentials.username,
    requestId: randomUUID(),
    executionId: randomUUID(),
  };
  const f = fixture(recovery);
  vi.mocked(f.api.readVersion).mockImplementation(() => {
    expect(f.state.value).toBeNull();
    expect(f.api.cleanupExecution).toHaveBeenCalledWith(recovery.requestId, recovery.executionId);
    return Promise.resolve({ raw: "NSP-CN-27.1.0-rel.1", product: "27.1.0", build: 1 });
  });
  expect(await f.backend.execute(request())).toMatchObject({
    ok: false,
    error: { code: "VALIDATION" },
  });
  expect(f.api.ensureWorkflow).not.toHaveBeenCalled();
  expect(f.api.retrieveTrust).not.toHaveBeenCalled();
  expect(f.commands).toHaveLength(0);
  expect(f.state.value).toBeNull();
  await expect(f.backend.prepareUnload("remove")).resolves.toBeUndefined();
});

it("permits cleanup-only recovery after restart even when product version cannot be read", async () => {
  const recovery = {
    version: 1,
    apiUrl: credentials.apiUrl,
    username: credentials.username,
    requestId: randomUUID(),
    executionId: randomUUID(),
  };
  const f = fixture(recovery);
  vi.mocked(f.api.readVersion).mockRejectedValue(
    new NspApiError("Version API is unavailable.", "VERSION"),
  );
  expect(await f.backend.execute(request("nspCapture.cleanup"))).toMatchObject({
    ok: true,
    status: { state: "idle" },
  });
  expect(f.api.readVersion).not.toHaveBeenCalled();
  expect(f.api.cleanupExecution).toHaveBeenCalledWith(recovery.requestId, recovery.executionId);
  expect(f.api.ensureWorkflow).not.toHaveBeenCalled();
  expect(f.commands).toHaveLength(0);
  expect(f.state.value).toBeNull();
  expect(f.api.close).toHaveBeenCalledOnce();
});

it("persists recovery before execution starts and reconciles it after host restart", async () => {
  const f = fixture();
  vi.mocked(f.api.retrieveTrust).mockImplementation(async (_id, callbacks) => {
    expect(f.state.value).toMatchObject({
      apiUrl: credentials.apiUrl,
      username: credentials.username,
    });
    expect(JSON.stringify(f.state.value)).not.toContain(credentials.password);
    await callbacks?.onExecution?.("11111111-1111-4111-8111-111111111111");
    throw new Error("response lost");
  });
  vi.mocked(f.api.cleanupExecution).mockRejectedValue(new Error("offline"));
  expect(await f.backend.execute(request())).toMatchObject({
    ok: false,
    error: { code: "REMOTE_CLEANUP" },
  });
  expect(f.commands).toHaveLength(0);
  expect(f.state.value).toMatchObject({ executionId: "11111111-1111-4111-8111-111111111111" });
  await expect(f.backend.prepareUnload("remove")).rejects.toThrow(/cleanup/u);
  expect(f.disconnect).not.toHaveBeenCalled();
  await f.backend.close();
  const restarted = new NspCaptureBackend(f.host, () => f.api);
  expect(await restarted.execute(request("nspCapture.status", {}))).toMatchObject({
    ok: true,
    status: { state: "cleanup-required" },
  });
  vi.mocked(f.api.cleanupExecution).mockResolvedValue();
  expect(await restarted.execute(request("nspCapture.cleanup"))).toMatchObject({ ok: true });
  expect(f.state.value).toBeNull();
  await restarted.prepareUnload("remove");
  expect(vi.mocked(f.host.deleteProfile)).not.toHaveBeenCalled();
  expect(f.disconnect).toHaveBeenCalledOnce();
});

it("refuses foreign recovery and profile ownership before contacting NSP", async () => {
  const f = fixture({
    version: 1,
    apiUrl: "https://other.example.test",
    username: "operator",
    requestId: randomUUID(),
  });
  expect(await f.backend.execute(request())).toMatchObject({ ok: false });
  expect(f.api.authenticate).not.toHaveBeenCalled();
  expect(f.api.cleanupExecution).not.toHaveBeenCalled();
  const other = fixture();
  expect(
    await other.backend.execute(
      request("nspCapture.connect", { ...credentials, profileId: "foreign" }),
    ),
  ).toMatchObject({ ok: false });
  expect(other.api.authenticate).not.toHaveBeenCalled();
});

it("coalesces duplicate in-flight requests, rejects overlapping operations, and cleans up on cancellation", async () => {
  const f = fixture();
  let release: (() => void) | undefined;
  vi.mocked(f.api.retrieveTrust).mockImplementation(async () => {
    await new Promise<void>((resolve) => {
      release = resolve;
    });
    return material;
  });
  const operation = request();
  const first = f.backend.execute(operation);
  await vi.waitFor(() => expect(release).toBeDefined());
  const duplicate = f.backend.execute(operation);
  expect(
    await f.backend.execute({
      ...operation,
      input: { ...credentials, apiUrl: "https://other.example.test" },
    }),
  ).toMatchObject({ ok: false });
  expect(await f.backend.execute(request())).toMatchObject({ ok: false });
  const cancelled = f.backend.execute(
    request("nspCapture.cancel", { requestId: operation.requestId }),
  );
  release!();
  expect(await first).toMatchObject({ ok: false });
  expect(await duplicate).toMatchObject({ ok: false });
  expect(await cancelled).toMatchObject({ ok: true, cancelled: true });
  expect(f.api.retrieveTrust).toHaveBeenCalledOnce();
  expect(f.api.cleanupExecution).toHaveBeenCalled();
  expect(f.commands).toHaveLength(0);
  expect(f.state.value).toBeNull();
});

it.each(["update", "remove"] as const)(
  "hot %s waits for retrieval cleanup and retains profiles",
  async (reason) => {
    const f = fixture();
    let release: (() => void) | undefined;
    vi.mocked(f.api.retrieveTrust).mockImplementation(async () => {
      await new Promise<void>((resolve) => {
        release = resolve;
      });
      return material;
    });
    const operation = f.backend.execute(request());
    await vi.waitFor(() => expect(release).toBeDefined());
    expect(await f.backend.beforeChange()).toBeDefined();
    const unload = f.backend.prepareUnload(reason);
    release!();
    await operation;
    await unload;
    expect(f.disconnect).toHaveBeenCalledOnce();
    expect(vi.mocked(f.host.deleteProfile)).not.toHaveBeenCalled();
    expect(f.state.value).toBeNull();
  },
);

it("keeps the journal when persistence fails and blocks new remote work", async () => {
  const f = fixture();
  vi.spyOn(f.host.recoveryState!, "write").mockRejectedValue(new Error("disk full"));
  expect(await f.backend.execute(request())).toMatchObject({ ok: false });
  expect(f.api.retrieveTrust).not.toHaveBeenCalled();
  expect(f.commands).toHaveLength(0);
});

it("checks recorded broker identity without requiring an EDA-style tunnel", async () => {
  const f = fixture();
  await expect(
    f.backend.validateProfile(toPluginProfileSource(source).data, source.brokers),
  ).resolves.toBeUndefined();
  await expect(
    f.backend.validateProfile(toPluginProfileSource(source).data, ["other:9192"]),
  ).rejects.toThrow();
});
