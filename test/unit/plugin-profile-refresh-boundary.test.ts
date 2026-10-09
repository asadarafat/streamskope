import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { expect, it, vi } from "vitest";

import {
  HOST_PROTOCOL_VERSION,
  type HostCommand,
  type ProfileSummary,
  type ProfileUpdateInput,
} from "../../src/features/kafka/contracts";
import type { PluginBackend, PluginBackendHost, PluginBackendModule } from "../../src/plugins/api";
import type { PluginManifest } from "../../src/plugins/contracts";
import { compatiblePluginProfileRefresh } from "../../src/plugins/profile-refresh-compatibility";
import { encodePluginPackage, pluginPackageSha256 } from "../../src/platform/node/plugins/package";
import { PluginRuntime } from "../../src/platform/node/plugins/runtime";
import { PluginStore } from "../../src/platform/node/plugins/store";
import { testHostExecute } from "../support/host-response";
import { pluginSecurityProfile, retainedPluginProfile } from "../support/plugin-profile-security";

it.each(["sasl", "clientIdentity", "services"] as const)(
  "rejects a plugin refresh that omits existing %s",
  (key) => {
    const draft = { ...retainedPluginProfile() };
    delete draft[key];
    expect(compatiblePluginProfileRefresh(draft, pluginSecurityProfile)).toBe(false);
    expect(compatiblePluginProfileRefresh(retainedPluginProfile(), pluginSecurityProfile)).toBe(
      true,
    );
  },
);

it("rejects copied safe summaries rather than treating presence flags as credential instructions", () => {
  expect(
    compatiblePluginProfileRefresh({
      ...retainedPluginProfile(),
      services: pluginSecurityProfile.services,
    }),
  ).toBe(false);
  expect(
    compatiblePluginProfileRefresh({
      ...retainedPluginProfile(),
      clientIdentity: pluginSecurityProfile.clientIdentity,
    }),
  ).toBe(false);
});

it("blocks old backend refresh before dispatch while accepting retained credentials and legacy profiles", async () => {
  const directory = await mkdtemp(join(tmpdir(), "streamskope-plugin-security-"));
  const store = new PluginStore(directory);
  const manifest: PluginManifest = {
    id: "sample.connection",
    name: "Sample",
    version: "1.0.0",
    apiVersion: 2 as const,
    backend: "backend.cjs",
    renderer: "renderer.js",
  };
  const bytes = encodePluginPackage(
    manifest,
    new Map([
      ["backend.cjs", Buffer.from("exports.activate = () => ({})")],
      ["renderer.js", Buffer.from("export default {}")],
    ]),
  );
  await store.install(bytes, pluginPackageSha256(bytes));
  let sdk: PluginBackendHost | undefined;
  let profiles: readonly ProfileSummary[] = [pluginSecurityProfile];
  const dispatched = vi.fn((command: HostCommand) =>
    Promise.resolve({
      command: command.command,
      id: command.id,
      version: HOST_PROTOCOL_VERSION,
      ok: true as const,
      result: { correlationId: "security" },
    }),
  );
  const runtime = new PluginRuntime({
    store,
    hostRelease: "v0.2.0",
    loadModule: (): Promise<PluginBackendModule> =>
      Promise.resolve({
        activate: (host): PluginBackend => {
          sdk = host;
          return {
            execute: (): Promise<null> => Promise.resolve(null),
            validateProfile: (): Promise<void> => Promise.resolve(),
            beforeExit: (): Promise<undefined> => Promise.resolve(undefined),
            resolveExit: (): Promise<boolean> => Promise.resolve(true),
            beforeChange: (): Promise<undefined> => Promise.resolve(undefined),
            prepareUnload: (): Promise<void> => Promise.resolve(),
            close: (): Promise<void> => Promise.resolve(),
          };
        },
      }),
  });
  runtime.bindHost({
    execute: testHostExecute(dispatched),
    profiles: () => Promise.resolve(profiles),
    connectionActive: () => false,
    deleteProfile: () => Promise.resolve(),
    disconnectPluginConnection: () => Promise.resolve(),
    recordActivity: () => undefined,
    failure: (_error, context) => ({
      activeStateChanged: false,
      code: "BACKEND_UNAVAILABLE",
      correlationId: context.correlationId,
      recovery: "Retry",
      retryable: false,
      stage: "backend",
      summary: "Unavailable",
    }),
  });
  const command = (profile: ProfileUpdateInput): HostCommand & { command: "profiles.update" } => ({
    command: "profiles.update",
    id: "refresh",
    version: HOST_PROTOCOL_VERSION,
    payload: { profileId: pluginSecurityProfile.id, profile },
  });
  try {
    await runtime.start();
    const legacyDraft: ProfileUpdateInput = {
      name: "Managed connection",
      brokers: ["broker.example:9093"],
      transport: "tls",
      trust: {
        kind: "pem",
        label: "broker.pem",
        material: { mode: "replace", value: "rotated-ca" },
        password: { mode: "clear" },
      },
    };
    await expect(sdk!.execute(command(legacyDraft))).rejects.toThrow(
      /saved profile is unchanged.*Update the plugin/u,
    );
    expect(dispatched).not.toHaveBeenCalled();
    expect(profiles).toEqual([pluginSecurityProfile]);
    await expect(sdk!.execute(command(retainedPluginProfile()))).resolves.toMatchObject({
      ok: true,
    });
    expect(dispatched).toHaveBeenCalledOnce();
    const legacy = { ...pluginSecurityProfile };
    delete legacy.sasl;
    delete legacy.clientIdentity;
    delete legacy.services;
    profiles = [legacy];
    await expect(sdk!.execute(command(legacyDraft))).resolves.toMatchObject({ ok: true });
    expect(dispatched).toHaveBeenCalledTimes(2);
  } finally {
    await runtime.close();
    await rm(directory, { recursive: true, force: true });
  }
});
