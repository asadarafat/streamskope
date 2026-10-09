// @vitest-environment jsdom
import { expect, it, vi } from "vitest";

import {
  HOST_PROTOCOL_VERSION,
  type HostCommand,
  type ProfileSummary,
  type StreamSkopeHost,
} from "../../src/features/kafka/contracts";
import { currentPluginHost } from "../../src/features/kafka/ui/PluginView";
import { testHostExecute } from "../support/host-response";
import { pluginSecurityProfile, retainedPluginProfile } from "../support/plugin-profile-security";

it("guards EDA renderer profile refresh using the latest context before the core host is called", async () => {
  const execute = vi.fn((command: HostCommand) =>
    Promise.resolve({
      command: command.command,
      id: command.id,
      version: HOST_PROTOCOL_VERSION,
      ok: true as const,
      result: { correlationId: "renderer-security" },
    }),
  );
  const host: StreamSkopeHost = {
    execute: testHostExecute(execute),
    subscribe: () => (): void => undefined,
    openExternalUrl: () => Promise.reject(new Error("Unexpected URL")),
  };
  const legacy = { ...pluginSecurityProfile };
  delete legacy.sasl;
  delete legacy.clientIdentity;
  delete legacy.services;
  let profiles: readonly ProfileSummary[] = [legacy];
  const plugin = currentPluginHost(host, undefined, "activation", () => profiles);
  const draft = { ...retainedPluginProfile() };
  delete draft.sasl;
  delete draft.clientIdentity;
  delete draft.services;
  const command: HostCommand & { command: "profiles.update" } = {
    command: "profiles.update",
    id: "legacy",
    version: HOST_PROTOCOL_VERSION,
    payload: { profileId: pluginSecurityProfile.id, profile: draft },
  };
  await expect(plugin.execute(command)).resolves.toMatchObject({ ok: true });
  profiles = [pluginSecurityProfile];
  await expect(plugin.execute(command)).rejects.toThrow(/Update the plugin.*profile editor/u);
  expect(execute).toHaveBeenCalledOnce();
  await expect(
    plugin.execute({
      ...command,
      payload: { ...command.payload, profile: retainedPluginProfile() },
    }),
  ).resolves.toMatchObject({ ok: true });
  expect(execute).toHaveBeenCalledTimes(2);
});
