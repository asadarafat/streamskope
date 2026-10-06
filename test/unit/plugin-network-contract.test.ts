import { describe, expect, it } from "vitest";

import {
  HOST_PROTOCOL_VERSION,
  HostContractValidationError,
  parseHostCommand,
  parseHostCommandResponse,
  parseHostEvent,
} from "../../src/features/kafka/contracts";
import { PLUGIN_NETWORK_LIMITS } from "../../src/plugins/network-validation";

const configuration = { mode: "custom", offline: false, proxyUrl: "http://proxy.example:8080" };
const snapshot = {
  revision: 1,
  configuration,
  credentialsConfigured: true,
  credentialStorage: "encrypted",
  nativeAvailable: true,
  supportedProxyProtocols: ["http", "https"],
};
const update = {
  configuration,
  credentials: { action: "replace", username: "proxy-user", password: "proxy-secret" },
};
const progress = {
  requestId: "acquisition",
  operation: "inspect",
  phase: "download",
  state: "running",
  receivedBytes: 40,
  totalBytes: 100,
};

function command(name: string, payload: unknown): unknown {
  return { command: name, id: "network-command", version: HOST_PROTOCOL_VERSION, payload };
}
function response(name: string, fields: Record<string, unknown>): unknown {
  return {
    command: name,
    id: "network-command",
    version: HOST_PROTOCOL_VERSION,
    ok: true,
    result: { correlationId: "network-correlation", ...fields },
  };
}
function event(payload: unknown): unknown {
  return {
    event: "plugins.network.progress",
    version: HOST_PROTOCOL_VERSION,
    sequence: 1,
    payload,
  };
}

describe("plugin network host protocol", () => {
  it("accepts complete remembered configuration with write-only credential actions", () => {
    for (const credentials of [update.credentials, { action: "unchanged" }, { action: "clear" }]) {
      const input = command("plugins.network.update", { ...update, credentials });
      expect(parseHostCommand(input)).toEqual(input);
    }
    for (const mode of ["system", "custom"]) {
      const input = command("plugins.network.update", {
        configuration: { ...configuration, mode, offline: true },
        credentials: { action: "unchanged" },
      });
      expect(parseHostCommand(input)).toEqual(input);
    }
    const reset = command("plugins.network.update", {
      configuration: { mode: "system", offline: false, proxyUrl: null },
      credentials: { action: "clear" },
    });
    expect(parseHostCommand(reset)).toEqual(reset);
    const canonical = parseHostCommand(
      command("plugins.network.update", {
        ...update,
        configuration: { ...configuration, proxyUrl: "HTTP://PROXY.EXAMPLE:8080/" },
      }),
    );
    expect(canonical).toMatchObject({ payload: { configuration } });
  });

  it("never accepts proxy credentials inside an endpoint or arbitrary network destinations", () => {
    const credentialedProxy = new URL(configuration.proxyUrl);
    credentialedProxy.username = update.credentials.username;
    credentialedProxy.password = update.credentials.password;
    for (const proxyUrl of [
      null,
      "socks5://proxy.example:1080",
      "file:///tmp/proxy",
      credentialedProxy.href,
      "http://proxy.example:8080/path",
      "http://proxy.example:8080?token=secret",
      "http://proxy.example:8080#token",
      "http://proxy.example:65536",
      "http://proxy.example:8080\n",
      "http://proxy.example\\",
      "x".repeat(2049),
    ]) {
      expect(() =>
        parseHostCommand(
          command("plugins.network.update", {
            ...update,
            configuration: { ...configuration, proxyUrl },
          }),
        ),
      ).toThrow(HostContractValidationError);
    }
    for (const credentials of [
      { action: "unchanged", password: "proxy-secret" },
      { action: "clear", username: "proxy-user" },
      { action: "replace", username: "proxy-user" },
      { action: "replace", username: "", password: "proxy-secret" },
      { action: "replace", username: "proxy-user", password: "" },
      { ...update.credentials, password: "x".repeat(PLUGIN_NETWORK_LIMITS.passwordCharacters + 1) },
      { ...update.credentials, username: "x".repeat(PLUGIN_NETWORK_LIMITS.usernameCharacters + 1) },
      { ...update.credentials, password: "secret\nheader" },
      { ...update.credentials, action: "read" },
    ]) {
      expect(() =>
        parseHostCommand(command("plugins.network.update", { ...update, credentials })),
      ).toThrow(HostContractValidationError);
    }
    expect(() =>
      parseHostCommand(
        command("plugins.network.update", { ...update, target: "https://arbitrary.test" }),
      ),
    ).toThrow(HostContractValidationError);
    expect(() =>
      parseHostCommand(
        command("plugins.network.update", {
          ...update,
          configuration: { mode: "system", offline: false, proxyUrl: null },
        }),
      ),
    ).toThrow(HostContractValidationError);
  });

  it("bounds simple commands and cancels only by an owned acquisition request identifier", () => {
    for (const name of ["plugins.network.get", "plugins.network.test"]) {
      const input = command(name, {});
      expect(parseHostCommand(input)).toEqual(input);
      expect(() => parseHostCommand(command(name, { url: "https://arbitrary.test" }))).toThrow(
        HostContractValidationError,
      );
    }
    const input = command("plugins.network.cancel", { requestId: "acquisition" });
    expect(parseHostCommand(input)).toEqual(input);
    for (const requestId of ["", "x".repeat(129), false]) {
      expect(() => parseHostCommand(command("plugins.network.cancel", { requestId }))).toThrow(
        HostContractValidationError,
      );
    }
    expect(parseHostCommandResponse(response("plugins.network.cancel", {}))).toEqual(
      response("plugins.network.cancel", {}),
    );
    expect(() =>
      parseHostCommandResponse(response("plugins.network.cancel", { configuration })),
    ).toThrow(HostContractValidationError);
  });

  it("represents encrypted, session-only, browser and unavailable settings without returning secrets", () => {
    for (const pluginNetwork of [
      snapshot,
      { ...snapshot, credentialStorage: "session" },
      {
        revision: 0,
        configuration: { mode: "system", offline: true, proxyUrl: null },
        credentialsConfigured: false,
        credentialStorage: "unavailable",
        nativeAvailable: false,
        supportedProxyProtocols: [],
      },
      {
        ...snapshot,
        configuration: null,
        credentialsConfigured: false,
        error: "Saved networking settings could not be read. Reset them before remote acquisition.",
      },
    ]) {
      for (const name of ["plugins.network.get", "plugins.network.update"]) {
        const input = response(name, { pluginNetwork });
        expect(parseHostCommandResponse(input)).toEqual(input);
      }
    }
    for (const changed of [
      { password: "proxy-secret" },
      { username: "proxy-user" },
      { configuration: null },
      { credentialStorage: "plaintext" },
      { credentialStorage: "unavailable" },
      { revision: -1 },
      { revision: Number.MAX_SAFE_INTEGER + 1 },
      { nativeAvailable: false },
      {
        nativeAvailable: false,
        supportedProxyProtocols: [],
        configuration: { ...configuration, mode: "system" },
      },
      { supportedProxyProtocols: ["http", "http"] },
      { supportedProxyProtocols: ["http", "https", "socks5"] },
      { configuration: { ...configuration, proxyUrl: null } },
      { configuration: { ...configuration, proxyUrl: "http://user:secret@proxy.example" } },
    ]) {
      expect(() =>
        parseHostCommandResponse(
          response("plugins.network.get", { pluginNetwork: { ...snapshot, ...changed } }),
        ),
      ).toThrow(HostContractValidationError);
    }
  });

  it("records applied settings revision and explicitly distinguishes an untested asset route", () => {
    const result = {
      settingsRevision: 2,
      checkedAt: "2026-10-06T12:00:00.000Z",
      scope: "catalog-and-assets",
    };
    for (const pluginNetworkTest of [
      result,
      {
        ...result,
        scope: "catalog-only",
        detail: "The catalog is reachable, but no published package asset is available to test.",
      },
    ]) {
      const input = response("plugins.network.test", { pluginNetworkTest });
      expect(parseHostCommandResponse(input)).toEqual(input);
    }
    for (const changed of [
      { scope: "catalog-only" },
      { scope: "homepage" },
      { settingsRevision: -1 },
      { checkedAt: "today" },
      { checkedAt: "2026-02-31T12:00:00.000Z" },
      { detail: "x".repeat(2049) },
      { proxyPassword: "secret" },
    ]) {
      expect(() =>
        parseHostCommandResponse(
          response("plugins.network.test", { pluginNetworkTest: { ...result, ...changed } }),
        ),
      ).toThrow(HostContractValidationError);
    }
  });

  it("accepts only bounded request-correlated progress without URL, path or credential fields", () => {
    for (const state of ["running", "succeeded", "cancelled", "failed"]) {
      const input = event({ ...progress, state });
      expect(parseHostEvent(input)).toEqual(input);
    }
    const unknownSize = event({
      requestId: progress.requestId,
      operation: "catalog",
      phase: "catalog",
      state: "running",
    });
    expect(parseHostEvent(unknownSize)).toEqual(unknownSize);
    for (const changed of [
      { requestId: "" },
      { requestId: "x".repeat(129) },
      { operation: "install" },
      { phase: "cleanup" },
      { state: "complete" },
      { receivedBytes: -1 },
      { receivedBytes: 1.5 },
      { receivedBytes: PLUGIN_NETWORK_LIMITS.transferBytes + 1 },
      { totalBytes: PLUGIN_NETWORK_LIMITS.transferBytes + 1 },
      { totalBytes: 39 },
      { url: "https://arbitrary.test" },
      { path: "/tmp/private-package" },
      { password: "proxy-secret" },
    ]) {
      expect(() => parseHostEvent(event({ ...progress, ...changed }))).toThrow(
        HostContractValidationError,
      );
    }
  });
});
