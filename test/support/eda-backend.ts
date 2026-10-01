import type { PluginBackendHost } from "../../src/plugins/api";
import { HOST_PROTOCOL_VERSION } from "../../src/features/kafka/contracts";
import { translateFacadeFailure } from "../../src/features/kafka/facade/facade-support";
import type { EdaCapturePort } from "../../plugins/eda/backend/eda-capture-port";

import { testHostExecute, testHostResponse } from "./host-response";

export function edaBackendHost(overrides: Partial<PluginBackendHost> = {}): PluginBackendHost {
  return {
    connectionActive: () => false,
    disconnectOwnedConnection: () => Promise.resolve(),
    profiles: () => Promise.resolve([]),
    deleteProfile: () => Promise.resolve(),
    recordActivity: () => undefined,
    publish: () => undefined,
    probeTopics: () => Promise.resolve([]),
    failure: (error, context) =>
      translateFacadeFailure(
        error,
        { ...context, activeStateChanged: false, connection: undefined },
        true,
      ).error,
    execute: testHostExecute((command) =>
      Promise.resolve(
        testHostResponse(command, {
          command: command.command,
          id: command.id,
          version: HOST_PROTOCOL_VERSION,
          ok: true,
          result: { correlationId: command.id },
        }),
      ),
    ),
    ...overrides,
  };
}
export function edaCapturePort(overrides: Partial<EdaCapturePort> = {}): EdaCapturePort {
  return {
    status: () => ({ state: "idle", tunnel: "closed", detail: "Idle" }),
    preflight: () => Promise.resolve({ state: "configured", detail: "Ready" }),
    close: () => Promise.resolve(),
    stop: () => Promise.resolve(),
    inspect: () => Promise.reject(new Error("Unexpected inspect")),
    deploy: () => Promise.reject(new Error("Unexpected deploy")),
    ...overrides,
  };
}
