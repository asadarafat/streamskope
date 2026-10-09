import { describe, expect, it } from "vitest";

import {
  HOST_PROTOCOL_VERSION,
  HostContractValidationError,
  parseHostCommand,
  parseHostCommandResponse,
  parseHostEvent,
} from "../../src/features/kafka/contracts";

const command = {
  command: "plugin.execute",
  id: "plugin-request",
  version: HOST_PROTOCOL_VERSION,
  payload: {
    pluginId: "example.capture",
    method: "capture.inspect",
    input: { host: "https://example.test" },
  },
};
const response = {
  command: "plugin.execute",
  id: command.id,
  version: HOST_PROTOCOL_VERSION,
  ok: true,
  result: { correlationId: "correlation", output: { status: "ready", topics: ["events"] } },
};
const event = {
  event: "plugin.event",
  version: HOST_PROTOCOL_VERSION,
  sequence: 1,
  payload: { pluginId: "example.capture", name: "capture.progress", data: { phase: "ready" } },
};

describe("generic plugin host protocol", () => {
  it("pairs transition metadata with the current protocol while accepting snapshots without it", () => {
    const payload = {
      revision: 1,
      plugins: [{ id: "example.capture", pending: null, restartRequired: false }],
    };
    const changed = { ...event, event: "plugins.changed", payload };
    expect(parseHostEvent(changed)).toEqual(changed);
    expect(() => parseHostEvent({ ...changed, version: HOST_PROTOCOL_VERSION - 1 })).toThrow(
      HostContractValidationError,
    );
    expect(() => parseHostCommand({ ...command, version: HOST_PROTOCOL_VERSION - 1 })).toThrow(
      HostContractValidationError,
    );
    const transition = {
      operationId: "12345678-1234-1234-1234-123456789abc",
      pluginId: "example.capture",
      activationId: "87654321-4321-4321-4321-cba987654321",
      operation: "install",
      stage: "prepare-unload",
      state: "waiting",
      startedAt: "2026-10-08T12:00:00.000Z",
      stageStartedAt: "2026-10-08T12:00:01.000Z",
      outstandingRequests: 2,
      outstandingConnections: 1,
      commit: "not-started",
    };
    const snapshot = {
      ...payload,
      plugins: [{ ...payload.plugins[0]!, transition }],
    };
    const progress = { ...changed, payload: snapshot };
    expect(parseHostEvent(progress)).toEqual(progress);
    const listed = {
      ...response,
      command: "plugins.list",
      result: { pluginSnapshot: snapshot, correlationId: "list" },
    };
    expect(parseHostCommandResponse(listed)).toEqual(listed);
    expect(() =>
      parseHostCommandResponse({ ...listed, version: HOST_PROTOCOL_VERSION - 1 }),
    ).toThrow(HostContractValidationError);
    for (const patch of [
      { operationId: "unbounded-or-arbitrary" },
      { activationId: "not-a-uuid" },
      { pluginId: "example.other" },
      { operation: "force-remove" },
      { stage: "credentials" },
      { state: "completed" },
      { commit: true },
      { startedAt: "today" },
      { stageStartedAt: "2026-10-08T11:00:00.000Z" },
      { state: "queued" },
      { stage: "queued" },
      { stage: "queued", state: "queued", commit: "confirmed" },
      { outstandingRequests: -1 },
      { outstandingRequests: 0.5 },
      { outstandingRequests: Number.MAX_SAFE_INTEGER + 1 },
      { outstandingConnections: "1" },
      { outstandingConnections: Number.NaN },
      { password: "must-never-cross-this-boundary" },
    ]) {
      expect(() =>
        parseHostEvent({
          ...progress,
          payload: {
            ...snapshot,
            plugins: [{ ...snapshot.plugins[0]!, transition: { ...transition, ...patch } }],
          },
        }),
      ).toThrow(HostContractValidationError);
    }
    const queued = {
      ...progress,
      payload: {
        ...snapshot,
        plugins: [
          {
            ...snapshot.plugins[0]!,
            transition: {
              ...transition,
              stage: "queued",
              state: "queued",
              stageStartedAt: transition.startedAt,
            },
          },
        ],
      },
    };
    expect(parseHostEvent(queued)).toEqual(queued);
  });
  it("validates cache-only requests, local retry and dated catalog provenance", () => {
    const cached = {
      ...command,
      command: "plugins.catalog",
      payload: { refresh: false },
    };
    expect(parseHostCommand(cached)).toEqual(cached);
    expect(() => parseHostCommand({ ...cached, payload: { refresh: "false" } })).toThrow(
      HostContractValidationError,
    );
    const retry = {
      ...command,
      command: "plugins.retry",
      payload: { pluginId: "example.capture" },
    };
    expect(parseHostCommand(retry)).toEqual(retry);
    const prepare = {
      ...retry,
      command: "plugins.change.prepare",
      payload: { ...retry.payload, operation: "retry" },
    };
    expect(parseHostCommand(prepare)).toEqual(prepare);
    const dated = {
      ...response,
      command: "plugins.catalog",
      result: {
        correlationId: "cached",
        pluginCatalog: {
          plugins: [],
          source: "cache",
          checkedAt: "2026-10-06T10:00:00.000Z",
          error: "Catalog unavailable.",
        },
      },
    };
    expect(parseHostCommandResponse(dated)).toEqual(dated);
    for (const checkedAt of ["yesterday", "2026-10-06", "2026-02-31T10:00:00.000Z", 123]) {
      expect(() =>
        parseHostCommandResponse({
          ...dated,
          result: { ...dated.result, pluginCatalog: { ...dated.result.pluginCatalog, checkedAt } },
        }),
      ).toThrow(HostContractValidationError);
    }
    expect(() =>
      parseHostCommandResponse({
        ...dated,
        result: {
          ...dated.result,
          pluginCatalog: { ...dated.result.pluginCatalog, source: "official" },
        },
      }),
    ).toThrow(HostContractValidationError);
    const retried = {
      ...response,
      command: "plugins.retry",
      result: { correlationId: "retry", pluginSnapshot: { revision: 1, plugins: [] } },
    };
    expect(parseHostCommandResponse(retried)).toEqual(retried);
  });
  it("carries lifecycle consent and activation identity, and rejects malformed change snapshots", () => {
    const prepare = {
      ...command,
      command: "plugins.change.prepare",
      payload: { pluginId: "example.capture", operation: "remove" },
    };
    expect(parseHostCommand(prepare)).toEqual(prepare);
    const remove = {
      ...command,
      command: "plugins.remove",
      payload: { pluginId: "example.capture", confirmationToken: "one-use-token" },
    };
    expect(parseHostCommand(remove)).toEqual(remove);
    const identified = {
      ...command,
      payload: { ...command.payload, activationId: "current-activation" },
    };
    expect(parseHostCommand(identified)).toEqual(identified);
    const changed = { ...event, event: "plugins.changed", payload: { revision: 3, plugins: [] } };
    expect(parseHostEvent(changed)).toEqual(changed);
    for (const revision of [-1, 1.5, "3", undefined, Number.MAX_SAFE_INTEGER + 1]) {
      expect(() =>
        parseHostEvent({ ...changed, payload: { ...changed.payload, revision } }),
      ).toThrow(HostContractValidationError);
    }
    expect(() =>
      parseHostCommand({ ...remove, payload: { ...remove.payload, confirmationToken: true } }),
    ).toThrow(HostContractValidationError);
    expect(() =>
      parseHostCommand({ ...identified, payload: { ...identified.payload, activationId: "" } }),
    ).toThrow(HostContractValidationError);
    const prompt = {
      pluginId: "example.capture",
      token: "one-use-token",
      title: "Remove plugin?",
      message: "Capture is running",
      detail: "Temporary records will be removed",
      confirmLabel: "Stop capture and remove",
    };
    const preparation = {
      ...response,
      command: "plugins.change.prepare",
      result: { correlationId: "prepare", pluginChange: prompt },
    };
    expect(parseHostCommandResponse(preparation)).toEqual(preparation);
    expect(() =>
      parseHostCommandResponse({
        ...preparation,
        result: { ...preparation.result, pluginChange: { ...prompt, token: "" } },
      }),
    ).toThrow(HostContractValidationError);
  });
  it("round-trips namespaced requests, responses and events without knowing the plugin", () => {
    expect(parseHostCommand(command)).toEqual(command);
    expect(parseHostCommandResponse(response)).toEqual(response);
    expect(parseHostEvent(event)).toEqual(event);
  });

  it("bounds plugin data at every host boundary", () => {
    const oversized = { text: "x".repeat(262_145) };
    expect(() =>
      parseHostCommand({ ...command, payload: { ...command.payload, input: oversized } }),
    ).toThrow(HostContractValidationError);
    expect(() =>
      parseHostCommandResponse({ ...response, result: { ...response.result, output: oversized } }),
    ).toThrow(HostContractValidationError);
    expect(() =>
      parseHostEvent({ ...event, payload: { ...event.payload, data: oversized } }),
    ).toThrow(HostContractValidationError);
    expect(() =>
      parseHostCommand({
        ...command,
        payload: { ...command.payload, input: { execute: (): void => undefined } },
      }),
    ).toThrow(HostContractValidationError);
  });

  it("rejects cross-boundary fields, invalid owners and the retired EDA core vocabulary", () => {
    expect(() =>
      parseHostCommand({
        ...command,
        payload: { ...command.payload, filename: "/tmp/plugin.cjs" },
      }),
    ).toThrow(HostContractValidationError);
    expect(() =>
      parseHostCommand({ ...command, payload: { ...command.payload, pluginId: "../plugin" } }),
    ).toThrow(HostContractValidationError);
    expect(() =>
      parseHostCommand({ ...command, command: "edaCapture.inspect", payload: {} }),
    ).toThrow(HostContractValidationError);
    expect(() =>
      parseHostCommandResponse({ ...response, result: { ...response.result, inspection: {} } }),
    ).toThrow(HostContractValidationError);
    expect(() => parseHostEvent({ ...event, event: "edaCapture.progress" })).toThrow(
      HostContractValidationError,
    );
  });

  it("bounds catalog responses and validates manifests before the renderer sees them", () => {
    const catalogResponse = {
      ...response,
      command: "plugins.catalog",
      result: { correlationId: "catalog", pluginCatalog: { plugins: [] } },
    };
    expect(parseHostCommandResponse(catalogResponse)).toEqual(catalogResponse);
    const manifest = {
      id: "example.capture",
      name: "Example capture",
      version: "1.0.0",
      apiVersion: 2,
      backend: "backend.cjs",
      renderer: "renderer.js",
    };
    expect(() =>
      parseHostCommandResponse({
        ...catalogResponse,
        result: {
          correlationId: "catalog",
          pluginCatalog: { plugins: Array.from({ length: 65 }, () => manifest) },
        },
      }),
    ).toThrow(HostContractValidationError);
    expect(() =>
      parseHostCommandResponse({
        ...catalogResponse,
        result: {
          correlationId: "catalog",
          pluginCatalog: { plugins: [{ ...manifest, apiVersion: 99 }] },
        },
      }),
    ).toThrow(HostContractValidationError);
  });
});
