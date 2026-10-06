// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { cleanup, render } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import {
  HOST_PROTOCOL_VERSION,
  type HostCommand,
  type HostCommandResponse,
  type HostEvent,
  type KafkaWriteOutcome,
  type StreamSkopeHost,
} from "../../src/features/kafka/contracts";
import {
  createInteractiveKafkaHost,
  createKafkaWorkspaceRegistration,
} from "../../src/features/kafka/ui/provider-workspace";
import type { StreamSkopeAppProperties } from "../../src/features/kafka/ui/StreamSkopeApp";
import { currentPluginHost } from "../../src/features/kafka/ui/PluginView";
import { testHostAccepted, testHostExecute } from "../support/host-response";

const rendered = vi.hoisted(() => ({
  hosts: [] as StreamSkopeHost[],
  imports: [] as Array<string | undefined>,
}));
vi.mock("../../src/features/kafka/ui/StreamSkopeApp", () => ({
  KafkaWorkspace: (properties: StreamSkopeAppProperties): React.JSX.Element => {
    rendered.hosts.push(properties.host);
    rendered.imports.push(properties.initialQueryImport);
    return <div>{properties.profilesPage}</div>;
  },
}));

afterEach(() => {
  cleanup();
  rendered.hosts.splice(0);
  rendered.imports.splice(0);
});

function deferred<T>(): {
  promise: Promise<T>;
  resolve: (value: T) => void;
  reject: (reason: unknown) => void;
} {
  let resolve: (value: T) => void = () => {
    throw new Error("Deferred fixture was not initialized.");
  };
  let reject: (reason: unknown) => void = () => {
    throw new Error("Deferred fixture was not initialized.");
  };
  const promise = new Promise<T>((accept, fail) => {
    resolve = accept;
    reject = fail;
  });
  return { promise, resolve, reject };
}

function recordingHost(dispatch?: (command: HostCommand) => Promise<unknown>): {
  host: StreamSkopeHost;
  commands: HostCommand[];
  urls: string[];
  listeners: Set<(event: HostEvent) => void>;
} {
  const commands: HostCommand[] = [],
    urls: string[] = [],
    listeners = new Set<(event: HostEvent) => void>();
  const host: StreamSkopeHost = {
    execute: testHostExecute((command) => {
      commands.push(command);
      return dispatch?.(command) ?? Promise.resolve(testHostAccepted(command, command.id));
    }),
    openExternalUrl: (url) => {
      urls.push(url);
      return Promise.resolve({ state: "accepted", version: HOST_PROTOCOL_VERSION });
    },
    subscribe: (listener) => {
      listeners.add(listener);
      return () => {
        listeners.delete(listener);
      };
    },
  };
  return { host, commands, urls, listeners };
}

const disconnect: Extract<HostCommand, { command: "connection.disconnect" }> = {
  command: "connection.disconnect",
  id: "disconnect",
  payload: {},
  version: HOST_PROTOCOL_VERSION,
};

function rejected(command: HostCommand): unknown {
  return {
    command: command.command,
    id: command.id,
    version: HOST_PROTOCOL_VERSION,
    ok: false,
    error: {
      activeStateChanged: false,
      code: "BACKEND_UNAVAILABLE",
      correlationId: command.id,
      recovery: "Private fixture recovery",
      retryable: false,
      stage: "backend",
      summary: "private-credential-never-display",
    },
  };
}

describe("Kafka activation command admission", () => {
  it("denies new requests before original dispatch while retaining subscriptions and allowing restored authority", async () => {
    const fixture = recordingHost();
    let interactive = true;
    const proxy = createInteractiveKafkaHost(fixture.host, () => interactive);
    const received: HostEvent[] = [];
    const stop = proxy.subscribe((event) => received.push(event));
    await expect(proxy.execute(disconnect)).resolves.toEqual(
      testHostAccepted(disconnect, disconnect.id),
    );
    interactive = false;
    const stopBlocked = proxy.subscribe(() => {
      throw new Error("Inactive subscription published.");
    });
    expect(fixture.listeners.size).toBe(1);
    stopBlocked();
    stopBlocked();
    const blocked = proxy.execute({ ...disconnect, id: "blocked-secret" });
    expect(fixture.commands).toEqual([disconnect]);
    await expect(blocked).rejects.toThrow("workspace is inactive");
    await expect(blocked).rejects.not.toThrow("blocked-secret");
    await expect(proxy.openExternalUrl("https://private.example/runbook")).rejects.toThrow(
      "workspace is inactive",
    );
    expect(fixture.urls).toEqual([]);
    const event: HostEvent = {
      event: "backend.availability",
      sequence: 1,
      version: HOST_PROTOCOL_VERSION,
      payload: { state: "ready" },
    };
    for (const listener of fixture.listeners) listener(event);
    expect(received).toEqual([event]);
    interactive = true;
    await expect(proxy.execute(disconnect)).resolves.toMatchObject({ ok: true });
    await expect(proxy.openExternalUrl("https://example.test/runbook")).resolves.toEqual({
      state: "accepted",
      version: HOST_PROTOCOL_VERSION,
    });
    expect(fixture.commands).toEqual([disconnect, disconnect]);
    stop();
    expect(fixture.listeners.size).toBe(0);
  });

  it.each(["acknowledged", "unknown"] as const)(
    "preserves an already admitted %s write result after permanent revocation, without retry",
    async (state) => {
      const pending = deferred<unknown>();
      const fixture = recordingHost(() => pending.promise);
      let interactive = true;
      const proxy = createInteractiveKafkaHost(fixture.host, () => interactive);
      const command: Extract<HostCommand, { command: "writes.apply" }> = {
        command: "writes.apply",
        id: "apply-once",
        payload: { planId: "reviewed-once" },
        version: HOST_PROTOCOL_VERSION,
      };
      const outcome: KafkaWriteOutcome = {
        state,
        detail: "Original dispatched outcome.",
        receipt: state === "acknowledged" ? { topic: "orders", partition: 2, offset: "143" } : null,
        verification: state === "acknowledged" ? "verified" : "unavailable",
      };
      const result = proxy.execute(command);
      expect(fixture.commands).toEqual([command]);
      interactive = false;
      const response: HostCommandResponse<"writes.apply"> = {
        command: "writes.apply",
        id: command.id,
        version: HOST_PROTOCOL_VERSION,
        ok: true,
        result: { correlationId: "write-receipt", outcome },
      };
      pending.resolve(response);
      const actual = await result;
      expect(actual).toEqual(response);
      if (!actual.ok) throw new Error("Expected original write result.");
      expect(actual.result.outcome).toEqual(outcome);
      await expect(proxy.execute({ ...disconnect, id: "follow-on" })).rejects.toThrow(
        "workspace is inactive",
      );
      expect(fixture.commands).toEqual([command]);
    },
  );

  it("preserves the original rejection of an already admitted request", async () => {
    const pending = deferred<unknown>(),
      fixture = recordingHost(() => pending.promise);
    let interactive = true;
    const proxy = createInteractiveKafkaHost(fixture.host, () => interactive);
    const request = proxy.execute(disconnect);
    interactive = false;
    const original = new Error("Original request failure.");
    pending.reject(original);
    await expect(request).rejects.toBe(original);
    expect(fixture.commands).toEqual([disconnect]);
  });

  it("keeps valid plugin lifetime wrappers behind the workspace admission gate", async () => {
    const fixture = recordingHost((command) =>
      Promise.resolve({
        command: command.command,
        id: command.id,
        version: HOST_PROTOCOL_VERSION,
        ok: true,
        result: { correlationId: command.id, output: { state: "ready" } },
      }),
    );
    let interactive = false;
    const lifetime = new AbortController();
    const plugin = currentPluginHost(
      createInteractiveKafkaHost(fixture.host, () => interactive),
      lifetime.signal,
      "live-plugin",
    );
    const command: Extract<HostCommand, { command: "plugin.execute" }> = {
      command: "plugin.execute",
      id: "plugin-capture",
      version: HOST_PROTOCOL_VERSION,
      payload: { pluginId: "sample.connection", method: "capture", input: {} },
    };
    await expect(plugin.execute(command)).rejects.toThrow("workspace is inactive");
    expect(fixture.commands).toEqual([]);
    interactive = true;
    await expect(plugin.execute(command)).resolves.toMatchObject({ ok: true });
    interactive = false;
    await expect(plugin.execute(command)).rejects.toThrow("workspace is inactive");
    expect(fixture.commands).toEqual([
      { ...command, payload: { ...command.payload, activationId: "live-plugin" } },
    ]);
  });
});

describe("Kafka workspace registration", () => {
  it("confirms original-host stop before dispatching disconnect, including an already disconnected retry", async () => {
    const stopping = deferred<unknown>();
    const fixture = recordingHost((command) =>
      command.command === "messages.stop" && fixture.commands.length === 1
        ? stopping.promise
        : Promise.resolve(testHostAccepted(command, command.id)),
    );
    const registration = createKafkaWorkspaceRegistration({ host: fixture.host });
    const first = registration.deactivate();
    expect(fixture.commands.map((command) => command.command)).toEqual(["messages.stop"]);
    const stopCommand = fixture.commands[0];
    if (stopCommand === undefined) throw new Error("Expected original host stop command.");
    stopping.resolve(testHostAccepted(stopCommand, stopCommand.id));
    await expect(first).resolves.toEqual({ state: "ready" });
    await expect(registration.deactivate()).resolves.toEqual({ state: "ready" });
    expect(fixture.commands.map((command) => command.command)).toEqual([
      "messages.stop",
      "connection.disconnect",
      "messages.stop",
      "connection.disconnect",
    ]);
    expect(
      fixture.commands.every(
        (command) =>
          command.version === HOST_PROTOCOL_VERSION && Object.keys(command.payload).length === 0,
      ),
    ).toBe(true);
  });

  it.each(["stop", "disconnect"] as const)(
    "keeps Kafka selected when %s returns a failed response and does not echo raw host detail",
    async (phase) => {
      const fixture = recordingHost((command) =>
        Promise.resolve(
          command.command === (phase === "stop" ? "messages.stop" : "connection.disconnect")
            ? rejected(command)
            : testHostAccepted(command, command.id),
        ),
      );
      const registration = createKafkaWorkspaceRegistration({
        host: fixture.host,
        initialQueryImport: "preserved-first-visit",
      });
      const firstVisit = render(
        registration.render({ isInteractive: () => true, profilesPage: null }),
      );
      firstVisit.unmount();
      const result = await registration.deactivate();
      expect(result).toMatchObject({ state: "blocked" });
      expect(JSON.stringify(result)).not.toContain("private-credential");
      expect(fixture.commands.map((command) => command.command)).toEqual(
        phase === "stop" ? ["messages.stop"] : ["messages.stop", "connection.disconnect"],
      );
      render(registration.render({ isInteractive: () => true, profilesPage: null }));
      expect(rendered.imports.at(-1)).toBe("preserved-first-visit");
    },
  );

  it("converts an unexpected host rejection to safe retry guidance", async () => {
    const fixture = recordingHost(() =>
      Promise.reject(new Error("private-credential-never-display")),
    );
    const result = await createKafkaWorkspaceRegistration({ host: fixture.host }).deactivate();
    expect(result).toMatchObject({ state: "blocked" });
    expect(JSON.stringify(result)).not.toContain("private-credential");
    expect(fixture.commands.map((command) => command.command)).toEqual(["messages.stop"]);
  });

  it("memoizes one gated host per activation and consumes query import only after confirmed deactivation", async () => {
    const fixture = recordingHost();
    const registration = createKafkaWorkspaceRegistration({
      host: fixture.host,
      initialQueryImport: "first-import",
      isInteractive: () => true,
      providerControl: <span>Wrong control</span>,
    });
    let interactive = true;
    const isInteractive = (): boolean => interactive;
    const view = render(
      registration.render({ isInteractive, profilesPage: <span>Initial control</span> }),
    );
    const first = rendered.hosts.at(-1);
    expect(first).toBeDefined();
    expect(first).not.toBe(fixture.host);
    expect(rendered.imports.at(-1)).toBe("first-import");
    interactive = false;
    view.rerender(registration.render({ isInteractive, profilesPage: <span>Busy control</span> }));
    expect(rendered.hosts.at(-1)).toBe(first);
    expect(rendered.imports.at(-1)).toBe("first-import");
    await expect(registration.deactivate()).resolves.toEqual({ state: "ready" });
    view.unmount();
    render(registration.render({ isInteractive: () => true, profilesPage: null }));
    expect(rendered.hosts.at(-1)).not.toBe(first);
    expect(rendered.imports.at(-1)).toBeUndefined();
  });
});
