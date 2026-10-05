// @vitest-environment jsdom

import { StrictMode } from "react";
import { cleanup, render } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import {
  NATS_PROTOCOL_VERSION,
  parseCorrelatedNatsResponse,
  type NatsCommand,
  type NatsCommandResponse,
  type NatsEvent,
  type NatsHost,
} from "../../src/features/nats/contracts";
import {
  createInteractiveNatsHost,
  createNatsWorkspaceRegistration,
} from "../../src/features/nats/ui/provider-workspace";
import type { NatsWorkspaceSource } from "../../src/features/nats/ui/workspace-types";

const rendered = vi.hoisted(() => ({ sources: [] as NatsWorkspaceSource[] }));
vi.mock("../../src/features/nats/ui/NatsWorkspace", () => ({
  NatsWorkspace: ({ source }: { source: NatsWorkspaceSource }): React.JSX.Element => {
    rendered.sources.push(source);
    return <div />;
  },
}));

afterEach(() => {
  cleanup();
  rendered.sources.splice(0);
});

function deferred<Value>(): { promise: Promise<Value>; resolve: (value: Value) => void } {
  let resolve!: (value: Value) => void;
  const promise = new Promise<Value>((complete) => {
    resolve = complete;
  });
  return { promise, resolve };
}

function receipt(command: NatsCommand): unknown {
  const subscription = {
    revision: 0,
    state: "idle",
    generation: null,
    subject: null,
    counters: {
      receivedRecords: 0,
      applicationOmittedRecords: 0,
      publishedRecords: 0,
      queuedRecords: 0,
      queuedBytes: 0,
      transportOmittedRecords: 0,
    },
  };
  return {
    command: command.command,
    id: command.id,
    version: NATS_PROTOCOL_VERSION,
    ok: true,
    result: {
      correlationId: command.id,
      subscription,
      ...(command.command === "connection.disconnect"
        ? { connection: { revision: 0, state: "disconnected", profile: null } }
        : {}),
    },
  };
}

function fixture(
  dispatch: (command: NatsCommand) => Promise<unknown> = (command) =>
    Promise.resolve(receipt(command)),
): {
  readonly host: NatsHost;
  readonly commands: NatsCommand[];
  readonly listeners: Set<(event: NatsEvent) => void>;
} {
  const commands: NatsCommand[] = [];
  const listeners = new Set<(event: NatsEvent) => void>();
  return {
    commands,
    listeners,
    host: {
      execute: async <Command extends NatsCommand>(
        command: Command,
      ): Promise<NatsCommandResponse<Command["command"]>> => {
        commands.push(command);
        return parseCorrelatedNatsResponse(await dispatch(command), command);
      },
      subscribe: (listener) => {
        listeners.add(listener);
        return (): void => {
          listeners.delete(listener);
        };
      },
    },
  };
}

const stop = {
  command: "subscription.stop",
  id: "admitted-stop",
  payload: {},
  version: NATS_PROTOCOL_VERSION,
} as const;

describe("Core NATS workspace ownership", () => {
  it("preserves admitted receipts and cleanup events while revoking new requests and listeners", async () => {
    const completion = deferred<unknown>();
    const original = fixture(() => completion.promise);
    let interactive = true;
    const gated = createInteractiveNatsHost(original.host, () => interactive);
    const observed: NatsEvent[] = [];
    const unsubscribe = gated.subscribe((event) => observed.push(event));
    const admitted = gated.execute(stop);
    interactive = false;
    await expect(gated.execute({ ...stop, id: "refused" })).rejects.toThrow(
      "workspace is inactive",
    );
    gated.subscribe(() => {
      throw new Error("Retired listener acquired ownership.");
    })();
    expect(original.commands).toEqual([stop]);
    expect(original.listeners.size).toBe(1);
    const event: NatsEvent = {
      version: NATS_PROTOCOL_VERSION,
      sequence: 4,
      event: "backend.availability",
      payload: { state: "ready" },
    };
    for (const listener of original.listeners) listener(event);
    expect(observed).toEqual([event]);
    const committed = receipt(stop);
    completion.resolve(committed);
    await expect(admitted).resolves.toEqual(committed);
    unsubscribe();
    expect(original.listeners.size).toBe(0);
  });

  it("resolves once at first render across StrictMode and uses the original port for ordered cleanup", async () => {
    const stopping = deferred<unknown>();
    const original = fixture((command) =>
      command.command === "subscription.stop"
        ? stopping.promise
        : Promise.resolve(receipt(command)),
    );
    const resolveSource = vi.fn((): NatsWorkspaceSource => ({
      state: "ready",
      host: original.host,
    }));
    const registration = createNatsWorkspaceRegistration({ resolveSource });
    expect(resolveSource).not.toHaveBeenCalled();
    let interactive = true;
    const controls = { isInteractive: (): boolean => interactive, providerControl: null };
    const view = render(<StrictMode>{registration.render(controls)}</StrictMode>);
    expect(resolveSource).toHaveBeenCalledTimes(1);
    const first = rendered.sources.at(-1);
    expect(first?.state).toBe("ready");
    if (first?.state !== "ready") throw new Error("Expected ready workspace.");
    expect(first.host).not.toBe(original.host);
    interactive = false;
    const switching = registration.deactivate();
    expect(original.commands.map((command) => command.command)).toEqual(["subscription.stop"]);
    const submitted = original.commands[0];
    if (submitted === undefined) throw new Error("Expected stop command.");
    stopping.resolve(receipt(submitted));
    await expect(switching).resolves.toEqual({ state: "ready" });
    expect(original.commands.map((command) => command.command)).toEqual([
      "subscription.stop",
      "connection.disconnect",
    ]);
    expect(
      original.commands.every(
        (command) =>
          command.version === NATS_PROTOCOL_VERSION && Object.keys(command.payload).length === 0,
      ),
    ).toBe(true);
    view.unmount();
    render(registration.render({ isInteractive: () => true, providerControl: null }));
    expect(resolveSource).toHaveBeenCalledTimes(1);
    expect(rendered.sources.at(-1)).not.toBe(first);
  });

  it.each(["subscription.stop", "connection.disconnect"] as const)(
    "blocks switching after failed %s without exposing private host detail",
    async (failed) => {
      const original = fixture((command) =>
        command.command === failed
          ? Promise.resolve({
              version: NATS_PROTOCOL_VERSION,
              command: command.command,
              id: command.id,
              ok: false,
              error: {
                stage: "lifecycle",
                operation: command.command,
                correlationId: command.id,
                code: "cleanup",
                summary: "private-token-or-certificate",
              },
            })
          : Promise.resolve(receipt(command)),
      );
      const registration = createNatsWorkspaceRegistration({
        resolveSource: () => ({ state: "ready", host: original.host }),
      });
      render(registration.render({ isInteractive: () => true, providerControl: null }));
      const result = await registration.deactivate();
      expect(result.state).toBe("blocked");
      expect(JSON.stringify(result)).not.toContain("private-token-or-certificate");
      expect(original.commands.map((command) => command.command)).toEqual(
        failed === "subscription.stop" ? [failed] : ["subscription.stop", failed],
      );
    },
  );

  it("uses safe stop retry guidance after an unexpected host rejection", async () => {
    const original = fixture(() => Promise.reject(new Error("private-token-or-certificate")));
    const registration = createNatsWorkspaceRegistration({
      resolveSource: () => ({ state: "ready", host: original.host }),
    });
    render(registration.render({ isInteractive: () => true, providerControl: null }));
    const result = await registration.deactivate();
    expect(result).toMatchObject({
      state: "blocked",
      summary: "Core NATS subscription could not be stopped.",
    });
    expect(JSON.stringify(result)).not.toContain("private-token-or-certificate");
    expect(original.commands.map((command) => command.command)).toEqual(["subscription.stop"]);
  });

  it("unavailable and never-rendered workspaces own no cleanup port", async () => {
    const resolveSource = vi.fn((): NatsWorkspaceSource => ({
      state: "unavailable",
      recovery: "Update this host.",
    }));
    const registration = createNatsWorkspaceRegistration({ resolveSource });
    await expect(registration.deactivate()).resolves.toEqual({ state: "ready" });
    expect(resolveSource).not.toHaveBeenCalled();
    render(registration.render({ isInteractive: () => true, providerControl: null }));
    expect(rendered.sources.at(-1)).toEqual({
      state: "unavailable",
      recovery: "Update this host.",
    });
    await expect(registration.deactivate()).resolves.toEqual({ state: "ready" });
    expect(resolveSource).toHaveBeenCalledTimes(1);
  });
});
