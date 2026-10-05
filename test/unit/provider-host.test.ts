import { describe, expect, it } from "vitest";

import {
  createProviderEndpoint,
  ProviderHostClosedError,
  ProviderStreamStopUnavailableError,
  ProviderHostRegistry,
  ProviderWireValidationError,
} from "../../src/platform/node/provider-host";
import { createProviderFixture } from "../support/provider-fixture";

describe("registered provider ownership", () => {
  it("routes only to registered owners and rejects foreign commands before execution", async () => {
    const first = createProviderFixture({ id: "first", version: 7 });
    const second = createProviderFixture({ id: "second", version: 9 });
    const registry = new ProviderHostRegistry([first.endpoint, second.endpoint]);
    expect(registry.get("missing")).toBeUndefined();
    const firstRoute = registry.get("first")!;
    const secondRoute = registry.get("second")!;

    await expect(firstRoute.dispatch(second.command())).rejects.toMatchObject({ stage: "command" });
    await expect(secondRoute.dispatch({ ...second.command(), version: 7 })).rejects.toMatchObject({
      stage: "command",
    });
    expect(first.requests).toHaveLength(0);
    expect(second.requests).toHaveLength(0);
    await expect(firstRoute.dispatch(first.command("set", "first-only"))).resolves.toMatchObject({
      provider: "first",
      value: "first-only",
    });
    await expect(secondRoute.dispatch(second.command())).resolves.toMatchObject({
      provider: "second",
      value: "fixture-initial",
    });
    expect(first.requests).toHaveLength(1);
    expect(second.requests).toHaveLength(1);
    await registry.shutdown();
  });

  it.each(["provider", "request", "action", "version"] as const)(
    "rejects an otherwise valid response with a foreign %s correlation",
    async (difference) => {
      const fixture = createProviderFixture({ id: "fixture", version: 7 });
      const registry = new ProviderHostRegistry([fixture.endpoint]);
      const command = fixture.command("read", "", "request");
      const response = fixture.response(command);
      fixture.nextResponse =
        difference === "provider"
          ? { ...response, provider: "other" }
          : difference === "request"
            ? { ...response, requestId: "other" }
            : difference === "action"
              ? fixture.response(fixture.command("set", "", "request"))
              : { ...response, version: 8 };
      await expect(registry.get("fixture")!.dispatch(command)).rejects.toMatchObject({
        name: "ProviderWireValidationError",
        stage: "response",
      });
      expect(fixture.requests).toEqual([command]);
      await registry.shutdown();
    },
  );

  it("closes admission synchronously and waits for every cleanup despite a synchronous failure", async () => {
    const first = createProviderFixture({ id: "first", version: 7 });
    const second = createProviderFixture({ id: "second", version: 9 });
    const registry = new ProviderHostRegistry([first.endpoint, second.endpoint]);
    const route = registry.get("second")!;
    let release = (): void => undefined;
    let reentrant: Promise<void> | undefined;
    const pending = new Promise<void>((resolve) => {
      release = resolve;
    });
    first.shutdownOperation = (): never => {
      reentrant = registry.shutdown();
      throw new Error("Injected cleanup failure.");
    };
    second.shutdownOperation = (): Promise<void> => pending;
    const closing = registry.shutdown();
    let settled = false;
    void closing.then(
      () => {
        settled = true;
      },
      () => {
        settled = true;
      },
    );
    expect(registry.shutdown()).toBe(closing);
    await expect(route.dispatch(second.command())).rejects.toBeInstanceOf(ProviderHostClosedError);
    expect(() => route.subscribe(() => undefined)).toThrow(ProviderHostClosedError);
    await Promise.resolve();
    expect(first.shutdownCalls).toBe(1);
    expect(second.shutdownCalls).toBe(1);
    expect(reentrant).toBe(closing);
    expect(settled).toBe(false);
    expect(second.requests).toHaveLength(0);
    release();
    await expect(closing).rejects.toMatchObject({
      name: "AggregateError",
      message: "Application providers did not stop cleanly.",
    });
    expect(settled).toBe(true);
    expect(registry.shutdown()).toBe(closing);
  });

  it("waits for a sibling before reporting an asynchronous cleanup rejection", async () => {
    const first = createProviderFixture({ id: "first", version: 7 });
    const second = createProviderFixture({ id: "second", version: 9 });
    const registry = new ProviderHostRegistry([first.endpoint, second.endpoint]);
    let release = (): void => undefined;
    let settled = false;
    first.shutdownOperation = (): Promise<void> => Promise.reject(new Error("Injected failure."));
    second.shutdownOperation = (): Promise<void> =>
      new Promise((resolve) => {
        release = resolve;
      });
    const closing = registry.shutdown();
    void closing.then(
      () => {
        settled = true;
      },
      () => {
        settled = true;
      },
    );
    await Promise.resolve();
    await Promise.resolve();
    expect(first.shutdownCalls).toBe(1);
    expect(second.shutdownCalls).toBe(1);
    expect(settled).toBe(false);
    release();
    await expect(closing).rejects.toBeInstanceOf(AggregateError);
    expect(settled).toBe(true);
  });

  it("rejects unsafe and duplicate registration identities before they can become routes", () => {
    const fixture = createProviderFixture({ id: "fixture", version: 7 });
    expect(() => new ProviderHostRegistry([fixture.endpoint, fixture.endpoint])).toThrow(
      "Duplicate",
    );
    expect(() => new ProviderHostRegistry([{ ...fixture.endpoint, id: "../fixture" }])).toThrow(
      "Invalid",
    );
    expect(() => new ProviderHostRegistry([{ ...fixture.endpoint, version: 0 }])).toThrow(
      "Invalid",
    );
  });

  it("uses a safe command rejection summary without echoing arbitrary parser errors", async () => {
    const fixture = createProviderFixture({ id: "fixture", version: 7 });
    const registry = new ProviderHostRegistry([fixture.endpoint]);
    await expect(registry.get("fixture")!.dispatch({})).rejects.toEqual(
      expect.objectContaining({
        name: "ProviderWireValidationError",
        stage: "command",
        message: "Provider command is invalid.",
      }),
    );
    await expect(registry.get("fixture")!.dispatch({})).rejects.toBeInstanceOf(
      ProviderWireValidationError,
    );
    expect(fixture.requests).toHaveLength(0);
    await registry.shutdown();
  });

  it("stops only the selected provider and preserves a deferred cleanup failure", async () => {
    const first = createProviderFixture({ id: "first", version: 7 });
    const second = createProviderFixture({ id: "second", version: 9 });
    let reject!: (error: Error) => void;
    const pending = new Promise<void>((_resolve, fail) => {
      reject = fail;
    });
    first.stopOperation = (): Promise<void> => pending;
    const registry = new ProviderHostRegistry([first.endpoint, second.endpoint]);
    const stopping = registry.get("first")!.stopStream();
    const failure = new Error("Injected selected-provider cleanup failure.");
    const rejected = expect(stopping).rejects.toBe(failure);
    expect(first.stopCalls).toBe(1);
    expect(second.stopCalls).toBe(0);
    expect(first.shutdownCalls + second.shutdownCalls).toBe(0);
    reject(failure);
    await rejected;
    await registry.shutdown();
  });

  it("retains selected cleanup access after external registry admission closes", async () => {
    const fixture = createProviderFixture({ id: "fixture", version: 7 });
    let release!: () => void;
    fixture.shutdownOperation = (): Promise<void> =>
      new Promise<void>((resolve) => {
        release = resolve;
      });
    const registry = new ProviderHostRegistry([fixture.endpoint]);
    const route = registry.get("fixture")!;
    const closing = registry.shutdown();
    await expect(route.dispatch(fixture.command())).rejects.toBeInstanceOf(ProviderHostClosedError);
    await expect(route.stopStream()).resolves.toBeUndefined();
    expect(fixture.stopCalls).toBe(1);
    await Promise.resolve();
    release();
    await closing;
  });

  it("does not claim a stream stopped when its owner supplies no cleanup capability", async () => {
    const fixture = createProviderFixture({ id: "fixture", version: 7 });
    const endpoint = createProviderEndpoint({
      id: "fixture",
      version: 7,
      parseCommand: fixture.parseCommand,
      execute: (command) => Promise.resolve(fixture.response(command)),
      correlateResponse: fixture.correlateResponse,
      parseEvent: fixture.codec.parseEvent,
      subscribe: (): (() => void) => (): void => undefined,
      availability: fixture.codec.availability,
      shutdown: () => Promise.resolve(),
    });
    await expect(endpoint.stopStream()).rejects.toBeInstanceOf(ProviderStreamStopUnavailableError);
  });
});
