import { describe, expect, it, vi } from "vitest";

import {
  HOST_PROTOCOL_VERSION,
  KAFKA_OPERATIONAL_PREFERENCE_DEFAULTS,
  type HostCommand,
} from "../../src/features/kafka/contracts";
import { KafkaOperationalPreferenceService } from "../../src/features/kafka/application/operational-preference-service";
import { InMemoryKafkaOperationalPreferenceStore } from "../../src/features/kafka/application/in-memory-operational-preference-store";
import { KafkaCommandProtection } from "../../src/features/kafka/facade/command-protection";
import {
  command,
  createFacade,
  RecordingActiveConnection,
  RecordingConnectionPort,
} from "../support/kafka-backend-facade-fixture";

function preferences(readOnly = true): KafkaOperationalPreferenceService {
  return new KafkaOperationalPreferenceService(
    new InMemoryKafkaOperationalPreferenceStore(
      { durability: "session", state: "ready" },
      {
        ...KAFKA_OPERATIONAL_PREFERENCE_DEFAULTS,
        protection: { readOnly, maskKey: false, maskHeaders: [], valuePaths: [] },
      },
    ),
  );
}
function request(name: HostCommand["command"]): HostCommand {
  return {
    command: name,
    id: "direct-host",
    version: HOST_PROTOCOL_VERSION,
    payload: {},
  } as HostCommand;
}
const writes = [
  "writes.apply",
  "schemas.register",
  "schemas.delete",
  "acls.create",
  "acls.delete",
  "transforms.delete",
  "topicConfiguration.apply",
  "latency.start",
  "trustAcquisition.apply",
  "trustAcquisition.material.fetch",
  "trustAcquisition.https.fetch",
  "plugin.execute",
  "plugins.install",
  "plugins.remove",
  "plugins.restart",
  "plugins.exit.resolve",
  "plugins.change.prepare",
] as const;

describe("host record protection", () => {
  it.each(writes)("rejects direct %s before it reaches any adapter or plugin", async (name) => {
    const port = new RecordingConnectionPort();
    const open = vi.spyOn(port, "openConnection");
    const facade = createFacade(port, undefined, undefined, undefined, preferences());
    const result = await facade.execute(request(name));
    expect(result).toMatchObject({
      ok: false,
      error: {
        code: "AUTHORIZATION_DENIED",
        summary: "Read-only mode blocks this operation.",
        retryable: false,
      },
    });
    expect(open).not.toHaveBeenCalled();
  });
  it("allows ordinary Kafka metadata connections in read-only mode", async () => {
    const port = new RecordingConnectionPort();
    port.openOperations.push(() => Promise.resolve(new RecordingActiveConnection()));
    const facade = createFacade(port, undefined, undefined, undefined, preferences());
    expect(await facade.execute(command("connection.connect", "connect"))).toMatchObject({
      ok: true,
    });
    expect(await facade.execute(command("connection.disconnect", "disconnect"))).toMatchObject({
      ok: true,
    });
  });
  it("rejects policy changes while connected without silently disconnecting", async () => {
    const port = new RecordingConnectionPort();
    port.openOperations.push(() => Promise.resolve(new RecordingActiveConnection()));
    const service = preferences();
    const facade = createFacade(port, undefined, undefined, undefined, service);
    await facade.execute(command("connection.connect", "connect"));
    const result = await facade.execute({
      ...request("preferences.update"),
      command: "preferences.update",
      payload: {
        patch: {
          protection: { ...KAFKA_OPERATIONAL_PREFERENCE_DEFAULTS.protection, readOnly: false },
        },
      },
    });
    expect(result).toMatchObject({
      ok: false,
      error: { summary: "Disconnect before changing record protection." },
    });
    expect(service.currentSnapshot().preferences.protection.readOnly).toBe(true);
    await facade.shutdown();
  });
  it("keeps protection when ordinary workbench defaults are reset", async () => {
    const service = preferences();
    const snapshot = await service.reset();
    expect(snapshot.preferences.protection.readOnly).toBe(true);
  });
  it("blocks all remote operations if stored preferences cannot be read", async () => {
    const service = new KafkaOperationalPreferenceService({
      capability: (): { durability: "durable"; state: "ready" } => ({
        durability: "durable",
        state: "ready",
      }),
      load: (): Promise<never> => Promise.reject(new Error("corrupt")),
      commit: (): Promise<void> => Promise.resolve(),
    });
    const facade = createFacade(
      new RecordingConnectionPort(),
      undefined,
      undefined,
      undefined,
      service,
    );
    expect(await facade.execute(command("connection.connect", "connect"))).toMatchObject({
      ok: false,
      error: { summary: "Record protection could not be loaded." },
    });
    const recovered = await service.reset();
    expect(recovered.preferences.protection).toEqual({
      readOnly: true,
      maskKey: true,
      maskHeaders: [],
      valuePaths: [""],
    });
  });
  it("prevents a policy save from racing an in-flight remote operation", async () => {
    const service = preferences(false);
    let finish!: () => void;
    const gate = new Promise<void>((resolve) => {
      finish = resolve;
    });
    const rejected = vi.fn();
    const guard = new KafkaCommandProtection({
      preferences: service,
      disconnected: (): boolean => true,
      managedProfile: (): Promise<boolean> => Promise.resolve(false),
      rejected,
    });
    const active = guard.execute(request("schemas.register"), "write", async () => {
      await gate;
      return {
        ...request("schemas.register"),
        command: "schemas.register",
        ok: true,
        result: { correlationId: "write" },
      };
    });
    await vi.waitFor(() => expect(service.currentSnapshot().store.state).toBe("ready"));
    await Promise.resolve();
    await Promise.resolve();
    const dispatch = vi.fn();
    expect(await guard.execute(request("preferences.reset"), "reset", dispatch)).toMatchObject({
      ok: false,
    });
    expect(dispatch).not.toHaveBeenCalled();
    finish();
    await active;
  });
  it("cancels a connection requested before a disconnect while protection is loading", async () => {
    let load!: () => void;
    const gate = new Promise<void>((resolve) => {
      load = resolve;
    });
    const service = new KafkaOperationalPreferenceService({
      capability: (): { durability: "session"; state: "ready" } => ({
        durability: "session",
        state: "ready",
      }),
      load: async (): Promise<undefined> => {
        await gate;
        return undefined;
      },
      commit: (): Promise<void> => Promise.resolve(),
    });
    const port = new RecordingConnectionPort();
    const open = vi.spyOn(port, "openConnection");
    const facade = createFacade(port, undefined, undefined, undefined, service);
    const connecting = facade.execute(command("connection.connect", "pending"));
    await facade.execute(command("connection.disconnect", "cancel"));
    load();
    expect(await connecting).toMatchObject({ ok: false, error: { code: "CANCELLED" } });
    expect(open).not.toHaveBeenCalled();
  });
  it("will not enable protection with unfinished plugin resources", async () => {
    const dispatch = vi.fn();
    const guard = new KafkaCommandProtection({
      preferences: preferences(),
      disconnected: (): boolean => true,
      pendingPluginWork: (): Promise<boolean> => Promise.resolve(true),
      managedProfile: (): Promise<boolean> => Promise.resolve(false),
      rejected: (): void => undefined,
    });
    expect(await guard.execute(request("preferences.reset"), "reset", dispatch)).toMatchObject({
      ok: false,
      error: { summary: "Finish plugin work before changing protection." },
    });
    expect(dispatch).not.toHaveBeenCalled();
  });
  it("blocks a managed connection before its lifecycle hook", async () => {
    const dispatch = vi.fn();
    const guard = new KafkaCommandProtection({
      preferences: preferences(),
      disconnected: (): boolean => true,
      managedProfile: (): Promise<boolean> => Promise.resolve(true),
      rejected: (): void => undefined,
    });
    expect(
      await guard.execute(
        {
          ...request("profiles.connect"),
          command: "profiles.connect",
          payload: { profileId: "managed" },
        },
        "profile",
        dispatch,
      ),
    ).toMatchObject({ ok: false });
    expect(dispatch).not.toHaveBeenCalled();
  });
});
