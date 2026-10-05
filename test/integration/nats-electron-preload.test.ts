import { describe, expect, it } from "vitest";

import {
  HOST_PROTOCOL_VERSION,
  type HostEvent,
  type StreamSkopeHost,
} from "../../src/features/kafka/contracts";
import {
  NATS_PROTOCOL_VERSION,
  type NatsCommandResponse,
  type NatsEvent,
  type NatsHost,
} from "../../src/features/nats/contracts";
import { providerIpcChannels } from "../../src/platform/electron/preload/channels";
import { exposeStreamSkopeHost } from "../../src/platform/electron/preload/host-bridge";
import { createNatsPreloadHost } from "../../src/platform/electron/preload/nats-host";
import type { PreloadIpcRenderer } from "../../src/platform/electron/preload/provider-wire";

type Listener = (event: unknown, value: unknown) => void;
const natsChannels = providerIpcChannels("nats");
const kafkaChannels = providerIpcChannels("kafka");
const command = {
  version: NATS_PROTOCOL_VERSION,
  id: "nats-list",
  command: "profiles.list",
  payload: {},
} as const;

function listResponse(): NatsCommandResponse<"profiles.list"> {
  return {
    version: NATS_PROTOCOL_VERSION,
    id: command.id,
    command: command.command,
    ok: true,
    result: {
      correlationId: "nats-list-correlation",
      profiles: {
        capability: { durability: "session", protection: "memory", state: "ready" },
        profiles: [],
      },
      connection: { state: "disconnected", profile: null },
      subscription: {
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
      },
    },
  };
}

class Ipc implements PreloadIpcRenderer {
  readonly invocations: Array<{ readonly channel: string; readonly value: unknown }> = [];
  readonly listeners = new Map<string, Set<Listener>>();
  readonly responses = new Map<string, unknown>([
    [natsChannels.subscribe, NATS_PROTOCOL_VERSION],
    [kafkaChannels.subscribe, HOST_PROTOCOL_VERSION],
    [natsChannels.command, listResponse()],
  ]);

  invoke(channel: string, value: unknown): Promise<unknown> {
    this.invocations.push({ channel, value });
    return Promise.resolve(this.responses.get(channel));
  }

  on(channel: string, listener: Listener): void {
    const listeners = this.listeners.get(channel) ?? new Set();
    listeners.add(listener);
    this.listeners.set(channel, listeners);
  }

  removeListener(channel: string, listener: Listener): void {
    this.listeners.get(channel)?.delete(listener);
  }

  emit(channel: string, value: unknown): void {
    for (const listener of [...(this.listeners.get(channel) ?? [])]) listener({}, value);
  }
}

describe("NATS native preload composition", () => {
  it("exposes an independent typed NATS client without acquiring an unused event stream", async () => {
    const ipc = new Ipc();
    const exposed = new Map<string, unknown>();
    exposeStreamSkopeHost(
      {
        exposeInMainWorld: (name, value): void => {
          exposed.set(name, value);
        },
      },
      ipc,
    );
    const providers = exposed.get("streamSkopeProviders") as {
      readonly kafka: StreamSkopeHost;
      readonly nats: NatsHost;
    };
    expect(Object.isFrozen(providers)).toBe(true);
    expect(providers.kafka).toBe(exposed.get("streamSkopeHost"));
    expect(Object.keys(providers.nats).sort()).toEqual(["execute", "subscribe"]);
    expect(ipc.invocations).toEqual([]);
    expect(ipc.listeners.size).toBe(0);
    await expect(providers.nats.execute(command)).resolves.toEqual(listResponse());
    expect(ipc.invocations).toEqual([{ channel: natsChannels.command, value: command }]);
  });

  it("uses NATS response validation and request correlation instead of accepting Kafka or another request", async () => {
    const ipc = new Ipc();
    const host = createNatsPreloadHost(ipc);
    ipc.responses.set(natsChannels.command, { ...listResponse(), version: HOST_PROTOCOL_VERSION });
    await expect(host.execute(command)).rejects.toThrow();
    ipc.responses.set(natsChannels.command, { ...listResponse(), id: "another-request" });
    await expect(host.execute(command)).rejects.toThrow();
    ipc.responses.set(natsChannels.command, listResponse());
    await expect(host.execute(command)).resolves.toEqual(listResponse());
    expect(ipc.invocations.every(({ channel }) => channel === natsChannels.command)).toBe(true);
  });

  it("keeps equal sequence ACKs and malformed-stream retirement isolated between NATS and Kafka", async () => {
    const ipc = new Ipc();
    const exposed = new Map<string, unknown>();
    exposeStreamSkopeHost(
      {
        exposeInMainWorld: (name, value): void => {
          exposed.set(name, value);
        },
      },
      ipc,
    );
    const providers = exposed.get("streamSkopeProviders") as {
      readonly kafka: StreamSkopeHost;
      readonly nats: NatsHost;
    };
    const natsEvents: NatsEvent[] = [];
    const kafkaEvents: HostEvent[] = [];
    const stopNats = providers.nats.subscribe((event) => {
      natsEvents.push(event);
    });
    const stopKafka = providers.kafka.subscribe((event) => {
      kafkaEvents.push(event);
    });
    const natsReady = {
      version: NATS_PROTOCOL_VERSION,
      sequence: 1,
      event: "backend.availability",
      payload: { state: "ready" },
    } as const;
    const kafkaReady = { ...natsReady, version: HOST_PROTOCOL_VERSION } as const;
    await Promise.resolve();
    ipc.emit(natsChannels.event, natsReady);
    ipc.emit(kafkaChannels.event, kafkaReady);
    expect(ipc.invocations.filter(({ channel }) => channel.endsWith("event-ack"))).toEqual([
      { channel: natsChannels.acknowledge, value: 1 },
      { channel: kafkaChannels.acknowledge, value: 1 },
    ]);
    ipc.emit(natsChannels.event, { ...kafkaReady, sequence: 2 });
    ipc.emit(kafkaChannels.event, { ...kafkaReady, sequence: 2 });
    expect(natsEvents).toHaveLength(2);
    expect(natsEvents[1]).toMatchObject({
      event: "backend.availability",
      payload: { state: "unavailable" },
    });
    expect(kafkaEvents).toEqual([kafkaReady, { ...kafkaReady, sequence: 2 }]);
    expect(ipc.listeners.get(natsChannels.event)?.size).toBe(0);
    expect(ipc.listeners.get(kafkaChannels.event)?.size).toBe(1);
    const requestsBefore = ipc.invocations.length;
    await expect(providers.nats.execute(command)).rejects.toThrow("unavailable");
    expect(ipc.invocations).toHaveLength(requestsBefore);
    expect(
      ipc.invocations.filter(({ channel }) => channel === natsChannels.acknowledge),
    ).toHaveLength(1);
    expect(
      ipc.invocations.filter(({ channel }) => channel === kafkaChannels.acknowledge),
    ).toHaveLength(2);
    stopNats();
    stopKafka();
    expect(ipc.listeners.get(kafkaChannels.event)?.size).toBe(0);
  });
});
