import type {
  NatsEngine,
  NatsMessageReceipt,
} from "../../src/features/nats/application/engine-port";
import type { NatsResolvedProfile } from "../../src/features/nats/application/profile-types";
import type { NatsSessionContext } from "../../src/features/nats/application/session";

export const natsContext: NatsSessionContext = {
  operation: "profiles.connect",
  correlationId: "fixture-context",
};
export function natsProfile(id = "profile-1"): NatsResolvedProfile {
  return {
    identity: { id, revision: 1, name: id },
    connection: {
      servers: ["nats://localhost:4222"],
      authentication: { mode: "none" },
      tls: { mode: "plaintext" },
    },
  };
}
export function copiedNatsReceipt(data = "fixture"): NatsMessageReceipt {
  return {
    kind: "record",
    record: {
      subject: "qualification.events",
      headers: [],
      headersTruncated: false,
      payload: { encoding: "utf8", data },
      payloadBytes: new TextEncoder().encode(data).byteLength,
      preview: data.slice(0, 100),
      receivedAt: "2026-10-05T13:00:00.000Z",
      timestampProvenance: "host-received",
    },
  };
}
export class NatsApplicationEngineFixture implements NatsEngine {
  connectionOptions: Parameters<NatsEngine["connect"]>[1] | undefined;
  subscriptionOptions: Parameters<NatsEngine["startSubscription"]>[1] | undefined;
  connectOperation: () => Promise<void> = () => Promise.resolve();
  startOperation: () => Promise<void> = () => Promise.resolve();
  stopOperation: () => Promise<void> = () => Promise.resolve();
  disconnectOperation: () => Promise<void> = () => Promise.resolve();
  shutdownOperation: () => Promise<void> = () => Promise.resolve();
  stopCalls = 0;
  disconnectCalls = 0;
  shutdownCalls = 0;
  connect(
    _input: Parameters<NatsEngine["connect"]>[0],
    options: Parameters<NatsEngine["connect"]>[1],
  ): Promise<void> {
    this.connectionOptions = options;
    return this.connectOperation();
  }
  startSubscription(
    _subject: string,
    options: Parameters<NatsEngine["startSubscription"]>[1],
  ): Promise<void> {
    this.subscriptionOptions = options;
    return this.startOperation();
  }
  stopSubscription(): Promise<void> {
    this.stopCalls += 1;
    return this.stopOperation();
  }
  disconnect(): Promise<void> {
    this.disconnectCalls += 1;
    return this.disconnectOperation();
  }
  shutdown(): Promise<void> {
    this.shutdownCalls += 1;
    return this.shutdownOperation();
  }
}
