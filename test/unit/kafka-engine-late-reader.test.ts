import { describe, expect, it } from "vitest";

import type { KafkaFetchRequest, SecureConnectionInput } from "../../src/features/kafka/contracts";
import { StreamSkopeKafkaEngine } from "../../src/features/kafka/engine/engine";
import type {
  KafkaAdminPort,
  KafkaRawMessage,
  OAuthToken,
} from "../../src/features/kafka/engine/types";

const connection: SecureConnectionInput = {
  name: "fixture",
  brokers: ["localhost:9092"],
  tls: { enabled: false },
};
const tailRequest = (): KafkaFetchRequest => ({ topic: "test", mode: "tail", maxMessages: 10 });
function adminFixture(): KafkaAdminPort & { closeCalls: number } {
  return {
    closeCalls: 0,
    close(): Promise<void> {
      this.closeCalls++;
      return Promise.resolve();
    },
    listTopics: (): Promise<readonly string[]> => Promise.resolve(["test"]),
    alterTopicConfiguration: (): Promise<void> => Promise.resolve(),
    describeTopicConfiguration: (): Promise<readonly never[]> => Promise.resolve([]),
    describeBrokerConfiguration: (): never => {
      throw new Error("Unexpected operation");
    },
    describeClusterMetadata: (): never => {
      throw new Error("Unexpected operation");
    },
    describeConsumerGroup: (): never => {
      throw new Error("Unexpected operation");
    },
    listConsumerGroups: (): never => {
      throw new Error("Unexpected operation");
    },
  };
}
class RecordingRawMessageStream {
  closeCalls = 0;
  constructor(_messages: readonly KafkaRawMessage[]) {}
  close(): Promise<void> {
    this.closeCalls++;
    return Promise.resolve();
  }
  async *[Symbol.asyncIterator](): AsyncIterator<KafkaRawMessage> {
    yield* await Promise.resolve<readonly KafkaRawMessage[]>([]);
  }
}

describe("engine late reader ownership", () => {
  it.each([true, false])(
    "keeps a reader opened during cancellation owned through concurrent connection close (first close fails: %s)",
    async (failFirstClose) => {
      const failure = new Error("late reader close failed");
      class LateReader extends RecordingRawMessageStream {
        override close(): Promise<void> {
          this.closeCalls++;
          return this.closeCalls === 1 && failFirstClose
            ? Promise.reject(failure)
            : Promise.resolve();
        }
      }
      const late = new LateReader([]);
      let complete!: (reader: RecordingRawMessageStream) => void;
      const opening = new Promise<RecordingRawMessageStream>((resolve) => {
        complete = resolve;
      });
      const admin = adminFixture();
      const engine = new StreamSkopeKafkaEngine({
        adminFactory: { create: (): KafkaAdminPort => admin },
        consumerFactory: { open: (): Promise<RecordingRawMessageStream> => opening },
        requestOAuthToken: (): Promise<OAuthToken> => Promise.resolve({ value: "active-token" }),
      });
      const active = await engine.openConnection(connection, new AbortController().signal);
      const abort = new AbortController();
      const read = active.openMessageStream(tailRequest(), abort.signal);
      const rejected = expect(read).rejects.toMatchObject({
        code: "CANCELLED",
        ...(failFirstClose ? { cleanupCause: failure } : {}),
      });
      abort.abort();
      let closed = false;
      const closing = active.close().then(() => {
        closed = true;
      });
      await Promise.resolve();
      expect(closed).toBe(false);
      expect(admin.closeCalls).toBe(0);
      complete(late);
      await rejected;
      await closing;
      expect(late.closeCalls).toBe(failFirstClose ? 2 : 1);
      expect(admin.closeCalls).toBe(1);
    },
  );

  it("settles pending-open ownership even when the consumer factory throws synchronously", async () => {
    const admin = adminFixture();
    const engine = new StreamSkopeKafkaEngine({
      adminFactory: { create: (): KafkaAdminPort => admin },
      consumerFactory: {
        open: (): never => {
          throw new Error("open failed");
        },
      },
      requestOAuthToken: (): Promise<OAuthToken> => Promise.resolve({ value: "active-token" }),
    });
    const active = await engine.openConnection(connection, new AbortController().signal);
    await expect(
      active.openMessageStream(tailRequest(), new AbortController().signal),
    ).rejects.toThrow();
    await active.close();
    expect(admin.closeCalls).toBe(1);
  });
});
