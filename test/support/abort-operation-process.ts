import { setImmediate as nextTurn } from "node:timers/promises";

import type {
  KafkaAdminPort,
  KafkaRawMessage,
  KafkaRawMessageStream,
} from "../../src/features/kafka/engine/types";
import { StreamSkopeKafkaEngine } from "../../src/features/kafka/engine/engine";
import { KafkaEngineFailure } from "../../src/features/kafka/engine/failure";
import type { KafkaLatencyNetworkResult } from "../../src/features/kafka/engine/latency-network";
import {
  PlatformaticLatencyProbe,
  type PlatformaticLatencyProducer,
} from "../../src/features/kafka/engine/platformatic-latency";

class MetadataAdmin implements KafkaAdminPort {
  listCalls = 0;
  closeCalls = 0;

  listTopics(): Promise<readonly string[]> {
    this.listCalls += 1;
    return this.listCalls === 1
      ? Promise.resolve(["fixture.events"])
      : Promise.reject(new Error("Fixture metadata read rejected."));
  }
  close(): Promise<void> {
    this.closeCalls += 1;
    return Promise.resolve();
  }
  alterTopicConfiguration(): never {
    throw new Error("Unexpected topic mutation in abort fixture.");
  }
  describeBrokerConfiguration(): never {
    throw new Error("Unexpected broker read in abort fixture.");
  }
  describeClusterMetadata(): never {
    throw new Error("Unexpected cluster read in abort fixture.");
  }
  describeConsumerGroup(): never {
    throw new Error("Unexpected group read in abort fixture.");
  }
  describeTopicConfiguration(): never {
    throw new Error("Unexpected topic configuration read in abort fixture.");
  }
  listConsumerGroups(): never {
    throw new Error("Unexpected group inventory in abort fixture.");
  }
}

class ProbeStream implements KafkaRawMessageStream {
  closeCalls = 0;
  pumpEnded = false;
  private closed = false;
  private finishRead: ((value: IteratorResult<KafkaRawMessage>) => void) | undefined;

  close(): Promise<void> {
    this.closeCalls += 1;
    this.closed = true;
    this.finishRead?.({ done: true, value: undefined });
    return Promise.resolve();
  }
  [Symbol.asyncIterator](): AsyncIterator<KafkaRawMessage> {
    return {
      next: async (): Promise<IteratorResult<KafkaRawMessage>> => {
        const result = this.closed
          ? { done: true as const, value: undefined }
          : await new Promise<IteratorResult<KafkaRawMessage>>((resolve) => {
              this.finishRead = resolve;
            });
        if (result.done) this.pumpEnded = true;
        return result;
      },
    };
  }
}

async function resultCode(operation: Promise<unknown>): Promise<string> {
  try {
    await operation;
    return "RESOLVED";
  } catch (error) {
    if (!(error instanceof KafkaEngineFailure)) throw error;
    return error.code;
  }
}

async function enginePreAbort(): Promise<{
  code: string;
  listCalls: number;
  adminCloseCalls: number;
}> {
  const admin = new MetadataAdmin();
  const engine = new StreamSkopeKafkaEngine({
    adminFactory: { create: (): KafkaAdminPort => admin },
  });
  const active = await engine.openConnection(
    { brokers: ["127.0.0.1:19092"], name: "Abort regression", tls: { enabled: false } },
    new AbortController().signal,
  );
  let code: string;
  try {
    code = await resultCode(active.listTopics(AbortSignal.abort()));
  } finally {
    await active.close();
    await active.close();
  }
  return { code, listCalls: admin.listCalls, adminCloseCalls: admin.closeCalls };
}

async function latencySendAbort(): Promise<{
  code: string;
  consumerCalls: number;
  sendCalls: number;
  streamCloseCalls: number;
  producerCloseCalls: number;
  producerForcedClose: boolean;
  pumpEnded: boolean;
}> {
  const stream = new ProbeStream();
  const controller = new AbortController();
  let consumerCalls = 0,
    sendCalls = 0,
    producerCloseCalls = 0,
    producerForcedClose = false;
  const producer: PlatformaticLatencyProducer = {
    send: () => {
      sendCalls += 1;
      controller.abort();
      return Promise.reject(new Error("Fixture send rejected after synchronous cancellation."));
    },
    close: (force) => {
      producerCloseCalls += 1;
      producerForcedClose = force === true;
      return Promise.resolve();
    },
  };
  const probe = new PlatformaticLatencyProbe({
    createConsumer: (): Promise<KafkaRawMessageStream> => {
      consumerCalls += 1;
      return Promise.resolve(stream);
    },
    createProducer: (): PlatformaticLatencyProducer => producer,
    createSampleId: (): string => "fixture-sample",
    probeNetwork: (): Promise<KafkaLatencyNetworkResult> =>
      Promise.resolve({
        endpoint: "127.0.0.1:19092",
        issues: [],
        tcpConnectMs: 1,
        tlsAttempted: false,
        tlsHandshakeMs: null,
      }),
  });
  const code = await resultCode(
    probe.run(
      {
        brokers: ["127.0.0.1:19092"],
        tlsEnabled: false,
        operationTimeoutMs: 5_000,
        runId: "abort-regression",
        request: {
          acknowledgements: -1,
          messageCount: 1,
          timeoutMs: 10_000,
          topic: "fixture.events",
        },
      },
      controller.signal,
    ),
  );
  return {
    code,
    consumerCalls,
    sendCalls,
    streamCloseCalls: stream.closeCalls,
    producerCloseCalls,
    producerForcedClose,
    pumpEnded: stream.pumpEnded,
  };
}

async function main(): Promise<void> {
  let unhandledRejections = 0;
  const observeRejection = (): void => {
    unhandledRejections += 1;
  };
  process.on("unhandledRejection", observeRejection);
  try {
    const scenario = process.argv[2];
    const evidence =
      scenario === "engine-pre-abort"
        ? await enginePreAbort()
        : scenario === "latency-send-abort"
          ? await latencySendAbort()
          : undefined;
    if (evidence === undefined) throw new Error("Unknown abort regression scenario.");
    await nextTurn();
    await nextTurn();
    process.stdout.write(JSON.stringify({ ...evidence, unhandledRejections }) + "\n");
  } finally {
    process.off("unhandledRejection", observeRejection);
  }
}

void main().catch((error: unknown) => {
  process.stderr.write(
    `${error instanceof Error ? (error.stack ?? error.message) : String(error)}\n`,
  );
  process.exitCode = 1;
});
