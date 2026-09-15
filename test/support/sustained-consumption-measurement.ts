import { KAFKA_MESSAGE_LIMITS, type HostEvent } from "../../src/kafka/contracts";
import { initialKafkaUiState, reduceKafkaHostEvent } from "../../src/kafka/ui/state";
import { ELECTRON_RUNTIME_EFFICIENCY_POLICY } from "../../tools/electron-runtime-efficiency-policy";

import {
  ControlledMessageStream,
  RecordingActiveConnection,
  RecordingConnectionPort,
  command,
  createFacade,
  message,
} from "./kafka-backend-facade-fixture";

export interface SustainedConsumptionMeasurementOptions {
  readonly messageCount?: number;
}

export interface SustainedConsumptionEvidence {
  readonly acceptedMessages: number;
  readonly deliveredMessages: number;
  readonly elapsedCpuMs: number;
  readonly maximumConcurrentFlushes: number;
  readonly maximumHostQueueMessages: number;
  readonly monitorHistorySamples: number;
  readonly pendingFlushesAfterStop: number;
  readonly rendererEvictions: number;
  readonly retainedMessages: number;
  readonly scheduledFlushes: number;
  readonly stopState: "failed" | "succeeded";
}

interface ScheduledFlush {
  readonly cancel: () => void;
  readonly run: () => void;
}

function elapsedCpuMilliseconds(started: NodeJS.CpuUsage): number {
  const elapsed = process.cpuUsage(started);
  return (elapsed.system + elapsed.user) / 1_000;
}

async function waitForDelivered(stream: ControlledMessageStream, target: number): Promise<void> {
  for (let attempt = 0; attempt < 20_000; attempt += 1) {
    if (stream.deliveredMessages >= target) {
      return;
    }
    await new Promise<void>((resolve) => {
      setImmediate(resolve);
    });
  }
  throw new Error(
    `Sustained consumer delivered ${String(stream.deliveredMessages)} of ${String(target)} records.`,
  );
}

export async function measureSustainedConsumption(
  options: SustainedConsumptionMeasurementOptions = {},
): Promise<SustainedConsumptionEvidence> {
  const messageCount =
    options.messageCount ?? ELECTRON_RUNTIME_EFFICIENCY_POLICY.messageStress.sustainedMessages;
  if (!Number.isSafeInteger(messageCount) || messageCount < KAFKA_MESSAGE_LIMITS.retainedMessages) {
    throw new RangeError(
      `Sustained measurement requires at least ${String(KAFKA_MESSAGE_LIMITS.retainedMessages)} messages.`,
    );
  }

  let pendingFlush: ScheduledFlush | undefined;
  let maximumConcurrentFlushes = 0;
  let scheduledFlushes = 0;
  const scheduler = (flush: () => void): (() => void) => {
    if (pendingFlush !== undefined) {
      throw new Error("The facade scheduled more than one concurrent message flush.");
    }
    scheduledFlushes += 1;
    let current = true;
    const scheduled: ScheduledFlush = {
      cancel: (): void => {
        if (!current) {
          return;
        }
        current = false;
        if (pendingFlush === scheduled) {
          pendingFlush = undefined;
        }
      },
      run: (): void => {
        if (!current) {
          return;
        }
        current = false;
        if (pendingFlush === scheduled) {
          pendingFlush = undefined;
        }
        flush();
      },
    };
    pendingFlush = scheduled;
    maximumConcurrentFlushes = Math.max(maximumConcurrentFlushes, 1);
    return scheduled.cancel;
  };

  const stream = new ControlledMessageStream();
  const activeConnection = new RecordingActiveConnection();
  activeConnection.messageStreamOperations.push(() => Promise.resolve(stream));
  const port = new RecordingConnectionPort();
  port.openOperations.push(() => Promise.resolve(activeConnection));
  const facade = createFacade(port, scheduler);
  let rendererState = initialKafkaUiState;
  let deliveredMessages = 0;
  let maximumHostQueueMessages = 0;
  facade.subscribe((event: HostEvent) => {
    if (event.event === "messages.batch") {
      deliveredMessages += event.payload.messages.length;
      rendererState = reduceKafkaHostEvent(rendererState, event);
    } else if (event.event === "streamMetrics.changed") {
      maximumHostQueueMessages = Math.max(
        maximumHostQueueMessages,
        event.payload.queue?.peakMessages ?? 0,
      );
      rendererState = reduceKafkaHostEvent(rendererState, event);
    }
  });

  const started = process.cpuUsage();
  const connectionResponse = await facade.execute(command("connection.connect", "soak-connect"));
  if (!connectionResponse.ok) {
    throw new Error(`Sustained consumer connection failed: ${connectionResponse.error.summary}`);
  }
  const startResponse = await facade.execute(command("messages.start", "soak-start"));
  if (!startResponse.ok) {
    throw new Error(`Sustained consumer start failed: ${startResponse.error.summary}`);
  }

  for (
    let batchStart = 0;
    batchStart < messageCount;
    batchStart += KAFKA_MESSAGE_LIMITS.batchMessages
  ) {
    const batchSize = Math.min(KAFKA_MESSAGE_LIMITS.batchMessages, messageCount - batchStart);
    for (let index = 0; index < batchSize; index += 1) {
      stream.push(message(String(batchStart + index)));
    }
    await waitForDelivered(stream, batchStart + batchSize);
    if (batchStart + batchSize < messageCount) {
      const scheduled = pendingFlush;
      if (scheduled === undefined) {
        throw new Error("Accepted messages did not schedule their bounded host flush.");
      }
      scheduled.run();
    }
  }

  const stopResponse = await facade.execute(command("messages.stop", "soak-stop"));
  const evidence: SustainedConsumptionEvidence = Object.freeze({
    acceptedMessages: stream.deliveredMessages,
    deliveredMessages,
    elapsedCpuMs: elapsedCpuMilliseconds(started),
    maximumConcurrentFlushes,
    maximumHostQueueMessages,
    monitorHistorySamples: rendererState.streamMonitor.history.length,
    pendingFlushesAfterStop: pendingFlush === undefined ? 0 : 1,
    rendererEvictions: rendererState.rendererDroppedMessages,
    retainedMessages: rendererState.messages.length,
    scheduledFlushes,
    stopState: stopResponse.ok ? "succeeded" : "failed",
  });
  await facade.shutdown();
  return evidence;
}

export function assertSustainedConsumptionEvidence(evidence: SustainedConsumptionEvidence): void {
  const expected = ELECTRON_RUNTIME_EFFICIENCY_POLICY.messageStress.sustainedMessages;
  if (evidence.acceptedMessages < expected || evidence.deliveredMessages !== expected) {
    throw new Error(
      `Sustained consumer processed ${String(evidence.acceptedMessages)} accepted and ${String(evidence.deliveredMessages)} delivered records; expected ${String(expected)}.`,
    );
  }
  if (
    evidence.maximumConcurrentFlushes !== 1 ||
    evidence.maximumHostQueueMessages > KAFKA_MESSAGE_LIMITS.queuedMessages ||
    evidence.retainedMessages > KAFKA_MESSAGE_LIMITS.retainedMessages ||
    evidence.pendingFlushesAfterStop !== 0 ||
    evidence.stopState !== "succeeded"
  ) {
    throw new Error(`Sustained consumer violated a lifecycle bound: ${JSON.stringify(evidence)}.`);
  }
}
