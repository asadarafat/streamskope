import {
  kafkaMessageRetainedBytes,
  type HostEvent,
  type KafkaExploredMessage,
} from "../../kafka/contracts";

export interface SseClientEventQueueOptions {
  readonly maxEvents: number;
  readonly maxMessageBytes: number;
  readonly maxMessages: number;
}

function positiveSafeInteger(value: number, name: string): number {
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new RangeError(`${name} must be a positive safe integer.`);
  }
  return value;
}

function messageBatch(event: HostEvent): readonly KafkaExploredMessage[] {
  return event.event === "messages.batch" ? event.payload.messages : [];
}

export class SseClientEventQueue {
  private droppedMessages = 0;
  private readonly events: HostEvent[] = [];
  private readonly maxEvents: number;
  private readonly maxMessageBytes: number;
  private readonly maxMessages: number;
  private messageBytes = 0;
  private messageCount = 0;

  constructor(options: SseClientEventQueueOptions) {
    this.maxEvents = positiveSafeInteger(options.maxEvents, "maxEvents");
    this.maxMessageBytes = positiveSafeInteger(options.maxMessageBytes, "maxMessageBytes");
    this.maxMessages = positiveSafeInteger(options.maxMessages, "maxMessages");
  }

  get length(): number {
    return this.events.length;
  }

  get queuedMessageBytes(): number {
    return this.messageBytes;
  }

  get queuedMessages(): number {
    return this.messageCount;
  }

  dequeue(): HostEvent | undefined {
    const event = this.events.shift();
    if (event === undefined) {
      return undefined;
    }
    for (const message of messageBatch(event)) {
      this.messageBytes -= kafkaMessageRetainedBytes(message);
      this.messageCount -= 1;
    }
    return this.withTransportDrops(event);
  }

  enqueue(event: HostEvent): boolean {
    if (event.event === "consumption.state" && event.payload.state === "loading") {
      this.resetMessageGeneration();
    }
    if (this.events.length >= this.maxEvents && !this.makeEventSlot(event)) {
      return false;
    }

    const queued =
      event.event === "messages.batch"
        ? {
            ...event,
            payload: {
              ...event.payload,
              messages: [...event.payload.messages],
            },
          }
        : event;
    this.events.push(queued);
    for (const message of messageBatch(queued)) {
      this.messageBytes += kafkaMessageRetainedBytes(message);
      this.messageCount += 1;
    }
    this.trimMessages();
    return true;
  }

  private dropMessageBatchAt(index: number): void {
    const event = this.events[index];
    if (event?.event !== "messages.batch") {
      return;
    }
    for (const message of event.payload.messages) {
      this.messageBytes -= kafkaMessageRetainedBytes(message);
      this.messageCount -= 1;
      this.droppedMessages += 1;
    }
    this.events.splice(index, 1);
  }

  private makeEventSlot(incoming: HostEvent): boolean {
    if (incoming.event !== "messages.batch") {
      return false;
    }
    const messageEventIndex = this.events.findIndex((queued) => queued.event === "messages.batch");
    if (messageEventIndex < 0) {
      return false;
    }
    this.dropMessageBatchAt(messageEventIndex);
    return true;
  }

  private resetMessageGeneration(): void {
    let messageEventIndex = this.events.findIndex((queued) => queued.event === "messages.batch");
    while (messageEventIndex >= 0) {
      this.dropMessageBatchAt(messageEventIndex);
      messageEventIndex = this.events.findIndex((queued) => queued.event === "messages.batch");
    }
    this.droppedMessages = 0;
  }

  private trimMessages(): void {
    while (this.messageCount > this.maxMessages || this.messageBytes > this.maxMessageBytes) {
      const batchIndex = this.events.findIndex(
        (event) => event.event === "messages.batch" && event.payload.messages.length > 0,
      );
      const event = this.events[batchIndex];
      if (batchIndex < 0 || event?.event !== "messages.batch") {
        return;
      }
      const [dropped, ...remaining] = event.payload.messages;
      if (dropped === undefined) {
        return;
      }
      this.messageBytes -= kafkaMessageRetainedBytes(dropped);
      this.messageCount -= 1;
      this.droppedMessages += 1;
      if (remaining.length === 0) {
        this.events.splice(batchIndex, 1);
      } else {
        this.events[batchIndex] = {
          ...event,
          payload: {
            ...event.payload,
            messages: remaining,
          },
        };
      }
    }
  }

  private withTransportDrops(event: HostEvent): HostEvent {
    if (event.event === "messages.batch") {
      return {
        ...event,
        payload: {
          ...event.payload,
          droppedMessages: event.payload.droppedMessages + this.droppedMessages,
        },
      };
    }
    if (event.event === "consumption.state") {
      return {
        ...event,
        payload: {
          ...event.payload,
          droppedMessages: event.payload.droppedMessages + this.droppedMessages,
        },
      };
    }
    return event;
  }
}
