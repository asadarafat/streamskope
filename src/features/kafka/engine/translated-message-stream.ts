import type { KafkaMessage, KafkaReadCoverage } from "../contracts";
import type { KafkaMessageStream } from "../application/types";
import type { KafkaReadCheckpoint } from "../application/read-checkpoint";
import { KafkaRecordLocatorError } from "../application/record-locator-errors";

import { mapKafkaAdminFailure } from "./failure";
import type { KafkaRawMessage, KafkaRawMessageStream } from "./types";

export class TranslatedKafkaMessageStream implements KafkaMessageStream {
  private readonly originals = new WeakMap<KafkaMessage, KafkaRawMessage>();

  acknowledge(message: KafkaMessage): void {
    const original = this.originals.get(message);
    if (original !== undefined) {
      this.rawStream.acknowledge?.(original);
      this.originals.delete(message);
    }
  }

  checkpoint(): KafkaReadCheckpoint | undefined {
    return this.rawStream.checkpoint?.();
  }

  coverage(): KafkaReadCoverage | undefined {
    return this.rawStream.coverage?.();
  }

  subscribeCoverage(listener: (coverage: KafkaReadCoverage) => void): () => void {
    return this.rawStream.subscribeCoverage?.(listener) ?? ((): void => undefined);
  }
  private closePromise: Promise<void> | undefined;

  constructor(
    private readonly rawStream: KafkaRawMessageStream,
    private readonly target: string,
    private readonly onClose: () => void,
    private readonly prepareRecord: (message: KafkaRawMessage) => Promise<KafkaMessage>,
    private readonly preparationController: AbortController,
  ) {}

  close(): Promise<void> {
    this.preparationController.abort();
    this.closePromise ??= this.rawStream
      .close()
      .then(this.onClose)
      .catch((error: unknown) => {
        this.closePromise = undefined;
        throw error;
      });
    return this.closePromise;
  }

  async *[Symbol.asyncIterator](): AsyncIterator<KafkaMessage> {
    try {
      for await (const raw of this.rawStream) {
        const message = await this.prepareRecord(raw);
        this.originals.set(message, raw);
        yield message;
      }
    } catch (error) {
      if (error instanceof KafkaRecordLocatorError) throw error;
      throw mapKafkaAdminFailure(error, this.target);
    }
  }
}
