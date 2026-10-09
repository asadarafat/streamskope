import { kafkaRawMessageRetainedBytes, utf8ByteLength, type KafkaMessage } from "../contracts";
import type { RecordReadSettings } from "../contracts/finite-record-read";
import { KAFKA_QUERY_LIMITS } from "../contracts/query-search";
import {
  KAFKA_RECORD_LOCATOR_LIMITS,
  kafkaRecordLocator,
  parseKafkaRecordLocatorLoadInput,
  sameKafkaRecordLocator,
  type KafkaRecordLocatorLoadInput,
  type KafkaRecordLocatorOutcome,
  type KafkaRecordLocatorReason,
} from "../contracts/record-locator";

import { connectionErrorChain } from "./connection-diagnostics";
import type { RecordReadScope } from "./connection-scope";
import { FiniteReadFailure, FiniteRecordRead, type FiniteReadResult } from "./finite-record-read";
import {
  KafkaRecordLocatorError,
  RecordLocatorOperationError,
  UnknownRecordLocatorRequestError,
} from "./record-locator-errors";

export {
  RecordLocatorOperationError,
  UnknownRecordLocatorRequestError,
} from "./record-locator-errors";

interface RecordLocatorServiceOptions {
  readonly scope: () => RecordReadScope | null;
  readonly settings: () => RecordReadSettings;
  readonly now?: () => number;
  readonly durationMs?: number;
}
interface OwnedLoad {
  readonly input: KafkaRecordLocatorLoadInput;
  readonly authority: AbortController;
  reader: FiniteRecordRead | undefined;
  task: Promise<KafkaRecordLocatorOutcome> | undefined;
  cancelling: Promise<void> | undefined;
  message: KafkaMessage | undefined;
  stopped: "cancelled" | "deadline" | "revoked" | undefined;
  settled: boolean;
  cleanupDebt: boolean;
}

function outcome(
  input: KafkaRecordLocatorLoadInput,
  state: KafkaRecordLocatorReason,
): KafkaRecordLocatorOutcome {
  return { ...input, state, detail: new KafkaRecordLocatorError(state).message };
}

function failureReason(failure: unknown): KafkaRecordLocatorReason {
  const chain = connectionErrorChain(failure);
  const known = chain.find((error) => error instanceof KafkaRecordLocatorError);
  if (known instanceof KafkaRecordLocatorError) return known.reason;
  if (chain.some((error) => error instanceof FiniteReadFailure && error.kind === "revoked"))
    return "revoked";
  if (
    chain.some((error) => {
      if (error === null || typeof error !== "object") return false;
      return ["code", "apiId"].some(
        (key) =>
          key in error &&
          [
            "AUTHORIZATION_DENIED",
            "KAFKA_AUTHENTICATION",
            "TOPIC_AUTHORIZATION_FAILED",
            "CLUSTER_AUTHORIZATION_FAILED",
            "SASL_AUTHENTICATION_FAILED",
          ].includes(String((error as Record<string, unknown>)[key])),
      );
    })
  )
    return "inaccessible";
  return "unavailable";
}

/** Reload one position through current protection; retain cleanup authority, never loaded payloads. */
export class RecordLocatorService {
  private current: OwnedLoad | undefined;
  private last: KafkaRecordLocatorLoadInput | undefined;
  private readonly now: () => number;
  private readonly durationMs: number;

  constructor(private readonly options: RecordLocatorServiceOptions) {
    this.now = options.now ?? Date.now;
    this.durationMs = options.durationMs ?? KAFKA_RECORD_LOCATOR_LIMITS.durationMs;
    if (
      !Number.isSafeInteger(this.durationMs) ||
      this.durationMs <= 0 ||
      this.durationMs > KAFKA_RECORD_LOCATOR_LIMITS.durationMs
    )
      throw new Error("Record reload deadline is outside the supported limit.");
  }

  load(value: KafkaRecordLocatorLoadInput): Promise<KafkaRecordLocatorOutcome> {
    const input = parseKafkaRecordLocatorLoadInput(value);
    const active = this.current;
    if (active !== undefined) {
      if (active.input.requestId !== input.requestId) throw new RecordLocatorOperationError("busy");
      if (active.cleanupDebt) throw new RecordLocatorOperationError("cleanup");
      if (
        active.input.requestId === input.requestId &&
        sameKafkaRecordLocator(active.input.locator, input.locator) &&
        active.task !== undefined &&
        active.stopped === undefined
      )
        return active.task;
      throw new RecordLocatorOperationError(
        active.input.requestId === input.requestId ? "reused-request" : "busy",
      );
    }
    if (this.last?.requestId === input.requestId)
      throw new RecordLocatorOperationError("reused-request");
    const owner: OwnedLoad = {
      input,
      authority: new AbortController(),
      reader: undefined,
      task: undefined,
      cancelling: undefined,
      message: undefined,
      stopped: undefined,
      settled: false,
      cleanupDebt: false,
    };
    // Reserve before any asynchronous work, including settings and scope preparation.
    this.current = owner;
    owner.task = Promise.resolve()
      .then(() => this.run(owner))
      .finally(() => {
        owner.settled = true;
        owner.task = undefined;
        owner.message = undefined;
        if (!owner.cleanupDebt) this.release(owner);
      });
    return owner.task;
  }

  async cancel(requestId: string): Promise<void> {
    const owner = this.current;
    if (owner?.input.requestId !== requestId) {
      if (this.last?.requestId === requestId) return;
      throw new UnknownRecordLocatorRequestError();
    }
    if (owner.cancelling !== undefined) return owner.cancelling;
    owner.stopped ??= "cancelled";
    owner.message = undefined;
    // Reserve before stop can synchronously invoke abort observers. Every concurrent caller
    // joins this exact cleanup attempt; a failed attempt alone permits a later retry.
    owner.cancelling = Promise.resolve()
      .then(() => this.finishCancellation(owner))
      .catch((error: unknown) => {
        owner.cancelling = undefined;
        throw error;
      });
    owner.reader?.stop("cancelled");
    return owner.cancelling;
  }

  private async finishCancellation(owner: OwnedLoad): Promise<void> {
    await owner.task?.catch(() => undefined);
    try {
      await owner.reader?.retryCleanup();
    } catch {
      owner.cleanupDebt = true;
      throw new RecordLocatorOperationError("cleanup");
    }
    owner.cleanupDebt = false;
    this.release(owner);
  }

  invalidate(): void {
    const owner = this.current;
    if (owner === undefined) return;
    owner.stopped = "revoked";
    owner.message = undefined;
    owner.authority.abort();
    owner.reader?.stop("revoked");
  }

  async idle(): Promise<void> {
    const owner = this.current;
    if (owner === undefined) return;
    await owner.task?.catch(() => undefined);
    if (owner.cleanupDebt) throw new RecordLocatorOperationError("cleanup");
  }

  private async run(owner: OwnedLoad): Promise<KafkaRecordLocatorOutcome> {
    const scope = this.options.scope();
    if (owner.stopped !== undefined)
      return outcome(owner.input, owner.stopped === "deadline" ? "unavailable" : owner.stopped);
    if (scope === null) return outcome(owner.input, "unavailable");
    let settings: string;
    try {
      settings = JSON.stringify(this.options.settings());
    } catch {
      return outcome(owner.input, "unavailable");
    }
    const assertCurrent = (): void => {
      if (
        this.current !== owner ||
        owner.authority.signal.aborted ||
        !scope.isCurrent() ||
        JSON.stringify(this.options.settings()) !== settings
      )
        throw new KafkaRecordLocatorError("revoked");
    };
    const locator = owner.input.locator;
    const reader = new FiniteRecordRead({
      scope,
      expectedLocator: locator,
      input: {
        topic: locator.topic,
        range: { mode: "earliest" },
        maxRecords: 1,
        search: {
          key: "",
          value: "",
          offset: "",
          timestamp: "",
          partition: locator.partition,
          offsetExact: locator.offset,
        },
      },
      limits: {
        scanRecords: KAFKA_QUERY_LIMITS.scanRecords,
        scanBytes: KAFKA_QUERY_LIMITS.scanBytes,
        passes: 1,
      },
      deadlineAt: this.now() + this.durationMs,
      authority: owner.authority.signal,
      assertCurrent,
      changed: (): void => undefined,
      now: this.now,
    });
    owner.reader = reader;
    const deadline = setTimeout(() => {
      owner.stopped ??= "deadline";
      owner.message = undefined;
      reader.stop("deadline");
    }, this.durationMs);
    deadline.unref?.();
    let result: FiniteReadResult | undefined;
    let failure: unknown;
    try {
      result = await reader.run((message) => {
        const actual = kafkaRecordLocator(message);
        if (actual === null) throw new KafkaRecordLocatorError("unavailable");
        if (
          actual.clusterId !== locator.clusterId ||
          actual.topicId !== locator.topicId ||
          actual.topic !== locator.topic
        )
          throw new KafkaRecordLocatorError("resource-replaced");
        if (actual.partition !== locator.partition || actual.offset !== locator.offset)
          throw new KafkaRecordLocatorError("unavailable");
        if (actual.leaderEpoch !== locator.leaderEpoch)
          throw new KafkaRecordLocatorError("record-replaced");
        if (
          kafkaRawMessageRetainedBytes(message) > KAFKA_RECORD_LOCATOR_LIMITS.responseBytes ||
          utf8ByteLength(JSON.stringify({ ...owner.input, state: "loaded", message })) >
            KAFKA_RECORD_LOCATOR_LIMITS.responseBytes
        )
          throw new KafkaRecordLocatorError("unavailable");
        owner.message = message;
        return Promise.resolve("committed");
      });
    } catch (error) {
      failure = error;
    } finally {
      clearTimeout(deadline);
    }
    try {
      await reader.idle();
    } catch {
      owner.cleanupDebt = true;
      owner.message = undefined;
      throw new RecordLocatorOperationError("cleanup");
    }
    try {
      assertCurrent();
    } catch {
      return outcome(owner.input, "revoked");
    }
    if (owner.stopped !== undefined)
      return outcome(owner.input, owner.stopped === "deadline" ? "unavailable" : owner.stopped);
    if (failure !== undefined) return outcome(owner.input, failureReason(failure));
    if (owner.message !== undefined)
      return { ...owner.input, state: "loaded", message: owner.message };
    const partition = result?.coverage?.partitions;
    const complete =
      result?.reason === "range-complete" &&
      result.source.clusterId === locator.clusterId &&
      result.source.topicId === locator.topicId &&
      partition?.length === 1 &&
      partition[0]?.partition === locator.partition &&
      partition[0].startOffset === locator.offset &&
      partition[0].endOffset === String(BigInt(locator.offset) + 1n) &&
      partition[0].nextOffset === partition[0].endOffset;
    return outcome(owner.input, complete ? "record-missing" : "unavailable");
  }

  private release(owner: OwnedLoad): void {
    owner.reader = undefined;
    owner.message = undefined;
    owner.task = undefined;
    if (this.current === owner) {
      this.last = owner.input;
      this.current = undefined;
    }
  }
}
