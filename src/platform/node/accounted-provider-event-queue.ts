import type { ProviderWireEvent } from "../providers/host";

export type DeliveryFailure =
  "event-limit" | "byte-limit" | "record-limit" | "record-byte-limit" | "event-validation";

export interface ProviderDeliveryCosts {
  readonly events: number;
  readonly serializedBytes: number;
  readonly records: number;
  readonly recordBytes: number;
}

export interface ProviderDeliveryLimits {
  readonly maxEvents: number;
  readonly maxSerializedBytes: number;
  readonly maxRecords: number;
  readonly maxRecordBytes?: number;
}

export interface ProviderDeliveryLease<Event extends ProviderWireEvent> {
  readonly event: Event;
  readonly complete: () => void;
}

export type ProviderDeliveryBegin<Event extends ProviderWireEvent> =
  | { readonly kind: "empty" }
  | { readonly kind: "failure"; readonly reason: DeliveryFailure }
  | { readonly kind: "lease"; readonly lease: ProviderDeliveryLease<Event> };

export interface ProviderDeliveryQueue<Event extends ProviderWireEvent> {
  readonly costs: ProviderDeliveryCosts;
  enqueue(event: Event): DeliveryFailure | undefined;
  begin(): ProviderDeliveryBegin<Event>;
  close(): void;
}

/** Replacement changes only this record array; the remaining JSON envelope stays intact. */
export interface ProviderDeliveryPolicy<Event extends ProviderWireEvent, Record> {
  readonly records: (event: Event) => readonly Record[] | undefined;
  readonly retainedRecordBytes: (record: Record) => number;
  readonly withRecords: (event: Event, records: readonly Record[]) => Event;
  readonly startsGeneration: (event: Event) => boolean;
  readonly decorateDrops: (event: Event, droppedRecords: number) => Event;
  readonly decorationReserveBytes: (event: Event) => number;
  readonly replacementKey: (event: Event) => string | undefined;
}

export interface ProviderDeliveryQueueOptions<Event extends ProviderWireEvent, Record> {
  readonly limits: ProviderDeliveryLimits;
  readonly overflow: "reject" | "evict-oldest-pending-records";
  readonly policy: ProviderDeliveryPolicy<Event, Record>;
}

interface Generation {
  dropped: number;
}

interface RecordCost {
  readonly serialized: number;
  readonly retained: number;
}

interface Entry<Event, Record> {
  event: Event;
  records: readonly Record[] | undefined;
  recordCosts: readonly RecordCost[];
  serializedBytes: number;
  recordBytes: number;
  readonly generation: Generation;
  readonly replacementKey: string | undefined;
}

function nonNegativeInteger(value: number): number {
  if (!Number.isSafeInteger(value) || value < 0) throw new RangeError("Invalid delivery cost.");
  return value;
}

function positiveInteger(value: number): number {
  if (nonNegativeInteger(value) === 0) throw new RangeError("Delivery limits must be positive.");
  return value;
}

function serializedBytes(value: unknown): number {
  const encoded = JSON.stringify(value);
  if (encoded === undefined) throw new Error("Provider events must have a JSON representation.");
  return Buffer.byteLength(encoded);
}

/** Pending and one owned write share budgets; only that write's completion releases its cost. */
export class AccountedProviderEventQueue<
  Event extends ProviderWireEvent,
  Record,
> implements ProviderDeliveryQueue<Event> {
  private readonly limits: ProviderDeliveryLimits;
  private readonly overflow: ProviderDeliveryQueueOptions<Event, Record>["overflow"];
  private readonly policy: ProviderDeliveryPolicy<Event, Record>;
  private readonly pending: Entry<Event, Record>[] = [];
  private inflight: Entry<Event, Record> | undefined;
  private generation: Generation = { dropped: 0 };
  private bytes = 0;
  private records = 0;
  private recordBytes = 0;
  private closed = false;

  constructor(options: ProviderDeliveryQueueOptions<Event, Record>) {
    this.limits = {
      maxEvents: positiveInteger(options.limits.maxEvents),
      maxSerializedBytes: positiveInteger(options.limits.maxSerializedBytes),
      maxRecords: positiveInteger(options.limits.maxRecords),
      ...(options.limits.maxRecordBytes === undefined
        ? {}
        : { maxRecordBytes: positiveInteger(options.limits.maxRecordBytes) }),
    };
    this.overflow = options.overflow;
    this.policy = options.policy;
  }

  get costs(): ProviderDeliveryCosts {
    return {
      events: this.pending.length + Number(this.inflight !== undefined),
      serializedBytes: this.bytes,
      records: this.records,
      recordBytes: this.recordBytes,
    };
  }

  enqueue(event: Event): DeliveryFailure | undefined {
    if (this.closed) return "event-validation";
    let entry: Entry<Event, Record> | undefined;
    try {
      const projection = this.measure(event);
      if (this.policy.startsGeneration(event)) {
        if (this.overflow === "evict-oldest-pending-records") {
          for (const previous of [...this.pending]) {
            if (previous.records !== undefined) this.remove(previous, true);
          }
        }
        this.generation = { dropped: 0 };
      }
      entry = { ...projection, generation: this.generation };
      const key = entry.replacementKey;
      if (key !== undefined) {
        const previous = this.pending.find((candidate) => candidate.replacementKey === key);
        if (previous !== undefined) this.remove(previous, false);
      }
      const nextBytes = nonNegativeInteger(this.bytes + entry.serializedBytes);
      const nextRecords = nonNegativeInteger(this.records + entry.recordCosts.length);
      const nextRecordBytes = nonNegativeInteger(this.recordBytes + entry.recordBytes);
      this.pending.push(entry);
      this.bytes = nextBytes;
      this.records = nextRecords;
      this.recordBytes = nextRecordBytes;
      const failure = this.overflow === "reject" ? this.limitFailure() : this.trim(entry);
      if (failure !== undefined && this.pending.includes(entry)) this.remove(entry, false);
      return failure;
    } catch {
      if (entry !== undefined && this.pending.includes(entry)) this.remove(entry, false);
      return "event-validation";
    }
  }

  begin(): ProviderDeliveryBegin<Event> {
    if (this.closed || this.inflight !== undefined) return { kind: "empty" };
    const entry = this.pending[0];
    if (entry === undefined) return { kind: "empty" };
    let event: Event;
    try {
      event = this.policy.decorateDrops(entry.event, entry.generation.dropped);
      this.assertProjection(event, entry.records);
      if (serializedBytes(event) > entry.serializedBytes)
        throw new Error("Decoration exceeds its reservation.");
    } catch {
      return { kind: "failure", reason: "event-validation" };
    }
    this.pending.shift();
    this.inflight = entry;
    return {
      kind: "lease",
      lease: {
        event,
        complete: (): void => {
          if (this.closed || this.inflight !== entry) return;
          this.inflight = undefined;
          this.releaseCosts(entry);
        },
      },
    };
  }

  close(): void {
    if (this.closed) return;
    this.closed = true;
    this.pending.length = 0;
    this.inflight = undefined;
    this.bytes = 0;
    this.records = 0;
    this.recordBytes = 0;
  }

  private measure(event: Event): Omit<Entry<Event, Record>, "generation"> {
    const sourceRecords = this.policy.records(event);
    const records = sourceRecords === undefined ? undefined : [...sourceRecords];
    const retained = records === undefined ? event : this.policy.withRecords(event, records);
    this.assertProjection(retained, records);
    const recordCosts = (records ?? []).map((record): RecordCost => ({
      serialized: serializedBytes(record),
      retained: nonNegativeInteger(this.policy.retainedRecordBytes(record)),
    }));
    const replacementKey = this.policy.replacementKey(event);
    if (records !== undefined && replacementKey !== undefined)
      throw new Error("Record batches cannot replace unsent controls.");
    return {
      event: retained,
      records,
      recordCosts,
      recordBytes: nonNegativeInteger(
        recordCosts.reduce((total, cost) => total + cost.retained, 0),
      ),
      serializedBytes: nonNegativeInteger(
        serializedBytes(retained) +
          nonNegativeInteger(this.policy.decorationReserveBytes(retained)),
      ),
      replacementKey,
    };
  }

  private limitFailure(): DeliveryFailure | undefined {
    if (this.pending.length + Number(this.inflight !== undefined) > this.limits.maxEvents)
      return "event-limit";
    if (this.bytes > this.limits.maxSerializedBytes) return "byte-limit";
    if (this.records > this.limits.maxRecords) return "record-limit";
    if (this.limits.maxRecordBytes !== undefined && this.recordBytes > this.limits.maxRecordBytes)
      return "record-byte-limit";
    return undefined;
  }

  private assertProjection(event: Event, records: readonly Record[] | undefined): void {
    const projected = this.policy.records(event);
    if (
      records === undefined
        ? projected !== undefined
        : projected === undefined ||
          projected.length !== records.length ||
          projected.some((record, index) => !Object.is(record, records[index]))
    )
      throw new Error("Provider record projection changed retained records.");
  }

  private trim(incoming: Entry<Event, Record>): DeliveryFailure | undefined {
    let failure = this.limitFailure();
    if (failure === undefined || incoming.records === undefined) return failure;
    while (failure !== undefined) {
      const previous = this.pending.find(
        (candidate) =>
          candidate.records !== undefined &&
          (failure === "event-limit" ? candidate !== incoming : candidate.recordCosts.length > 0),
      );
      if (previous === undefined) return failure;
      if (failure === "event-limit") this.remove(previous, true);
      else this.trimPrefix(previous);
      failure = this.limitFailure();
    }
    return undefined;
  }

  private trimPrefix(entry: Entry<Event, Record>): void {
    const records = entry.records;
    if (records === undefined) throw new Error("Control records cannot be trimmed.");
    let count = 0;
    let removedEncoded = 0;
    let removedRetained = 0;
    for (const cost of entry.recordCosts) {
      count += 1;
      removedEncoded += cost.serialized;
      removedRetained += cost.retained;
      if (count === records.length) {
        this.remove(entry, true);
        return;
      }
      if (
        this.bytes - removedEncoded - count <= this.limits.maxSerializedBytes &&
        this.records - count <= this.limits.maxRecords &&
        (this.limits.maxRecordBytes === undefined ||
          this.recordBytes - removedRetained <= this.limits.maxRecordBytes)
      )
        break;
    }
    const dropped = nonNegativeInteger(entry.generation.dropped + count);
    const remaining = records.slice(count);
    const retained = this.policy.withRecords(entry.event, remaining);
    this.assertProjection(retained, remaining);
    entry.generation.dropped = dropped;
    entry.event = retained;
    entry.records = remaining;
    entry.recordCosts = entry.recordCosts.slice(count);
    entry.serializedBytes -= removedEncoded + count;
    entry.recordBytes -= removedRetained;
    this.bytes -= removedEncoded + count;
    this.records -= count;
    this.recordBytes -= removedRetained;
  }

  private remove(entry: Entry<Event, Record>, dropped: boolean): void {
    const index = this.pending.indexOf(entry);
    if (index < 0) return;
    if (dropped)
      entry.generation.dropped = nonNegativeInteger(
        entry.generation.dropped + entry.recordCosts.length,
      );
    this.pending.splice(index, 1);
    this.releaseCosts(entry);
  }

  private releaseCosts(entry: Entry<Event, Record>): void {
    this.bytes -= entry.serializedBytes;
    this.records -= entry.recordCosts.length;
    this.recordBytes -= entry.recordBytes;
  }
}

/** Control providers use the same bounded owner without inventing a record shape. */
export function createControlDeliveryPolicy<
  Event extends ProviderWireEvent,
>(): ProviderDeliveryPolicy<Event, never> {
  return {
    records: (): undefined => undefined,
    retainedRecordBytes: (): number => 0,
    withRecords: (event): Event => event,
    startsGeneration: (): boolean => false,
    decorateDrops: (event): Event => event,
    decorationReserveBytes: (): number => 0,
    replacementKey: (): undefined => undefined,
  };
}
