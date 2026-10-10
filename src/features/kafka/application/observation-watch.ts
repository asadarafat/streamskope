import {
  emptyObservationWatch,
  type ObservationWatchSnapshot,
} from "../contracts/observation-watch";
import { parseObservationInput } from "../contracts/observation-validation";
import {
  OBSERVATION_LIMITS,
  observationIdentity,
  type ObservationInput,
  type ObservationCapture,
} from "../contracts/observations";

import type { ObservationScope } from "./connection-scope";
import { ObservationOperationError, observationIssue } from "./observation-errors";
import { ObservationService } from "./observation-service";

interface WatchOwner {
  readonly scope: ObservationScope;
  readonly input: ObservationInput;
  revoked: boolean;
  identity?: { readonly clusterId: string; readonly topicId: string };
}

/** One opted-in host watch, one original scope and at most one next-attempt timeout. */
export class ObservationWatch {
  private value = emptyObservationWatch();
  private owner: WatchOwner | undefined;
  private lastScope: ObservationScope | undefined;
  private operation: Promise<void> | undefined;
  private manualOperation: Promise<ObservationCapture> | undefined;
  private manual: { revoked: boolean } | undefined;
  private timer: ReturnType<typeof setTimeout> | undefined;

  constructor(
    private readonly service: ObservationService,
    private readonly scope: () => ObservationScope | null,
    private readonly changed: (snapshot: ObservationWatchSnapshot) => void = () => undefined,
    private readonly captured: (
      capture: ObservationCapture,
      input: ObservationInput,
    ) => void = () => undefined,
    private readonly now = Date.now,
  ) {}

  get active(): boolean {
    return (
      this.owner !== undefined ||
      this.manual !== undefined ||
      this.operation !== undefined ||
      this.service.busy
    );
  }

  snapshot(): ObservationWatchSnapshot {
    let current = false;
    try {
      this.lastScope?.assertCurrent(new AbortController().signal);
      current = this.lastScope !== undefined && this.value.lastSampleId !== null;
    } catch {
      /* A revoked original scope cannot qualify current evidence. */
    }
    return structuredClone({ ...this.value, current });
  }

  async start(request: ObservationInput): Promise<ObservationWatchSnapshot> {
    this.assertIdle();
    const input = parseObservationInput(request);
    const scope = this.scope();
    if (!scope)
      throw new ObservationOperationError(
        "OBSERVATION_DISCONNECTED",
        "No Kafka connection is active.",
        "Connect a profile, then explicitly start observing.",
        true,
      );
    scope.assertCurrent(new AbortController().signal);
    if (!scope.observeTopicHealth)
      throw new ObservationOperationError(
        "UNSUPPORTED_OPERATION",
        "This adapter cannot collect observations.",
        "Use a supported Kafka connection.",
        false,
      );
    // A new explicitly authorized watch starts a new continuity segment.
    this.service.cancel();
    this.lastScope = undefined;
    const owner: WatchOwner = { scope, input, revoked: false };
    this.owner = owner;
    this.value = {
      ...emptyObservationWatch(),
      revision: this.value.revision,
      id: crypto.randomUUID(),
      repeated: true,
      input,
      connectionName: scope.connectionName,
    };
    await this.collect(owner);
    return this.snapshot();
  }

  async capture(request: ObservationInput): Promise<ObservationCapture> {
    this.assertIdle();
    const input = parseObservationInput(request);
    const scope = this.scope();
    this.lastScope = undefined;
    const admission = { revoked: false };
    this.manual = admission;
    const operation = this.service
      .captureOwned(input, scope)
      .then((capture): ObservationCapture => {
        this.lastScope = admission.revoked ? undefined : (scope ?? undefined);
        this.retain(capture, input, scope?.connectionName ?? null);
        this.update({
          phase: admission.revoked ? "stopping" : "stopped",
          nextCaptureAt: admission.revoked ? null : this.now() + OBSERVATION_LIMITS.intervalMs,
          error: null,
        });
        return capture;
      })
      .catch((error: unknown): never => {
        if (!admission.revoked) {
          const { code, summary, recovery, retryable } = observationIssue(error, "end-offsets");
          this.update({
            phase: "failed",
            nextCaptureAt: this.now() + OBSERVATION_LIMITS.intervalMs,
            error: { code, summary, recovery, retryable },
          });
        }
        throw error;
      })
      .finally((): void => {
        if (this.manualOperation === operation) this.manualOperation = undefined;
        if (this.manual === admission) this.manual = undefined;
        if (this.value.phase === "stopping") this.update({ phase: "stopped", nextCaptureAt: null });
      });
    this.manualOperation = operation;
    this.value = {
      ...emptyObservationWatch(),
      revision: this.value.revision,
      id: crypto.randomUUID(),
      input,
      connectionName: scope?.connectionName ?? null,
    };
    this.update({ phase: "capturing" });
    return operation;
  }

  /** Revocation is synchronous; settlement alone releases original capture ownership. */
  invalidate(): void {
    this.revoke(true);
  }

  private revoke(clearEvidence: boolean): void {
    if (this.owner) this.owner.revoked = true;
    if (this.manual) this.manual.revoked = true;
    if (clearEvidence) this.lastScope = undefined;
    if (this.timer !== undefined) clearTimeout(this.timer);
    this.timer = undefined;
    this.service.cancel();
    if (this.operation === undefined && !this.service.busy) this.owner = undefined;
    this.update({
      phase:
        this.operation !== undefined || this.manual !== undefined || this.service.busy
          ? "stopping"
          : "stopped",
      nextCaptureAt: null,
    });
  }

  async idle(): Promise<void> {
    await Promise.allSettled([this.operation, this.manualOperation, this.service.idle()]);
    if (this.value.phase === "stopping") {
      this.owner = undefined;
      this.update({ phase: "stopped", nextCaptureAt: null });
    }
  }

  async stop(): Promise<ObservationWatchSnapshot> {
    this.revoke(false);
    await this.idle();
    return this.snapshot();
  }

  forget(): void {
    this.lastScope = undefined;
    this.update({
      lastSampleId: null,
      lastSeriesId: null,
      clusterId: null,
      topicId: null,
      error: null,
    });
  }

  private assertIdle(): void {
    if (this.active)
      throw new ObservationOperationError(
        "OBSERVATION_BUSY",
        "Observation work is already active.",
        "Stop the current watch and wait for its original reads to finish before starting other observation work.",
        true,
      );
  }

  private collect(owner: WatchOwner): Promise<void> {
    const operation = Promise.resolve()
      .then(async (): Promise<void> => {
        if (owner.revoked || this.owner !== owner) return;
        const capture = await this.service.captureOwned(owner.input, owner.scope, owner.identity);
        if (owner.revoked || this.owner !== owner) return;
        owner.identity = { clusterId: capture.series.clusterId, topicId: capture.series.topicId };
        this.lastScope = owner.scope;
        this.retain(capture, owner.input, owner.scope.connectionName);
        try {
          this.captured(capture, owner.input);
        } catch {
          /* Delivery does not own collection. */
        }
      })
      .catch((error: unknown): void => {
        if (owner.revoked || this.owner !== owner) return;
        const { code, summary, recovery, retryable } = observationIssue(error, "end-offsets");
        this.lastScope = undefined;
        owner.revoked = true;
        this.update({
          phase: "failed",
          nextCaptureAt: this.now() + OBSERVATION_LIMITS.intervalMs,
          error: { code, summary, recovery, retryable },
        });
      })
      .finally((): void => {
        if (this.operation === operation) this.operation = undefined;
        if (this.owner !== owner) return;
        if (owner.revoked) {
          this.owner = undefined;
          if (this.value.phase === "stopping")
            this.update({ phase: "stopped", nextCaptureAt: null });
          return;
        }
        const nextCaptureAt = this.now() + OBSERVATION_LIMITS.intervalMs;
        this.timer = setTimeout((): void => {
          this.timer = undefined;
          if (!owner.revoked && this.owner === owner) void this.collect(owner);
        }, OBSERVATION_LIMITS.intervalMs);
        this.update({ phase: "waiting", nextCaptureAt });
      });
    this.operation = operation;
    this.update({ phase: "capturing", nextCaptureAt: null });
    return operation;
  }

  private retain(
    capture: ObservationCapture,
    input: ObservationInput,
    connectionName: string | null,
  ): void {
    this.value = {
      ...this.value,
      input: parseObservationInput(input),
      connectionName,
      clusterId: capture.series.clusterId,
      topicId: capture.series.topicId,
      lastSeriesId: observationIdentity(capture.series),
      lastSampleId: capture.series.samples.at(-1)!.id,
    };
  }

  private update(change: Partial<ObservationWatchSnapshot>): void {
    this.value = { ...this.value, ...change, revision: this.value.revision + 1 };
    try {
      this.changed(this.snapshot());
    } catch {
      /* An observer cannot release or cancel authority. */
    }
  }
}
