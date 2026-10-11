import {
  EXTERNAL_METRIC_LIMITS,
  type ExternalMetricsInput,
  type ExternalMetricsSnapshot,
} from "../contracts/external-metrics";
import {
  parseExternalMetricsInput,
  parseExternalMetricsSnapshot,
} from "../contracts/external-metrics-validation";

import type { MetricsReadScope } from "./connection-scope";
import type { KafkaClusterServiceContext } from "./types";
import { ObservationOperationError } from "./observation-errors";

export interface ExternalMetricsPort {
  capture(
    context: KafkaClusterServiceContext,
    input: ExternalMetricsInput,
    signal: AbortSignal,
  ): Promise<ExternalMetricsSnapshot>;
}

/** One manual read retains its original service authority through actual HTTP settlement. */
export class ExternalMetricsService {
  private original: MetricsReadScope | undefined;
  private controller: AbortController | undefined;
  private operation: Promise<ExternalMetricsSnapshot> | undefined;
  constructor(
    private readonly scope: () => MetricsReadScope | null,
    private readonly port: ExternalMetricsPort | undefined,
  ) {}
  cancel(): void {
    this.controller?.abort();
  }
  idle(): Promise<void> {
    return Promise.allSettled([this.operation]).then(() => {
      if (this.original?.cleanupUnresolved()) throw this.cleanupFailure();
    });
  }
  private cleanupFailure(): ObservationOperationError {
    return new ObservationOperationError(
      "OBSERVATION_BUSY",
      "The original metrics HTTP cleanup is unresolved.",
      "Keep the original connection and inspect host diagnostics. Retry cancellation after its actual request/socket close is confirmed; a deadline cannot release ownership.",
      true,
      { stage: "backend" },
    );
  }
  capture(value: ExternalMetricsInput): Promise<ExternalMetricsSnapshot> {
    if (this.operation)
      return Promise.reject(
        new ObservationOperationError(
          "OBSERVATION_BUSY",
          "A metrics read is already running.",
          "Wait for original request cleanup or cancel the read.",
          true,
          { stage: "backend" },
        ),
      );
    if (this.original?.cleanupUnresolved()) return Promise.reject(this.cleanupFailure());
    const input = parseExternalMetricsInput(value);
    const owner = this.scope();
    if (!owner || !owner.isCurrent() || !this.port)
      return Promise.reject(
        new ObservationOperationError(
          "UNSUPPORTED_OPERATION",
          "Prometheus metrics are unavailable for this connection.",
          "Configure the optional Prometheus endpoint in the Kafka profile, connect it, then select its exact job and instance.",
          false,
          { stage: "backend" },
        ),
      );
    this.original = owner;
    this.controller = new AbortController();
    const signal = AbortSignal.any([
      this.controller.signal,
      AbortSignal.timeout(EXTERNAL_METRIC_LIMITS.deadlineMs),
    ]);
    const port = this.port;
    this.operation = owner
      .read((context, combined) => {
        if (context.baseUrl !== input.endpoint)
          throw new ObservationOperationError(
            "VALIDATION",
            "The profile metrics URL differs from the connected service.",
            "Reconnect the edited profile before reading its selected exporter.",
            false,
            { stage: "validation" },
          );
        return port.capture(context, input, combined);
      }, signal)
      .then((snapshot) => {
        signal.throwIfAborted();
        if (!owner.isCurrent())
          throw new ObservationOperationError(
            "CANCELLED",
            "The original metrics connection changed.",
            "Reconnect and explicitly read the selected exporter again.",
            true,
            { stage: "backend" },
          );
        const parsed = parseExternalMetricsSnapshot(snapshot);
        if (
          parsed.endpoint !== input.endpoint ||
          parsed.job !== input.job ||
          parsed.instance !== input.instance
        )
          throw new ObservationOperationError(
            "OBSERVATION_INCOMPLETE",
            "The metrics response differs from the requested exporter.",
            "Verify the configured source and select its exact labels again.",
            false,
            { stage: "backend" },
          );
        return parsed;
      })
      .finally(() => {
        this.operation = undefined;
        this.controller = undefined;
        if (!owner.cleanupUnresolved()) this.original = undefined;
      });
    return this.operation;
  }
}
