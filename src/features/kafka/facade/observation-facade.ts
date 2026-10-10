import {
  HOST_PROTOCOL_VERSION,
  type HostCommandResponse,
  type HostCommand,
  type HostEvent,
} from "../contracts";
import type { ObservationCommand } from "../contracts/observation-protocol";
import { observationIdentity, observationLag } from "../contracts/observations";
import { ObservationWatch } from "../application/observation-watch";
import type { ObservationWatchSnapshot } from "../contracts/observation-watch";
import type { ObservationCapture, ObservationInput } from "../contracts/observations";
import { ObservationService } from "../application/observation-service";
import type { ObservationStore } from "../application/observation-store";
import type { KafkaApplicationSession } from "../application";
import { ObservationOperationError } from "../application/observation-errors";

import { failureResponse, translateFacadeFailure, type ActivityInput } from "./facade-support";
export class ObservationFacade {
  private readonly service: ObservationService;
  private readonly watch: ObservationWatch;
  private readonly alerts = new Map<string, string>();
  constructor(
    session: Pick<KafkaApplicationSession, "observationScope">,
    store: ObservationStore | undefined,
    private readonly activity: (input: ActivityInput) => void,
    events?: { readonly publish: (event: HostEvent) => void; readonly nextSequence: () => number },
  ) {
    this.service = new ObservationService(() => session.observationScope(), store);
    this.watch = new ObservationWatch(
      this.service,
      () => session.observationScope(),
      (payload: ObservationWatchSnapshot): void => {
        events?.publish({
          event: "observations.watch.changed",
          payload,
          version: HOST_PROTOCOL_VERSION,
          sequence: events.nextSequence(),
        });
      },
      (capture, input): void => this.recordCapture(capture, input, crypto.randomUUID()),
    );
  }
  cancel(): void {
    this.watch.invalidate();
  }
  idle(): Promise<void> {
    return this.watch.idle();
  }
  async execute(command: ObservationCommand, correlationId: string): Promise<HostCommandResponse> {
    try {
      const base = { id: command.id, version: HOST_PROTOCOL_VERSION, ok: true as const };
      switch (command.command) {
        case "observations.watch.status":
          return {
            ...base,
            command: command.command,
            result: { correlationId, watch: this.watch.snapshot() },
          };
        case "observations.watch.start":
          return {
            ...base,
            command: command.command,
            result: { correlationId, watch: await this.watch.start(command.payload) },
          };
        case "observations.watch.stop":
          return {
            ...base,
            command: command.command,
            result: { correlationId, watch: await this.watch.stop() },
          };
        case "observations.capture": {
          const capture = await this.watch.capture(command.payload);
          this.recordCapture(capture, command.payload, correlationId);
          return { ...base, command: command.command, result: { correlationId, capture } };
        }
        case "observations.history":
          return {
            ...base,
            command: command.command,
            result: { correlationId, snapshot: await this.service.history() },
          };
        case "observations.clear":
          await this.watch.stop();
          this.watch.forget();
          this.alerts.clear();
          return {
            ...base,
            command: command.command,
            result: { correlationId, snapshot: await this.service.clear() },
          };
        case "observations.cancel":
          await this.watch.stop();
          return { ...base, command: command.command, result: { correlationId } };
      }
    } catch (error) {
      const translated = translateFacadeFailure(
        error,
        { activeStateChanged: false, connection: undefined, correlationId },
        true,
      );
      return failureResponse(command, {
        ...translated.error,
        ...(error instanceof ObservationOperationError && error.retryAfterMs !== undefined
          ? { retryAfterMs: error.retryAfterMs }
          : {}),
      });
    }
  }
  private recordCapture(
    capture: ObservationCapture,
    input: ObservationInput,
    correlationId: string,
  ): void {
    const sample = capture.series.samples.at(-1)!;
    const key = observationIdentity(capture.series),
      alert = sample.alerts.map((v) => v.metric + ":" + String(v.threshold)).join(",");
    if (
      sample.alerts.some(
        (a) =>
          !(this.alerts.get(key) ?? "").split(",").includes(a.metric + ":" + String(a.threshold)),
      )
    )
      this.activity({
        correlationId,
        operation: "Observation threshold",
        object: capture.series.topic,
        detail:
          "A configured local observation threshold was exceeded. Inspect the sample source, time and coverage in Observed health.",
        outcome: "succeeded",
        severity: "warning",
      });
    const retainedLag =
      observationLag(sample) === null && input.thresholds.lag !== null
        ? (this.alerts.get(key) ?? "")
            .split(",")
            .filter((v) => v.startsWith("lag:"))
            .join(",")
        : "";
    this.alerts.set(key, [alert, retainedLag].filter(Boolean).join(","));
    if (this.alerts.size > 8) this.alerts.delete(this.alerts.keys().next().value!);
  }
}

/** HostCommand is already closed by the paired protocol parser. */
export function isObservationCommand(command: HostCommand): command is ObservationCommand {
  return command.command.startsWith("observations.");
}
