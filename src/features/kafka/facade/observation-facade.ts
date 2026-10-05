import { HOST_PROTOCOL_VERSION, type HostCommandResponse } from "../contracts";
import type { ObservationCommand } from "../contracts/observation-protocol";
import { observationIdentity, observationLag } from "../contracts/observations";
import { ObservationService } from "../application/observation-service";
import type { ObservationStore } from "../application/observation-store";
import type { KafkaApplicationSession } from "../application";
import { ObservationOperationError } from "../application/observation-errors";

import { failureResponse, translateFacadeFailure, type ActivityInput } from "./facade-support";
export class ObservationFacade {
  private readonly service: ObservationService;
  private readonly alerts = new Map<string, string>();
  constructor(
    session: KafkaApplicationSession,
    store: ObservationStore | undefined,
    private readonly activity: (input: ActivityInput) => void,
  ) {
    this.service = new ObservationService(() => session.observationScope(), store);
  }
  cancel(): void {
    this.service.cancel();
  }
  idle(): Promise<void> {
    return this.service.idle();
  }
  async execute(command: ObservationCommand, correlationId: string): Promise<HostCommandResponse> {
    try {
      const base = { id: command.id, version: HOST_PROTOCOL_VERSION, ok: true as const };
      switch (command.command) {
        case "observations.capture": {
          const capture = await this.service.capture(command.payload),
            sample = capture.series.samples.at(-1)!;
          const key = observationIdentity(capture.series),
            alert = sample.alerts.map((v) => v.metric + ":" + String(v.threshold)).join(",");
          if (
            sample.alerts.some(
              (a) =>
                !(this.alerts.get(key) ?? "")
                  .split(",")
                  .includes(a.metric + ":" + String(a.threshold)),
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
            observationLag(sample) === null && command.payload.thresholds.lag !== null
              ? (this.alerts.get(key) ?? "")
                  .split(",")
                  .filter((v) => v.startsWith("lag:"))
                  .join(",")
              : "";
          this.alerts.set(key, [alert, retainedLag].filter(Boolean).join(","));
          if (this.alerts.size > 8) this.alerts.delete(this.alerts.keys().next().value!);
          return { ...base, command: command.command, result: { correlationId, capture } };
        }
        case "observations.history":
          return {
            ...base,
            command: command.command,
            result: { correlationId, snapshot: await this.service.history() },
          };
        case "observations.clear":
          this.alerts.clear();
          return {
            ...base,
            command: command.command,
            result: { correlationId, snapshot: await this.service.clear() },
          };
        case "observations.cancel":
          this.service.cancel();
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
}
