import { HOST_PROTOCOL_VERSION, type HostCommand, type HostCommandResponse } from "../contracts";
import { CORRELATION_TRACE_LIMITS } from "../contracts/correlation-trace";
import { traceCorrelation } from "../application/correlation-trace-service";
import { RecordCodecService } from "../application/record-codec-service";
import type { RecordCodecPort, SchemaLookupPort } from "../application/record-codec-types";
import type { KafkaApplicationSession } from "../application";

import { failureResponse, successResponse } from "./facade-support";

type Command = Extract<HostCommand, { command: "records.trace" | "records.trace.cancel" }>;
export class CorrelationTraceFacade {
  private readonly codec?: RecordCodecService;
  private active: { id: string; controller: AbortController } | undefined;
  constructor(
    private readonly session: KafkaApplicationSession,
    codec?: RecordCodecPort,
    lookup?: SchemaLookupPort,
  ) {
    if (codec && lookup) this.codec = new RecordCodecService(lookup, codec);
  }
  invalidate(): void {
    this.active?.controller.abort();
    this.codec?.clear();
  }
  async execute(command: Command, correlationId: string): Promise<HostCommandResponse> {
    if (command.command === "records.trace.cancel") {
      if (this.active?.id === command.payload.traceId) this.active.controller.abort();
      return successResponse(command, correlationId);
    }
    const context = this.session.writeContext();
    if (!context || this.active)
      return failureResponse(command, {
        code: "VALIDATION",
        stage: "kafka",
        correlationId,
        retryable: false,
        activeStateChanged: false,
        summary: "Connect Kafka and finish or cancel the active trace.",
        recovery:
          "Only one correlation trace can run per connection. It does not stop the message reader.",
      });
    const controller = new AbortController();
    this.active = { id: command.payload.traceId, controller };
    try {
      const signal = AbortSignal.any([
        controller.signal,
        AbortSignal.timeout(CORRELATION_TRACE_LIMITS.durationMs),
      ]);
      const trace = await traceCorrelation(
        context.connection,
        context.connectionName,
        command.payload,
        signal,
        this.codec,
      );
      return {
        command: command.command,
        id: command.id,
        version: HOST_PROTOCOL_VERSION,
        ok: true,
        result: { correlationId, trace },
      };
    } finally {
      this.active = undefined;
    }
  }
}
