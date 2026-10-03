import { HOST_PROTOCOL_VERSION, type HostCommand, type HostCommandResponse } from "../contracts";
import { RecordCodecService } from "../application/record-codec-service";
import type { RecordCodecPort, SchemaLookupPort } from "../application/record-codec-types";
import type { KafkaApplicationSession } from "../application";

export class RecordCodecFacade {
  private readonly service: RecordCodecService | undefined;
  private readonly pending = new Set<AbortController>();
  constructor(
    private readonly session: KafkaApplicationSession,
    codec?: RecordCodecPort,
    lookup?: SchemaLookupPort,
  ) {
    if (codec && lookup) this.service = new RecordCodecService(lookup, codec);
  }
  invalidate(): void {
    for (const controller of this.pending) controller.abort();
    this.service?.clear();
  }
  async execute(
    command: Extract<HostCommand, { command: "records.decode" }>,
    correlationId: string,
  ): Promise<HostCommandResponse<"records.decode">> {
    const reply = (
      decoded: import("../contracts/record-codec").RecordDecodeResult,
    ): HostCommandResponse<"records.decode"> => ({
      command: command.command,
      id: command.id,
      version: HOST_PROTOCOL_VERSION,
      ok: true,
      result: { correlationId, decoded },
    });
    if (!this.service || this.pending.size >= 2 || this.session.snapshot().state !== "connected")
      return reply({
        state: "error",
        format: command.payload.format,
        code: "unavailable",
        detail: "Connect Kafka and wait for active decoding to finish before trying again.",
      });
    const controller = new AbortController();
    this.pending.add(controller);
    const signal = AbortSignal.any([controller.signal, AbortSignal.timeout(15_000)]);
    try {
      return reply(
        await this.service.decode(
          command.payload,
          this.session.clusterServiceContext("schemaRegistry"),
          signal,
        ),
      );
    } finally {
      this.pending.delete(controller);
    }
  }
}
