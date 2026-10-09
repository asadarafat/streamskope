import type { KafkaRecordProtection } from "../../features/kafka/contracts";
import type { RecordCodecPreferences } from "../../features/kafka/contracts/structured-record";
import { RecordCodecService } from "../../features/kafka/application/record-codec-service";
import type {
  RecordCodecPort,
  SchemaLookupPort,
} from "../../features/kafka/application/record-codec-types";
import { StructuredRecordService } from "../../features/kafka/application/structured-record-service";
import { protectKafkaRecord } from "../../features/kafka/application/record-protection";
import { NodeBoundedJsonHttp } from "../../features/kafka/engine/bounded-json-http";
import { SchemaRegistryHttpAdapter } from "../../features/kafka/engine/schema-registry-http";
import type { StreamSkopeKafkaEngineOptions } from "../../features/kafka/engine/types";

import { createHostRecordCodec } from "./record-codec";

/** All host entry points prepare and protect records before any content predicate. */
export function createHostRecordPipeline(
  settings: () => {
    readonly codecs: RecordCodecPreferences;
    readonly protection: KafkaRecordProtection;
  },
  ports?: { readonly codec: RecordCodecPort; readonly lookup: SchemaLookupPort },
): Pick<StreamSkopeKafkaEngineOptions, "prepareRecord" | "protectRecord"> {
  const service = new StructuredRecordService(
    new RecordCodecService(
      ports?.lookup ?? new SchemaRegistryHttpAdapter(new NodeBoundedJsonHttp()),
      ports?.codec ?? createHostRecordCodec(),
    ),
  );
  return {
    prepareRecord: (message, context, signal) =>
      service.prepare(message, settings().codecs, context, signal),
    protectRecord: (message) => protectKafkaRecord(message, settings().protection),
  };
}
