import type { RecordDecodeInput, RecordDecodeResult } from "../contracts/record-codec";
import type { SchemaDefinitionInput } from "../contracts/schema-registry-types";

import type { KafkaClusterServiceContext } from "./types";

export interface RegisteredSchema extends SchemaDefinitionInput {
  readonly id: number;
}
export interface SchemaLookupPort {
  byId(
    context: KafkaClusterServiceContext,
    id: number,
    signal: AbortSignal,
  ): Promise<RegisteredSchema>;
  byVersion(
    context: KafkaClusterServiceContext,
    subject: string,
    version: number,
    signal: AbortSignal,
  ): Promise<RegisteredSchema>;
}

// Dependencies precede their dependents. Names come only from declared Registry references.
export interface CodecSchemaBundle {
  readonly root: RegisteredSchema;
  readonly dependencies: readonly { readonly name: string; readonly schema: RegisteredSchema }[];
}
export interface RecordCodecPort {
  decode(
    input: RecordDecodeInput,
    bundle: CodecSchemaBundle | null,
    signal: AbortSignal,
  ): Promise<RecordDecodeResult>;
}
export interface RecordCodecWorkerInput {
  readonly input: RecordDecodeInput;
  readonly bundle: CodecSchemaBundle | null;
}

export interface SchemaSamplePort {
  generate(
    input: import("../contracts/schema-samples").SchemaSampleInput,
    bundle: CodecSchemaBundle,
    signal: AbortSignal,
  ): Promise<import("../contracts/schema-samples").SchemaSamples>;
}

export interface SchemaClientPort {
  generateClient(
    input: import("../contracts/schema-inspection").SchemaInspectionInput,
    bundle: CodecSchemaBundle,
    signal: AbortSignal,
  ): Promise<import("../contracts/schema-client").SchemaClient>;
}
