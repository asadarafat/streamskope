import { Worker } from "node:worker_threads";

import type { SchemaInspectionInput } from "../contracts/schema-inspection";
import {
  parseSchemaAuthoringResult,
  type SchemaAuthoringInput,
  type SchemaAuthoringResult,
} from "../contracts/schema-authoring";
import { parseSchemaClient, type SchemaClient } from "../contracts/schema-client";
import {
  RECORD_CODEC_LIMITS,
  parseRecordDecodeResult,
  type RecordDecodeInput,
  type RecordDecodeResult,
} from "../contracts/record-codec";
import {
  parseSchemaSamples,
  type SchemaSampleInput,
  type SchemaSamples,
} from "../contracts/schema-samples";
import type {
  CodecSchemaBundle,
  RecordCodecPort,
  SchemaSamplePort,
  SchemaAuthoringPort,
  RecordCodecWorkerInput,
} from "../application/record-codec-types";

import type { SchemaClientWorkerInput } from "./schema-client-generator";
import type { SchemaSampleWorkerInput, SchemaSampleWorkerResult } from "./schema-sample-parser";
import type { SchemaAuthoringWorkerInput } from "./schema-record-encoder";

export class BoundedRecordCodec implements RecordCodecPort, SchemaSamplePort, SchemaAuthoringPort {
  private active = 0;
  constructor(private readonly worker: { script: string; execArgv: readonly string[] }) {}

  decode(
    input: RecordDecodeInput,
    bundle: CodecSchemaBundle | null,
    signal: AbortSignal,
  ): Promise<RecordDecodeResult> {
    return this.run({ input, bundle }, signal, parseRecordDecodeResult);
  }

  generate(
    input: SchemaSampleInput,
    bundle: CodecSchemaBundle,
    signal: AbortSignal,
  ): Promise<SchemaSamples> {
    return this.run({ kind: "generate", input, bundle }, signal, (value: unknown) => {
      if (!value || typeof value !== "object" || !("ok" in value))
        throw new Error("Invalid generator response.");
      const response = value as SchemaSampleWorkerResult;
      if (!response.ok)
        throw new Error(
          typeof response.detail === "string"
            ? response.detail.slice(0, 256)
            : "Generation failed.",
        );
      return parseSchemaSamples(response.samples);
    });
  }

  generateClient(
    input: SchemaInspectionInput,
    bundle: CodecSchemaBundle,
    signal: AbortSignal,
  ): Promise<SchemaClient> {
    return this.run({ kind: "client", input, bundle }, signal, parseSchemaClient);
  }
  author(
    input: SchemaAuthoringInput,
    bundle: CodecSchemaBundle,
    signal: AbortSignal,
  ): Promise<SchemaAuthoringResult> {
    return this.run({ kind: "author", input, bundle }, signal, parseSchemaAuthoringResult);
  }
  private run<T>(
    data:
      | RecordCodecWorkerInput
      | SchemaSampleWorkerInput
      | SchemaClientWorkerInput
      | SchemaAuthoringWorkerInput,
    signal: AbortSignal,
    parse: (value: unknown) => T,
  ): Promise<T> {
    signal.throwIfAborted();
    if (this.active >= 2) return Promise.reject(new Error("Decoder capacity reached"));
    this.active++;
    return new Promise((resolve, reject) => {
      let worker: Worker;
      try {
        worker = new Worker(this.worker.script, {
          execArgv: [...this.worker.execArgv],
          workerData: data,
          resourceLimits: {
            maxOldGenerationSizeMb: 64,
            maxYoungGenerationSizeMb: 16,
            stackSizeMb: 4,
          },
        });
      } catch (error) {
        this.active--;
        reject(error instanceof Error ? error : new Error("Decoder failed"));
        return;
      }
      let settled = false;
      const finish = (complete: () => void): void => {
        if (settled) return;
        settled = true;
        clearTimeout(timer);
        signal.removeEventListener("abort", abort);
        worker.removeAllListeners();
        void worker.terminate();
        this.active--;
        complete();
      };
      const abort = (): void => finish(() => reject(new Error("Decoder cancelled")));
      const timer = setTimeout(
        () => finish(() => reject(new Error("Decoder deadline exceeded"))),
        RECORD_CODEC_LIMITS.workerMs,
      );
      worker.once("message", (value: unknown) =>
        finish(() => {
          try {
            resolve(parse(value));
          } catch (error) {
            reject(error instanceof Error ? error : new Error("Decoder failed"));
          }
        }),
      );
      worker.once("error", () =>
        finish(() => reject(new Error("Decoder resource limit or failure"))),
      );
      worker.once("exit", () => finish(() => reject(new Error("Decoder exited without a result"))));
      signal.addEventListener("abort", abort, { once: true });
      if (signal.aborted) abort();
    });
  }
}
