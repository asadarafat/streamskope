import { parentPort, workerData } from "node:worker_threads";

import type { RecordCodecWorkerInput } from "../application/record-codec-types";

import { parseStructuredRecord } from "./record-codec-parser";
import { generateSchemaSamples, type SchemaSampleWorkerInput } from "./schema-sample-parser";

const request = workerData as RecordCodecWorkerInput | SchemaSampleWorkerInput;
parentPort?.postMessage(
  "kind" in request && request.kind === "generate"
    ? generateSchemaSamples(request)
    : parseStructuredRecord(request as RecordCodecWorkerInput),
);
