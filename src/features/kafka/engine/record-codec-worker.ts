import { parentPort, workerData } from "node:worker_threads";

import type { RecordCodecWorkerInput } from "../application/record-codec-types";

import { parseStructuredRecord } from "./record-codec-parser";

parentPort?.postMessage(parseStructuredRecord(workerData as RecordCodecWorkerInput));
