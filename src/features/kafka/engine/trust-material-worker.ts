import { parentPort, workerData } from "node:worker_threads";

import type { KafkaProfileTrustDecoderInput } from "../application";

import type { TrustMaterialWorkerReply } from "./trust-material";
import { parseTrustMaterial } from "./trust-material-parser";
import { KafkaTrustMaterialError, KafkaTruststorePasswordError } from "./trust-material-shared";

function complete(reply: TrustMaterialWorkerReply): void {
  parentPort?.postMessage(reply);
}

try {
  complete({
    ok: true,
    result: parseTrustMaterial(workerData as KafkaProfileTrustDecoderInput),
  });
} catch (error) {
  complete({
    code: error instanceof KafkaTruststorePasswordError ? "TRUSTSTORE_PASSWORD" : "TRUST_MATERIAL",
    ok: false,
  });
  if (!(
    error instanceof KafkaTrustMaterialError || error instanceof KafkaTruststorePasswordError
  )) {
    process.exitCode = 1;
  }
}
