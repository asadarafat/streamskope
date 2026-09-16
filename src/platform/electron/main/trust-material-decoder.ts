import { existsSync } from "node:fs";
import { join, resolve } from "node:path";

import { StreamSkopeTrustMaterialDecoder } from "../../../features/kafka/engine/trust-material";

export function createHostTrustMaterialDecoder(): StreamSkopeTrustMaterialDecoder {
  const builtWorker = join(__dirname, "trust-material-worker.cjs");
  if (existsSync(builtWorker)) {
    return new StreamSkopeTrustMaterialDecoder({
      execArgv: [],
      script: builtWorker,
    });
  }

  const sourceWorker = resolve(process.cwd(), "src/features/kafka/engine/trust-material-worker.ts");
  if (!existsSync(sourceWorker)) {
    throw new Error("The StreamSkope trust-material worker is unavailable.");
  }
  return new StreamSkopeTrustMaterialDecoder({
    execArgv: ["--import", "tsx"],
    script: sourceWorker,
  });
}
