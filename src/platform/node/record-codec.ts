import { existsSync } from "node:fs";
import { join, resolve } from "node:path";

import { BoundedRecordCodec } from "../../features/kafka/engine/record-codec";

export function createHostRecordCodec(): BoundedRecordCodec {
  const built = join(__dirname, "record-codec-worker.cjs");
  return existsSync(built)
    ? new BoundedRecordCodec({ script: built, execArgv: [] })
    : new BoundedRecordCodec({
        script: resolve(process.cwd(), "src/features/kafka/engine/record-codec-worker.ts"),
        execArgv: ["--import", "tsx"],
      });
}
