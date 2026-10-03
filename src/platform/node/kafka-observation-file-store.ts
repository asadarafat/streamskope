import type { ObservationStore } from "../../features/kafka/application/observation-store";
import {
  OBSERVATION_LIMITS,
  type ObservationHistory,
} from "../../features/kafka/contracts/observations";
import { parseObservationHistory } from "../../features/kafka/contracts/observation-validation";

import { readBoundedFile } from "./bounded-file";
import {
  createAtomicPrivateFileTempId,
  writeAtomicPrivateTextFile,
} from "./atomic-private-text-file";
export class AtomicObservationFileStore implements ObservationStore {
  readonly durability = "durable" as const;
  constructor(private readonly path: string) {}
  async load(): Promise<ObservationHistory> {
    try {
      return parseObservationHistory(
        JSON.parse(
          (await readBoundedFile(this.path, OBSERVATION_LIMITS.fileBytes)).toString("utf8"),
        ) as unknown,
      );
    } catch (error) {
      if (error !== null && typeof error === "object" && "code" in error && error.code === "ENOENT")
        return { schemaVersion: 1, series: [] };
      throw new Error(
        "Observation history is unreadable or unsupported. It has not been replaced.",
        { cause: error },
      );
    }
  }
  async commit(history: ObservationHistory): Promise<void> {
    const contents = JSON.stringify(parseObservationHistory(history)) + "\n";
    if (Buffer.byteLength(contents) > OBSERVATION_LIMITS.fileBytes)
      throw new Error("Observation history exceeds its file limit.");
    await writeAtomicPrivateTextFile({
      path: this.path,
      contents,
      createTempId: createAtomicPrivateFileTempId,
    });
  }
}
