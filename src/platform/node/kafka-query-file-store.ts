import {
  KAFKA_QUERY_LIBRARY_LIMITS,
  parseKafkaQueryLibraryDocument,
  type KafkaSavedQuery,
} from "../../features/kafka/contracts";
import { KafkaQueryLibraryError, type KafkaQueryStore } from "../../features/kafka/application";

import { readBoundedFile } from "./bounded-file";
import {
  createAtomicPrivateFileTempId,
  writeAtomicPrivateTextFile,
} from "./atomic-private-text-file";

export class AtomicKafkaQueryFileStore implements KafkaQueryStore {
  readonly durability = "durable" as const;
  constructor(private readonly path: string) {}

  async load(): Promise<readonly KafkaSavedQuery[]> {
    try {
      const bytes = await readBoundedFile(this.path, KAFKA_QUERY_LIBRARY_LIMITS.fileBytes);
      return parseKafkaQueryLibraryDocument(JSON.parse(bytes.toString("utf8")) as unknown).queries;
    } catch (error) {
      if (error !== null && typeof error === "object" && "code" in error && error.code === "ENOENT")
        return [];
      throw new KafkaQueryLibraryError(
        "Saved-query storage is unreadable or uses an unsupported schema. The file was not replaced.",
      );
    }
  }

  async commit(queries: readonly KafkaSavedQuery[]): Promise<void> {
    const contents =
      JSON.stringify(parseKafkaQueryLibraryDocument({ schemaVersion: 1, queries })) + "\n";
    if (Buffer.byteLength(contents, "utf8") > KAFKA_QUERY_LIBRARY_LIMITS.fileBytes)
      throw new KafkaQueryLibraryError("The saved-query library exceeds its 1 MiB storage limit.");
    try {
      await writeAtomicPrivateTextFile({
        path: this.path,
        contents,
        createTempId: createAtomicPrivateFileTempId,
      });
    } catch {
      throw new KafkaQueryLibraryError(
        "Saved-query storage could not commit the change. Check application-data permissions and retry.",
      );
    }
  }
}
