import {
  KafkaTrustRecipeError,
  type KafkaConnectionTemplateDocument,
  type KafkaLegacyTemplateSource,
} from "../../features/kafka/application";
import { parseConnectionTemplateSnapshotPayload } from "../../features/kafka/contracts/connection-template-validation";

import { readBoundedFile } from "./bounded-file";

/** Never writes or seeds the legacy catalog; each review reads the preserved source again. */
export class LegacyKafkaConnectionTemplateFile implements KafkaLegacyTemplateSource {
  constructor(
    private readonly path: string,
    private readonly maximumFileBytes = 4 * 1_048_576,
  ) {}

  async load(signal?: AbortSignal): Promise<KafkaConnectionTemplateDocument | undefined> {
    try {
      const bytes = await readBoundedFile(this.path, this.maximumFileBytes, { signal });
      const document: unknown = JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(bytes));
      if (
        document === null ||
        typeof document !== "object" ||
        Array.isArray(document) ||
        !("version" in document) ||
        document.version !== 1 ||
        !("catalogs" in document) ||
        Object.keys(document).some((key) => !["version", "catalogs"].includes(key))
      )
        throw new Error("Unsupported legacy catalog");
      const snapshot = parseConnectionTemplateSnapshotPayload(
        { catalogs: document.catalogs, store: { durability: "durable", state: "ready" } },
        "legacy",
      );
      return { catalogs: snapshot.catalogs };
    } catch (error) {
      if (error instanceof Error && error.name === "AbortError") throw error;
      if (error !== null && typeof error === "object" && "code" in error && error.code === "ENOENT")
        return undefined;
      throw new KafkaTrustRecipeError(
        "TEMPLATE_CORRUPT",
        "The legacy catalog is unreadable or unsupported.",
        "Preserve the legacy file and restore a valid backup before converting it.",
      );
    }
  }
}
