import {
  TRUST_RECIPE_LIMITS,
  parseTrustAcquisitionRecipeDocument,
  parseTrustRecipeJson,
  type ConnectionTemplateStoreCapability,
  type TrustAcquisitionRecipeDocument,
} from "../../features/kafka/contracts";
import {
  KafkaTrustRecipeError,
  type KafkaTrustRecipeStore,
} from "../../features/kafka/application";

import {
  createAtomicPrivateFileTempId,
  writeAtomicPrivateTextFile,
} from "./atomic-private-text-file";
import { readBoundedFile } from "./bounded-file";

export class AtomicKafkaTrustRecipeFileStore implements KafkaTrustRecipeStore {
  private unavailable = false;

  constructor(private readonly path: string) {}

  capability(): ConnectionTemplateStoreCapability {
    return this.unavailable
      ? {
          durability: "durable",
          state: "unavailable",
          recovery: "Preserve the recipe file and restore a valid backup before restarting.",
        }
      : { durability: "durable", state: "ready" };
  }

  async load(signal?: AbortSignal): Promise<TrustAcquisitionRecipeDocument | undefined> {
    signal?.throwIfAborted();
    try {
      const buffer = await readBoundedFile(this.path, TRUST_RECIPE_LIMITS.documentBytes, {
        signal,
      });
      const contents = new TextDecoder("utf-8", { fatal: true }).decode(buffer);
      return parseTrustAcquisitionRecipeDocument(
        parseTrustRecipeJson(contents, TRUST_RECIPE_LIMITS.documentBytes),
      );
    } catch (error) {
      if (error instanceof Error && error.name === "AbortError") throw error;
      if (error !== null && typeof error === "object" && "code" in error && error.code === "ENOENT")
        return undefined;
      this.unavailable = true;
      throw new KafkaTrustRecipeError(
        "TEMPLATE_CORRUPT",
        "Trust acquisition template storage is unreadable or unsupported.",
        "Preserve its contents and restore a valid backup before restarting.",
      );
    }
  }

  async commit(document: TrustAcquisitionRecipeDocument, signal?: AbortSignal): Promise<void> {
    signal?.throwIfAborted();
    if (this.unavailable)
      throw new KafkaTrustRecipeError(
        "TEMPLATE_STORE_UNAVAILABLE",
        "Template storage is unavailable.",
        "Preserve the original file and restore a valid backup before restarting.",
      );
    const validated = parseTrustAcquisitionRecipeDocument(document);
    try {
      await writeAtomicPrivateTextFile({
        path: this.path,
        contents: JSON.stringify(validated),
        createTempId: createAtomicPrivateFileTempId,
        ...(signal === undefined ? {} : { signal }),
      });
    } catch (error) {
      if (error instanceof Error && error.name === "AbortError") throw error;
      throw new KafkaTrustRecipeError(
        "TEMPLATE_STORE_UNAVAILABLE",
        "Trust acquisition templates could not be committed.",
        "Check application-data permissions and retry; the previous committed data remains authoritative.",
      );
    }
  }
}
