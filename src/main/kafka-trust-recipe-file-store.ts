import { open } from "node:fs/promises";

import {
  TRUST_RECIPE_LIMITS,
  parseTrustAcquisitionRecipeDocument,
  parseTrustRecipeJson,
  type ConnectionTemplateStoreCapability,
  type TrustAcquisitionRecipeDocument,
} from "../kafka/contracts";
import { KafkaTrustRecipeError, type KafkaTrustRecipeStore } from "../kafka/application";

import {
  createAtomicPrivateFileTempId,
  writeAtomicPrivateTextFile,
} from "./atomic-private-text-file";

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
      const file = await open(this.path, "r");
      try {
        const info = await file.stat();
        if (!info.isFile() || info.size > TRUST_RECIPE_LIMITS.documentBytes)
          throw new Error("Recipe file bound");
        const buffer = Buffer.alloc(Math.min(info.size + 1, TRUST_RECIPE_LIMITS.documentBytes + 1));
        let offset = 0;
        while (offset < buffer.length) {
          signal?.throwIfAborted();
          const { bytesRead } = await file.read(buffer, offset, buffer.length - offset, null);
          if (bytesRead === 0) break;
          offset += bytesRead;
        }
        if (offset > info.size) throw new Error("Recipe file changed during read");
        const contents = new TextDecoder("utf-8", { fatal: true }).decode(
          buffer.subarray(0, offset),
        );
        return parseTrustAcquisitionRecipeDocument(
          parseTrustRecipeJson(contents, TRUST_RECIPE_LIMITS.documentBytes),
        );
      } finally {
        await file.close();
      }
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
