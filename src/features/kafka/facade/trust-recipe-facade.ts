import {
  HOST_PROTOCOL_VERSION,
  exportTrustAcquisitionRecipe,
  parseTrustAcquisitionRecipeImport,
  trustRecipeComparableName,
  type HostCommand,
  type HostCommandResponse,
  type TrustRecipeReviewResult,
  type TrustAcquisitionRecipe,
} from "../contracts";
import { KafkaTrustRecipeError, type KafkaProfileService } from "../application";

import { failureResponse, successResponse, translateFacadeFailure } from "./facade-support";
import type { TemplateFacadeBindings } from "./template-facade";

type RecipeHostCommand = Extract<HostCommand, { readonly command: `recipes.${string}` }>;

export function isTrustRecipeCommand(command: HostCommand): command is RecipeHostCommand {
  return command.command.startsWith("recipes.");
}

const operations: Record<RecipeHostCommand["command"], string> = {
  "recipes.list": "Load trust acquisition templates",
  "recipes.create": "Create trust acquisition template",
  "recipes.update": "Update trust acquisition template",
  "recipes.delete": "Delete trust acquisition template",
  "recipes.usage": "Review template usage",
  "recipes.duplicate": "Duplicate trust acquisition template",
  "recipes.import.preview": "Review trust acquisition template import",
  "recipes.export": "Export trust acquisition template",
  "recipes.legacy.preview": "Review legacy trust acquisition templates",
  "recipes.legacy.convert": "Convert legacy trust acquisition template",
};

function recipeObject(recipe: TrustAcquisitionRecipe): string {
  return `${recipe.name} · ${recipe.id} · revision ${String(recipe.revision)}`;
}

export async function executeTrustRecipeCommand(
  command: RecipeHostCommand,
  correlationId: string,
  bindings: TemplateFacadeBindings & { readonly profiles: KafkaProfileService },
): Promise<HostCommandResponse> {
  const library = bindings.templates.recipes;
  const operation = operations[command.command];
  let object = "Trust acquisition templates";
  try {
    if (
      command.command === "recipes.update" ||
      command.command === "recipes.delete" ||
      command.command === "recipes.usage" ||
      command.command === "recipes.export" ||
      command.command === "recipes.duplicate"
    ) {
      object = `${command.payload.id} · revision ${String(command.payload.revision)}`;
      object = recipeObject(await library.resolve(command.payload.id, command.payload.revision));
    }
    if (command.command === "recipes.usage") {
      const usage = await bindings.profiles.recipeUsage(command.payload.id);
      return {
        command: command.command,
        id: command.id,
        version: HOST_PROTOCOL_VERSION,
        ok: true,
        result: { correlationId, usage },
      };
    }
    if (command.command === "recipes.legacy.preview") {
      const legacy = await library.previewLegacy();
      bindings.recordActivity({
        operation,
        object: "Legacy trust acquisition templates",
        correlationId,
        outcome: "succeeded",
        severity: "info",
        detail: "Legacy review prepared without conversion or external execution.",
      });
      return {
        command: command.command,
        id: command.id,
        version: HOST_PROTOCOL_VERSION,
        ok: true,
        result: { correlationId, legacy },
      };
    }
    if (command.command === "recipes.import.preview" || command.command === "recipes.export") {
      const result =
        command.command === "recipes.import.preview"
          ? { correlationId, draft: parseTrustAcquisitionRecipeImport(command.payload.contents) }
          : await (async (): Promise<TrustRecipeReviewResult> => {
              const recipe = await library.resolve(command.payload.id, command.payload.revision);
              const content = exportTrustAcquisitionRecipe(recipe);
              return {
                correlationId,
                document: {
                  content,
                  byteSize: new TextEncoder().encode(content).byteLength,
                  fileName: "trust-acquisition-template.json",
                  mediaType: "application/json" as const,
                },
                warning:
                  "Review before sharing: literal commands and URLs may contain hard-coded secrets. Parameter defaults and profile credentials are excluded.",
              };
            })();
      bindings.recordActivity({
        operation,
        object,
        correlationId,
        outcome: "succeeded",
        severity: "info",
        detail:
          "Template review prepared. No template was saved, acquired, or applied to a profile.",
      });
      return {
        command: command.command,
        id: command.id,
        version: HOST_PROTOCOL_VERSION,
        ok: true,
        result,
      };
    }
    let snapshot;
    switch (command.command) {
      case "recipes.list":
        snapshot = await library.list();
        break;
      case "recipes.create":
        snapshot = await library.create(command.payload);
        break;
      case "recipes.legacy.convert":
        snapshot = await library.convertLegacy(
          command.payload,
          undefined,
          command.payload.expectedSourceRevision,
        );
        break;
      case "recipes.update":
        snapshot = await library.update(
          command.payload.id,
          command.payload.revision,
          command.payload.recipe,
        );
        break;
      case "recipes.delete":
        snapshot = await bindings.profiles.withConfirmedRecipeUsage(
          command.payload.id,
          command.payload.confirmedProfileIds ?? [],
          () => library.delete(command.payload.id, command.payload.revision),
        );
        break;
      case "recipes.duplicate":
        snapshot = await library.duplicate(
          command.payload.id,
          command.payload.revision,
          command.payload.name,
        );
        break;
    }
    if (snapshot.store.state !== "ready")
      throw new KafkaTrustRecipeError(
        "TEMPLATE_STORE_UNAVAILABLE",
        "Trust acquisition templates are unavailable.",
        snapshot.store.recovery ?? "Check template storage and restart.",
      );
    if (command.command !== "recipes.list" && command.command !== "recipes.delete") {
      const payload = command.payload;
      const changed =
        command.command === "recipes.update"
          ? snapshot.recipes.find((recipe) => recipe.id === command.payload.id)
          : "name" in payload
            ? snapshot.recipes.find(
                (recipe) =>
                  trustRecipeComparableName(recipe.name) ===
                  trustRecipeComparableName(payload.name),
              )
            : undefined;
      if (changed !== undefined) object = recipeObject(changed);
    }
    bindings.publish({
      event: "recipes.changed",
      payload: snapshot,
      sequence: bindings.nextSequence(),
      version: HOST_PROTOCOL_VERSION,
    });
    bindings.recordActivity({
      operation,
      object,
      correlationId,
      outcome: "succeeded",
      severity: "info",
      detail:
        "The requested template operation completed; no remote acquisition or profile mutation was performed.",
    });
    return successResponse(command, correlationId);
  } catch (error) {
    const translated = translateFacadeFailure(
      error,
      { activeStateChanged: false, connection: undefined, correlationId },
      bindings.available(),
    );
    if (
      command.command !== "recipes.import.preview" &&
      command.command !== "recipes.export" &&
      command.command !== "recipes.legacy.preview"
    ) {
      bindings.publish({
        event: "recipes.changed",
        payload: library.currentSnapshot(),
        sequence: bindings.nextSequence(),
        version: HOST_PROTOCOL_VERSION,
      });
    }
    bindings.recordActivity({
      operation,
      object,
      correlationId,
      outcome: "failed",
      severity: "error",
      detail: translated.detail,
    });
    return failureResponse(command, translated.error);
  }
}
