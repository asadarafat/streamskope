import { parseDesktopTextDocument } from "../../../platform/desktop";

import {
  parseConnectionTemplateStoreCapability,
  parseConnectionTemplateSnapshotPayload,
} from "./connection-template-validation";
import { PROFILE_TRUST_KINDS, PROFILE_LIMITS } from "./profile-types";
import { parseTrustAcquisitionRecipeImport } from "./trust-recipe-exchange";
import { TRUST_RECIPE_LIMITS, type TrustAcquisitionRecipeSnapshot } from "./trust-recipe-types";
import {
  parseTrustAcquisitionRecipeInput,
  parseTrustAcquisitionRecipeDocument,
} from "./trust-recipe-validation";
import type {
  HostCommand,
  HostCommandName,
  HostCommandResponse,
  HOST_PROTOCOL_VERSION,
} from "./types";
import { HostContractValidationError } from "./validation-error";
import {
  emptyRecord,
  boundedUtf8Text,
  declaredValue,
  nullableText,
  exactKeys,
  positiveBoundedInteger,
  record,
  text,
} from "./validation-primitives";

export function parseTrustRecipeHostCommand(
  command: HostCommandName,
  id: string,
  value: unknown,
  version: typeof HOST_PROTOCOL_VERSION,
): HostCommand | undefined {
  if (command === "recipes.legacy.convert") {
    const input = record(value, "command.payload");
    exactKeys(
      input,
      ["name", "kind", "materialName", "passwordName", "oauthName", "expectedSourceRevision"],
      "command.payload",
    );
    return {
      command,
      id,
      version,
      payload: {
        name: text(input.name, "command.payload.name", TRUST_RECIPE_LIMITS.nameCharacters),
        kind: declaredValue(input.kind, PROFILE_TRUST_KINDS, "command.payload.kind"),
        materialName: text(
          input.materialName,
          "command.payload.materialName",
          TRUST_RECIPE_LIMITS.nameCharacters,
        ),
        passwordName: nullableText(
          input.passwordName,
          "command.payload.passwordName",
          TRUST_RECIPE_LIMITS.nameCharacters,
        ),
        oauthName: nullableText(
          input.oauthName,
          "command.payload.oauthName",
          TRUST_RECIPE_LIMITS.nameCharacters,
        ),
        expectedSourceRevision: parseSourceRevision(
          input.expectedSourceRevision,
          "command.payload.expectedSourceRevision",
        ),
      },
    };
  }
  if (command === "recipes.import.preview") {
    const input = record(value, "command.payload");
    exactKeys(input, ["contents"], "command.payload");
    return {
      command,
      id,
      version,
      payload: {
        contents: boundedUtf8Text(
          input.contents,
          "command.payload.contents",
          TRUST_RECIPE_LIMITS.exchangeBytes,
        ),
      },
    };
  }
  if (command === "recipes.list" || command === "recipes.legacy.preview")
    return { command, id, version, payload: emptyRecord(value, "command.payload") };
  if (command === "recipes.create")
    return {
      command,
      id,
      version,
      payload: parseTrustAcquisitionRecipeInput(value, "command.payload"),
    };
  if (
    command !== "recipes.update" &&
    command !== "recipes.delete" &&
    command !== "recipes.usage" &&
    command !== "recipes.duplicate" &&
    command !== "recipes.export"
  )
    return undefined;
  const input = record(value, "command.payload");
  exactKeys(
    input,
    command === "recipes.update"
      ? ["id", "revision", "recipe"]
      : command === "recipes.duplicate"
        ? ["id", "revision", "name"]
        : command === "recipes.delete"
          ? ["id", "revision", "confirmedProfileIds"]
          : ["id", "revision"],
    "command.payload",
  );
  const identity = {
    id: text(input.id, "command.payload.id", TRUST_RECIPE_LIMITS.idCharacters),
    revision: positiveBoundedInteger(
      input.revision,
      "command.payload.revision",
      Number.MAX_SAFE_INTEGER,
    ),
  };
  if (command === "recipes.delete") {
    if (input.confirmedProfileIds === undefined) return { command, id, version, payload: identity };
    if (
      !Array.isArray(input.confirmedProfileIds) ||
      input.confirmedProfileIds.length > PROFILE_LIMITS.profiles
    )
      throw new HostContractValidationError(
        "command.payload.confirmedProfileIds",
        "must be a bounded profile list",
      );
    const confirmedProfileIds = input.confirmedProfileIds.map((value: unknown) =>
      text(value, "command.payload.confirmedProfileIds", PROFILE_LIMITS.idCharacters),
    );
    if (new Set(confirmedProfileIds).size !== confirmedProfileIds.length)
      throw new HostContractValidationError(
        "command.payload.confirmedProfileIds",
        "must not contain duplicate profiles",
      );
    return { command, id, version, payload: { ...identity, confirmedProfileIds } };
  }
  if (command === "recipes.update")
    return {
      command,
      id,
      version,
      payload: {
        ...identity,
        recipe: parseTrustAcquisitionRecipeInput(input.recipe, "command.payload.recipe"),
      },
    };
  if (command === "recipes.duplicate")
    return {
      command,
      id,
      version,
      payload: {
        ...identity,
        name: text(input.name, "command.payload.name", TRUST_RECIPE_LIMITS.nameCharacters),
      },
    };
  return { command, id, version, payload: identity };
}

export function parseTrustRecipeReviewResponse(
  command: HostCommandName,
  id: string,
  value: unknown,
  version: typeof HOST_PROTOCOL_VERSION,
): HostCommandResponse | undefined {
  if (command === "recipes.usage") {
    const input = record(value, "response.result");
    exactKeys(input, ["correlationId", "usage"], "response.result");
    if (!Array.isArray(input.usage) || input.usage.length > PROFILE_LIMITS.profiles)
      throw new HostContractValidationError(
        "response.result.usage",
        "must be a bounded profile list",
      );
    const usage = input.usage.map((value: unknown) => {
      const profile = record(value, "response.result.usage");
      exactKeys(profile, ["id", "name", "revision"], "response.result.usage");
      return {
        id: text(profile.id, "response.result.usage.id", PROFILE_LIMITS.idCharacters),
        name: text(profile.name, "response.result.usage.name", PROFILE_LIMITS.nameCharacters),
        revision: positiveBoundedInteger(
          profile.revision,
          "response.result.usage.revision",
          Number.MAX_SAFE_INTEGER,
        ),
      };
    });
    return {
      command,
      id,
      version,
      ok: true,
      result: {
        correlationId: text(input.correlationId, "response.result.correlationId", 128),
        usage,
      },
    };
  }
  if (command === "recipes.legacy.preview") {
    const input = record(value, "response.result");
    exactKeys(input, ["correlationId", "legacy"], "response.result");
    const correlationId = text(input.correlationId, "response.result.correlationId", 128);
    if (input.legacy === null)
      return { command, id, version, ok: true, result: { correlationId, legacy: null } };
    const source = record(input.legacy, "response.result.legacy");
    exactKeys(source, ["catalogs", "sourceRevision"], "response.result.legacy");
    const { catalogs } = parseConnectionTemplateSnapshotPayload(
      { catalogs: source.catalogs, store: { durability: "session", state: "ready" } },
      "response.result.legacy",
    );
    return {
      command,
      id,
      version,
      ok: true,
      result: {
        correlationId,
        legacy: {
          catalogs,
          sourceRevision: parseSourceRevision(
            source.sourceRevision,
            "response.result.legacy.sourceRevision",
          ),
        },
      },
    };
  }
  if (command !== "recipes.import.preview" && command !== "recipes.export") return undefined;
  const input = record(value, "response.result");
  exactKeys(
    input,
    command === "recipes.import.preview"
      ? ["correlationId", "draft"]
      : ["correlationId", "document", "warning"],
    "response.result",
  );
  const correlationId = text(input.correlationId, "response.result.correlationId", 128);
  if (command === "recipes.import.preview")
    return {
      command,
      id,
      version,
      ok: true,
      result: {
        correlationId,
        draft: parseTrustAcquisitionRecipeInput(input.draft, "response.result.draft"),
      },
    };
  const recordDocument = record(input.document, "response.result.document");
  const content = boundedUtf8Text(
    recordDocument.content,
    "response.result.document.content",
    TRUST_RECIPE_LIMITS.exchangeBytes,
  );
  parseTrustAcquisitionRecipeImport(content);
  const document = parseDesktopTextDocument(input.document);
  const warning = text(input.warning, "response.result.warning", 512);
  return { command, id, version, ok: true, result: { correlationId, document, warning } };
}

function parseSourceRevision(value: unknown, path: string): string {
  const revision = text(value, path, 64);
  if (!/^[a-f0-9]{64}$/u.test(revision))
    throw new HostContractValidationError(path, "must be a source revision");
  return revision;
}

export function parseTrustRecipeSnapshot(
  value: unknown,
  path: string,
): TrustAcquisitionRecipeSnapshot {
  const input = record(value, path);
  exactKeys(input, ["recipes", "store"], path);
  const { recipes } = parseTrustAcquisitionRecipeDocument(
    { version: 1, recipes: input.recipes },
    path,
  );
  const store = parseConnectionTemplateStoreCapability(input.store, `${path}.store`);
  if (store.state === "unavailable" && recipes.length !== 0)
    throw new HostContractValidationError(
      `${path}.recipes`,
      "must be empty while storage is unavailable",
    );
  return { recipes, store };
}
