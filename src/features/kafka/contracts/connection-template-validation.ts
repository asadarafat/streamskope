import {
  canonicalConnectionTemplateName,
  validateConnectionTemplateInput,
} from "./connection-template";
import {
  CONNECTION_TEMPLATE_CATALOGS,
  CONNECTION_TEMPLATE_LIMITS,
  CONNECTION_TEMPLATE_STORE_DURABILITIES,
  CONNECTION_TEMPLATE_STORE_STATES,
  type ConnectionTemplateCatalog,
  type ConnectionTemplateCatalogSnapshot,
  type ConnectionTemplateInput,
  type ConnectionTemplateSnapshot,
  type ConnectionTemplateStoreCapability,
} from "./connection-template-types";
import { HostContractValidationError } from "./validation-error";
import { declaredValue, exactKeys, nullableText, record, text } from "./validation-primitives";

function maximumTemplateCharacters(catalog: ConnectionTemplateCatalog): number {
  return catalog === "oauth-endpoint"
    ? CONNECTION_TEMPLATE_LIMITS.endpointCharacters
    : CONNECTION_TEMPLATE_LIMITS.commandCharacters;
}

export function parseConnectionTemplateInputPayload(
  value: unknown,
  path: string,
): ConnectionTemplateInput {
  const payload = record(value, path);
  exactKeys(payload, ["catalog", "name", "template"], path);
  const catalog = declaredValue(payload.catalog, CONNECTION_TEMPLATE_CATALOGS, `${path}.catalog`);
  const input = {
    catalog,
    name: text(payload.name, `${path}.name`, CONNECTION_TEMPLATE_LIMITS.nameCharacters),
    template: text(payload.template, `${path}.template`, maximumTemplateCharacters(catalog)),
  };
  const issue = validateConnectionTemplateInput(input)[0];
  if (issue !== undefined) {
    throw new HostContractValidationError(`${path}.${issue.field}`, issue.message);
  }
  return input;
}

export function parseConnectionTemplateIdentityPayload(
  value: unknown,
  path: string,
): {
  readonly catalog: ConnectionTemplateCatalog;
  readonly name: string;
} {
  const payload = record(value, path);
  exactKeys(payload, ["catalog", "name"], path);
  return {
    catalog: declaredValue(payload.catalog, CONNECTION_TEMPLATE_CATALOGS, `${path}.catalog`),
    name: text(payload.name, `${path}.name`, CONNECTION_TEMPLATE_LIMITS.nameCharacters),
  };
}

export function parseConnectionTemplateUpdatePayload(
  value: unknown,
  path: string,
): ConnectionTemplateInput & { readonly originalName: string } {
  const payload = record(value, path);
  exactKeys(payload, ["catalog", "name", "originalName", "template"], path);
  const input = parseConnectionTemplateInputPayload(
    {
      catalog: payload.catalog,
      name: payload.name,
      template: payload.template,
    },
    path,
  );
  return {
    ...input,
    originalName: text(
      payload.originalName,
      `${path}.originalName`,
      CONNECTION_TEMPLATE_LIMITS.nameCharacters,
    ),
  };
}

export function parseConnectionTemplateStoreCapability(
  value: unknown,
  path: string,
): ConnectionTemplateStoreCapability {
  const store = record(value, path);
  exactKeys(store, ["durability", "recovery", "state"], path);
  const durability = declaredValue(
    store.durability,
    CONNECTION_TEMPLATE_STORE_DURABILITIES,
    `${path}.durability`,
  );
  const state = declaredValue(store.state, CONNECTION_TEMPLATE_STORE_STATES, `${path}.state`);
  return Object.hasOwn(store, "recovery")
    ? {
        durability,
        recovery: text(store.recovery, `${path}.recovery`, 2_048),
        state,
      }
    : { durability, state };
}

function parseEntry(
  value: unknown,
  catalog: ConnectionTemplateCatalog,
  path: string,
): {
  readonly name: string;
  readonly template: string;
} {
  const entry = record(value, path);
  exactKeys(entry, ["name", "template"], path);
  const input = {
    catalog,
    name: text(entry.name, `${path}.name`, CONNECTION_TEMPLATE_LIMITS.nameCharacters),
    template: text(entry.template, `${path}.template`, maximumTemplateCharacters(catalog)),
  };
  if (input.name !== canonicalConnectionTemplateName(input.name)) {
    throw new HostContractValidationError(`${path}.name`, "must be canonical");
  }
  const issue = validateConnectionTemplateInput(input)[0];
  if (issue !== undefined) {
    throw new HostContractValidationError(`${path}.${issue.field}`, issue.message);
  }
  return { name: input.name, template: input.template };
}

function parseCatalog(
  value: unknown,
  expectedCatalog: ConnectionTemplateCatalog,
  path: string,
): ConnectionTemplateCatalogSnapshot {
  const catalog = record(value, path);
  exactKeys(catalog, ["catalog", "entries", "selectedName"], path);
  const parsedCatalog = declaredValue(
    catalog.catalog,
    CONNECTION_TEMPLATE_CATALOGS,
    `${path}.catalog`,
  );
  if (parsedCatalog !== expectedCatalog) {
    throw new HostContractValidationError(`${path}.catalog`, `must equal ${expectedCatalog}`);
  }
  if (
    !Array.isArray(catalog.entries) ||
    catalog.entries.length > CONNECTION_TEMPLATE_LIMITS.entriesPerCatalog
  ) {
    throw new HostContractValidationError(
      `${path}.entries`,
      `must contain at most ${CONNECTION_TEMPLATE_LIMITS.entriesPerCatalog} entries`,
    );
  }
  const entries = catalog.entries.map((entry, index) =>
    parseEntry(entry, parsedCatalog, `${path}.entries[${index}]`),
  );
  const names = entries.map((entry) =>
    canonicalConnectionTemplateName(entry.name).toLocaleLowerCase("en-US"),
  );
  if (new Set(names).size !== names.length) {
    throw new HostContractValidationError(`${path}.entries`, "must have unique canonical names");
  }
  const selectedName = nullableText(
    catalog.selectedName,
    `${path}.selectedName`,
    CONNECTION_TEMPLATE_LIMITS.nameCharacters,
  );
  if (selectedName !== null && !entries.some((entry) => entry.name === selectedName)) {
    throw new HostContractValidationError(
      `${path}.selectedName`,
      "must identify an entry in this catalog",
    );
  }
  return { catalog: parsedCatalog, entries, selectedName };
}

export function parseConnectionTemplateSnapshotPayload(
  value: unknown,
  path: string,
): ConnectionTemplateSnapshot {
  const payload = record(value, path);
  exactKeys(payload, ["catalogs", "store"], path);
  const rawCatalogs = payload.catalogs;
  if (!Array.isArray(rawCatalogs) || rawCatalogs.length !== CONNECTION_TEMPLATE_CATALOGS.length) {
    throw new HostContractValidationError(
      `${path}.catalogs`,
      `must contain exactly ${CONNECTION_TEMPLATE_CATALOGS.length} catalogs`,
    );
  }
  const catalogs = CONNECTION_TEMPLATE_CATALOGS.map((catalog, index) =>
    parseCatalog(rawCatalogs[index], catalog, `${path}.catalogs[${index}]`),
  );
  const store = parseConnectionTemplateStoreCapability(payload.store, `${path}.store`);
  if (
    store.state === "unavailable" &&
    catalogs.some((catalog) => catalog.entries.length > 0 || catalog.selectedName !== null)
  ) {
    throw new HostContractValidationError(
      `${path}.catalogs`,
      "must be empty while template storage is unavailable",
    );
  }
  return { catalogs, store };
}
