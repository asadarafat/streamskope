import { PROFILE_LIMITS, PROFILE_TRUST_KINDS, type ProfileTrustKind } from "./profile-types";
import { parseProfileBindingInput } from "./profile-binding";
import { parseHttpsUrl } from "./https-trust-validation";
import { parseHttpsProfileAccess } from "./https-profile-access";
import type { HttpsTrustAuthentication } from "./https-trust-types";
import { TRUST_RECIPE_LIMITS } from "./trust-recipe-types";
import {
  parseRemoteSshEndpoint,
  parseRemoteSshUsername,
  parseRemoteSshHostKeyFingerprint,
} from "./remote-ssh-access";
export { parseRemoteSshEndpoint, parseRemoteSshHostKeyFingerprint } from "./remote-ssh-access";
import {
  REMOTE_TRUST_ACQUISITION_LIMITS,
  TRUST_CERTIFICATE_EVIDENCE_LIMITS,
  type TrustCertificateEvidence,
  type AcquiredTlsConnectionInput,
  type RemoteSshHostKeySummary,
  type RemoteSshTargetInput,
  type RemoteSshAuthentication,
  type RemoteTrustAcquisitionMaterialSummary,
  type RemoteTrustAcquisitionSummary,
  type RemoteTrustMaterialFetchInput,
  type RemoteTrustHostKeyDiscoveryInput,
  type TrustAcquisitionEditor,
  type HttpsTrustMaterialFetchInput,
} from "./remote-trust-types";
import { HostContractValidationError } from "./validation-error";
import type {
  HostCommand,
  HostCommandName,
  HostCommandResponse,
  HOST_PROTOCOL_VERSION,
} from "./types";
import {
  canonicalIsoTimestamp,
  boundedText,
  declaredValue,
  exactKeys,
  nullableText,
  optionalText,
  positiveBoundedInteger,
  record,
  text,
  truth,
} from "./validation-primitives";

export function parseRemoteTrustHostKeyDiscovery(
  value: unknown,
  path: string,
): RemoteTrustHostKeyDiscoveryInput {
  const payload = record(value, path);
  exactKeys(payload, ["target", "editor"], path);
  return {
    target: parseRemoteSshEndpoint(payload.target, `${path}.target`),
    ...(payload.editor === undefined
      ? {}
      : { editor: parseTrustAcquisitionEditor(payload.editor, `${path}.editor`) }),
  };
}

export function parseTrustAcquisitionEditor(value: unknown, path: string): TrustAcquisitionEditor {
  const editor = record(value, path);
  exactKeys(editor, ["id", "generation"], path);
  return {
    id: text(editor.id, `${path}.id`, PROFILE_LIMITS.idCharacters),
    generation: positiveBoundedInteger(
      editor.generation,
      `${path}.generation`,
      Number.MAX_SAFE_INTEGER,
    ),
  };
}

export function parseTrustEditorCommand(
  command: HostCommandName,
  id: string,
  value: unknown,
  version: typeof HOST_PROTOCOL_VERSION,
): HostCommand | undefined {
  if (command === "trustAcquisition.editor.open") {
    const input = record(value, "command.payload");
    exactKeys(input, ["profile"], "command.payload");
    if (input.profile === undefined) return { command, id, version, payload: {} };
    const profile = record(input.profile, "command.payload.profile");
    exactKeys(profile, ["id", "revision"], "command.payload.profile");
    return {
      command,
      id,
      version,
      payload: {
        profile: {
          id: text(profile.id, "command.payload.profile.id", PROFILE_LIMITS.idCharacters),
          revision: positiveBoundedInteger(
            profile.revision,
            "command.payload.profile.revision",
            Number.MAX_SAFE_INTEGER,
          ),
        },
      },
    };
  }
  if (command === "trustAcquisition.editor.advance")
    return { command, id, version, payload: parseTrustAcquisitionEditor(value, "command.payload") };
  if (command === "trustAcquisition.editor.close" || command === "trustAcquisition.apply") {
    const input = record(value, "command.payload");
    exactKeys(
      input,
      command === "trustAcquisition.apply" ? ["editorId", "acquisitionId"] : ["editorId"],
      "command.payload",
    );
    const editorId = text(input.editorId, "command.payload.editorId", PROFILE_LIMITS.idCharacters);
    if (command === "trustAcquisition.editor.close")
      return { command, id, version, payload: { editorId } };
    return {
      command,
      id,
      version,
      payload: {
        editorId,
        acquisitionId: text(
          input.acquisitionId,
          "command.payload.acquisitionId",
          PROFILE_LIMITS.idCharacters,
        ),
      },
    };
  }
  return undefined;
}

export function parseRemoteTrustResponse(
  command: HostCommandName,
  id: string,
  result: Record<string, unknown>,
  version: typeof HOST_PROTOCOL_VERSION,
): HostCommandResponse | undefined {
  if (command === "trustAcquisition.editor.open") {
    exactKeys(result, ["correlationId", "editor"], "response.result");
    return {
      command,
      id,
      version,
      ok: true,
      result: {
        correlationId: text(result.correlationId, "response.result.correlationId", 128),
        editor: parseTrustAcquisitionEditor(result.editor, "response.result.editor"),
      },
    };
  }
  if (
    ![
      "trustAcquisition.capabilities",
      "trustAcquisition.hostKey.discover",
      "trustAcquisition.material.fetch",
      "trustAcquisition.https.fetch",
      "trustAcquisition.password.fetch",
    ].includes(command)
  )
    return undefined;
  const correlationId = text(result.correlationId, "response.result.correlationId", 128);
  if (command === "trustAcquisition.capabilities") {
    exactKeys(result, ["correlationId", "sshAgent", "methods"], "response.result");
    let methods: ("ssh" | "https")[] | undefined;
    if (result.methods !== undefined) {
      if (
        !Array.isArray(result.methods) ||
        result.methods.length > 2 ||
        new Set(result.methods).size !== result.methods.length
      )
        throw new HostContractValidationError(
          "response.result.methods",
          "must be a bounded unique method list",
        );
      methods = result.methods.map((method) =>
        declaredValue(method, ["ssh", "https"], "response.result.methods"),
      );
    }
    return {
      command,
      id,
      ok: true,
      version,
      result: {
        correlationId,
        ...(methods === undefined ? {} : { methods }),
        sshAgent: declaredValue(
          result.sshAgent,
          ["configured", "unavailable"],
          "response.result.sshAgent",
        ),
      },
    };
  }
  if (command === "trustAcquisition.hostKey.discover") {
    exactKeys(result, ["correlationId", "hostKey"], "response.result");
    return {
      command,
      id,
      ok: true,
      version,
      result: {
        correlationId,
        hostKey: parseRemoteSshHostKeySummary(result.hostKey, "response.result.hostKey"),
      },
    };
  }
  exactKeys(result, ["acquisition", "correlationId"], "response.result");
  return {
    command,
    id,
    ok: true,
    version,
    result: {
      correlationId,
      acquisition: parseRemoteTrustAcquisitionSummary(
        result.acquisition,
        "response.result.acquisition",
      ),
    },
  };
}

export function parseRemoteTrustCancellation(
  value: unknown,
  path: string,
): { readonly requestId: string; readonly editorId?: string } {
  const payload = record(value, path);
  exactKeys(payload, ["requestId", "editorId"], path);
  return {
    requestId: text(payload.requestId, `${path}.requestId`, 128),
    ...(payload.editorId === undefined
      ? {}
      : { editorId: text(payload.editorId, `${path}.editorId`, PROFILE_LIMITS.idCharacters) }),
  };
}

export function parseAcquiredTls(value: unknown, path: string): AcquiredTlsConnectionInput {
  const tls = record(value, path);
  exactKeys(tls, ["acquisitionId", "enabled", "kind", "editorId"], path);
  if (tls.enabled !== true) {
    throw new HostContractValidationError(`${path}.enabled`, "must be true");
  }
  return {
    acquisitionId: text(tls.acquisitionId, `${path}.acquisitionId`, PROFILE_LIMITS.idCharacters),
    ...(tls.editorId === undefined
      ? {}
      : { editorId: text(tls.editorId, `${path}.editorId`, PROFILE_LIMITS.idCharacters) }),
    enabled: true,
    kind: declaredValue(tls.kind, PROFILE_TRUST_KINDS, `${path}.kind`),
  };
}

export function parseRemoteSshTarget(value: unknown, path: string): RemoteSshTargetInput {
  const target = record(value, path);
  exactKeys(
    target,
    ["host", "hostKeyFingerprint", "password", "port", "username", "authentication"],
    path,
  );
  const endpoint = parseRemoteSshEndpoint(
    {
      host: target.host,
      port: target.port,
    },
    path,
  );
  const hostKeyFingerprint = parseRemoteSshHostKeyFingerprint(
    target.hostKeyFingerprint,
    `${path}.hostKeyFingerprint`,
  );
  const username = parseRemoteSshUsername(target.username, `${path}.username`);
  if (target.authentication !== undefined) {
    if (Object.hasOwn(target, "password")) {
      throw new HostContractValidationError(path, "must select exactly one authentication mode");
    }
    return {
      ...endpoint,
      hostKeyFingerprint,
      username,
      authentication: parseSshAuthentication(target.authentication, `${path}.authentication`),
    };
  }
  const password = text(
    target.password,
    `${path}.password`,
    REMOTE_TRUST_ACQUISITION_LIMITS.passwordCharacters,
  );
  if (password.includes("\u0000")) {
    throw new HostContractValidationError(`${path}.password`, "must not contain NUL");
  }
  return {
    ...endpoint,
    hostKeyFingerprint,
    password,
    username,
  };
}

function parseSshAuthentication(value: unknown, path: string): RemoteSshAuthentication {
  const input = record(value, path);
  const mode = declaredValue(input.mode, ["password", "private-key", "agent"], `${path}.mode`);
  if (mode === "agent") {
    exactKeys(input, ["mode"], path);
    return { mode };
  }
  const secret = (value: unknown, field: string, maximum: number): string => {
    const result = text(value, `${path}.${field}`, maximum);
    if (result.includes("\u0000"))
      throw new HostContractValidationError(`${path}.${field}`, "must not contain NUL");
    return result;
  };
  if (mode === "password") {
    exactKeys(input, ["mode", "password"], path);
    return {
      mode,
      password: secret(
        input.password,
        "password",
        REMOTE_TRUST_ACQUISITION_LIMITS.passwordCharacters,
      ),
    };
  }
  exactKeys(input, ["mode", "privateKey", "passphrase"], path);
  return {
    mode,
    privateKey: secret(
      input.privateKey,
      "privateKey",
      REMOTE_TRUST_ACQUISITION_LIMITS.privateKeyCharacters,
    ),
    ...(input.passphrase === undefined
      ? {}
      : {
          passphrase: secret(
            input.passphrase,
            "passphrase",
            REMOTE_TRUST_ACQUISITION_LIMITS.passwordCharacters,
          ),
        }),
  };
}

export function parseRemoteSshHostKeySummary(
  value: unknown,
  path: string,
): RemoteSshHostKeySummary {
  const hostKey = record(value, path);
  exactKeys(hostKey, ["fingerprint", "target", "review"], path);
  let review: RemoteSshHostKeySummary["review"];
  if (hostKey.review !== undefined) {
    const value = record(hostKey.review, `${path}.review`);
    exactKeys(value, ["id", "expiresAt", "confirmationRequired"], `${path}.review`);
    review = {
      id: text(value.id, `${path}.review.id`, PROFILE_LIMITS.idCharacters),
      expiresAt: canonicalIsoTimestamp(value.expiresAt, `${path}.review.expiresAt`),
      confirmationRequired: truth(
        value.confirmationRequired,
        `${path}.review.confirmationRequired`,
      ),
    };
  }
  return {
    ...(review === undefined ? {} : { review }),
    fingerprint: parseRemoteSshHostKeyFingerprint(hostKey.fingerprint, `${path}.fingerprint`),
    target: parseRemoteSshEndpoint(hostKey.target, `${path}.target`),
  };
}

export function parseRemoteTrustMaterialFetchInput(
  value: unknown,
  path: string,
): RemoteTrustMaterialFetchInput {
  const payload = record(value, path);
  exactKeys(
    payload,
    [
      "acquisitionId",
      "kind",
      "label",
      "target",
      "recipe",
      "secretParameters",
      "truststorePassword",
      "profile",
      "editor",
      "identityId",
      "acceptIdentity",
    ],
    path,
  );
  const recipe =
    payload.recipe === undefined
      ? undefined
      : parseProfileBindingInput(payload.recipe, `${path}.recipe`);
  if (recipe?.mode === "clear" || (recipe !== undefined && payload.acquisitionId !== undefined)) {
    throw new HostContractValidationError(
      `${path}.recipe`,
      "must select a recipe for a new complete acquisition",
    );
  }
  if (
    recipe === undefined &&
    (payload.secretParameters !== undefined || payload.truststorePassword !== undefined)
  ) {
    throw new HostContractValidationError(
      path,
      "ephemeral recipe inputs require a recipe reference",
    );
  }
  const shared = parseRecipeAcquisitionFields(payload, path, recipe !== undefined);
  const acquisitionId = optionalText(payload, "acquisitionId", path, PROFILE_LIMITS.idCharacters);
  const base = {
    ...shared,
    ...(payload.identityId === undefined
      ? {}
      : {
          identityId: text(payload.identityId, `${path}.identityId`, PROFILE_LIMITS.idCharacters),
        }),
    ...(payload.acceptIdentity === undefined
      ? {}
      : { acceptIdentity: truth(payload.acceptIdentity, `${path}.acceptIdentity`) }),
    ...(payload.editor === undefined
      ? {}
      : { editor: parseTrustAcquisitionEditor(payload.editor, `${path}.editor`) }),
    ...(recipe === undefined ? {} : { recipe }),
    target: parseRemoteSshTarget(payload.target, `${path}.target`),
  };
  return acquisitionId === undefined ? base : { ...base, acquisitionId };
}

function parseRecipeAcquisitionFields(
  payload: Record<string, unknown>,
  path: string,
  hasRecipe: boolean,
): Pick<
  RemoteTrustMaterialFetchInput,
  "profile" | "secretParameters" | "truststorePassword" | "kind" | "label"
> {
  let secretParameters: Readonly<Record<string, string>> | undefined;
  let profile: RemoteTrustMaterialFetchInput["profile"];
  if (payload.profile !== undefined) {
    if (!hasRecipe)
      throw new HostContractValidationError(`${path}.profile`, "requires a recipe reference");
    const value = record(payload.profile, `${path}.profile`);
    exactKeys(value, ["id", "revision"], `${path}.profile`);
    profile = {
      id: text(value.id, `${path}.profile.id`, PROFILE_LIMITS.idCharacters),
      revision: positiveBoundedInteger(
        value.revision,
        `${path}.profile.revision`,
        Number.MAX_SAFE_INTEGER,
      ),
    };
  }
  if (payload.secretParameters !== undefined) {
    const parameters = record(payload.secretParameters, `${path}.secretParameters`);
    if (Object.keys(parameters).length > TRUST_RECIPE_LIMITS.parameters)
      throw new HostContractValidationError(
        `${path}.secretParameters`,
        "exceeds the parameter limit",
      );
    secretParameters = Object.fromEntries(
      Object.entries(parameters).map(([key, value]) => {
        if (
          !/^[A-Za-z][A-Za-z0-9_]*$/u.test(key) ||
          key.length > TRUST_RECIPE_LIMITS.parameterKeyCharacters ||
          ["host", "constructor", "prototype"].includes(key)
        )
          throw new HostContractValidationError(
            `${path}.secretParameters`,
            "contains an invalid identifier",
          );
        return [
          key,
          text(value, `${path}.secretParameters`, TRUST_RECIPE_LIMITS.parameterValueCharacters),
        ];
      }),
    );
  }
  return {
    ...(profile === undefined ? {} : { profile }),
    ...(secretParameters === undefined ? {} : { secretParameters }),
    ...(payload.truststorePassword === undefined
      ? {}
      : {
          truststorePassword: text(
            payload.truststorePassword,
            `${path}.truststorePassword`,
            PROFILE_LIMITS.clientSecretCharacters,
          ),
        }),
    kind: declaredValue(payload.kind, PROFILE_TRUST_KINDS, `${path}.kind`),
    label: text(payload.label, `${path}.label`, PROFILE_LIMITS.trustLabelCharacters),
  };
}

export function parseHttpsTrustMaterialFetchInput(
  value: unknown,
  path: string,
): HttpsTrustMaterialFetchInput {
  const payload = record(value, path);
  exactKeys(
    payload,
    [
      "editor",
      "profile",
      "recipe",
      "kind",
      "label",
      "secretParameters",
      "truststorePassword",
      "api",
    ],
    path,
  );
  const recipe = parseProfileBindingInput(payload.recipe, `${path}.recipe`);
  if (recipe.mode !== "replace")
    throw new HostContractValidationError(`${path}.recipe`, "must select a recipe");
  const api = record(payload.api, `${path}.api`);
  exactKeys(api, ["host", "authentication", "tls"], `${path}.api`);
  const tls = record(api.tls, `${path}.api.tls`);
  const mode = declaredValue(tls.mode, ["system", "custom", "retain"], `${path}.api.tls.mode`);
  exactKeys(tls, mode === "custom" ? ["mode", "caPem"] : ["mode"], `${path}.api.tls`);
  if (mode === "retain" && payload.profile === undefined)
    throw new HostContractValidationError(
      `${path}.api.tls`,
      "retained API CA requires a saved profile revision",
    );
  const host = parseHttpsProfileAccess(
    { host: api.host, username: "", tls: mode === "retain" ? "custom" : mode },
    `${path}.api`,
  ).host;
  const auth = record(api.authentication, `${path}.api.authentication`);
  const authMode = declaredValue(
    auth.mode,
    ["none", "bearer", "basic"],
    `${path}.api.authentication.mode`,
  );
  exactKeys(
    auth,
    authMode === "none"
      ? ["mode"]
      : authMode === "bearer"
        ? ["mode", "token"]
        : ["mode", "username", "password"],
    `${path}.api.authentication`,
  );
  const credential = (key: string): string => {
    const result = text(auth[key], `${path}.api.authentication.${key}`, 4096);
    if (/[\p{Cc}]/u.test(result))
      throw new HostContractValidationError(
        `${path}.api.authentication.${key}`,
        "must not contain control characters",
      );
    return result;
  };
  let authentication: HttpsTrustAuthentication;
  if (authMode === "none") authentication = { mode: "none" };
  else if (authMode === "bearer") authentication = { mode: "bearer", token: credential("token") };
  else {
    const username = credential("username");
    if (username.includes(":"))
      throw new HostContractValidationError(
        `${path}.api.authentication.username`,
        "must not contain a colon",
      );
    authentication = { mode: "basic", username, password: credential("password") };
  }
  return {
    ...parseRecipeAcquisitionFields(payload, path, true),
    editor: parseTrustAcquisitionEditor(payload.editor, `${path}.editor`),
    recipe,
    api: {
      host,
      authentication,
      tls:
        mode === "custom"
          ? {
              mode,
              caPem: text(
                tls.caPem,
                `${path}.api.tls.caPem`,
                REMOTE_TRUST_ACQUISITION_LIMITS.materialBytes,
              ),
            }
          : { mode },
    },
  };
}

export function parseRemoteTrustIdentity(
  value: unknown,
  path: string,
): {
  readonly acquisitionId: string;
  readonly editorId?: string;
} {
  const payload = record(value, path);
  exactKeys(payload, ["acquisitionId", "editorId"], path);
  return {
    ...(payload.editorId === undefined
      ? {}
      : { editorId: text(payload.editorId, `${path}.editorId`, PROFILE_LIMITS.idCharacters) }),
    acquisitionId: text(
      payload.acquisitionId,
      `${path}.acquisitionId`,
      PROFILE_LIMITS.idCharacters,
    ),
  };
}

function parseRemoteTrustMaterialSummary(
  value: unknown,
  path: string,
): RemoteTrustAcquisitionMaterialSummary | null {
  if (value === null) {
    return null;
  }
  const material = record(value, path);
  exactKeys(
    material,
    [
      "byteCount",
      "kind",
      "label",
      "templateName",
      "evidence",
      "expiredCertificates",
      "notYetValidCertificates",
    ],
    path,
  );
  if (
    material.evidence === undefined &&
    (material.expiredCertificates !== undefined || material.notYetValidCertificates !== undefined)
  ) {
    throw new HostContractValidationError(path, "validity warnings require certificate evidence");
  }
  return {
    ...(material.evidence === undefined
      ? {}
      : {
          evidence: parseCertificateEvidence(material.evidence, `${path}.evidence`),
          expiredCertificates: truth(material.expiredCertificates, `${path}.expiredCertificates`),
          notYetValidCertificates: truth(
            material.notYetValidCertificates,
            `${path}.notYetValidCertificates`,
          ),
        }),
    byteCount: positiveBoundedInteger(
      material.byteCount,
      `${path}.byteCount`,
      REMOTE_TRUST_ACQUISITION_LIMITS.materialBytes,
    ),
    kind: declaredValue(
      material.kind,
      PROFILE_TRUST_KINDS,
      `${path}.kind`,
    ) satisfies ProfileTrustKind,
    label: text(material.label, `${path}.label`, PROFILE_LIMITS.trustLabelCharacters),
    templateName: text(material.templateName, `${path}.templateName`, 128),
  };
}

function parseCertificateEvidence(value: unknown, path: string): TrustCertificateEvidence {
  const evidence = record(value, path);
  exactKeys(evidence, ["count", "truncated", "certificates", "validity"], path);
  let validity: TrustCertificateEvidence["validity"];
  if (evidence.validity !== undefined) {
    const range = record(evidence.validity, `${path}.validity`);
    exactKeys(range, ["earliestExpiry", "latestStart"], `${path}.validity`);
    validity = {
      earliestExpiry: canonicalIsoTimestamp(
        range.earliestExpiry,
        `${path}.validity.earliestExpiry`,
      ),
      latestStart: canonicalIsoTimestamp(range.latestStart, `${path}.validity.latestStart`),
    };
  }
  const count = positiveBoundedInteger(evidence.count, `${path}.count`, 65_536);
  const truncated = truth(evidence.truncated, `${path}.truncated`);
  const limits = TRUST_CERTIFICATE_EVIDENCE_LIMITS;
  if (
    !Array.isArray(evidence.certificates) ||
    evidence.certificates.length !== Math.min(count, limits.entries) ||
    truncated !== count > limits.entries
  )
    throw new HostContractValidationError(
      path,
      "must contain bounded matching certificate evidence",
    );
  return {
    count,
    truncated,
    ...(validity === undefined ? {} : { validity }),
    certificates: evidence.certificates.map((value, index) => {
      const certificatePath = `${path}.certificates[${index}]`;
      const certificate = record(value, certificatePath);
      exactKeys(
        certificate,
        ["subject", "issuer", "validFrom", "validTo", "fingerprint", "truncated"],
        certificatePath,
      );
      const fingerprint = text(certificate.fingerprint, `${certificatePath}.fingerprint`, 95);
      if (!/^(?:[A-F0-9]{2}:){31}[A-F0-9]{2}$/u.test(fingerprint))
        throw new HostContractValidationError(
          certificatePath,
          "must contain a SHA-256 fingerprint",
        );
      return {
        subject: boundedText(
          certificate.subject,
          `${certificatePath}.subject`,
          limits.nameCharacters,
        ),
        issuer: boundedText(certificate.issuer, `${certificatePath}.issuer`, limits.nameCharacters),
        validFrom: canonicalIsoTimestamp(certificate.validFrom, `${certificatePath}.validFrom`),
        validTo: canonicalIsoTimestamp(certificate.validTo, `${certificatePath}.validTo`),
        fingerprint,
        truncated: truth(certificate.truncated, `${certificatePath}.truncated`),
      };
    }),
  };
}

export function parseRemoteTrustAcquisitionSummary(
  value: unknown,
  path: string,
): RemoteTrustAcquisitionSummary {
  const acquisition = record(value, path);
  exactKeys(
    acquisition,
    ["expiresAt", "id", "material", "password", "target", "editor", "createdAt", "recipe", "oauth"],
    path,
  );
  let recipe: RemoteTrustAcquisitionSummary["recipe"];
  if (acquisition.recipe !== undefined) {
    const value = record(acquisition.recipe, `${path}.recipe`);
    exactKeys(value, ["id", "revision", "source"], `${path}.recipe`);
    recipe = {
      id: text(value.id, `${path}.recipe.id`, TRUST_RECIPE_LIMITS.idCharacters),
      revision: positiveBoundedInteger(
        value.revision,
        `${path}.recipe.revision`,
        Number.MAX_SAFE_INTEGER,
      ),
      source: declaredValue(
        value.source,
        ["file", "stdout", "legacy-tempfile", "https"],
        `${path}.recipe.source`,
      ),
    };
  }
  let oauth: RemoteTrustAcquisitionSummary["oauth"];
  if (acquisition.oauth !== undefined) {
    const value = record(acquisition.oauth, `${path}.oauth`);
    exactKeys(value, ["endpoint", "clientId", "scope"], `${path}.oauth`);
    const endpoint = text(
      value.endpoint,
      `${path}.oauth.endpoint`,
      PROFILE_LIMITS.tokenEndpointCharacters,
    );
    let url: URL;
    try {
      url = new URL(endpoint);
    } catch {
      throw new HostContractValidationError(`${path}.oauth.endpoint`, "must be an HTTP/HTTPS URL");
    }
    if (!["http:", "https:"].includes(url.protocol) || url.username || url.password || url.hash)
      throw new HostContractValidationError(
        `${path}.oauth.endpoint`,
        "must be a credential-free HTTP/HTTPS URL",
      );
    oauth = {
      endpoint,
      clientId: boundedText(
        value.clientId,
        `${path}.oauth.clientId`,
        PROFILE_LIMITS.clientIdCharacters,
      ),
      scope: boundedText(value.scope, `${path}.oauth.scope`, PROFILE_LIMITS.scopeCharacters),
    };
  }
  const password = record(acquisition.password, `${path}.password`);
  exactKeys(password, ["present", "templateName"], `${path}.password`);
  const passwordPresent = truth(password.present, `${path}.password.present`);
  const templateName = nullableText(password.templateName, `${path}.password.templateName`, 128);
  if (passwordPresent !== (templateName !== null)) {
    throw new HostContractValidationError(
      `${path}.password.templateName`,
      "must be present exactly when a password was acquired",
    );
  }
  const target = record(acquisition.target, `${path}.target`);
  exactKeys(
    target,
    target.origin === undefined
      ? ["host", "hostKeyFingerprint", "port"]
      : ["host", "port", "origin"],
    `${path}.target`,
  );
  const endpoint = parseRemoteSshEndpoint(
    {
      host: target.host,
      port: target.port,
    },
    `${path}.target`,
  );
  let parsedTarget: RemoteTrustAcquisitionSummary["target"];
  if (target.origin !== undefined) {
    const origin = text(target.origin, `${path}.target.origin`, TRUST_RECIPE_LIMITS.urlCharacters);
    const url = parseHttpsUrl(origin, `${path}.target.origin`);
    if (
      url.origin !== origin ||
      url.hostname !== endpoint.host ||
      Number(url.port || 443) !== endpoint.port ||
      recipe?.source !== "https"
    )
      throw new HostContractValidationError(`${path}.target`, "must match the HTTPS recipe origin");
    parsedTarget = { ...endpoint, origin };
  } else {
    if (recipe?.source === "https")
      throw new HostContractValidationError(
        `${path}.target`,
        "HTTPS requires an origin, not an SSH identity",
      );
    parsedTarget = {
      ...endpoint,
      hostKeyFingerprint: parseRemoteSshHostKeyFingerprint(
        target.hostKeyFingerprint,
        `${path}.target.hostKeyFingerprint`,
      ),
    };
  }
  return {
    expiresAt: canonicalIsoTimestamp(acquisition.expiresAt, `${path}.expiresAt`),
    ...(acquisition.createdAt === undefined
      ? {}
      : { createdAt: canonicalIsoTimestamp(acquisition.createdAt, `${path}.createdAt`) }),
    ...(recipe === undefined ? {} : { recipe }),
    ...(oauth === undefined ? {} : { oauth }),
    ...(acquisition.editor === undefined
      ? {}
      : { editor: parseTrustAcquisitionEditor(acquisition.editor, `${path}.editor`) }),
    id: text(acquisition.id, `${path}.id`, PROFILE_LIMITS.idCharacters),
    material: parseRemoteTrustMaterialSummary(acquisition.material, `${path}.material`),
    password: {
      present: passwordPresent,
      templateName,
    },
    target: parsedTarget,
  };
}
