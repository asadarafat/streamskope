import {
  PROFILE_LIMITS,
  REMOTE_TRUST_ACQUISITION_LIMITS,
  parseRemoteSshHostKeyFingerprint,
  type ProfileTrustKind,
  type RemoteSshHostKeySummary,
  type RemoteSshTargetInput,
  type RemoteTrustAcquisitionSummary,
  type RemoteTrustMaterialFetchInput,
  type RemoteTrustHostKeyDiscoveryInput,
} from "../contracts";
import type {
  TrustAcquisitionEditor,
  AcceptedSshIdentity,
  HttpsTrustMaterialFetchInput,
} from "../contracts/remote-trust-types";
import { resolveHttpsGet } from "../contracts/https-trust-validation";

import { TrustAcquisitionLifecycle, type PendingAcquisition } from "./trust-acquisition-lifecycle";
import type { TrustAcquisitionRecord } from "./trust-acquisition-record";
import type { KafkaTrustRecipeLibrary } from "./trust-recipe-library";
import type { KafkaProfileTrustDecoder } from "./profile-types";
import {
  resolveTrustRecipeExecution,
  resolveTrustRecipeParameters,
  resolveTrustRecipeOAuth,
  type TrustRecipeExecution,
} from "./trust-recipe-execution";
import {
  KafkaTrustAcquisitionIncompleteError,
  KafkaTrustAcquisitionMaterialError,
  KafkaTrustAcquisitionPasswordError,
  KafkaTrustAcquisitionValidationError,
} from "./trust-acquisition-errors";
import type {
  KafkaRemoteTrustPort,
  KafkaResolvedTrustAcquisition,
  KafkaTrustAcquisitionServiceOptions,
} from "./trust-acquisition-types";

function defaultCreateId(): string {
  return globalThis.crypto.randomUUID();
}

function defaultCreateRemotePath(): string {
  return `/tmp/streamskope-${globalThis.crypto.randomUUID()}.trust`;
}

function defaultNow(): Date {
  return new Date();
}

function safeTarget(target: RemoteSshTargetInput): string {
  return `${target.host}:${String(target.port)}`;
}

function stripSurroundingLineEndings(value: string): string {
  return value.replace(/^(?:\r\n|\r|\n)+|(?:\r\n|\r|\n)+$/gu, "");
}

function quotedShellValue(value: string): string {
  return `'${value.replaceAll("'", "'\"'\"'")}'`;
}

function remoteDirectory(remotePath: string): string {
  const separator = remotePath.lastIndexOf("/");
  return separator <= 0 ? "/" : remotePath.slice(0, separator);
}

function expandMaterialCommand(
  template: string,
  remotePath: string,
  password: string | undefined,
): string {
  return template
    .replaceAll("{truststorePath}", quotedShellValue(remotePath))
    .replaceAll("{destDir}", quotedShellValue(remoteDirectory(remotePath)))
    .replaceAll("{storepass}", quotedShellValue(password ?? ""));
}

function binaryBase64(bytes: Uint8Array): string {
  const chunks: string[] = [];
  const chunkSize = 32_768;
  for (let index = 0; index < bytes.length; index += chunkSize) {
    chunks.push(String.fromCharCode(...bytes.subarray(index, index + chunkSize)));
  }
  return globalThis.btoa(chunks.join(""));
}

function encodedMaterial(bytes: Uint8Array, kind: ProfileTrustKind): string {
  if (kind !== "pem") {
    return binaryBase64(bytes);
  }
  try {
    return new TextDecoder("utf-8", { fatal: true }).decode(bytes);
  } catch {
    throw new KafkaTrustAcquisitionMaterialError();
  }
}

function sameTarget(left: TrustAcquisitionRecord["target"], right: RemoteSshTargetInput): boolean {
  return (
    left.host === right.host &&
    left.hostKeyFingerprint === right.hostKeyFingerprint &&
    left.port === right.port
  );
}

export class KafkaTrustAcquisitionService {
  private readonly createId;
  private readonly createRemotePath;
  private readonly now;
  private readonly lifecycle: TrustAcquisitionLifecycle;
  private readonly resolveProfileBinding;
  private readonly resolveProfileApiCa;
  private readonly https;

  constructor(
    private readonly recipes: KafkaTrustRecipeLibrary,
    private readonly remote: KafkaRemoteTrustPort,
    private readonly trustDecoder: KafkaProfileTrustDecoder,
    options: KafkaTrustAcquisitionServiceOptions = {},
  ) {
    this.createId = options.createId ?? defaultCreateId;
    this.createRemotePath = options.createRemotePath ?? defaultCreateRemotePath;
    this.now = options.now ?? defaultNow;
    this.lifecycle = new TrustAcquisitionLifecycle(this.createId, this.now);
    this.resolveProfileBinding = options.resolveProfileBinding;
    this.resolveProfileApiCa = options.resolveProfileApiCa;
    this.https = options.https;
  }

  clear(): void {
    this.lifecycle.clear();
  }
  openEditor(identity?: AcceptedSshIdentity): TrustAcquisitionEditor {
    return this.lifecycle.openEditor(identity);
  }
  closeEditor(editorId: string): void {
    this.lifecycle.closeEditor(editorId);
  }
  advanceEditor(editorId: string, generation: number): void {
    this.lifecycle.advanceEditor(editorId, generation);
  }
  apply(acquisitionId: string, editorId: string): void {
    this.lifecycle.apply(acquisitionId, editorId);
  }
  consume(acquisitionId: string): void {
    this.lifecycle.consume(acquisitionId);
  }
  discard(acquisitionId: string, editorId?: string): void {
    this.lifecycle.discard(acquisitionId, editorId);
  }
  cancel(requestId: string, editorId?: string): void {
    this.lifecycle.cancel(requestId, editorId);
  }

  capabilities(): import("../contracts/remote-trust-types").TrustAcquisitionCapabilities {
    return {
      sshAgent: this.remote.agentStatus?.() ?? "unavailable",
      methods: this.https === undefined ? ["ssh"] : ["ssh", "https"],
    };
  }

  discoverHostKey(
    input: RemoteTrustHostKeyDiscoveryInput,
    signal?: AbortSignal,
    requestId?: string,
  ): Promise<RemoteSshHostKeySummary> {
    return this.lifecycle.run(
      undefined,
      async (operation) => {
        const signal = operation.controller.signal;
        signal?.throwIfAborted();
        const startedAt = this.now().getTime();
        const fingerprint = parseRemoteSshHostKeyFingerprint(
          await this.remote.discoverHostKey({ target: input.target }, signal),
          "remote.hostKeyFingerprint",
        );
        signal?.throwIfAborted();
        let review: RemoteSshHostKeySummary["review"];
        if (input.editor !== undefined) {
          review = this.lifecycle.reviewIdentity(
            input.editor.id,
            input.target,
            fingerprint,
            startedAt,
          );
        }
        return {
          ...(review === undefined ? {} : { review }),
          fingerprint,
          target: { host: input.target.host, port: input.target.port },
        };
      },
      signal,
      requestId,
      input.editor,
    );
  }

  fetchMaterial(
    input: RemoteTrustMaterialFetchInput,
    signal?: AbortSignal,
    requestId?: string,
  ): Promise<RemoteTrustAcquisitionSummary> {
    return this.lifecycle.run(
      undefined,
      (operation) => this.completeDirectMaterialFetch(input, operation),
      signal,
      requestId,
      input.editor,
    );
  }

  fetchHttpsMaterial(
    input: HttpsTrustMaterialFetchInput,
    signal?: AbortSignal,
    requestId?: string,
  ): Promise<RemoteTrustAcquisitionSummary> {
    return this.lifecycle.run(
      undefined,
      async (operation) => {
        if (this.https === undefined)
          throw new KafkaTrustAcquisitionValidationError(
            "This host does not support HTTPS acquisition.",
          );
        const signal = operation.controller.signal;
        const recipe =
          input.profile === undefined
            ? await this.recipes.resolve(input.recipe.recipeId, input.recipe.recipeRevision, signal)
            : (
                await this.resolveProfileBinding?.(
                  input.profile.id,
                  input.profile.revision,
                  input.recipe,
                  signal,
                )
              )?.recipe;
        if (recipe === undefined || recipe.method !== "https" || recipe.kind !== input.kind)
          throw new KafkaTrustAcquisitionValidationError(
            "Select a matching HTTPS recipe for this profile.",
          );
        this.lifecycle.setDeadline(operation, recipe.timeoutSeconds);
        const values = resolveTrustRecipeParameters(
          recipe,
          input.recipe.overrides,
          input.secretParameters ?? {},
          input.api.host,
        );
        const endpoint = new URL(
          resolveHttpsGet(recipe.https.material, recipe.parameters, values).url,
        );
        const oauth = resolveTrustRecipeOAuth(recipe, values);
        let tls = input.api.tls;
        if (tls.mode === "retain") {
          if (input.profile === undefined || this.resolveProfileApiCa === undefined)
            throw new KafkaTrustAcquisitionValidationError(
              "Select an API CA before acquiring trust.",
            );
          tls = {
            mode: "custom",
            caPem: await this.resolveProfileApiCa(input.profile.id, input.profile.revision, signal),
          };
        }
        const result = await this.https.fetch({
          definition: recipe.https,
          parameters: recipe.parameters,
          values,
          authentication: input.api.authentication,
          tls,
          ...(input.truststorePassword === undefined ? {} : { password: input.truststorePassword }),
          signal,
        });
        try {
          signal.throwIfAborted();
          const decoded = await this.decodeMaterial(
            result.bytes,
            input.kind,
            result.password,
            signal,
          );
          this.lifecycle.assertCurrent(operation);
          const next: TrustAcquisitionRecord = {
            editor: input.editor,
            createdAtMs: operation.createdAtMs,
            expiresAtMs: operation.expiresAtMs,
            id: operation.id,
            recipe: { id: recipe.id, revision: recipe.revision, source: "https" },
            ...(oauth === undefined ? {} : { oauth }),
            target: {
              host: endpoint.hostname,
              port: Number(endpoint.port || 443),
              origin: endpoint.origin,
            },
            ...(result.password === undefined
              ? {}
              : { password: result.password, passwordTemplateName: recipe.name }),
            material: { ...decoded, label: input.label, templateName: recipe.name },
          };
          operation.candidate = next;
          return this.summary(next);
        } finally {
          result.bytes.fill(0);
        }
      },
      signal,
      requestId,
      input.editor,
    );
  }

  resolve(
    acquisitionId: string,
    expectedKind: ProfileTrustKind,
    editorId?: string,
  ): KafkaResolvedTrustAcquisition {
    const record = this.lifecycle.require(acquisitionId);
    this.lifecycle.assertOwner(record, editorId);
    if (record.material === undefined || record.material.kind !== expectedKind) {
      throw new KafkaTrustAcquisitionIncompleteError(acquisitionId);
    }
    return {
      caPem: record.material.caPem,
      ...(record.access === undefined ? {} : { access: { ...record.access } }),
      ...(record.editor === undefined || record.target.origin !== undefined
        ? {}
        : {
            identity: {
              host: record.target.host,
              port: record.target.port,
              fingerprint: record.target.hostKeyFingerprint,
            },
          }),
      ...(editorId === undefined ? {} : { lifetimeSignal: this.lifecycle.editorSignal(editorId) }),
      id: record.id,
      kind: record.material.kind,
      label: record.material.label,
      material: record.material.encoded,
      ...(record.password === undefined ? {} : { password: record.password }),
    };
  }

  private async completeDirectMaterialFetch(
    input: RemoteTrustMaterialFetchInput,
    operation: PendingAcquisition,
  ): Promise<RemoteTrustAcquisitionSummary> {
    this.lifecycle.acceptIdentity(input, operation);
    if (input.recipe === undefined)
      throw new KafkaTrustAcquisitionValidationError(
        "Select an explicit recipe before acquiring trust material.",
      );
    if ("acquisitionId" in input)
      throw new KafkaTrustAcquisitionValidationError(
        "A recipe acquisition must create a new candidate; existing trust is unchanged.",
      );
    if (input.profile !== undefined && this.resolveProfileBinding === undefined)
      throw new KafkaTrustAcquisitionValidationError("Profile binding resolution is unavailable.");
    const recipe =
      input.profile === undefined
        ? await this.recipes.resolve(
            input.recipe.recipeId,
            input.recipe.recipeRevision,
            operation.controller.signal,
          )
        : (
            await this.resolveProfileBinding!(
              input.profile.id,
              input.profile.revision,
              input.recipe,
              operation.controller.signal,
            )
          ).recipe;
    if (recipe.method !== "ssh")
      throw new KafkaTrustAcquisitionValidationError(
        "This acquisition path requires an SSH recipe.",
      );
    if (recipe.kind !== input.kind)
      throw new KafkaTrustAcquisitionValidationError(
        "The candidate format must match the selected recipe.",
      );
    const plan = resolveTrustRecipeExecution(
      recipe,
      input.recipe.overrides,
      input.secretParameters ?? {},
      input.target.host,
    );
    this.lifecycle.setDeadline(operation, recipe.timeoutSeconds);
    let password: string | undefined;
    if (plan.password.source === "ask") {
      password = input.truststorePassword;
      if (password === undefined) throw new KafkaTrustAcquisitionPasswordError();
    } else if (plan.password.source === "command") {
      if (input.truststorePassword !== undefined)
        throw new KafkaTrustAcquisitionValidationError(
          "This recipe acquires its own truststore password.",
        );
      password = stripSurroundingLineEndings(
        await this.remote.fetchPassword(
          { command: plan.password.command, target: input.target },
          operation.controller.signal,
        ),
      );
    } else if (input.truststorePassword !== undefined) {
      throw new KafkaTrustAcquisitionValidationError(
        "PEM acquisition does not accept a truststore password.",
      );
    }
    if (password !== undefined) this.stagePassword(password, recipe.name, input.target, operation);
    return await this.completeMaterialFetch(input, operation, {
      ...plan,
      name: recipe.name,
      recipe: { id: recipe.id, revision: recipe.revision, source: recipe.ssh.source },
    });
  }

  private async completeMaterialFetch(
    input: RemoteTrustMaterialFetchInput,
    operation: PendingAcquisition,
    plan: TrustRecipeExecution & {
      readonly name: string;
      readonly recipe: NonNullable<RemoteTrustAcquisitionSummary["recipe"]>;
    },
  ): Promise<RemoteTrustAcquisitionSummary> {
    const signal = operation.controller.signal;
    signal?.throwIfAborted();
    const existing = operation.candidate;
    if (existing === undefined) {
      if (input.kind !== "pem") {
        throw new KafkaTrustAcquisitionIncompleteError();
      }
    } else if (!sameTarget(existing.target, input.target)) {
      throw new KafkaTrustAcquisitionValidationError(
        "The SSH target must match the acquisition that owns the password.",
        safeTarget(input.target),
      );
    }
    const selected = { name: plan.name, template: plan.material.value };
    signal?.throwIfAborted();
    const remotePath = this.createRemotePath();
    if (!/^\/tmp\/streamskope-[A-Za-z0-9-]+\.trust$/u.test(remotePath)) {
      throw new KafkaTrustAcquisitionValidationError(
        "The host could not create a bounded remote destination.",
      );
    }
    const command = expandMaterialCommand(selected.template, remotePath, existing?.password);
    const bytes = await this.remote.fetchMaterial(
      plan.material.source === "file"
        ? {
            source: "file",
            remotePath: plan.material.value,
            maximumBytes: REMOTE_TRUST_ACQUISITION_LIMITS.materialBytes,
            target: input.target,
          }
        : plan.material.source === "stdout"
          ? {
              source: "command",
              command: plan.material.value,
              maximumBytes: REMOTE_TRUST_ACQUISITION_LIMITS.materialBytes,
              target: input.target,
            }
          : {
              command,
              maximumBytes: REMOTE_TRUST_ACQUISITION_LIMITS.materialBytes,
              remotePath,
              target: input.target,
            },
      signal,
    );
    signal?.throwIfAborted();
    if (plan.material.source === "stdout" && bytes.length === 0) {
      throw new KafkaTrustAcquisitionMaterialError(
        "empty-command-output",
        safeTarget(input.target),
      );
    }
    const decoded = await this.decodeMaterial(bytes, input.kind, existing?.password, signal);
    const material = decoded.encoded;
    this.lifecycle.assertCurrent(operation);
    const { createdAtMs, expiresAtMs, id } = operation;
    const next: TrustAcquisitionRecord = {
      ...(operation.editor === undefined
        ? {}
        : {
            access: {
              host: input.target.host,
              port: input.target.port,
              username: input.target.username,
              authentication: input.target.authentication?.mode ?? "password",
            },
          }),
      recipe: plan.recipe,
      ...(plan.oauth === undefined ? {} : { oauth: plan.oauth }),
      ...(operation.editor === undefined ? {} : { editor: operation.editor }),
      createdAtMs,
      expiresAtMs,
      id,
      material: {
        ...(decoded.evidence === undefined ? {} : { evidence: decoded.evidence }),
        byteCount: bytes.length,
        caPem: decoded.caPem,
        encoded: material,
        kind: decoded.kind,
        label: input.label,
        templateName: selected.name,
      },
      ...(existing?.password === undefined ? {} : { password: existing.password }),
      ...(existing?.passwordTemplateName === undefined
        ? {}
        : { passwordTemplateName: existing.passwordTemplateName }),
      target: {
        host: input.target.host,
        hostKeyFingerprint: input.target.hostKeyFingerprint,
        port: input.target.port,
      },
    };
    operation.candidate = next;
    return this.summary(next);
  }

  private async decodeMaterial(
    bytes: Uint8Array,
    kind: ProfileTrustKind,
    password: string | undefined,
    signal: AbortSignal,
  ): Promise<Omit<NonNullable<TrustAcquisitionRecord["material"]>, "label" | "templateName">> {
    signal.throwIfAborted();
    if (bytes.length < 1 || bytes.length > REMOTE_TRUST_ACQUISITION_LIMITS.materialBytes)
      throw new KafkaTrustAcquisitionMaterialError();
    const encoded = encodedMaterial(bytes, kind);
    if (encoded.length > PROFILE_LIMITS.trustEncodedCharacters)
      throw new KafkaTrustAcquisitionMaterialError();
    const decoded = await this.trustDecoder.decode(
      { kind, material: encoded, ...(password === undefined ? {} : { password }) },
      signal,
    );
    signal.throwIfAborted();
    if (decoded.kind !== kind) throw new KafkaTrustAcquisitionMaterialError();
    return { ...decoded, byteCount: bytes.length, encoded };
  }

  private stagePassword(
    password: string,
    templateName: string,
    target: RemoteSshTargetInput,
    operation: PendingAcquisition,
  ): void {
    if (
      password.length < 1 ||
      password.length > REMOTE_TRUST_ACQUISITION_LIMITS.passwordCharacters ||
      new TextEncoder().encode(password).length > REMOTE_TRUST_ACQUISITION_LIMITS.commandOutputBytes
    ) {
      throw new KafkaTrustAcquisitionPasswordError();
    }
    this.lifecycle.assertCurrent(operation);
    const { createdAtMs, expiresAtMs, id } = operation;
    const record: TrustAcquisitionRecord = {
      ...(operation.editor === undefined ? {} : { editor: operation.editor }),
      createdAtMs,
      expiresAtMs,
      id,
      password,
      passwordTemplateName: templateName,
      target: {
        host: target.host,
        hostKeyFingerprint: target.hostKeyFingerprint,
        port: target.port,
      },
    };
    operation.candidate = record;
  }

  private summary(record: TrustAcquisitionRecord): RemoteTrustAcquisitionSummary {
    const nowMs = this.now().getTime();
    return {
      ...(record.editor === undefined ? {} : { editor: { ...record.editor } }),
      createdAt: new Date(record.createdAtMs).toISOString(),
      ...(record.recipe === undefined ? {} : { recipe: { ...record.recipe } }),
      ...(record.oauth === undefined ? {} : { oauth: { ...record.oauth } }),
      expiresAt: new Date(record.expiresAtMs).toISOString(),
      id: record.id,
      material:
        record.material === undefined
          ? null
          : {
              ...(record.material.evidence === undefined
                ? {}
                : {
                    evidence: record.material.evidence,
                    expiredCertificates:
                      record.material.evidence.validity === undefined
                        ? record.material.evidence.certificates.some(
                            (certificate) => Date.parse(certificate.validTo) < nowMs,
                          )
                        : Date.parse(record.material.evidence.validity.earliestExpiry) < nowMs,
                    notYetValidCertificates:
                      record.material.evidence.validity === undefined
                        ? record.material.evidence.certificates.some(
                            (certificate) => Date.parse(certificate.validFrom) > nowMs,
                          )
                        : Date.parse(record.material.evidence.validity.latestStart) > nowMs,
                  }),
              byteCount: record.material.byteCount,
              kind: record.material.kind,
              label: record.material.label,
              templateName: record.material.templateName,
            },
      password: {
        present: record.password !== undefined,
        templateName: record.passwordTemplateName ?? null,
      },
      target: { ...record.target },
    };
  }
}
