import {
  PROFILE_LIMITS,
  REMOTE_TRUST_ACQUISITION_LIMITS,
  TRUST_RECIPE_LIMITS,
  parseRemoteSshHostKeyFingerprint,
  type ProfileTrustKind,
  type RemoteSshHostKeySummary,
  type RemoteSshTargetInput,
  type RemoteTrustAcquisitionSummary,
  type RemoteTrustMaterialFetchInput,
  type RemoteTrustHostKeyDiscoveryInput,
  type RemoteTrustPasswordFetchInput,
} from "../contracts";
import type {
  TrustAcquisitionEditor,
  AcceptedSshIdentity,
  HttpsTrustMaterialFetchInput,
} from "../contracts/remote-trust-types";
import { resolveHttpsGet } from "../contracts/https-trust-validation";

import type { KafkaConnectionTemplateService } from "./connection-template-service";
import type { KafkaProfileTrustDecoder } from "./profile-types";
import {
  resolveTrustRecipeExecution,
  resolveTrustRecipeParameters,
  resolveTrustRecipeOAuth,
  type TrustRecipeExecution,
} from "./trust-recipe-execution";
import {
  KafkaTrustAcquisitionCapacityError,
  KafkaTrustAcquisitionExpiredError,
  KafkaTrustAcquisitionIncompleteError,
  KafkaTrustAcquisitionMaterialError,
  KafkaTrustAcquisitionNotFoundError,
  KafkaTrustAcquisitionPasswordError,
  KafkaTrustAcquisitionValidationError,
  KafkaTrustAcquisitionTimeoutError,
  KafkaTrustAcquisitionCancelledError,
  KafkaTrustAcquisitionIdentityError,
} from "./trust-acquisition-errors";
import type {
  KafkaRemoteTrustPort,
  KafkaResolvedTrustAcquisition,
  KafkaTrustAcquisitionServiceOptions,
} from "./trust-acquisition-types";

interface TrustAcquisitionRecord {
  readonly access?: import("../contracts/remote-ssh-access").RemoteSshAccess;
  readonly recipe?: RemoteTrustAcquisitionSummary["recipe"];
  readonly oauth?: RemoteTrustAcquisitionSummary["oauth"];
  readonly editor?: TrustAcquisitionEditor;
  readonly applied?: boolean;
  readonly createdAtMs: number;
  readonly expiresAtMs: number;
  readonly id: string;
  readonly target: RemoteTrustAcquisitionSummary["target"];
  readonly password?: string;
  readonly passwordTemplateName?: string;
  readonly material?: {
    readonly evidence?: import("../contracts/remote-trust-types").TrustCertificateEvidence;
    readonly byteCount: number;
    readonly caPem: string;
    readonly encoded: string;
    readonly kind: ProfileTrustKind;
    readonly label: string;
    readonly templateName: string;
  };
}

interface PendingAcquisition {
  readonly startedAtMs: number;
  priorElapsedMs: number;
  deadline?: ReturnType<typeof setTimeout>;
  readonly editor?: TrustAcquisitionEditor;
  readonly requestId?: string;
  candidate?: TrustAcquisitionRecord;
  readonly id: string;
  readonly createdAtMs: number;
  readonly expiresAtMs: number;
  readonly controller: AbortController;
}

interface TrustEditorState {
  readonly acceptedIdentity?: AcceptedSshIdentity;
  review?: {
    readonly id: string;
    readonly identity: AcceptedSshIdentity;
    readonly expiresAtMs: number;
    readonly confirmationRequired: boolean;
    readonly elapsedMs: number;
  };
  generation: number;
  expiresAtMs: number;
  readonly controller: AbortController;
}

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
  private readonly editors = new Map<string, TrustEditorState>();
  private readonly acquisitions = new Map<string, TrustAcquisitionRecord>();
  private readonly createId;
  private readonly createRemotePath;
  private readonly pending = new Map<string, PendingAcquisition>();
  private readonly now;
  private readonly resolveProfileBinding;
  private readonly resolveProfileApiCa;
  private readonly https;

  constructor(
    private readonly templates: KafkaConnectionTemplateService,
    private readonly remote: KafkaRemoteTrustPort,
    private readonly trustDecoder: KafkaProfileTrustDecoder,
    options: KafkaTrustAcquisitionServiceOptions = {},
  ) {
    this.createId = options.createId ?? defaultCreateId;
    this.createRemotePath = options.createRemotePath ?? defaultCreateRemotePath;
    this.now = options.now ?? defaultNow;
    this.resolveProfileBinding = options.resolveProfileBinding;
    this.resolveProfileApiCa = options.resolveProfileApiCa;
    this.https = options.https;
  }

  clear(): void {
    for (const operation of this.pending.values()) {
      operation.controller.abort();
      delete operation.candidate;
    }
    this.acquisitions.clear();
    for (const id of this.editors.keys()) this.closeEditor(id);
  }

  openEditor(acceptedIdentity?: AcceptedSshIdentity): TrustAcquisitionEditor {
    this.pruneExpired();
    if (this.editors.size >= REMOTE_TRUST_ACQUISITION_LIMITS.acquisitions)
      throw new KafkaTrustAcquisitionCapacityError(REMOTE_TRUST_ACQUISITION_LIMITS.acquisitions);
    const id = this.createId();
    this.editors.set(id, {
      ...(acceptedIdentity === undefined ? {} : { acceptedIdentity: { ...acceptedIdentity } }),
      generation: 1,
      expiresAtMs: this.now().getTime() + REMOTE_TRUST_ACQUISITION_LIMITS.ttlMs,
      controller: new AbortController(),
    });
    return { id, generation: 1 };
  }

  closeEditor(editorId: string): void {
    this.editors.get(editorId)?.controller.abort(new KafkaTrustAcquisitionCancelledError());
    this.editors.delete(editorId);
    for (const operation of this.pending.values()) {
      if (operation.editor?.id === editorId) {
        operation.controller.abort(new KafkaTrustAcquisitionCancelledError());
        delete operation.candidate;
      }
    }
    for (const [id, candidate] of this.acquisitions) {
      if (candidate.editor?.id === editorId) this.acquisitions.delete(id);
    }
  }

  advanceEditor(editorId: string, generation: number): void {
    const editor = this.requireEditor(editorId);
    if (!Number.isSafeInteger(generation) || generation <= editor.generation)
      throw new KafkaTrustAcquisitionValidationError(
        "Refresh the changed editor before acquiring again.",
      );
    editor.generation = generation;
    delete editor.review;
    editor.expiresAtMs = this.now().getTime() + REMOTE_TRUST_ACQUISITION_LIMITS.ttlMs;
    for (const operation of this.pending.values()) {
      if (operation.editor?.id === editorId) {
        operation.controller.abort(new KafkaTrustAcquisitionCancelledError());
        delete operation.candidate;
      }
    }
    for (const [id, candidate] of this.acquisitions) {
      if (candidate.editor?.id === editorId && candidate.applied !== true)
        this.acquisitions.delete(id);
    }
  }

  apply(acquisitionId: string, editorId: string): void {
    const record = this.require(acquisitionId);
    this.assertOwner(record, editorId);
    if (record.material === undefined)
      throw new KafkaTrustAcquisitionIncompleteError(acquisitionId);
    this.acquisitions.set(acquisitionId, { ...record, applied: true });
  }

  private requireEditor(id: string): TrustEditorState {
    const editor = this.editors.get(id);
    if (editor === undefined)
      throw new KafkaTrustAcquisitionValidationError(
        "This profile editor is closed. Reopen it before acquiring trust.",
      );
    if (this.now().getTime() >= editor.expiresAtMs) {
      this.closeEditor(id);
      throw new KafkaTrustAcquisitionExpiredError(id);
    }
    return editor;
  }

  private assertOwner(record: TrustAcquisitionRecord, editorId?: string): void {
    if (record.editor?.id !== editorId)
      throw new KafkaTrustAcquisitionValidationError(
        "The candidate belongs to a different profile editor.",
      );
    if (editorId !== undefined) this.requireEditor(editorId);
  }

  capabilities(): import("../contracts/remote-trust-types").TrustAcquisitionCapabilities {
    return {
      sshAgent: this.remote.agentStatus?.() ?? "unavailable",
      methods: this.https === undefined ? ["ssh"] : ["ssh", "https"],
    };
  }

  consume(acquisitionId: string): void {
    const operation = this.pending.get(acquisitionId);
    if (operation !== undefined) {
      operation.controller.abort();
      delete operation.candidate;
    }
    this.acquisitions.delete(acquisitionId);
  }

  discard(acquisitionId: string, editorId?: string): void {
    const record = this.acquisitions.get(acquisitionId);
    if (record !== undefined) this.assertOwner(record, editorId);
    const operation = this.pending.get(acquisitionId);
    if (operation !== undefined && operation.editor?.id !== editorId)
      throw new KafkaTrustAcquisitionValidationError(
        "The pending acquisition belongs to a different profile editor.",
      );
    this.consume(acquisitionId);
  }

  cancel(requestId: string, editorId?: string): void {
    for (const operation of this.pending.values()) {
      if (operation.requestId === requestId) {
        if (operation.editor?.id !== editorId)
          throw new KafkaTrustAcquisitionValidationError(
            "The pending acquisition belongs to a different profile editor.",
          );
        operation.controller.abort(new KafkaTrustAcquisitionCancelledError());
        delete operation.candidate;
      }
    }
  }

  discoverHostKey(
    input: RemoteTrustHostKeyDiscoveryInput,
    signal?: AbortSignal,
    requestId?: string,
  ): Promise<RemoteSshHostKeySummary> {
    return this.run(
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
          const editor = this.requireEditor(input.editor.id);
          const saved = editor.acceptedIdentity;
          const known =
            saved !== undefined &&
            saved.host.toLowerCase() === input.target.host.toLowerCase() &&
            saved.port === input.target.port;
          if (known && saved.fingerprint !== fingerprint)
            throw new KafkaTrustAcquisitionIdentityError(
              `${input.target.host}:${input.target.port}`,
            );
          const expiresAtMs = this.now().getTime() + 120_000;
          const id = this.createId();
          editor.review = {
            id,
            identity: { host: input.target.host, port: input.target.port, fingerprint },
            expiresAtMs,
            confirmationRequired: !known,
            elapsedMs: this.now().getTime() - startedAt,
          };
          review = {
            id,
            expiresAt: new Date(expiresAtMs).toISOString(),
            confirmationRequired: !known,
          };
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
    return this.run(
      input.acquisitionId,
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
    return this.run(
      undefined,
      async (operation) => {
        if (this.https === undefined)
          throw new KafkaTrustAcquisitionValidationError(
            "This host does not support HTTPS acquisition.",
          );
        const signal = operation.controller.signal;
        const recipe =
          input.profile === undefined
            ? await this.templates.recipes.resolve(
                input.recipe.recipeId,
                input.recipe.recipeRevision,
                signal,
              )
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
        this.setDeadline(operation, recipe.timeoutSeconds);
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
          this.assertCurrent(operation);
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

  fetchPassword(
    input: RemoteTrustPasswordFetchInput,
    signal?: AbortSignal,
    requestId?: string,
  ): Promise<RemoteTrustAcquisitionSummary> {
    return this.run(
      undefined,
      (operation) => this.completePasswordFetch(input, operation),
      signal,
      requestId,
    );
  }

  resolve(
    acquisitionId: string,
    expectedKind: ProfileTrustKind,
    editorId?: string,
  ): KafkaResolvedTrustAcquisition {
    const record = this.require(acquisitionId);
    this.assertOwner(record, editorId);
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
      ...(editorId === undefined
        ? {}
        : { lifetimeSignal: this.requireEditor(editorId).controller.signal }),
      id: record.id,
      kind: record.material.kind,
      label: record.material.label,
      material: record.material.encoded,
      ...(record.password === undefined ? {} : { password: record.password }),
    };
  }

  private assertCapacity(): void {
    this.pruneExpired();
    if (
      new Set([...this.acquisitions.keys(), ...this.pending.keys()]).size >=
      REMOTE_TRUST_ACQUISITION_LIMITS.acquisitions
    ) {
      throw new KafkaTrustAcquisitionCapacityError(REMOTE_TRUST_ACQUISITION_LIMITS.acquisitions);
    }
  }

  private async completeDirectMaterialFetch(
    input: RemoteTrustMaterialFetchInput,
    operation: PendingAcquisition,
  ): Promise<RemoteTrustAcquisitionSummary> {
    if (input.editor !== undefined) {
      const editor = this.requireEditor(input.editor.id);
      const review = editor.review;
      if (
        review === undefined ||
        review.id !== input.identityId ||
        review.expiresAtMs <= this.now().getTime() ||
        review.identity.host !== input.target.host ||
        review.identity.port !== input.target.port ||
        review.identity.fingerprint !== input.target.hostKeyFingerprint
      )
        throw new KafkaTrustAcquisitionValidationError(
          "The SSH identity review is missing, expired or belongs to another target. Acquire again.",
        );
      if (review.confirmationRequired && input.acceptIdentity !== true)
        throw new KafkaTrustAcquisitionValidationError(
          "Accept the displayed SSH identity before sending credentials.",
        );
      operation.priorElapsedMs = review.elapsedMs;
      this.setDeadline(operation, TRUST_RECIPE_LIMITS.defaultTimeoutSeconds);
      delete editor.review;
    }
    if (input.recipe !== undefined) {
      if (input.acquisitionId !== undefined)
        throw new KafkaTrustAcquisitionValidationError(
          "A recipe acquisition must create a new candidate; existing trust is unchanged.",
        );
      if (input.profile !== undefined && this.resolveProfileBinding === undefined)
        throw new KafkaTrustAcquisitionValidationError(
          "Profile binding resolution is unavailable.",
        );
      const recipe =
        input.profile === undefined
          ? await this.templates.recipes.resolve(
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
      this.setDeadline(operation, recipe.timeoutSeconds);
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
      if (password !== undefined)
        this.stagePassword(password, recipe.name, input.target, operation);
      return await this.completeMaterialFetch(input, operation, {
        ...plan,
        name: recipe.name,
        recipe: { id: recipe.id, revision: recipe.revision, source: recipe.ssh.source },
      });
    }
    if (input.kind === "pem" || input.acquisitionId !== undefined) {
      return this.completeMaterialFetch(input, operation);
    }

    const password = await this.completePasswordFetch({ target: input.target }, operation);
    return this.completeMaterialFetch({ ...input, acquisitionId: password.id }, operation);
  }

  private async completeMaterialFetch(
    input: RemoteTrustMaterialFetchInput,
    operation: PendingAcquisition,
    plan?: TrustRecipeExecution & {
      readonly name: string;
      readonly recipe: NonNullable<RemoteTrustAcquisitionSummary["recipe"]>;
    },
  ): Promise<RemoteTrustAcquisitionSummary> {
    const signal = operation.controller.signal;
    signal?.throwIfAborted();
    const existing =
      input.acquisitionId === undefined && operation.candidate === undefined
        ? undefined
        : (operation.candidate ?? this.require(input.acquisitionId!));
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
    const selected =
      plan === undefined
        ? await this.templates.selected("truststore-fetch", signal)
        : { name: plan.name, template: plan.material.value };
    signal?.throwIfAborted();
    const remotePath = this.createRemotePath();
    if (!/^\/tmp\/streamskope-[A-Za-z0-9-]+\.trust$/u.test(remotePath)) {
      throw new KafkaTrustAcquisitionValidationError(
        "The host could not create a bounded remote destination.",
      );
    }
    const command = expandMaterialCommand(selected.template, remotePath, existing?.password);
    const bytes = await this.remote.fetchMaterial(
      plan?.material.source === "file"
        ? {
            source: "file",
            remotePath: plan.material.value,
            maximumBytes: REMOTE_TRUST_ACQUISITION_LIMITS.materialBytes,
            target: input.target,
          }
        : plan?.material.source === "stdout"
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
    if (plan?.material.source === "stdout" && bytes.length === 0) {
      throw new KafkaTrustAcquisitionMaterialError(
        "empty-command-output",
        safeTarget(input.target),
      );
    }
    const decoded = await this.decodeMaterial(bytes, input.kind, existing?.password, signal);
    const material = decoded.encoded;
    this.assertCurrent(operation);
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
      ...(plan === undefined ? {} : { recipe: plan.recipe }),
      ...(plan?.oauth === undefined ? {} : { oauth: plan.oauth }),
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

  private async completePasswordFetch(
    input: RemoteTrustPasswordFetchInput,
    operation: PendingAcquisition,
  ): Promise<RemoteTrustAcquisitionSummary> {
    const signal = operation.controller.signal;
    signal?.throwIfAborted();
    const selected = await this.templates.selected("truststore-password", signal);
    signal?.throwIfAborted();
    const output = await this.remote.fetchPassword(
      {
        command: selected.template,
        target: input.target,
      },
      signal,
    );
    signal?.throwIfAborted();
    this.stagePassword(stripSurroundingLineEndings(output), selected.name, input.target, operation);
    return this.summary(operation.candidate!);
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
    this.assertCurrent(operation);
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

  private async run<T>(
    acquisitionId: string | undefined,
    work: (operation: PendingAcquisition) => Promise<T>,
    signal?: AbortSignal,
    requestId?: string,
    editor?: TrustAcquisitionEditor,
  ): Promise<T> {
    signal?.throwIfAborted();
    if (editor !== undefined && this.requireEditor(editor.id).generation !== editor.generation)
      throw new KafkaTrustAcquisitionValidationError(
        "The acquisition belongs to an obsolete editor generation.",
      );
    if (
      editor !== undefined &&
      [...this.pending.values()].some((operation) => operation.editor?.id === editor.id)
    )
      throw new KafkaTrustAcquisitionValidationError(
        "This editor already has an operation in progress.",
      );
    if (
      requestId !== undefined &&
      [...this.pending.values()].some((operation) => operation.requestId === requestId)
    ) {
      throw new KafkaTrustAcquisitionValidationError(
        "This request already has an operation in progress.",
      );
    }
    const existing = acquisitionId === undefined ? undefined : this.require(acquisitionId);
    if (existing !== undefined) this.assertOwner(existing, editor?.id);
    if (existing === undefined) this.assertCapacity();
    else if (this.pending.has(existing.id)) {
      throw new KafkaTrustAcquisitionValidationError(
        "This acquisition already has an operation in progress.",
      );
    }
    const createdAtMs = existing?.createdAtMs ?? this.now().getTime();
    const operation: PendingAcquisition = {
      startedAtMs: this.now().getTime(),
      priorElapsedMs: 0,
      ...(editor === undefined ? {} : { editor: { ...editor } }),
      ...(requestId === undefined ? {} : { requestId }),
      id: existing?.id ?? this.createId(),
      createdAtMs,
      expiresAtMs: existing?.expiresAtMs ?? createdAtMs + REMOTE_TRUST_ACQUISITION_LIMITS.ttlMs,
      controller: new AbortController(),
    };
    const abort = (): void => operation.controller.abort(signal?.reason);
    signal?.addEventListener("abort", abort, { once: true });
    this.pending.set(operation.id, operation);
    this.setDeadline(operation, TRUST_RECIPE_LIMITS.defaultTimeoutSeconds);
    try {
      const result = await work(operation);
      this.assertCurrent(operation);
      if (operation.candidate !== undefined)
        this.acquisitions.set(operation.id, operation.candidate);
      return result;
    } finally {
      clearTimeout(operation.deadline);
      delete operation.candidate;
      signal?.removeEventListener("abort", abort);
      this.pending.delete(operation.id);
    }
  }

  private setDeadline(operation: PendingAcquisition, seconds: number): void {
    clearTimeout(operation.deadline);
    const remaining =
      seconds * 1_000 - operation.priorElapsedMs - (this.now().getTime() - operation.startedAtMs);
    if (remaining <= 0) operation.controller.abort(new KafkaTrustAcquisitionTimeoutError());
    else
      operation.deadline = setTimeout(
        () => operation.controller.abort(new KafkaTrustAcquisitionTimeoutError()),
        remaining,
      );
    operation.controller.signal.throwIfAborted();
  }

  private assertCurrent(operation: PendingAcquisition): void {
    operation.controller.signal.throwIfAborted();
    if (
      operation.editor !== undefined &&
      this.requireEditor(operation.editor.id).generation !== operation.editor.generation
    )
      throw new KafkaTrustAcquisitionCancelledError();
    if (this.now().getTime() >= operation.expiresAtMs) {
      this.acquisitions.delete(operation.id);
      throw new KafkaTrustAcquisitionExpiredError(operation.id);
    }
  }

  private pruneExpired(): void {
    const nowMs = this.now().getTime();
    for (const [id, editor] of this.editors) {
      if (nowMs >= editor.expiresAtMs) this.closeEditor(id);
    }
    for (const operation of this.pending.values()) {
      if (nowMs >= operation.expiresAtMs) {
        operation.controller.abort(new KafkaTrustAcquisitionExpiredError(operation.id));
      }
    }
    for (const [id, record] of this.acquisitions) {
      if (nowMs >= record.expiresAtMs) {
        this.acquisitions.delete(id);
        this.pending.get(id)?.controller.abort(new KafkaTrustAcquisitionExpiredError(id));
      }
    }
  }

  private require(acquisitionId: string): TrustAcquisitionRecord {
    const record = this.acquisitions.get(acquisitionId);
    if (record === undefined) {
      throw new KafkaTrustAcquisitionNotFoundError(acquisitionId);
    }
    if (this.now().getTime() >= record.expiresAtMs) {
      this.acquisitions.delete(acquisitionId);
      throw new KafkaTrustAcquisitionExpiredError(acquisitionId);
    }
    return record;
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
