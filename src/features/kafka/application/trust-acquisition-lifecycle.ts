import {
  REMOTE_TRUST_ACQUISITION_LIMITS,
  TRUST_RECIPE_LIMITS,
  type RemoteTrustMaterialFetchInput,
  type RemoteSshEndpointInput,
  type RemoteSshHostKeySummary,
} from "../contracts";
import type { AcceptedSshIdentity, TrustAcquisitionEditor } from "../contracts/remote-trust-types";

import {
  KafkaTrustAcquisitionCapacityError,
  KafkaTrustAcquisitionExpiredError,
  KafkaTrustAcquisitionIncompleteError,
  KafkaTrustAcquisitionNotFoundError,
  KafkaTrustAcquisitionValidationError,
  KafkaTrustAcquisitionTimeoutError,
  KafkaTrustAcquisitionCancelledError,
  KafkaTrustAcquisitionIdentityError,
} from "./trust-acquisition-errors";
import type { TrustAcquisitionRecord } from "./trust-acquisition-record";

export interface PendingAcquisition {
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

// Owns editor leases, pending requests and candidate lifetimes independently of transport.
export class TrustAcquisitionLifecycle {
  private readonly editors = new Map<string, TrustEditorState>();
  private readonly acquisitions = new Map<string, TrustAcquisitionRecord>();
  private readonly pending = new Map<string, PendingAcquisition>();

  constructor(
    private readonly createId: () => string,
    private readonly now: () => Date,
  ) {}

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

  reviewIdentity(
    editorId: string,
    target: RemoteSshEndpointInput,
    fingerprint: string,
    startedAtMs: number,
  ): NonNullable<RemoteSshHostKeySummary["review"]> {
    const editor = this.requireEditor(editorId);
    const saved = editor.acceptedIdentity;
    const known =
      saved !== undefined &&
      saved.host.toLowerCase() === target.host.toLowerCase() &&
      saved.port === target.port;
    if (known && saved.fingerprint !== fingerprint)
      throw new KafkaTrustAcquisitionIdentityError(`${target.host}:${target.port}`);
    const expiresAtMs = this.now().getTime() + 120_000;
    const id = this.createId();
    editor.review = {
      id,
      identity: { host: target.host, port: target.port, fingerprint },
      expiresAtMs,
      confirmationRequired: !known,
      elapsedMs: this.now().getTime() - startedAtMs,
    };
    return {
      id,
      expiresAt: new Date(expiresAtMs).toISOString(),
      confirmationRequired: !known,
    };
  }

  acceptIdentity(input: RemoteTrustMaterialFetchInput, operation: PendingAcquisition): void {
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
  }

  editorSignal(editorId: string): AbortSignal {
    return this.requireEditor(editorId).controller.signal;
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

  assertOwner(record: TrustAcquisitionRecord, editorId?: string): void {
    if (record.editor?.id !== editorId)
      throw new KafkaTrustAcquisitionValidationError(
        "The candidate belongs to a different profile editor.",
      );
    if (editorId !== undefined) this.requireEditor(editorId);
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

  private assertCapacity(): void {
    this.pruneExpired();
    if (
      new Set([...this.acquisitions.keys(), ...this.pending.keys()]).size >=
      REMOTE_TRUST_ACQUISITION_LIMITS.acquisitions
    ) {
      throw new KafkaTrustAcquisitionCapacityError(REMOTE_TRUST_ACQUISITION_LIMITS.acquisitions);
    }
  }

  async run<T>(
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

  setDeadline(operation: PendingAcquisition, seconds: number): void {
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

  assertCurrent(operation: PendingAcquisition): void {
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

  require(acquisitionId: string): TrustAcquisitionRecord {
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
}
