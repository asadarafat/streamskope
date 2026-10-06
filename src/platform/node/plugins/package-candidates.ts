import { randomUUID } from "node:crypto";

import type { PluginPackageInspection } from "../../../plugins/contracts";

import { pluginProblem } from "./problem";

export const PLUGIN_PACKAGE_REVIEW_TTL_MS = 5 * 60_000;
const MAX_CANDIDATES = 8;
interface Candidate {
  readonly value: PluginPackageInspection;
  readonly release: () => void;
  timer: ReturnType<typeof setTimeout> | undefined;
  busy: boolean;
  discardRequested: boolean;
}

/** Opaque reviews bind one exact cached archive; expiry releases its eviction pin. */
export class PluginPackageCandidates {
  private readonly candidates = new Map<string, Candidate>();
  private closed = false;
  create(
    value: Omit<PluginPackageInspection, "candidateId" | "expiresAt">,
    release: () => void,
  ): PluginPackageInspection {
    if (this.closed) throw pluginProblem("The plugin package host is closing.");
    if (this.candidates.size >= MAX_CANDIDATES)
      throw pluginProblem("Too many plugin package reviews are open. Close a review and retry.");
    const candidateId = randomUUID();
    const expiresAt = new Date(Date.now() + PLUGIN_PACKAGE_REVIEW_TTL_MS).toISOString();
    const timer = setTimeout(() => this.discard(candidateId), PLUGIN_PACKAGE_REVIEW_TTL_MS);
    timer.unref();
    const inspection = { ...value, candidateId, expiresAt };
    this.candidates.set(candidateId, {
      value: inspection,
      release,
      timer,
      busy: false,
      discardRequested: false,
    });
    return inspection;
  }
  get(candidateId: string): PluginPackageInspection {
    const candidate = this.candidates.get(candidateId);
    if (
      candidate === undefined ||
      (!candidate.busy && Date.parse(candidate.value.expiresAt) <= Date.now())
    ) {
      this.discard(candidateId);
      throw pluginProblem(
        "This plugin package review expired or was closed. Select and review the package again.",
      );
    }
    return candidate.value;
  }
  begin(candidateId: string): PluginPackageInspection {
    const value = this.get(candidateId);
    const candidate = this.candidates.get(candidateId)!;
    if (candidate.busy)
      throw pluginProblem("This reviewed plugin package is already being installed.");
    candidate.busy = true;
    clearTimeout(candidate.timer);
    candidate.timer = undefined;
    return value;
  }
  failed(candidateId: string): void {
    const candidate = this.candidates.get(candidateId);
    if (candidate === undefined) return;
    candidate.busy = false;
    const remaining = Date.parse(candidate.value.expiresAt) - Date.now();
    if (remaining <= 0 || candidate.discardRequested || this.closed) this.succeeded(candidateId);
    else {
      candidate.timer = setTimeout(() => this.discard(candidateId), remaining);
      candidate.timer.unref();
    }
  }
  succeeded(candidateId: string): void {
    const candidate = this.candidates.get(candidateId);
    if (candidate === undefined) return;
    this.candidates.delete(candidateId);
    clearTimeout(candidate.timer);
    candidate.release();
  }
  discard(candidateId: string): void {
    const candidate = this.candidates.get(candidateId);
    if (candidate === undefined) return;
    if (candidate.busy) {
      candidate.discardRequested = true;
      return;
    }
    this.succeeded(candidateId);
  }
  close(): void {
    this.closed = true;
    for (const id of this.candidates.keys()) this.discard(id);
  }
}
