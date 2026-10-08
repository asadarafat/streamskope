import { randomUUID } from "node:crypto";

import type {
  PluginChangeOperation,
  PluginChangePrompt,
  PluginChangeWarning,
} from "../../../plugins/contracts";

import { pluginProblem } from "./problem";

interface ChangeBinding {
  readonly pluginId: string;
  readonly operation: PluginChangeOperation;
  readonly activationId: string;
  readonly candidateId?: string;
  readonly candidateSha256?: string;
}
interface CandidateBinding {
  readonly candidateId: string;
  readonly sha256: string;
}

function bindingFor(
  pluginId: string,
  operation: PluginChangeOperation,
  activationId: string,
  candidate: CandidateBinding | undefined,
): ChangeBinding {
  return {
    pluginId,
    operation,
    activationId,
    ...(candidate === undefined
      ? {}
      : { candidateId: candidate.candidateId, candidateSha256: candidate.sha256 }),
  };
}

interface Confirmation extends ChangeBinding {
  readonly fingerprint: string;
  readonly expires: number;
}

/** One-use, short-lived consent bound to the exact backend, work and verified candidate. */
export class PluginChangeAuthority {
  private readonly confirmations = new Map<string, Confirmation>();

  prepare(
    pluginId: string,
    operation: PluginChangeOperation,
    activationId: string,
    warning: PluginChangeWarning,
    candidate?: CandidateBinding,
  ): PluginChangePrompt {
    const binding = bindingFor(pluginId, operation, activationId, candidate);
    const now = Date.now();
    for (const [token, entry] of this.confirmations) {
      if (entry.expires <= now || entry.pluginId === binding.pluginId)
        this.confirmations.delete(token);
    }
    if (this.confirmations.size >= 32)
      this.confirmations.delete(this.confirmations.keys().next().value!);
    const token = randomUUID();
    this.confirmations.set(token, {
      ...binding,
      fingerprint: JSON.stringify(warning),
      expires: now + 5 * 60_000,
    });
    const verb = binding.operation === "install" ? "update" : binding.operation;
    return {
      pluginId: binding.pluginId,
      token,
      title: `${verb[0]!.toUpperCase()}${verb.slice(1)} plugin?`,
      message: warning.message,
      detail: warning.detail,
      confirmLabel: `Stop capture and ${verb}`,
    };
  }

  async confirm(
    pluginId: string,
    operation: PluginChangeOperation,
    activationId: string | undefined,
    token: string | undefined,
    candidate: CandidateBinding | undefined,
    review: () => Promise<PluginChangeWarning | undefined>,
  ): Promise<void> {
    const confirmation = token === undefined ? undefined : this.confirmations.get(token);
    if (token !== undefined) this.confirmations.delete(token);
    if (activationId === undefined) return;
    const binding = bindingFor(pluginId, operation, activationId, candidate);
    const warning = await review();
    if (warning === undefined) return;
    if (
      confirmation === undefined ||
      confirmation.pluginId !== binding.pluginId ||
      confirmation.operation !== binding.operation ||
      confirmation.activationId !== binding.activationId ||
      confirmation.expires <= Date.now() ||
      confirmation.fingerprint !== JSON.stringify(warning) ||
      confirmation.candidateId !== binding.candidateId ||
      confirmation.candidateSha256 !== binding.candidateSha256
    )
      throw pluginProblem(
        "Plugin work changed or requires confirmation. Review the change again before continuing.",
        "Retry the plugin update or removal and confirm stopping its active work.",
      );
  }

  clear(): void {
    this.confirmations.clear();
  }
}
