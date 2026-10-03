import type { HostCommand, HostCommandBase, HostCommandResponse } from "./types";
import { record, exactKeys, text } from "./validation-primitives";
import {
  environmentTopic,
  parseEnvironmentProfile,
  parseEnvironmentInput,
  parseEnvironmentSnapshot,
  parseEnvironmentReview,
  parseEnvironmentOutcome,
  type EnvironmentInput,
  type EnvironmentProfile,
} from "./environment-snapshot";
export type EnvironmentHostCommand =
  | (HostCommandBase & {
      readonly command: "environments.capture";
      readonly payload: {
        readonly topics: readonly string[];
        readonly profile: EnvironmentProfile | null;
      };
    })
  | (HostCommandBase & {
      readonly command: "environments.review";
      readonly payload: EnvironmentInput;
    })
  | (HostCommandBase & {
      readonly command: "environments.apply";
      readonly payload: { readonly planId: string; readonly confirmation: string };
    });
export function parseEnvironmentCommand(
  command: HostCommand["command"],
  id: string,
  value: unknown,
  version: HostCommand["version"],
): HostCommand | undefined {
  if (command === "environments.capture") {
    const p = record(value, "capture");
    exactKeys(p, ["topics", "profile"], "capture");
    if (!Array.isArray(p.topics) || !p.topics.length || p.topics.length > 20)
      throw new Error("Select 1–20 topics.");
    return {
      command,
      id,
      version,
      payload: {
        topics: p.topics.map(environmentTopic),
        profile: parseEnvironmentProfile(p.profile),
      },
    };
  }
  if (command === "environments.review")
    return { command, id, version, payload: parseEnvironmentInput(value) };
  if (command === "environments.apply") {
    const p = record(value, "apply");
    exactKeys(p, ["planId", "confirmation"], "apply");
    return {
      command,
      id,
      version,
      payload: {
        planId: text(p.planId, "planId", 128),
        confirmation: text(p.confirmation, "confirmation", 256),
      },
    };
  }
  return undefined;
}
export function parseEnvironmentResponse(
  command: HostCommand["command"],
  id: string,
  result: Record<string, unknown>,
  version: HostCommand["version"],
): HostCommandResponse | undefined {
  if (!command.startsWith("environments.")) return undefined;
  const correlationId = text(result.correlationId, "correlationId", 128);
  if (command === "environments.capture") {
    exactKeys(result, ["correlationId", "snapshot"], "result");
    return {
      command,
      id,
      version,
      ok: true,
      result: { correlationId, snapshot: parseEnvironmentSnapshot(result.snapshot) },
    };
  }
  if (command === "environments.review") {
    exactKeys(result, ["correlationId", "review"], "result");
    return {
      command,
      id,
      version,
      ok: true,
      result: { correlationId, review: parseEnvironmentReview(result.review) },
    };
  }
  if (command === "environments.apply") {
    exactKeys(result, ["correlationId", "outcome"], "result");
    return {
      command,
      id,
      version,
      ok: true,
      result: { correlationId, outcome: parseEnvironmentOutcome(result.outcome) },
    };
  }
  return undefined;
}
