import {
  isAclReviewCommandName,
  parseAclReviewPayload,
  parseAclReviewResult,
  type AclReviewCommand,
  type AclReviewCommandName,
  type AclReviewSuccess,
} from "./acl-review-commands";
import type { HostCommand, HostCommandBase, HostCommandResponse } from "./types";

function commandEnvelope<Name extends AclReviewCommandName>(
  command: Name,
  id: string,
  value: unknown,
  version: HostCommandBase["version"],
): AclReviewCommand<Name> {
  return { command, id, version, payload: parseAclReviewPayload(command, value) };
}

function successEnvelope<Name extends AclReviewCommandName>(
  command: Name,
  id: string,
  value: unknown,
  version: HostCommandBase["version"],
): AclReviewSuccess<Name> {
  return { command, id, version, ok: true, result: parseAclReviewResult(command, value) };
}

export function parseAclReviewCommand(
  command: HostCommand["command"],
  id: string,
  value: unknown,
  version: HostCommand["version"],
): HostCommand | undefined {
  return isAclReviewCommandName(command) ? commandEnvelope(command, id, value, version) : undefined;
}

export function parseAclReviewResponse(
  command: HostCommand["command"],
  id: string,
  result: Record<string, unknown>,
  version: HostCommand["version"],
): HostCommandResponse | undefined {
  return isAclReviewCommandName(command)
    ? successEnvelope(command, id, result, version)
    : undefined;
}
