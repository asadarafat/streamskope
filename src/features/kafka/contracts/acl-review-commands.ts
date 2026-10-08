import {
  parseAclChangeInput,
  parseAclChangeReview,
  parseTopicAccessInput,
  parseTopicAccessExplanation,
} from "./acl-review";
import { parseKafkaWriteOutcome } from "./reviewed-writes";
import type { HostCommand, HostCommandBase } from "./types";
import { exactKeys, record, text } from "./validation-primitives";

interface AclReviewDescriptor {
  readonly access: "remote-read" | "remote-write";
  readonly parsePayload: (value: unknown) => unknown;
  readonly parseResult: (value: unknown) => { readonly correlationId: string };
}

/** The ACL review family owns its wire parsers and required connection access together. */
const definitions = Object.freeze({
  "acls.access.explain": Object.freeze({
    access: "remote-read",
    parsePayload: parseTopicAccessInput,
    parseResult: (value: unknown) => {
      const result = record(value, "aclResult");
      exactKeys(result, ["correlationId", "explanation"], "aclResult");
      return {
        correlationId: text(result.correlationId, "correlationId", 128),
        explanation: parseTopicAccessExplanation(result.explanation),
      };
    },
  }),
  "acls.change.review": Object.freeze({
    access: "remote-read",
    parsePayload: parseAclChangeInput,
    parseResult: (value: unknown) => {
      const result = record(value, "aclResult");
      exactKeys(result, ["correlationId", "review"], "aclResult");
      return {
        correlationId: text(result.correlationId, "correlationId", 128),
        review: parseAclChangeReview(result.review),
      };
    },
  }),
  "acls.change.apply": Object.freeze({
    access: "remote-write",
    parsePayload: (value: unknown) => {
      const payload = record(value, "aclApply");
      exactKeys(payload, ["planId", "confirmation"], "aclApply");
      return {
        planId: text(payload.planId, "planId", 128),
        confirmation: text(payload.confirmation, "confirmation", 8192),
      };
    },
    parseResult: (value: unknown) => {
      const result = record(value, "aclResult");
      exactKeys(result, ["correlationId", "outcome"], "aclResult");
      return {
        correlationId: text(result.correlationId, "correlationId", 128),
        outcome: parseKafkaWriteOutcome(result.outcome),
      };
    },
  }),
} satisfies Record<string, AclReviewDescriptor>);

export type AclReviewCommandName = keyof typeof definitions;
export type AclReviewPayloads = {
  readonly [Name in AclReviewCommandName]: Readonly<
    ReturnType<(typeof definitions)[Name]["parsePayload"]>
  >;
};
export type AclReviewResults = {
  readonly [Name in AclReviewCommandName]: Readonly<
    ReturnType<(typeof definitions)[Name]["parseResult"]>
  >;
};
export type AclReviewCommandDescriptors = {
  readonly [Name in AclReviewCommandName]: {
    readonly access: (typeof definitions)[Name]["access"];
    readonly parsePayload: (value: unknown) => AclReviewPayloads[Name];
    readonly parseResult: (value: unknown) => AclReviewResults[Name];
  };
};
export type AclReviewCommand<Name extends AclReviewCommandName = AclReviewCommandName> = {
  readonly [Command in Name]: HostCommandBase & {
    readonly command: Command;
    readonly payload: AclReviewPayloads[Command];
  };
}[Name];
export type AclReviewSuccess<Name extends AclReviewCommandName = AclReviewCommandName> = {
  readonly [Command in Name]: HostCommandBase & {
    readonly command: Command;
    readonly ok: true;
    readonly result: AclReviewResults[Command];
  };
}[Name];

// The mapped view retains payload/result association when indexing by a generic command name.
export const ACL_REVIEW_DESCRIPTORS: AclReviewCommandDescriptors = definitions;

export function isAclReviewCommandName(name: string): name is AclReviewCommandName {
  return Object.hasOwn(definitions, name);
}

export const ACL_REVIEW_COMMANDS = Object.freeze(
  Object.keys(definitions).filter(isAclReviewCommandName),
);

export const ACL_REVIEW_COMMAND_ACCESS = Object.freeze(
  // Object.fromEntries loses its closed key mapping; every entry comes from these descriptors.
  Object.fromEntries(
    ACL_REVIEW_COMMANDS.map((name) => [name, ACL_REVIEW_DESCRIPTORS[name].access]),
  ) as { readonly [Name in AclReviewCommandName]: (typeof definitions)[Name]["access"] },
);

export function isAclReviewCommand(command: HostCommand): command is AclReviewCommand {
  return isAclReviewCommandName(command.command);
}

export function parseAclReviewPayload<Name extends AclReviewCommandName>(
  name: Name,
  value: unknown,
): AclReviewPayloads[Name] {
  return ACL_REVIEW_DESCRIPTORS[name].parsePayload(value);
}

export function parseAclReviewResult<Name extends AclReviewCommandName>(
  name: Name,
  value: unknown,
): AclReviewResults[Name] {
  return ACL_REVIEW_DESCRIPTORS[name].parseResult(value);
}
