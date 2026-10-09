import {
  parseKafkaTopicAnnotation,
  parseKafkaTopicAnnotationSnapshot,
  parseKafkaTopicCatalogSnapshot,
  type KafkaTopicAnnotation,
  type KafkaTopicAnnotationSnapshot,
  type KafkaTopicCatalogSnapshot,
} from "./topic-catalog";
import {
  parseKafkaTopicIdentity,
  parseKafkaTopicName,
  sameKafkaTopicIdentity,
  type KafkaTopicIdentity,
} from "./topic-identity";
import type { HostCommand, HostCommandBase, HostCommandResponse } from "./types";
import { HostContractValidationError } from "./validation-error";
import { emptyRecord, exactKeys, record, text } from "./validation-primitives";

export type TopicCatalogCommand = HostCommandBase &
  (
    | { readonly command: "catalog.list"; readonly payload: Record<string, never> }
    | { readonly command: "catalog.load"; readonly payload: { readonly topic: string } }
    | {
        readonly command: "catalog.put";
        readonly payload: {
          readonly annotation: KafkaTopicAnnotation;
          readonly expected: KafkaTopicAnnotation | null;
        };
      }
    | {
        readonly command: "catalog.delete";
        readonly payload: {
          readonly identity: KafkaTopicIdentity;
          readonly expected: KafkaTopicAnnotation;
        };
      }
  );
export interface TopicCatalogResults {
  readonly "catalog.list": {
    readonly correlationId: string;
    readonly snapshot: KafkaTopicCatalogSnapshot;
  };
  readonly "catalog.load": {
    readonly correlationId: string;
    readonly snapshot: KafkaTopicAnnotationSnapshot;
  };
  readonly "catalog.put": TopicCatalogResults["catalog.load"];
  readonly "catalog.delete": TopicCatalogResults["catalog.load"];
}

export function parseTopicCatalogCommand(
  command: HostCommand["command"],
  id: string,
  value: unknown,
  version: HostCommand["version"],
): TopicCatalogCommand | undefined {
  if (command === "catalog.list")
    return { command, id, version, payload: emptyRecord(value, "catalog") };
  if (command !== "catalog.load" && command !== "catalog.put" && command !== "catalog.delete")
    return undefined;
  const payload = record(value, "catalog");
  if (command === "catalog.load") {
    exactKeys(payload, ["topic"], "catalog");
    return {
      command,
      id,
      version,
      payload: { topic: parseKafkaTopicName(payload.topic, "catalog.topic") },
    };
  }
  exactKeys(
    payload,
    [command === "catalog.put" ? "annotation" : "identity", "expected"],
    "catalog",
  );
  const annotation =
    command === "catalog.put" ? parseKafkaTopicAnnotation(payload.annotation) : undefined;
  const identity = annotation?.identity ?? parseKafkaTopicIdentity(payload.identity);
  const expected =
    command === "catalog.put" && payload.expected === null
      ? null
      : parseKafkaTopicAnnotation(payload.expected, "catalog.expected");
  if (expected !== null && !sameKafkaTopicIdentity(identity, expected.identity))
    throw new HostContractValidationError(
      "catalog.expected",
      "must match the selected topic identity",
    );
  if (command === "catalog.put")
    return { command, id, version, payload: { annotation: annotation!, expected } };
  return { command, id, version, payload: { identity, expected: expected! } };
}

export function parseTopicCatalogResponse(
  command: HostCommand["command"],
  id: string,
  result: Record<string, unknown>,
  version: HostCommand["version"],
): HostCommandResponse | undefined {
  if (
    command !== "catalog.list" &&
    command !== "catalog.load" &&
    command !== "catalog.put" &&
    command !== "catalog.delete"
  )
    return undefined;
  exactKeys(result, ["correlationId", "snapshot"], "catalogResult");
  const correlationId = text(result.correlationId, "catalogResult.correlationId", 128);
  if (command === "catalog.list")
    return {
      command,
      id,
      version,
      ok: true,
      result: { correlationId, snapshot: parseKafkaTopicCatalogSnapshot(result.snapshot) },
    };
  return {
    command,
    id,
    version,
    ok: true,
    result: { correlationId, snapshot: parseKafkaTopicAnnotationSnapshot(result.snapshot) },
  };
}

export function assertTopicCatalogResponse(
  response: HostCommandResponse,
  command: HostCommand,
): void {
  if (
    response.ok &&
    response.command === "catalog.load" &&
    command.command === "catalog.load" &&
    response.result.snapshot.identity.topic !== command.payload.topic
  )
    throw new HostContractValidationError(
      "response.result.snapshot",
      "must match the requested topic",
    );
  if (
    response.ok &&
    ((response.command === "catalog.put" &&
      command.command === "catalog.put" &&
      (!sameKafkaTopicIdentity(
        response.result.snapshot.identity,
        command.payload.annotation.identity,
      ) ||
        JSON.stringify(response.result.snapshot.annotation) !==
          JSON.stringify(command.payload.annotation))) ||
      (response.command === "catalog.delete" &&
        command.command === "catalog.delete" &&
        (!sameKafkaTopicIdentity(response.result.snapshot.identity, command.payload.identity) ||
          response.result.snapshot.annotation !== null)))
  )
    throw new HostContractValidationError(
      "response.result.snapshot",
      "must match the submitted local notes operation",
    );
}

export function isTopicCatalogCommand(command: HostCommand): command is TopicCatalogCommand {
  return (
    command.command === "catalog.list" ||
    command.command === "catalog.load" ||
    command.command === "catalog.put" ||
    command.command === "catalog.delete"
  );
}
