import { parseConnectCommand, parseConnectResponse } from "./connect-protocol";
import { parseRecoveryCommand, parseRecoveryResponse } from "./recovery-protocol";
import { parseCorrelationCommand, parseCorrelationResponse } from "./correlation-protocol";
import { parseSampleResponse, parseSampleCommand } from "./schema-sample-protocol";
import { parseSchemaInspectionInput, parseSchemaInspection } from "./schema-inspection";
import {
  parseKafkaWriteInput,
  parseKafkaWriteReview,
  parseKafkaWriteOutcome,
} from "./reviewed-writes";
import { parseKafkaOriginalRecord } from "./record-bytes";
import { parseRecordDecodeInput, parseRecordDecodeResult } from "./record-codec";
import { parseKafkaSavedQuery, parseKafkaQueryLibrarySnapshot } from "./query-library";
import { parseKafkaReadCoverage } from "./query-search";
import { parseKafkaFetchRequest } from "./fetch-validation";
export { parseKafkaFetchRequest } from "./fetch-validation";
import { isHostAcknowledgementCommand } from "./host-command-results";
import {
  parseTrustRecipeHostCommand,
  parseTrustRecipeSnapshot,
  parseTrustRecipeReviewResponse,
} from "./trust-recipe-protocol";
import {
  parsePluginHostCommand,
  parsePluginHostEvent,
  parsePluginHostResponse,
} from "./plugin-protocol";
import { parseActivity } from "./activity-validation";
import {
  parseHostTextDocument,
  parseKafkaClusterDiagnosticsSnapshot,
} from "./cluster-diagnostics-validation";
import {
  parseKafkaConsumerGroupDetailSnapshot,
  parseKafkaConsumerGroupIdentity,
  parseKafkaConsumerGroupInventorySnapshot,
} from "./consumer-group-validation";
import {
  parseKafkaLatencyChangedEvent,
  parseKafkaLatencyHistorySnapshot,
  parseKafkaLatencyStartCommand,
  parseKafkaLatencyTextDocument,
} from "./latency-validation";
import {
  parseClusterServiceEndpoints,
  parseProfileCreateInput,
  parseProfileIdPayload,
  parseProfileStoreCapability,
  parseProfileSummary,
  parseProfileTestInput,
  parseProfileUpdateInput,
} from "./profile-validation";
import {
  parseKafkaOperationalPreferenceSnapshot,
  parseKafkaOperationalPreferenceUpdateInput,
} from "./operational-preference-validation";
import {
  parseAcquiredTls,
  parseRemoteTrustResponse,
  parseTrustEditorCommand,
  parseRemoteTrustIdentity,
  parseRemoteTrustCancellation,
  parseRemoteTrustHostKeyDiscovery,
  parseRemoteTrustMaterialFetchInput,
  parseHttpsTrustMaterialFetchInput,
} from "./remote-trust-validation";
import { validateSecureConnectionInput } from "./secure-connection-validation";
import { parseKafkaStreamMonitorSnapshot } from "./stream-monitor-validation";
import {
  parseClusterServiceHostCommand,
  parseClusterServiceHostEvent,
} from "./cluster-service-validation";
import {
  parseKafkaLiveRuleCapability,
  parseKafkaLiveRuleEvaluation,
  parseKafkaRuleNotification,
} from "./live-rule-validation";
import { parseProtocolVersion } from "./protocol-validation";
import { kafkaMessageRetainedBytes, kafkaRawMessageRetainedBytes } from "./message-limits";
import {
  HOST_COMMANDS,
  HOST_ERROR_CODES,
  HOST_ERROR_STAGES,
  HOST_EVENTS,
  KAFKA_MESSAGE_LIMITS,
  SECURE_CONNECTION_LIMITS,
  type HostCommand,
  type HostCommandResponse,
  type HostError,
  type HostSecureConnectionInput,
  type HostEvent,
  type KafkaExploredMessage,
  type OAuthConnectionInput,
  type TlsConnectionInput,
} from "./types";
import { PROFILE_LIMITS } from "./profile-types";
import { parseProfileBindingDetailResult } from "./profile-binding";
import {
  parseKafkaRuleCreatePayload,
  parseKafkaRuleEvaluationInput,
  parseKafkaRuleEvaluationReport,
  parseKafkaRuleIdentityPayload,
  parseKafkaRuleSnapshot,
  parseKafkaRuleUpdatePayload,
} from "./rule-validation";
import { HostContractValidationError } from "./validation-error";
import {
  parseKafkaTopicConfigurationHistorySnapshot,
  parseKafkaTopicConfigurationIdentity,
  parseKafkaTopicConfigurationOperationInput,
  parseKafkaTopicConfigurationSnapshot,
} from "./topic-configuration-validation";
import {
  boundedText,
  boundedUtf8Text,
  declaredValue,
  emptyRecord,
  exactKeys,
  nonNegativeInteger,
  nullableBoundedUtf8Text,
  nullableText,
  optionalText,
  parseBoundedBrokers as parseBrokers,
  record,
  text,
  truth,
  type UnknownRecord,
} from "./validation-primitives";

export { HostContractValidationError } from "./validation-error";
export { validateSecureConnectionInput } from "./secure-connection-validation";

function parseOAuth(value: unknown, path: string): OAuthConnectionInput {
  const oauth = record(value, path);
  exactKeys(oauth, ["clientId", "clientSecret", "scope", "tokenEndpoint"], path);
  return {
    clientId: boundedText(
      oauth.clientId,
      `${path}.clientId`,
      SECURE_CONNECTION_LIMITS.clientIdCharacters,
    ),
    clientSecret: boundedText(
      oauth.clientSecret,
      `${path}.clientSecret`,
      SECURE_CONNECTION_LIMITS.clientSecretCharacters,
    ),
    scope: boundedText(oauth.scope, `${path}.scope`, SECURE_CONNECTION_LIMITS.scopeCharacters),
    tokenEndpoint: boundedText(
      oauth.tokenEndpoint,
      `${path}.tokenEndpoint`,
      SECURE_CONNECTION_LIMITS.tokenEndpointCharacters,
    ),
  };
}

function parseTls(value: unknown, path: string): TlsConnectionInput {
  const tls = record(value, path);
  if (tls.enabled === false) {
    exactKeys(tls, ["enabled"], path);
    return { enabled: false };
  }
  if (tls.enabled !== true) {
    throw new HostContractValidationError(`${path}.enabled`, "must be true or false");
  }
  exactKeys(tls, ["caPem", "enabled"], path);
  return {
    caPem: boundedText(tls.caPem, `${path}.caPem`, SECURE_CONNECTION_LIMITS.caPemCharacters),
    enabled: true,
  };
}

function parseConnection(value: unknown, path: string): HostSecureConnectionInput {
  const connection = record(value, path);
  exactKeys(connection, ["brokers", "name", "oauth", "services", "tls"], path);
  const brokers = parseBrokers(
    connection.brokers,
    `${path}.brokers`,
    SECURE_CONNECTION_LIMITS.brokers,
    SECURE_CONNECTION_LIMITS.brokerCharacters,
  );
  const oauth = Object.hasOwn(connection, "oauth")
    ? parseOAuth(connection.oauth, `${path}.oauth`)
    : undefined;
  const base = {
    brokers,
    name: boundedText(connection.name, `${path}.name`, SECURE_CONNECTION_LIMITS.nameCharacters),
    tls: Object.hasOwn(record(connection.tls, `${path}.tls`), "acquisitionId")
      ? parseAcquiredTls(connection.tls, `${path}.tls`)
      : parseTls(connection.tls, `${path}.tls`),
  };
  const parsed = oauth === undefined ? base : { ...base, oauth };
  const withServices = Object.hasOwn(connection, "services")
    ? {
        ...parsed,
        services: parseClusterServiceEndpoints(connection.services, `${path}.services`),
      }
    : parsed;
  const issue = validateSecureConnectionInput(withServices)[0];
  if (issue !== undefined) {
    throw new HostContractValidationError(`${path}.${issue.field}`, issue.message);
  }
  return withServices;
}

function parseHostError(value: unknown, path: string): HostError {
  const error = record(value, path);
  exactKeys(
    error,
    [
      "activeStateChanged",
      "code",
      "correlationId",
      "recovery",
      "retryable",
      "stage",
      "summary",
      "target",
    ],
    path,
  );
  const target = optionalText(error, "target", path, 2_048);
  const base = {
    activeStateChanged: truth(error.activeStateChanged, `${path}.activeStateChanged`),
    code: declaredValue(error.code, HOST_ERROR_CODES, `${path}.code`),
    correlationId: text(error.correlationId, `${path}.correlationId`, 128),
    recovery: text(error.recovery, `${path}.recovery`, 2_048),
    retryable: truth(error.retryable, `${path}.retryable`),
    stage: declaredValue(error.stage, HOST_ERROR_STAGES, `${path}.stage`),
    summary: text(error.summary, `${path}.summary`, 2_048),
  };
  return target === undefined ? base : { ...base, target };
}

export function parseHostCommand(value: unknown): HostCommand {
  const envelope = record(value, "command");
  exactKeys(envelope, ["command", "id", "payload", "version"], "command");
  const version = parseProtocolVersion(envelope.version, "command.version");
  const id = text(envelope.id, "command.id", 128);
  const command = declaredValue(envelope.command, HOST_COMMANDS, "command.command");
  const clusterServiceCommand =
    parseConnectCommand(command, id, envelope.payload, version) ??
    parseRecoveryCommand(command, id, envelope.payload, version) ??
    parseCorrelationCommand(command, id, envelope.payload, version) ??
    parseSampleCommand(command, id, envelope.payload, version) ??
    parsePluginHostCommand(command, id, envelope.payload, version) ??
    parseTrustEditorCommand(command, id, envelope.payload, version) ??
    parseTrustRecipeHostCommand(command, id, envelope.payload, version) ??
    parseClusterServiceHostCommand(command, id, envelope.payload, version);
  if (clusterServiceCommand !== undefined) {
    return clusterServiceCommand;
  }

  switch (command) {
    case "schemas.inspect":
      return { command, id, version, payload: parseSchemaInspectionInput(envelope.payload) };
    case "records.decode":
      return { command, id, version, payload: parseRecordDecodeInput(envelope.payload) };
    case "writes.review":
      return { command, id, version, payload: parseKafkaWriteInput(envelope.payload) };
    case "writes.apply": {
      const payload = record(envelope.payload, "command.payload");
      exactKeys(payload, ["planId"], "command.payload");
      return {
        command,
        id,
        version,
        payload: { planId: text(payload.planId, "command.payload.planId", 128) },
      };
    }
    case "connection.connect":
    case "connection.test":
      return {
        command,
        id,
        payload: parseConnection(envelope.payload, "command.payload"),
        version,
      };
    case "connection.disconnect":
    case "consumerGroups.list":
    case "clusterDetails.export":
    case "clusterDetails.load":
    case "latency.export":
    case "latency.stop":
    case "messages.stop":
    case "queries.list":
    case "preferences.get":
    case "preferences.reset":
    case "profiles.list":
    case "rules.list":
    case "topics.list":
      return {
        command,
        id,
        payload: emptyRecord(envelope.payload, "command.payload"),
        version,
      };
    case "consumerGroups.load":
      return {
        command,
        id,
        payload: parseKafkaConsumerGroupIdentity(envelope.payload, "command.payload"),
        version,
      };
    case "queries.put": {
      const payload = record(envelope.payload, "command.payload");
      exactKeys(payload, ["query"], "command.payload");
      return { command, id, version, payload: { query: parseKafkaSavedQuery(payload.query) } };
    }
    case "queries.delete": {
      const payload = record(envelope.payload, "command.payload");
      exactKeys(payload, ["id"], "command.payload");
      return { command, id, version, payload: { id: text(payload.id, "command.payload.id", 128) } };
    }
    case "preferences.update":
      return {
        command,
        id,
        payload: parseKafkaOperationalPreferenceUpdateInput(envelope.payload, "command.payload"),
        version,
      };
    case "latency.start":
      return parseKafkaLatencyStartCommand(id, envelope.payload, version);
    case "profiles.create": {
      const payload = record(envelope.payload, "command.payload");
      exactKeys(payload, ["profile"], "command.payload");
      return {
        command,
        id,
        payload: {
          profile: parseProfileCreateInput(payload.profile, "command.payload.profile"),
        },
        version,
      };
    }
    case "profiles.update": {
      const payload = record(envelope.payload, "command.payload");
      exactKeys(payload, ["profile", "profileId"], "command.payload");
      return {
        command,
        id,
        payload: {
          profile: parseProfileUpdateInput(payload.profile, "command.payload.profile"),
          profileId: text(
            payload.profileId,
            "command.payload.profileId",
            PROFILE_LIMITS.idCharacters,
          ),
        },
        version,
      };
    }
    case "profiles.test":
      return {
        command,
        id,
        payload: parseProfileTestInput(envelope.payload, "command.payload"),
        version,
      };
    case "profiles.connect":
    case "profiles.binding.get":
    case "profiles.delete":
      return {
        command,
        id,
        payload: parseProfileIdPayload(envelope.payload, "command.payload"),
        version,
      };

    case "trustAcquisition.hostKey.discover":
      return {
        command,
        id,
        payload: parseRemoteTrustHostKeyDiscovery(envelope.payload, "command.payload"),
        version,
      };
    case "trustAcquisition.capabilities":
      return { command, id, version, payload: emptyRecord(envelope.payload, "command.payload") };

    case "trustAcquisition.https.fetch":
      return {
        command,
        id,
        version,
        payload: parseHttpsTrustMaterialFetchInput(envelope.payload, "command.payload"),
      };
    case "trustAcquisition.material.fetch":
      return {
        command,
        id,
        payload: parseRemoteTrustMaterialFetchInput(envelope.payload, "command.payload"),
        version,
      };
    case "trustAcquisition.cancel":
      return {
        command,
        id,
        payload: parseRemoteTrustCancellation(envelope.payload, "command.payload"),
        version,
      };
    case "trustAcquisition.discard":
      return {
        command,
        id,
        payload: parseRemoteTrustIdentity(envelope.payload, "command.payload"),
        version,
      };
    case "rules.create":
    case "rules.validate":
      return {
        command,
        id,
        payload: parseKafkaRuleCreatePayload(envelope.payload, "command.payload"),
        version,
      };
    case "rules.update":
      return {
        command,
        id,
        payload: parseKafkaRuleUpdatePayload(envelope.payload, "command.payload"),
        version,
      };
    case "rules.delete":
      return {
        command,
        id,
        payload: parseKafkaRuleIdentityPayload(envelope.payload, "command.payload"),
        version,
      };
    case "rules.evaluate":
      return {
        command,
        id,
        payload: parseKafkaRuleEvaluationInput(envelope.payload, "command.payload"),
        version,
      };
    case "messages.start": {
      return {
        command,
        id,
        payload: parseKafkaFetchRequest(envelope.payload, "command.payload"),
        version,
      };
    }
    case "topicConfiguration.load":
    case "topicConfiguration.history":
      return {
        command,
        id,
        payload: parseKafkaTopicConfigurationIdentity(envelope.payload, "command.payload"),
        version,
      };
    case "topicConfiguration.apply":
    case "topicConfiguration.validate":
      return {
        command,
        id,
        payload: parseKafkaTopicConfigurationOperationInput(envelope.payload, "command.payload"),
        version,
      };
  }
  throw new HostContractValidationError("command.command", "is not implemented by this protocol");
}

export function parseHostCommandResponse(value: unknown): HostCommandResponse {
  const envelope = record(value, "response");
  const ok = truth(envelope.ok, "response.ok");
  exactKeys(
    envelope,
    ok ? ["command", "id", "ok", "result", "version"] : ["command", "error", "id", "ok", "version"],
    "response",
  );
  const version = parseProtocolVersion(envelope.version, "response.version");
  const id = text(envelope.id, "response.id", 128);
  const command = declaredValue(envelope.command, HOST_COMMANDS, "response.command");

  if (!ok) {
    return {
      command,
      error: parseHostError(envelope.error, "response.error"),
      id,
      ok: false,
      version,
    };
  }

  const result = record(envelope.result, "response.result");
  const sampleResponse =
    parseConnectResponse(command, id, result, version) ??
    parseRecoveryResponse(command, id, result, version) ??
    parseCorrelationResponse(command, id, result, version) ??
    parseSampleResponse(command, id, version, result);
  if (sampleResponse) return sampleResponse;
  if (command === "schemas.inspect") {
    exactKeys(result, ["correlationId", "inspection"], "response.result");
    return {
      command,
      id,
      version,
      ok: true,
      result: {
        correlationId: text(result.correlationId, "response.result.correlationId", 128),
        inspection: parseSchemaInspection(result.inspection),
      },
    };
  }
  if (command === "records.decode") {
    exactKeys(result, ["correlationId", "decoded"], "response.result");
    return {
      command,
      id,
      version,
      ok: true,
      result: {
        correlationId: text(result.correlationId, "response.result.correlationId", 128),
        decoded: parseRecordDecodeResult(result.decoded),
      },
    };
  }
  if (command === "writes.review") {
    exactKeys(result, ["correlationId", "review"], "response.result");
    return {
      command,
      id,
      version,
      ok: true,
      result: {
        correlationId: text(result.correlationId, "response.result.correlationId", 128),
        review: parseKafkaWriteReview(result.review),
      },
    };
  }
  if (command === "writes.apply") {
    exactKeys(result, ["correlationId", "outcome"], "response.result");
    return {
      command,
      id,
      version,
      ok: true,
      result: {
        correlationId: text(result.correlationId, "response.result.correlationId", 128),
        outcome: parseKafkaWriteOutcome(result.outcome),
      },
    };
  }
  const captureResponse = parsePluginHostResponse(command, id, result, version);
  if (captureResponse !== undefined) return captureResponse;
  const remoteResponse = parseRemoteTrustResponse(command, id, result, version);
  if (remoteResponse !== undefined) return remoteResponse;
  if (command === "queries.list" || command === "queries.put" || command === "queries.delete") {
    exactKeys(result, ["correlationId", "snapshot"], "response.result");
    return {
      command,
      id,
      ok: true,
      version,
      result: {
        correlationId: text(result.correlationId, "response.result.correlationId", 128),
        snapshot: parseKafkaQueryLibrarySnapshot(result.snapshot),
      },
    };
  }
  if (command === "profiles.binding.get")
    return { command, id, ok: true, version, result: parseProfileBindingDetailResult(result) };
  const recipeReview = parseTrustRecipeReviewResponse(command, id, result, version);
  if (recipeReview !== undefined) return recipeReview;
  if (command === "clusterDetails.export" || command === "latency.export") {
    exactKeys(result, ["correlationId", "document"], "response.result");
    return {
      command,
      id,
      ok: true,
      result: {
        correlationId: text(result.correlationId, "response.result.correlationId", 128),
        document:
          command === "clusterDetails.export"
            ? parseHostTextDocument(result.document, "response.result.document")
            : parseKafkaLatencyTextDocument(result.document, "response.result.document"),
      },
      version,
    };
  }
  if (
    command === "preferences.get" ||
    command === "preferences.update" ||
    command === "preferences.reset"
  ) {
    exactKeys(result, ["correlationId", "snapshot"], "response.result");
    return {
      command,
      id,
      ok: true,
      result: {
        correlationId: text(result.correlationId, "response.result.correlationId", 128),
        snapshot: parseKafkaOperationalPreferenceSnapshot(
          result.snapshot,
          "response.result.snapshot",
        ),
      },
      version,
    };
  }
  if (command === "profiles.create" || command === "profiles.update") {
    exactKeys(result, ["correlationId", "profileId"], "response.result");
    return {
      command,
      id,
      ok: true,
      version,
      result: {
        correlationId: text(result.correlationId, "response.result.correlationId", 128),
        ...(result.profileId === undefined
          ? {}
          : { profileId: text(result.profileId, "response.result.profileId", 128) }),
      },
    };
  }
  if (!isHostAcknowledgementCommand(command))
    throw new HostContractValidationError(
      "response.command",
      "requires its command-specific result",
    );
  exactKeys(result, ["correlationId"], "response.result");
  return {
    command,
    id,
    ok: true,
    version,
    result: { correlationId: text(result.correlationId, "response.result.correlationId", 128) },
  };
}

export function parseCorrelatedHostResponse<Command extends HostCommand>(
  value: unknown,
  command: Command,
): HostCommandResponse<Command["command"]> {
  const response = parseHostCommandResponse(value);
  if (response.id !== command.id || response.command !== command.command) {
    throw new HostContractValidationError(
      "response",
      "must match the submitted command identifier and name",
    );
  }
  // The parser validated the result shape, and the checks above established its discriminant.
  return response as HostCommandResponse<Command["command"]>;
}

function parseStringArray(value: unknown, path: string, maximumItems: number): readonly string[] {
  if (!Array.isArray(value) || value.length > maximumItems) {
    throw new HostContractValidationError(path, `must be an array of at most ${maximumItems}`);
  }
  return value.map((item, index) => text(item, `${path}[${index}]`, 512));
}

function parseHeaders(value: unknown, path: string): Readonly<Record<string, string>> {
  const headers = record(value, path);
  if (Object.keys(headers).length > KAFKA_MESSAGE_LIMITS.headerCount) {
    throw new HostContractValidationError(
      path,
      `must contain at most ${KAFKA_MESSAGE_LIMITS.headerCount} headers`,
    );
  }
  return Object.fromEntries(
    Object.entries(headers).map(([key, headerValue]) => [
      boundedUtf8Text(key, `${path}.key`, KAFKA_MESSAGE_LIMITS.headerKeyBytes),
      boundedUtf8Text(headerValue, `${path}.${key}`, KAFKA_MESSAGE_LIMITS.headerValueBytes),
    ]),
  );
}

function parseMessage(value: unknown, path: string): KafkaExploredMessage {
  const message = record(value, path);
  exactKeys(
    message,
    [
      "headers",
      "id",
      "key",
      "offset",
      "originalByteSize",
      "original",
      "partition",
      "payload",
      "payloadTruncated",
      "preview",
      "ruleEvaluation",
      "timestamp",
      "topic",
      "truncated",
    ],
    path,
  );
  const parsed: KafkaExploredMessage = {
    ...(message.original === undefined
      ? {}
      : { original: parseKafkaOriginalRecord(message.original, `${path}.original`) }),
    headers: parseHeaders(message.headers, `${path}.headers`),
    id: text(message.id, `${path}.id`, 256),
    key: nullableBoundedUtf8Text(message.key, `${path}.key`, KAFKA_MESSAGE_LIMITS.messageBytes),
    offset: text(message.offset, `${path}.offset`, 128),
    originalByteSize: nonNegativeInteger(message.originalByteSize, `${path}.originalByteSize`),
    partition: nonNegativeInteger(message.partition, `${path}.partition`),
    payload: nullableBoundedUtf8Text(
      message.payload,
      `${path}.payload`,
      KAFKA_MESSAGE_LIMITS.messageBytes,
    ),
    ...(message.payloadTruncated === undefined
      ? {}
      : { payloadTruncated: truth(message.payloadTruncated, `${path}.payloadTruncated`) }),
    preview: boundedUtf8Text(message.preview, `${path}.preview`, KAFKA_MESSAGE_LIMITS.previewBytes),
    ruleEvaluation: parseKafkaLiveRuleEvaluation(message.ruleEvaluation, `${path}.ruleEvaluation`),
    timestamp: text(message.timestamp, `${path}.timestamp`, 128),
    topic: text(message.topic, `${path}.topic`, 512),
    truncated: truth(message.truncated, `${path}.truncated`),
  };
  if (kafkaRawMessageRetainedBytes(parsed) > KAFKA_MESSAGE_LIMITS.messageBytes) {
    throw new HostContractValidationError(
      path,
      `retained record data must total at most ${KAFKA_MESSAGE_LIMITS.messageBytes} UTF-8 bytes`,
    );
  }
  return parsed;
}

function parseOptionalError(value: UnknownRecord, path: string): HostError | undefined {
  return Object.hasOwn(value, "error") ? parseHostError(value.error, `${path}.error`) : undefined;
}

function withOptionalError<T extends object>(
  base: T,
  error: HostError | undefined,
): T & { readonly error?: HostError } {
  return error === undefined ? base : { ...base, error };
}

export function parseHostEvent(value: unknown): HostEvent {
  const envelope = record(value, "event");
  exactKeys(envelope, ["event", "payload", "sequence", "version"], "event");
  const version = parseProtocolVersion(envelope.version, "event.version");
  const sequence = nonNegativeInteger(envelope.sequence, "event.sequence");
  const event = declaredValue(envelope.event, HOST_EVENTS, "event.event");
  const payload = record(envelope.payload, "event.payload");
  const clusterServiceEvent =
    parsePluginHostEvent(event, payload, sequence, version) ??
    parseClusterServiceHostEvent(event, payload, sequence, version, parseHostError);
  if (clusterServiceEvent !== undefined) {
    return clusterServiceEvent;
  }

  switch (event) {
    case "backend.availability": {
      exactKeys(payload, ["recovery", "state"], "event.payload");
      const recovery = optionalText(payload, "recovery", "event.payload", 2_048);
      return {
        event,
        payload:
          recovery === undefined
            ? {
                state: declaredValue(
                  payload.state,
                  ["ready", "unavailable"],
                  "event.payload.state",
                ),
              }
            : {
                recovery,
                state: declaredValue(
                  payload.state,
                  ["ready", "unavailable"],
                  "event.payload.state",
                ),
              },
        sequence,
        version,
      };
    }
    case "connection.state": {
      exactKeys(payload, ["connectionName", "error", "state"], "event.payload");
      const connectionPayload = withOptionalError(
        {
          connectionName: nullableText(payload.connectionName, "event.payload.connectionName", 256),
          state: declaredValue(
            payload.state,
            ["disconnected", "connecting", "connected", "disconnecting", "failed"],
            "event.payload.state",
          ),
        },
        parseOptionalError(payload, "event.payload"),
      );
      return { event, payload: connectionPayload, sequence, version };
    }
    case "topics.changed": {
      exactKeys(payload, ["error", "refreshedAt", "state", "topics"], "event.payload");
      const topicPayload = withOptionalError(
        {
          refreshedAt: nullableText(payload.refreshedAt, "event.payload.refreshedAt", 128),
          state: declaredValue(
            payload.state,
            ["loading", "ready", "denied", "failed"],
            "event.payload.state",
          ),
          topics: parseStringArray(payload.topics, "event.payload.topics", 100_000),
        },
        parseOptionalError(payload, "event.payload"),
      );
      return { event, payload: topicPayload, sequence, version };
    }
    case "consumerGroups.changed":
      return {
        event,
        payload: parseKafkaConsumerGroupInventorySnapshot(payload, "event.payload", parseHostError),
        sequence,
        version,
      };
    case "consumerGroup.changed":
      return {
        event,
        payload: parseKafkaConsumerGroupDetailSnapshot(payload, "event.payload", parseHostError),
        sequence,
        version,
      };
    case "consumption.state": {
      exactKeys(
        payload,
        [
          "coverage",
          "droppedMessages",
          "error",
          "receivedMessages",
          "request",
          "ruleEvaluation",
          "state",
        ],
        "event.payload",
      );
      const request =
        payload.request === null
          ? null
          : parseKafkaFetchRequest(payload.request, "event.payload.request");
      const consumptionPayload = withOptionalError(
        {
          ...(payload.coverage === undefined
            ? {}
            : { coverage: parseKafkaReadCoverage(payload.coverage, "event.payload.coverage") }),
          droppedMessages: nonNegativeInteger(
            payload.droppedMessages,
            "event.payload.droppedMessages",
          ),
          receivedMessages: nonNegativeInteger(
            payload.receivedMessages,
            "event.payload.receivedMessages",
          ),
          request,
          ruleEvaluation: parseKafkaLiveRuleCapability(
            payload.ruleEvaluation,
            "event.payload.ruleEvaluation",
          ),
          state: declaredValue(
            payload.state,
            [
              "unavailable",
              "loading",
              "fetching",
              "streaming",
              "complete",
              "stopped",
              "empty",
              "failed",
            ],
            "event.payload.state",
          ),
        },
        parseOptionalError(payload, "event.payload"),
      );
      return { event, payload: consumptionPayload, sequence, version };
    }
    case "messages.batch": {
      exactKeys(payload, ["droppedMessages", "messages", "topic"], "event.payload");
      if (
        !Array.isArray(payload.messages) ||
        payload.messages.length > KAFKA_MESSAGE_LIMITS.batchMessages
      ) {
        throw new HostContractValidationError(
          "event.payload.messages",
          `must contain at most ${KAFKA_MESSAGE_LIMITS.batchMessages} messages`,
        );
      }
      const messages = payload.messages.map((message, index) =>
        parseMessage(message, `event.payload.messages[${index}]`),
      );
      const batchBytes = messages.reduce(
        (bytes, message) => bytes + kafkaMessageRetainedBytes(message),
        0,
      );
      if (batchBytes > KAFKA_MESSAGE_LIMITS.batchBytes) {
        throw new HostContractValidationError(
          "event.payload.messages",
          `retained message data must total at most ${KAFKA_MESSAGE_LIMITS.batchBytes} UTF-8 bytes`,
        );
      }
      return {
        event,
        payload: {
          droppedMessages: nonNegativeInteger(
            payload.droppedMessages,
            "event.payload.droppedMessages",
          ),
          messages,
          topic: text(payload.topic, "event.payload.topic", 512),
        },
        sequence,
        version,
      };
    }
    case "activity.recorded":
      return {
        event,
        payload: parseActivity(payload, "event.payload"),
        sequence,
        version,
      };
    case "profiles.changed": {
      exactKeys(payload, ["profiles", "store"], "event.payload");
      if (!Array.isArray(payload.profiles) || payload.profiles.length > PROFILE_LIMITS.profiles) {
        throw new HostContractValidationError(
          "event.payload.profiles",
          `must contain at most ${PROFILE_LIMITS.profiles} profiles`,
        );
      }
      const profiles = payload.profiles.map((profile, index) =>
        parseProfileSummary(profile, `event.payload.profiles[${index}]`),
      );
      const store = parseProfileStoreCapability(payload.store, "event.payload.store");
      if (store.state === "unavailable" && profiles.length > 0) {
        throw new HostContractValidationError(
          "event.payload.profiles",
          "must be empty while the profile store is unavailable",
        );
      }
      return {
        event,
        payload: { profiles, store },
        sequence,
        version,
      };
    }
    case "recipes.changed":
      return {
        event,
        payload: parseTrustRecipeSnapshot(payload, "event.payload"),
        sequence,
        version,
      };
    case "preferences.changed":
      return {
        event,
        payload: parseKafkaOperationalPreferenceSnapshot(payload, "event.payload"),
        sequence,
        version,
      };
    case "rules.changed":
      return {
        event,
        payload: parseKafkaRuleSnapshot(payload, "event.payload"),
        sequence,
        version,
      };
    case "rules.evaluation":
      return {
        event,
        payload: parseKafkaRuleEvaluationReport(payload, "event.payload"),
        sequence,
        version,
      };
    case "rules.notification":
      return {
        event,
        payload: parseKafkaRuleNotification(payload, "event.payload"),
        sequence,
        version,
      };
    case "topicConfiguration.changed":
      return {
        event,
        payload: parseKafkaTopicConfigurationSnapshot(payload, "event.payload", parseHostError),
        sequence,
        version,
      };
    case "topicConfiguration.history":
      return {
        event,
        payload: parseKafkaTopicConfigurationHistorySnapshot(payload, "event.payload"),
        sequence,
        version,
      };
    case "clusterDetails.changed":
      return {
        event,
        payload: parseKafkaClusterDiagnosticsSnapshot(payload, "event.payload", parseHostError),
        sequence,
        version,
      };
    case "latency.changed":
      return parseKafkaLatencyChangedEvent(payload, sequence, version, parseHostError);
    case "latency.history.changed":
      return {
        event,
        payload: parseKafkaLatencyHistorySnapshot(payload, "event.payload"),
        sequence,
        version,
      };
    case "streamMetrics.changed":
      return {
        event,
        payload: parseKafkaStreamMonitorSnapshot(payload, "event.payload", parseKafkaFetchRequest),
        sequence,
        version,
      };
  }
  throw new HostContractValidationError("event.event", "is not implemented by this protocol");
}
