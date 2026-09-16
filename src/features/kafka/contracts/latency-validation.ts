import { utf8ByteLength } from "./message-limits";
import {
  KAFKA_LATENCY_ACKNOWLEDGEMENTS,
  KAFKA_LATENCY_HISTORY_LIMIT,
  KAFKA_LATENCY_ISSUE_STAGES,
  KAFKA_LATENCY_LIMITS,
  KAFKA_LATENCY_SCHEMA,
  KAFKA_LATENCY_STATES,
  type KafkaLatencyBrokerMetric,
  type KafkaLatencyHistoryEntry,
  type KafkaLatencyHistoryMetric,
  type KafkaLatencyHistorySnapshot,
  type KafkaLatencyMetricSummary,
  type KafkaLatencyProbeEvidence,
  type KafkaLatencyProbeIssue,
  type KafkaLatencyProbeRequest,
  type KafkaLatencySnapshot,
} from "./latency-types";
import type { HostTextDocument } from "./text-document-types";
import type { HostCommand, HostError, HostEvent } from "./types";
import { HostContractValidationError } from "./validation-error";
import {
  boundedUtf8Text,
  canonicalIsoTimestamp,
  declaredValue,
  exactKeys,
  nonNegativeInteger,
  record,
  text,
} from "./validation-primitives";

function boundedInteger(value: unknown, path: string, minimum: number, maximum: number): number {
  const parsed = nonNegativeInteger(value, path);
  if (parsed < minimum || parsed > maximum) {
    throw new HostContractValidationError(
      path,
      `must be between ${minimum} and ${maximum}, inclusive`,
    );
  }
  return parsed;
}

function duration(value: unknown, path: string): number {
  if (
    typeof value !== "number" ||
    !Number.isFinite(value) ||
    value < 0 ||
    value > KAFKA_LATENCY_LIMITS.maxDurationMs
  ) {
    throw new HostContractValidationError(
      path,
      `must be a finite duration from 0 through ${KAFKA_LATENCY_LIMITS.maxDurationMs}`,
    );
  }
  return value;
}

function nullableDuration(value: unknown, path: string): number | null {
  return value === null ? null : duration(value, path);
}

function acknowledgements(value: unknown, path: string): -1 | 0 | 1 {
  if (typeof value !== "number" || !KAFKA_LATENCY_ACKNOWLEDGEMENTS.includes(value as -1 | 0 | 1)) {
    throw new HostContractValidationError(
      path,
      `must be one of ${KAFKA_LATENCY_ACKNOWLEDGEMENTS.join(", ")}`,
    );
  }
  return value as -1 | 0 | 1;
}

function metric(value: unknown, path: string): KafkaLatencyMetricSummary {
  const summary = record(value, path);
  exactKeys(summary, ["averageMs", "p95Ms", "samples"], path);
  const parsed: KafkaLatencyMetricSummary = {
    averageMs: duration(summary.averageMs, `${path}.averageMs`),
    p95Ms: duration(summary.p95Ms, `${path}.p95Ms`),
    samples: boundedInteger(
      summary.samples,
      `${path}.samples`,
      1,
      KAFKA_LATENCY_LIMITS.maxMetricSamples,
    ),
  };
  if (parsed.samples === 1 && parsed.p95Ms !== parsed.averageMs) {
    throw new HostContractValidationError(path, "contains inconsistent one-sample statistics");
  }
  return parsed;
}

function nullableMetric(value: unknown, path: string): KafkaLatencyMetricSummary | null {
  return value === null ? null : metric(value, path);
}

function historyMetric(value: unknown, path: string): KafkaLatencyHistoryMetric {
  const summary = record(value, path);
  exactKeys(summary, ["averageMs", "p95Ms"], path);
  return {
    averageMs: duration(summary.averageMs, `${path}.averageMs`),
    p95Ms: duration(summary.p95Ms, `${path}.p95Ms`),
  };
}

function nullableHistoryMetric(value: unknown, path: string): KafkaLatencyHistoryMetric | null {
  return value === null ? null : historyMetric(value, path);
}

function issues(value: unknown, path: string): readonly KafkaLatencyProbeIssue[] {
  if (!Array.isArray(value) || value.length > KAFKA_LATENCY_LIMITS.issues) {
    throw new HostContractValidationError(
      path,
      `must contain at most ${KAFKA_LATENCY_LIMITS.issues} issues`,
    );
  }
  const parsed = value.map((candidate, index) => {
    const issuePath = `${path}[${index}]`;
    const issue = record(candidate, issuePath);
    exactKeys(issue, ["recovery", "stage", "summary"], issuePath);
    return {
      recovery: text(issue.recovery, `${issuePath}.recovery`, 2_048),
      stage: declaredValue(issue.stage, KAFKA_LATENCY_ISSUE_STAGES, `${issuePath}.stage`),
      summary: text(issue.summary, `${issuePath}.summary`, 2_048),
    };
  });
  if (new Set(parsed.map((issue) => issue.stage)).size !== parsed.length) {
    throw new HostContractValidationError(path, "must contain at most one issue per stage");
  }
  return parsed;
}

function brokerMetrics(value: unknown, path: string): readonly KafkaLatencyBrokerMetric[] {
  if (!Array.isArray(value) || value.length > KAFKA_LATENCY_LIMITS.brokerMetrics) {
    throw new HostContractValidationError(
      path,
      `must contain at most ${KAFKA_LATENCY_LIMITS.brokerMetrics} broker metrics`,
    );
  }
  const parsed = value.map((candidate, index) => {
    const metricPath = `${path}[${index}]`;
    const broker = record(candidate, metricPath);
    exactKeys(broker, ["broker", "nodeId", "summary"], metricPath);
    return {
      broker: text(broker.broker, `${metricPath}.broker`, KAFKA_LATENCY_LIMITS.endpointCharacters),
      nodeId: nonNegativeInteger(broker.nodeId, `${metricPath}.nodeId`),
      summary: metric(broker.summary, `${metricPath}.summary`),
    };
  });
  if (new Set(parsed.map((candidate) => candidate.nodeId)).size !== parsed.length) {
    throw new HostContractValidationError(path, "must contain unique broker node IDs");
  }
  if (
    parsed.some((candidate, index) => index > 0 && parsed[index - 1]!.nodeId > candidate.nodeId)
  ) {
    throw new HostContractValidationError(path, "must be sorted by broker node ID");
  }
  return parsed;
}

function identifiers(value: unknown, path: string): readonly string[] {
  if (!Array.isArray(value) || value.length > KAFKA_LATENCY_LIMITS.maxSampleIds) {
    throw new HostContractValidationError(
      path,
      `must contain at most ${KAFKA_LATENCY_LIMITS.maxSampleIds} sample identifiers`,
    );
  }
  const parsed = value.map((candidate, index) =>
    text(candidate, `${path}[${index}]`, KAFKA_LATENCY_LIMITS.identifierCharacters),
  );
  if (new Set(parsed).size !== parsed.length) {
    throw new HostContractValidationError(path, "must contain unique sample identifiers");
  }
  return parsed;
}

export function parseKafkaLatencyProbeRequest(
  value: unknown,
  path = "latency",
): KafkaLatencyProbeRequest {
  const request = record(value, path);
  exactKeys(request, ["acknowledgements", "messageCount", "timeoutMs", "topic"], path);
  return {
    acknowledgements: acknowledgements(request.acknowledgements, `${path}.acknowledgements`),
    messageCount: boundedInteger(
      request.messageCount,
      `${path}.messageCount`,
      KAFKA_LATENCY_LIMITS.minMessageCount,
      KAFKA_LATENCY_LIMITS.maxMessageCount,
    ),
    timeoutMs: boundedInteger(
      request.timeoutMs,
      `${path}.timeoutMs`,
      KAFKA_LATENCY_LIMITS.minTimeoutMs,
      KAFKA_LATENCY_LIMITS.maxTimeoutMs,
    ),
    topic: text(request.topic, `${path}.topic`, KAFKA_LATENCY_LIMITS.topicCharacters),
  };
}

export function parseKafkaLatencyHistorySnapshot(
  value: unknown,
  path = "latency.history",
): KafkaLatencyHistorySnapshot {
  const snapshot = record(value, path);
  exactKeys(snapshot, ["connectionName", "entries"], path);
  if (!Array.isArray(snapshot.entries) || snapshot.entries.length > KAFKA_LATENCY_HISTORY_LIMIT) {
    throw new HostContractValidationError(
      `${path}.entries`,
      `must contain at most ${String(KAFKA_LATENCY_HISTORY_LIMIT)} summaries`,
    );
  }
  const entries: KafkaLatencyHistoryEntry[] = snapshot.entries.map((value, index) => {
    const entryPath = `${path}.entries[${String(index)}]`;
    const entry = record(value, entryPath);
    exactKeys(
      entry,
      [
        "acknowledgements",
        "completedAt",
        "endToEnd",
        "fetch",
        "issueCount",
        "observedMessages",
        "producer",
        "requestedMessages",
        "runId",
        "state",
        "topic",
      ],
      entryPath,
    );
    const requestedMessages = boundedInteger(
      entry.requestedMessages,
      `${entryPath}.requestedMessages`,
      KAFKA_LATENCY_LIMITS.minMessageCount,
      KAFKA_LATENCY_LIMITS.maxMessageCount,
    );
    const observedMessages = boundedInteger(
      entry.observedMessages,
      `${entryPath}.observedMessages`,
      0,
      requestedMessages,
    );
    const issueCount = boundedInteger(
      entry.issueCount,
      `${entryPath}.issueCount`,
      0,
      KAFKA_LATENCY_LIMITS.issues,
    );
    const state = declaredValue(entry.state, ["partial", "ready"], `${entryPath}.state`);
    const parsed: KafkaLatencyHistoryEntry = {
      acknowledgements: acknowledgements(entry.acknowledgements, `${entryPath}.acknowledgements`),
      completedAt: canonicalIsoTimestamp(entry.completedAt, `${entryPath}.completedAt`),
      endToEnd: nullableHistoryMetric(entry.endToEnd, `${entryPath}.endToEnd`),
      fetch: nullableHistoryMetric(entry.fetch, `${entryPath}.fetch`),
      issueCount,
      observedMessages,
      producer: nullableHistoryMetric(entry.producer, `${entryPath}.producer`),
      requestedMessages,
      runId: text(entry.runId, `${entryPath}.runId`, KAFKA_LATENCY_LIMITS.identifierCharacters),
      state,
      topic: text(entry.topic, `${entryPath}.topic`, KAFKA_LATENCY_LIMITS.topicCharacters),
    };
    if (
      (state === "ready" &&
        (issueCount !== 0 ||
          observedMessages !== requestedMessages ||
          parsed.endToEnd === null ||
          parsed.fetch === null ||
          parsed.producer === null)) ||
      (state === "partial" &&
        issueCount === 0 &&
        observedMessages === requestedMessages &&
        parsed.endToEnd !== null &&
        parsed.fetch !== null &&
        parsed.producer !== null)
    ) {
      throw new HostContractValidationError(entryPath, `contains inconsistent ${state} summary`);
    }
    return parsed;
  });
  const connectionName =
    snapshot.connectionName === null
      ? null
      : text(
          snapshot.connectionName,
          `${path}.connectionName`,
          KAFKA_LATENCY_LIMITS.profileNameCharacters,
        );
  if (
    (entries.length === 0) !== (connectionName === null) ||
    new Set(entries.map((entry) => entry.runId)).size !== entries.length
  ) {
    throw new HostContractValidationError(
      path,
      "must contain one connection-owned completion history",
    );
  }
  return { connectionName, entries };
}

export function parseKafkaLatencyStartCommand(
  id: string,
  payload: unknown,
  version: Extract<HostCommand, { readonly command: "latency.start" }>["version"],
): Extract<HostCommand, { readonly command: "latency.start" }> {
  return {
    command: "latency.start",
    id,
    payload: parseKafkaLatencyProbeRequest(payload, "command.payload"),
    version,
  };
}

export function parseKafkaLatencyEvidence(value: unknown, path: string): KafkaLatencyProbeEvidence {
  const evidence = record(value, path);
  exactKeys(
    evidence,
    [
      "acknowledgements",
      "completedAt",
      "connection",
      "endToEnd",
      "fetch",
      "issues",
      "network",
      "observedMessages",
      "producer",
      "requestedMessages",
      "runId",
      "sampleIds",
      "schema",
      "startedAt",
      "topic",
    ],
    path,
  );
  if (evidence.schema !== KAFKA_LATENCY_SCHEMA) {
    throw new HostContractValidationError(`${path}.schema`, `must equal ${KAFKA_LATENCY_SCHEMA}`);
  }
  const connection = record(evidence.connection, `${path}.connection`);
  exactKeys(connection, ["endpoint", "name"], `${path}.connection`);
  const fetch = record(evidence.fetch, `${path}.fetch`);
  exactKeys(fetch, ["perBroker", "summary"], `${path}.fetch`);
  const network = record(evidence.network, `${path}.network`);
  exactKeys(network, ["endpoint", "tcpConnectMs", "tlsHandshakeMs"], `${path}.network`);
  const producer = record(evidence.producer, `${path}.producer`);
  exactKeys(producer, ["semantics", "summary"], `${path}.producer`);

  const requestedMessages = boundedInteger(
    evidence.requestedMessages,
    `${path}.requestedMessages`,
    KAFKA_LATENCY_LIMITS.minMessageCount,
    KAFKA_LATENCY_LIMITS.maxMessageCount,
  );
  const observedMessages = boundedInteger(
    evidence.observedMessages,
    `${path}.observedMessages`,
    0,
    requestedMessages,
  );
  const parsedIssues = issues(evidence.issues, `${path}.issues`);
  const parsedEndToEnd = nullableMetric(evidence.endToEnd, `${path}.endToEnd`);
  const parsedProducer = nullableMetric(producer.summary, `${path}.producer.summary`);
  const parsedFetch = nullableMetric(fetch.summary, `${path}.fetch.summary`);
  const perBroker = brokerMetrics(fetch.perBroker, `${path}.fetch.perBroker`);
  const issueStages = new Set(parsedIssues.map((candidate) => candidate.stage));
  const parsed: KafkaLatencyProbeEvidence = {
    acknowledgements: acknowledgements(evidence.acknowledgements, `${path}.acknowledgements`),
    completedAt: canonicalIsoTimestamp(evidence.completedAt, `${path}.completedAt`),
    connection: {
      endpoint: text(
        connection.endpoint,
        `${path}.connection.endpoint`,
        KAFKA_LATENCY_LIMITS.endpointCharacters,
      ),
      name: text(
        connection.name,
        `${path}.connection.name`,
        KAFKA_LATENCY_LIMITS.profileNameCharacters,
      ),
    },
    endToEnd: parsedEndToEnd,
    fetch: {
      perBroker,
      summary: parsedFetch,
    },
    issues: parsedIssues,
    network: {
      endpoint: text(
        network.endpoint,
        `${path}.network.endpoint`,
        KAFKA_LATENCY_LIMITS.endpointCharacters,
      ),
      tcpConnectMs: nullableDuration(network.tcpConnectMs, `${path}.network.tcpConnectMs`),
      tlsHandshakeMs: nullableDuration(network.tlsHandshakeMs, `${path}.network.tlsHandshakeMs`),
    },
    observedMessages,
    producer: {
      semantics: declaredValue(
        producer.semantics,
        ["acknowledged", "send-completion"] as const,
        `${path}.producer.semantics`,
      ),
      summary: parsedProducer,
    },
    requestedMessages,
    runId: text(evidence.runId, `${path}.runId`, KAFKA_LATENCY_LIMITS.identifierCharacters),
    sampleIds: identifiers(evidence.sampleIds, `${path}.sampleIds`),
    schema: KAFKA_LATENCY_SCHEMA,
    startedAt: canonicalIsoTimestamp(evidence.startedAt, `${path}.startedAt`),
    topic: text(evidence.topic, `${path}.topic`, KAFKA_LATENCY_LIMITS.topicCharacters),
  };

  if (
    Date.parse(parsed.completedAt) < Date.parse(parsed.startedAt) ||
    (parsed.acknowledgements === 0 && parsed.producer.semantics !== "send-completion") ||
    (parsed.acknowledgements !== 0 && parsed.producer.semantics !== "acknowledged") ||
    (parsed.endToEnd === null) !== (parsed.observedMessages === 0) ||
    (parsed.endToEnd !== null && parsed.endToEnd.samples !== parsed.observedMessages) ||
    (parsed.producer.summary !== null &&
      parsed.producer.summary.samples > parsed.requestedMessages) ||
    (parsed.fetch.summary === null) !== (parsed.fetch.perBroker.length === 0) ||
    (parsed.fetch.summary !== null &&
      parsed.fetch.summary.samples !==
        parsed.fetch.perBroker.reduce((total, broker) => total + broker.summary.samples, 0)) ||
    issueStages.has("tcp") !== (parsed.network.tcpConnectMs === null) ||
    issueStages.has("tls") !== (parsed.network.tlsHandshakeMs === null) ||
    issueStages.has("fetch") !== (parsed.fetch.summary === null) ||
    issueStages.has("produce") !==
      (parsed.producer.summary === null ||
        parsed.producer.summary.samples < parsed.requestedMessages) ||
    issueStages.has("end-to-end") !== parsed.observedMessages < parsed.requestedMessages
  ) {
    throw new HostContractValidationError(path, "contains inconsistent latency evidence");
  }
  if (utf8ByteLength(JSON.stringify(parsed)) > KAFKA_LATENCY_LIMITS.snapshotBytes) {
    throw new HostContractValidationError(
      path,
      `must serialize within ${KAFKA_LATENCY_LIMITS.snapshotBytes} UTF-8 bytes`,
    );
  }
  return parsed;
}

export function parseKafkaLatencySnapshot(
  value: unknown,
  path: string,
  parseError: (value: unknown, path: string) => HostError,
): KafkaLatencySnapshot {
  const snapshot = record(value, path);
  exactKeys(snapshot, ["error", "evidence", "request", "state"], path);
  const state = declaredValue(snapshot.state, KAFKA_LATENCY_STATES, `${path}.state`);
  if (state === "unavailable" || state === "idle") {
    if (
      snapshot.evidence !== null ||
      snapshot.request !== null ||
      Object.hasOwn(snapshot, "error")
    ) {
      throw new HostContractValidationError(path, `contains inconsistent ${state} state`);
    }
    return { evidence: null, request: null, state };
  }
  if (state === "running") {
    if (snapshot.evidence !== null || Object.hasOwn(snapshot, "error")) {
      throw new HostContractValidationError(path, "contains inconsistent running state");
    }
    return {
      evidence: null,
      request: parseKafkaLatencyProbeRequest(snapshot.request, `${path}.request`),
      state,
    };
  }
  if (state === "cancelled" || state === "failed") {
    if (
      snapshot.evidence !== null ||
      snapshot.request === null ||
      !Object.hasOwn(snapshot, "error")
    ) {
      throw new HostContractValidationError(path, `contains inconsistent ${state} state`);
    }
    return {
      error: parseError(snapshot.error, `${path}.error`),
      evidence: null,
      request: parseKafkaLatencyProbeRequest(snapshot.request, `${path}.request`),
      state,
    };
  }
  if (snapshot.request !== null || snapshot.evidence === null || Object.hasOwn(snapshot, "error")) {
    throw new HostContractValidationError(path, `contains inconsistent ${state} state`);
  }
  const evidence = parseKafkaLatencyEvidence(snapshot.evidence, `${path}.evidence`);
  if (
    (state === "ready" &&
      (evidence.issues.length > 0 ||
        evidence.observedMessages !== evidence.requestedMessages ||
        evidence.producer.summary?.samples !== evidence.requestedMessages ||
        evidence.endToEnd?.samples !== evidence.requestedMessages ||
        evidence.fetch.summary === null ||
        evidence.network.tcpConnectMs === null ||
        evidence.network.tlsHandshakeMs === null)) ||
    (state === "partial" && evidence.issues.length === 0)
  ) {
    throw new HostContractValidationError(path, `contains inconsistent ${state} evidence`);
  }
  return { evidence, request: null, state };
}

export function parseKafkaLatencyChangedEvent(
  value: unknown,
  sequence: number,
  version: Extract<HostEvent, { readonly event: "latency.changed" }>["version"],
  parseError: (value: unknown, path: string) => HostError,
): Extract<HostEvent, { readonly event: "latency.changed" }> {
  return {
    event: "latency.changed",
    payload: parseKafkaLatencySnapshot(value, "event.payload", parseError),
    sequence,
    version,
  };
}

export function parseKafkaLatencyTextDocument(value: unknown, path: string): HostTextDocument {
  const document = record(value, path);
  exactKeys(document, ["byteSize", "content", "fileName", "mediaType"], path);
  const content = boundedUtf8Text(
    document.content,
    `${path}.content`,
    KAFKA_LATENCY_LIMITS.exportBytes,
  );
  const byteSize = nonNegativeInteger(document.byteSize, `${path}.byteSize`);
  if (byteSize !== utf8ByteLength(content)) {
    throw new HostContractValidationError(`${path}.byteSize`, "must equal the content UTF-8 size");
  }
  const fileName = text(
    document.fileName,
    `${path}.fileName`,
    KAFKA_LATENCY_LIMITS.fileNameCharacters,
  );
  if (
    !fileName.endsWith(".json") ||
    fileName.includes("/") ||
    fileName.includes("\\") ||
    fileName === ".json"
  ) {
    throw new HostContractValidationError(`${path}.fileName`, "must be a safe JSON file name");
  }
  if (document.mediaType !== "application/json") {
    throw new HostContractValidationError(`${path}.mediaType`, "must equal application/json");
  }
  let decoded: unknown;
  try {
    decoded = JSON.parse(content) as unknown;
  } catch {
    throw new HostContractValidationError(`${path}.content`, "must contain valid JSON");
  }
  const parsed = parseKafkaLatencyEvidence(decoded, `${path}.content`);
  if (`${JSON.stringify(parsed, null, 2)}\n` !== content) {
    throw new HostContractValidationError(
      `${path}.content`,
      "must use the canonical indented latency-evidence representation",
    );
  }
  return {
    byteSize,
    content,
    fileName,
    mediaType: "application/json",
  };
}
