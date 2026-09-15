import {
  KAFKA_CLUSTER_DIAGNOSTIC_LIMITS,
  parseHostTextDocument,
  parseKafkaClusterDetailsDocument,
  utf8ByteLength,
  type HostTextDocument,
  type KafkaClusterConfigurationIssue,
  type KafkaClusterDetailsDocument,
  type KafkaClusterProfileContext,
  type KafkaConfigurationEntry,
} from "../contracts";

import { ConnectionAttemptSupersededError, NoActiveKafkaConnectionError } from "./session";
import { KafkaClusterDiagnosticsValidationError } from "./cluster-diagnostics-errors";
import type {
  KafkaClusterDiagnosticsLoadResult,
  KafkaClusterDiagnosticsServiceOptions,
  KafkaClusterDiagnosticsSessionPort,
} from "./cluster-diagnostics-types";

interface CachedClusterDetails {
  readonly connectionName: string;
  readonly connectionTarget: string;
  readonly document: KafkaClusterDetailsDocument;
  fresh: boolean;
}

interface StructuredFailure extends Error {
  readonly code?: string;
}

function isStructuredFailure(error: unknown): error is StructuredFailure {
  return error instanceof Error && "code" in error && typeof error.code === "string";
}

function isCancellation(error: unknown, signal: AbortSignal | undefined): boolean {
  return (
    signal?.aborted === true ||
    error instanceof ConnectionAttemptSupersededError ||
    error instanceof NoActiveKafkaConnectionError ||
    (isStructuredFailure(error) && error.code === "CANCELLED") ||
    (error instanceof Error && error.name === "AbortError")
  );
}

function configurationIssue(error: unknown): KafkaClusterConfigurationIssue {
  if (isStructuredFailure(error) && error.code === "AUTHORIZATION_DENIED") {
    return {
      code: "authorization-denied",
      recovery:
        "Grant DESCRIBE_CONFIGS for the selected broker or continue with cluster metadata only.",
      summary: "Broker configuration is not permitted for this connection.",
    };
  }
  return {
    code: "unavailable",
    recovery: "Retry cluster details after verifying broker access and availability.",
    summary:
      isStructuredFailure(error) && error.code === "TIMEOUT"
        ? "Broker configuration timed out."
        : "Broker configuration is temporarily unavailable.",
  };
}

function safeConfiguration(
  entries: readonly KafkaConfigurationEntry[],
): readonly KafkaConfigurationEntry[] {
  return entries
    .map((entry) => ({
      ...entry,
      synonyms: entry.synonyms.map((synonym) => ({
        ...synonym,
        value: entry.isSensitive ? null : synonym.value,
      })),
      value: entry.isSensitive ? null : entry.value,
    }))
    .sort((left, right) => left.name.localeCompare(right.name));
}

function exportFileSegment(document: KafkaClusterDetailsDocument): string {
  const source = document.cluster.clusterId ?? document.profile.name;
  const normalized = source
    .normalize("NFKD")
    .replaceAll(/[^A-Za-z0-9._-]+/gu, "-")
    .replaceAll(/^-+|-+$/gu, "")
    .slice(0, 96);
  return normalized.length === 0 ? "unknown" : normalized;
}

export class KafkaClusterDiagnosticsService {
  private cached: CachedClusterDetails | undefined;
  private generation = 0;
  private readonly now;

  constructor(
    private readonly session: KafkaClusterDiagnosticsSessionPort,
    options: KafkaClusterDiagnosticsServiceOptions = {},
  ) {
    this.now = options.now ?? ((): Date => new Date());
  }

  clear(): void {
    this.generation += 1;
    this.cached = undefined;
  }

  currentDocument(): KafkaClusterDetailsDocument | null {
    return this.cached !== undefined && this.cached.fresh && this.cacheMatchesCurrent()
      ? this.cached.document
      : null;
  }

  exportDocument(): HostTextDocument {
    const document = this.currentDocument();
    if (document === null) {
      throw new KafkaClusterDiagnosticsValidationError(
        "Refresh cluster details for the current connection before exporting.",
        "cluster-details",
      );
    }
    const content = `${JSON.stringify(document, null, 2)}\n`;
    const byteSize = utf8ByteLength(content);
    if (byteSize > KAFKA_CLUSTER_DIAGNOSTIC_LIMITS.exportBytes) {
      throw new KafkaClusterDiagnosticsValidationError(
        `Cluster-detail JSON exceeds the ${String(KAFKA_CLUSTER_DIAGNOSTIC_LIMITS.exportBytes)} byte export limit.`,
        "cluster-details-export",
      );
    }
    return parseHostTextDocument(
      {
        byteSize,
        content,
        fileName: `streamskope-cluster-${exportFileSegment(document)}.json`,
        mediaType: "application/json",
      },
      "clusterDetails.export",
    );
  }

  async load(
    profile: KafkaClusterProfileContext,
    signal?: AbortSignal,
  ): Promise<KafkaClusterDiagnosticsLoadResult> {
    const generation = ++this.generation;
    const context = this.requireMatchingContext(profile);
    if (!this.cacheMatches(context.connectionName, context.connectionTarget)) {
      this.cached = undefined;
    } else if (this.cached !== undefined) {
      this.cached.fresh = false;
    }

    const metadata = await this.session.describeClusterMetadata(signal);
    this.assertCurrent(generation, context.connectionName, context.connectionTarget, signal);
    const brokers = [...metadata.brokers].sort((left, right) => left.nodeId - right.nodeId);
    const controllerIsBroker =
      metadata.controllerId !== null &&
      brokers.some((broker) => broker.nodeId === metadata.controllerId);
    const configurationSourceBrokerId = controllerIsBroker
      ? metadata.controllerId
      : (brokers[0]?.nodeId ?? null);

    let configuration: readonly KafkaConfigurationEntry[] = [];
    let issue: KafkaClusterConfigurationIssue | undefined;
    if (configurationSourceBrokerId === null) {
      issue = {
        code: "no-brokers",
        recovery: "Verify broker registration and retry cluster details.",
        summary: "Kafka reported no brokers, so broker configuration is unavailable.",
      };
    } else {
      try {
        configuration = safeConfiguration(
          await this.session.describeBrokerConfiguration(configurationSourceBrokerId, signal),
        );
      } catch (error) {
        this.assertCurrent(generation, context.connectionName, context.connectionTarget, signal);
        if (isCancellation(error, signal)) {
          throw error;
        }
        issue = configurationIssue(error);
      }
    }
    this.assertCurrent(generation, context.connectionName, context.connectionTarget, signal);

    let document: KafkaClusterDetailsDocument;
    try {
      document = parseKafkaClusterDetailsDocument(
        {
          cluster: {
            brokers,
            clusterId: metadata.clusterId,
            configuration,
            ...(issue === undefined ? {} : { configurationIssue: issue }),
            configurationSourceBrokerId,
            controllerId: metadata.controllerId,
          },
          endpoint: context.connectionTarget,
          fetchedAt: this.now().toISOString(),
          profile,
        },
        "clusterDetails",
      );
    } catch (error) {
      throw new KafkaClusterDiagnosticsValidationError(
        "Kafka returned invalid or oversized cluster diagnostic metadata.",
        context.connectionTarget,
        { cause: error },
      );
    }
    this.assertCurrent(generation, context.connectionName, context.connectionTarget, signal);
    this.cached = {
      connectionName: context.connectionName,
      connectionTarget: context.connectionTarget,
      document,
      fresh: true,
    };
    return {
      document,
      state: issue === undefined ? "ready" : "partial",
    };
  }

  staleDocument(): KafkaClusterDetailsDocument | null {
    return this.cached !== undefined && !this.cached.fresh && this.cacheMatchesCurrent()
      ? this.cached.document
      : null;
  }

  private assertCurrent(
    generation: number,
    connectionName: string,
    connectionTarget: string,
    signal: AbortSignal | undefined,
  ): void {
    const context = this.session.activeConnectionContext();
    if (
      generation !== this.generation ||
      signal?.aborted === true ||
      context === null ||
      context.connectionName !== connectionName ||
      context.connectionTarget !== connectionTarget
    ) {
      throw new ConnectionAttemptSupersededError();
    }
  }

  private cacheMatches(connectionName: string, connectionTarget: string): boolean {
    return (
      this.cached !== undefined &&
      this.cached.connectionName === connectionName &&
      this.cached.connectionTarget === connectionTarget
    );
  }

  private cacheMatchesCurrent(): boolean {
    const context = this.session.activeConnectionContext();
    return context !== null && this.cacheMatches(context.connectionName, context.connectionTarget);
  }

  private requireMatchingContext(profile: KafkaClusterProfileContext): {
    readonly connectionName: string;
    readonly connectionTarget: string;
  } {
    const context = this.session.activeConnectionContext();
    if (context === null) {
      throw new NoActiveKafkaConnectionError("reading cluster details");
    }
    if (
      profile.name !== context.connectionName ||
      profile.brokers.length !== context.connectionBrokers.length ||
      profile.brokers.some((broker, index) => broker !== context.connectionBrokers[index])
    ) {
      throw new KafkaClusterDiagnosticsValidationError(
        "The requested profile does not match the current Kafka connection.",
        profile.id ?? profile.name,
      );
    }
    return context;
  }
}
