import {
  KAFKA_TOPIC_CONFIGURATION_LIMITS,
  KAFKA_TOPIC_CONFIGURATION_PRESETS,
  KAFKA_TOPIC_CONFIGURATION_REDACTION,
  parseKafkaTopicConfigurationOperationInput,
  parseKafkaTopicConfigurationTopic,
  type KafkaTopicConfigurationAction,
  type KafkaTopicConfigurationChange,
  type KafkaTopicConfigurationEntry,
  type KafkaTopicConfigurationHistoryChange,
  type KafkaTopicConfigurationHistoryEntry,
  type KafkaTopicConfigurationHistorySnapshot,
  type KafkaTopicConfigurationHistoryStoreCapability,
  type KafkaTopicConfigurationOperationInput,
} from "../contracts";

import { cloneKafkaTopicConfigurationHistoryDocument } from "./in-memory-topic-configuration-history-store";
import { ConnectionAttemptSupersededError, NoActiveKafkaConnectionError } from "./session";
import { KafkaTopicConfigurationValidationError } from "./topic-configuration-errors";
import type {
  KafkaTopicConfigurationConnectionContext,
  KafkaTopicConfigurationHistoryDocument,
  KafkaTopicConfigurationHistoryRead,
  KafkaTopicConfigurationHistoryStore,
  KafkaTopicConfigurationOperationResult,
  KafkaTopicConfigurationServiceOptions,
  KafkaTopicConfigurationSessionPort,
  KafkaTopicConfigurationView,
} from "./topic-configuration-types";

const HISTORY_UNAVAILABLE_RECOVERY =
  "Preserve the history file, restore a known-good copy, or move it aside after confirming a backup.";
const REFRESH_WARNING =
  "Kafka applied the named changes, but refreshed configuration is unavailable.";

function defaultHistoryId(): string {
  return globalThis.crypto.randomUUID();
}

function isAbort(error: unknown): boolean {
  return error instanceof Error && error.name === "AbortError";
}

function isObsoleteOperation(error: unknown): boolean {
  return (
    isAbort(error) ||
    error instanceof ConnectionAttemptSupersededError ||
    error instanceof NoActiveKafkaConnectionError
  );
}

function isStructuredSafeError(error: unknown): error is Error & {
  readonly code: string;
  readonly recovery: string;
} {
  return (
    error instanceof Error &&
    "code" in error &&
    typeof error.code === "string" &&
    "recovery" in error &&
    typeof error.recovery === "string"
  );
}

function safeFailureDetail(error: unknown): string {
  const detail = isStructuredSafeError(error)
    ? error.message
    : "The topic configuration operation failed.";
  return detail.slice(0, KAFKA_TOPIC_CONFIGURATION_LIMITS.historyErrorCharacters);
}

function safeEntries(
  entries: readonly KafkaTopicConfigurationEntry[],
): readonly KafkaTopicConfigurationEntry[] {
  if (entries.length > KAFKA_TOPIC_CONFIGURATION_LIMITS.configurationEntries) {
    throw new KafkaTopicConfigurationValidationError(
      "Kafka returned too many topic configuration entries.",
      undefined,
      "VALIDATION",
    );
  }
  const names = new Set<string>();
  return entries
    .map((entry) => {
      if (names.has(entry.name)) {
        throw new KafkaTopicConfigurationValidationError(
          `Kafka returned duplicate topic configuration metadata for ${entry.name}.`,
          entry.name,
          "VALIDATION",
        );
      }
      names.add(entry.name);
      if (entry.synonyms.length > KAFKA_TOPIC_CONFIGURATION_LIMITS.synonymsPerEntry) {
        throw new KafkaTopicConfigurationValidationError(
          `Kafka returned too many synonyms for ${entry.name}.`,
          entry.name,
          "VALIDATION",
        );
      }
      return {
        ...entry,
        synonyms: entry.synonyms.map((synonym) => ({
          ...synonym,
          value: entry.isSensitive ? null : synonym.value,
        })),
        value: entry.isSensitive ? null : entry.value,
      };
    })
    .sort((left, right) => left.name.localeCompare(right.name));
}

function emptyHistoryDocument(): KafkaTopicConfigurationHistoryDocument {
  return { entries: [] };
}

export class KafkaTopicConfigurationService {
  private readonly createHistoryId;
  private historyDocument = emptyHistoryDocument();
  private historyFailure: unknown;
  private historyLoaded = false;
  private historyLoad: Promise<void> | undefined;
  private mutationTail: Promise<void> = Promise.resolve();
  private readonly now;

  constructor(
    private readonly session: KafkaTopicConfigurationSessionPort,
    private readonly historyStore: KafkaTopicConfigurationHistoryStore,
    options: KafkaTopicConfigurationServiceOptions = {},
  ) {
    this.createHistoryId = options.createHistoryId ?? defaultHistoryId;
    this.now = options.now ?? ((): Date => new Date());
  }

  apply(
    input: KafkaTopicConfigurationOperationInput,
    signal?: AbortSignal,
  ): Promise<KafkaTopicConfigurationOperationResult> {
    return this.enqueue(() => this.completeOperation("apply", input, signal));
  }

  async history(
    topicInput: string,
    signal?: AbortSignal,
  ): Promise<KafkaTopicConfigurationHistoryRead> {
    const topic = this.validTopic(topicInput);
    const context = this.requireContext("reading topic configuration history");
    await this.ensureHistoryLoaded(signal);
    return {
      ...(this.historyFailure === undefined ? {} : { failure: this.historyFailure }),
      snapshot: this.historySnapshot(topic, context),
    };
  }

  load(topicInput: string, signal?: AbortSignal): Promise<KafkaTopicConfigurationView> {
    const topic = this.validTopic(topicInput);
    const context = this.requireContext("reading topic configuration");
    return this.describe(topic, context, signal);
  }

  validate(
    input: KafkaTopicConfigurationOperationInput,
    signal?: AbortSignal,
  ): Promise<KafkaTopicConfigurationOperationResult> {
    return this.enqueue(() => this.completeOperation("validate", input, signal));
  }

  private async commitHistory(
    entry: KafkaTopicConfigurationHistoryEntry,
    signal?: AbortSignal,
  ): Promise<void> {
    await this.ensureHistoryLoaded(signal);
    if (this.historyFailure !== undefined) {
      return;
    }
    const next = {
      entries: [entry, ...this.historyDocument.entries].slice(
        0,
        KAFKA_TOPIC_CONFIGURATION_LIMITS.historyEntries,
      ),
    };
    try {
      await this.historyStore.commit(next, signal);
      this.historyDocument = cloneKafkaTopicConfigurationHistoryDocument(next);
    } catch (error) {
      if (isAbort(error)) {
        throw error;
      }
      this.historyFailure = new Error("Topic configuration history is unavailable.", {
        cause: error,
      });
    }
  }

  private async completeOperation(
    action: KafkaTopicConfigurationAction,
    input: KafkaTopicConfigurationOperationInput,
    signal?: AbortSignal,
  ): Promise<KafkaTopicConfigurationOperationResult> {
    const parsed = this.validOperation(input);
    this.assertPresetAttribution(parsed);
    const context = this.requireContext(
      `${action === "apply" ? "applying" : "validating"} topic configuration`,
    );
    let baseline: KafkaTopicConfigurationView | undefined;
    try {
      baseline = await this.describe(parsed.topic, context, signal);
      this.assertWritable(parsed.changes, baseline.entries, parsed.topic);
      await this.session.alterTopicConfiguration(
        parsed.topic,
        parsed.changes,
        action === "validate",
        signal,
      );
    } catch (error) {
      if (!isObsoleteOperation(error)) {
        await this.recordHistory(
          action,
          parsed,
          context,
          baseline?.entries ?? [],
          false,
          {
            error: safeFailureDetail(error),
          },
          signal,
        );
      }
      throw error;
    }

    let configuration = baseline;
    let refreshFailure: unknown;
    let warning: string | undefined;
    if (action === "apply") {
      try {
        configuration = await this.describe(parsed.topic, context, signal);
      } catch (error) {
        refreshFailure = error;
        warning = REFRESH_WARNING;
      }
    }
    await this.recordHistory(
      action,
      parsed,
      context,
      baseline.entries,
      true,
      warning === undefined ? {} : { warning },
      signal,
    );
    const history = this.historySnapshot(parsed.topic, context);
    return {
      configuration,
      history,
      ...(this.historyFailure === undefined ? {} : { historyFailure: this.historyFailure }),
      ...(refreshFailure === undefined ? {} : { refreshFailure }),
    };
  }

  private async describe(
    topic: string,
    context: KafkaTopicConfigurationConnectionContext,
    signal?: AbortSignal,
  ): Promise<KafkaTopicConfigurationView> {
    const entries = safeEntries(await this.session.describeTopicConfiguration(topic, signal));
    const current = this.requireContext("reading topic configuration");
    if (
      current.connectionName !== context.connectionName ||
      current.connectionTarget !== context.connectionTarget
    ) {
      throw new NoActiveKafkaConnectionError("reading topic configuration");
    }
    return {
      ...context,
      entries,
      refreshedAt: this.now().toISOString(),
      topic,
    };
  }

  private enqueue<T>(operation: () => Promise<T>): Promise<T> {
    const result = this.mutationTail.then(operation, operation);
    this.mutationTail = result.then(
      () => undefined,
      () => undefined,
    );
    return result;
  }

  private async ensureHistoryLoaded(signal?: AbortSignal): Promise<void> {
    if (this.historyLoaded) {
      return;
    }
    if (this.historyLoad !== undefined) {
      return this.historyLoad;
    }
    const operation = this.loadHistory(signal);
    this.historyLoad = operation;
    operation.then(
      () => {
        if (this.historyLoad === operation) {
          this.historyLoad = undefined;
        }
      },
      () => {
        if (this.historyLoad === operation) {
          this.historyLoad = undefined;
        }
      },
    );
    return operation;
  }

  private async loadHistory(signal?: AbortSignal): Promise<void> {
    try {
      const document = await this.historyStore.load(signal);
      this.historyDocument = cloneKafkaTopicConfigurationHistoryDocument(
        document ?? emptyHistoryDocument(),
      );
    } catch (error) {
      if (isAbort(error)) {
        throw error;
      }
      this.historyDocument = emptyHistoryDocument();
      this.historyFailure = new Error("Topic configuration history is unavailable.", {
        cause: error,
      });
    }
    this.historyLoaded = true;
  }

  private historyCapability(): KafkaTopicConfigurationHistoryStoreCapability {
    const capability = this.historyStore.capability();
    return this.historyFailure === undefined
      ? capability
      : {
          durability: capability.durability,
          recovery: capability.recovery ?? HISTORY_UNAVAILABLE_RECOVERY,
          state: "unavailable",
        };
  }

  private historyChanges(
    changes: readonly KafkaTopicConfigurationChange[],
    baseline: readonly KafkaTopicConfigurationEntry[],
  ): readonly KafkaTopicConfigurationHistoryChange[] {
    const entries = new Map(baseline.map((entry) => [entry.name, entry]));
    return changes.map((change) => {
      const previous = entries.get(change.name);
      const isSensitive = change.isSensitive || previous?.isSensitive === true;
      const from =
        previous === undefined
          ? undefined
          : isSensitive
            ? KAFKA_TOPIC_CONFIGURATION_REDACTION
            : previous.value;
      return {
        ...(from === undefined ? {} : { from }),
        isSensitive,
        name: change.name,
        to: isSensitive ? KAFKA_TOPIC_CONFIGURATION_REDACTION : change.value,
        wasDefault: previous?.isDefault ?? false,
      };
    });
  }

  private historySnapshot(
    topic: string,
    context: KafkaTopicConfigurationConnectionContext,
  ): KafkaTopicConfigurationHistorySnapshot {
    const capability = this.historyCapability();
    const entries =
      capability.state === "unavailable"
        ? []
        : this.historyDocument.entries
            .filter(
              (entry) =>
                entry.topic === topic &&
                entry.connectionName === context.connectionName &&
                entry.connectionTarget === context.connectionTarget,
            )
            .slice(0, KAFKA_TOPIC_CONFIGURATION_LIMITS.historyVisibleEntries)
            .map((entry) => ({
              ...entry,
              changes: entry.changes.map((change) => ({ ...change })),
            }));
    return {
      connectionName: context.connectionName,
      entries,
      store: capability,
      topic,
    };
  }

  private async recordHistory(
    action: KafkaTopicConfigurationAction,
    input: KafkaTopicConfigurationOperationInput,
    context: KafkaTopicConfigurationConnectionContext,
    baseline: readonly KafkaTopicConfigurationEntry[],
    success: boolean,
    detail: { readonly error?: string; readonly warning?: string },
    signal?: AbortSignal,
  ): Promise<void> {
    await this.commitHistory(
      {
        action,
        at: this.now().toISOString(),
        changes: this.historyChanges(input.changes, baseline),
        connectionName: context.connectionName,
        connectionTarget: context.connectionTarget,
        ...(detail.error === undefined ? {} : { error: detail.error }),
        id: this.createHistoryId(),
        ...(input.presetId === undefined ? {} : { presetId: input.presetId }),
        success,
        topic: input.topic,
        ...(detail.warning === undefined ? {} : { warning: detail.warning }),
      },
      signal,
    );
  }

  private assertWritable(
    changes: readonly KafkaTopicConfigurationChange[],
    baseline: readonly KafkaTopicConfigurationEntry[],
    topic: string,
  ): void {
    const readOnly = new Set(baseline.filter((entry) => entry.readOnly).map((entry) => entry.name));
    const rejected = changes
      .filter((change) => readOnly.has(change.name))
      .map((change) => change.name);
    if (rejected.length > 0) {
      throw new KafkaTopicConfigurationValidationError(
        `Read-only topic configurations cannot be changed: ${rejected.join(", ")}.`,
        topic,
      );
    }
  }

  private assertPresetAttribution(input: KafkaTopicConfigurationOperationInput): void {
    if (input.presetId === undefined) {
      return;
    }
    const preset = KAFKA_TOPIC_CONFIGURATION_PRESETS.find(
      (candidate) => candidate.id === input.presetId,
    );
    const values =
      preset === undefined
        ? new Map<string, string>()
        : new Map<string, string>(preset.changes.map((change) => [change.name, change.value]));
    const misleading = input.changes.find((change) => values.get(change.name) !== change.value);
    if (misleading !== undefined) {
      throw new KafkaTopicConfigurationValidationError(
        `Preset ${input.presetId} does not define ${misleading.name} with the submitted value.`,
        input.topic,
        "VALIDATION",
      );
    }
  }

  private requireContext(operation: string): KafkaTopicConfigurationConnectionContext {
    const context = this.session.activeConnectionContext();
    if (context === null) {
      throw new NoActiveKafkaConnectionError(operation);
    }
    return context;
  }

  private validOperation(
    input: KafkaTopicConfigurationOperationInput,
  ): KafkaTopicConfigurationOperationInput {
    try {
      return parseKafkaTopicConfigurationOperationInput(input, "topicConfiguration");
    } catch (error) {
      throw new KafkaTopicConfigurationValidationError(
        error instanceof Error
          ? `The topic configuration request is invalid: ${error.message}.`
          : "The topic configuration request is invalid.",
        typeof input.topic === "string" ? input.topic : undefined,
        "VALIDATION",
        { cause: error },
      );
    }
  }

  private validTopic(topic: string): string {
    try {
      return parseKafkaTopicConfigurationTopic(topic, "topicConfiguration.topic");
    } catch (error) {
      throw new KafkaTopicConfigurationValidationError(
        error instanceof Error
          ? `The topic configuration request is invalid: ${error.message}.`
          : "The topic configuration request is invalid.",
        typeof topic === "string" ? topic : undefined,
        "VALIDATION",
        { cause: error },
      );
    }
  }
}
