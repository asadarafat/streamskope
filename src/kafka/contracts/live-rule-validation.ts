import { kafkaLiveRuleEvidenceBytes } from "./message-limits";
import {
  KAFKA_LIVE_RULE_CAPABILITY_STATES,
  KAFKA_LIVE_RULE_EVALUATION_STATES,
  KAFKA_LIVE_RULE_LIMITS,
  KAFKA_LIVE_RULE_UNAVAILABLE_REASONS,
  KAFKA_RULE_NOTIFICATION_LIMITS,
  type KafkaLiveRuleCapability,
  type KafkaLiveRuleError,
  type KafkaLiveRuleEvaluation,
  type KafkaLiveRuleMatch,
  type KafkaRuleNotification,
  type KafkaRuleNotificationMatch,
} from "./live-rule-types";
import { KAFKA_RULE_LIMITS, KAFKA_RULE_SEVERITIES, maximumKafkaRuleSeverity } from "./rule-types";
import { canonicalKafkaRuleName } from "./rule-validation";
import { HostContractValidationError } from "./validation-error";
import {
  boundedUtf8Text,
  declaredValue,
  exactKeys,
  nonNegativeInteger,
  record,
  text,
} from "./validation-primitives";

function boundedInteger(value: unknown, path: string, maximum: number): number {
  const parsed = nonNegativeInteger(value, path);
  if (parsed > maximum) {
    throw new HostContractValidationError(path, `must be no greater than ${String(maximum)}`);
  }
  return parsed;
}

function canonicalName(value: unknown, path: string): string {
  const name = text(value, path, KAFKA_RULE_LIMITS.nameCharacters);
  if (name !== canonicalKafkaRuleName(name)) {
    throw new HostContractValidationError(path, "must be a canonical rule name");
  }
  return name;
}

function parseMatch(value: unknown, path: string): KafkaLiveRuleMatch {
  const match = record(value, path);
  exactKeys(match, ["level", "name"], path);
  return {
    level: declaredValue(match.level, KAFKA_RULE_SEVERITIES, `${path}.level`),
    name: canonicalName(match.name, `${path}.name`),
  };
}

function parseNotificationMatch(value: unknown, path: string): KafkaRuleNotificationMatch {
  const match = record(value, path);
  exactKeys(match, ["count", "level", "name"], path);
  const count = boundedInteger(
    match.count,
    `${path}.count`,
    KAFKA_RULE_NOTIFICATION_LIMITS.activeMatches,
  );
  if (count === 0) {
    throw new HostContractValidationError(`${path}.count`, "must be greater than zero");
  }
  return {
    count,
    level: declaredValue(match.level, KAFKA_RULE_SEVERITIES, `${path}.level`),
    name: canonicalName(match.name, `${path}.name`),
  };
}

function parseError(value: unknown, path: string): KafkaLiveRuleError {
  const error = record(value, path);
  exactKeys(error, ["diagnostic", "name"], path);
  return {
    diagnostic: boundedUtf8Text(
      error.diagnostic,
      `${path}.diagnostic`,
      KAFKA_LIVE_RULE_LIMITS.diagnosticBytes,
    ),
    name: canonicalName(error.name, `${path}.name`),
  };
}

function boundedArray<T>(
  value: unknown,
  path: string,
  parse: (entry: unknown, entryPath: string) => T,
): readonly T[] {
  if (!Array.isArray(value) || value.length > KAFKA_LIVE_RULE_LIMITS.entries) {
    throw new HostContractValidationError(
      path,
      `must contain at most ${String(KAFKA_LIVE_RULE_LIMITS.entries)} entries`,
    );
  }
  return value.map((entry, index) => parse(entry, `${path}[${String(index)}]`));
}

function assertUniqueEvidenceNames(
  activeMatches: readonly KafkaLiveRuleMatch[],
  suppressedMatches: readonly KafkaLiveRuleMatch[],
  errors: readonly KafkaLiveRuleError[],
  path: string,
): void {
  const names = [...activeMatches, ...suppressedMatches, ...errors].map((entry) =>
    entry.name.toLocaleLowerCase("en-US"),
  );
  if (new Set(names).size !== names.length) {
    throw new HostContractValidationError(path, "must identify each rule at most once");
  }
}

function assertAvailableState(
  evaluation: KafkaLiveRuleEvaluation,
  hasReason: boolean,
  path: string,
): void {
  const partialCondition =
    evaluation.errorCount > 0 || evaluation.omittedEvidence > 0 || evaluation.omittedRules > 0;
  if (evaluation.state === "evaluated") {
    if (hasReason || partialCondition) {
      throw new HostContractValidationError(
        path,
        "an evaluated result cannot contain unavailable or partial evidence",
      );
    }
    return;
  }
  if (evaluation.state === "partial") {
    if (hasReason || !partialCondition) {
      throw new HostContractValidationError(
        path,
        "a partial result must contain omitted or error evidence and no unavailable reason",
      );
    }
    return;
  }
  if (
    !hasReason ||
    evaluation.activeMatchCount !== 0 ||
    evaluation.activeMatches.length !== 0 ||
    evaluation.durationMicros !== 0 ||
    evaluation.errorCount !== 0 ||
    evaluation.errors.length !== 0 ||
    evaluation.evaluatedRules !== 0 ||
    evaluation.omittedEvidence !== 0 ||
    evaluation.omittedRules !== 0 ||
    evaluation.suppressedMatchCount !== 0 ||
    evaluation.suppressedMatches.length !== 0
  ) {
    throw new HostContractValidationError(
      path,
      "an unavailable result must contain only its unavailable reason",
    );
  }
}

export function parseKafkaLiveRuleEvaluation(
  value: unknown,
  path: string,
): KafkaLiveRuleEvaluation {
  const evaluation = record(value, path);
  exactKeys(
    evaluation,
    [
      "activeMatchCount",
      "activeMatches",
      "durationMicros",
      "errorCount",
      "errors",
      "evaluatedRules",
      "highestActiveSeverity",
      "omittedEvidence",
      "omittedRules",
      "reason",
      "state",
      "suppressedMatchCount",
      "suppressedMatches",
    ],
    path,
  );
  const activeMatches = boundedArray(evaluation.activeMatches, `${path}.activeMatches`, parseMatch);
  const suppressedMatches = boundedArray(
    evaluation.suppressedMatches,
    `${path}.suppressedMatches`,
    parseMatch,
  );
  const errors = boundedArray(evaluation.errors, `${path}.errors`, parseError);
  const activeMatchCount = boundedInteger(
    evaluation.activeMatchCount,
    `${path}.activeMatchCount`,
    KAFKA_LIVE_RULE_LIMITS.applicableRules,
  );
  const suppressedMatchCount = boundedInteger(
    evaluation.suppressedMatchCount,
    `${path}.suppressedMatchCount`,
    KAFKA_LIVE_RULE_LIMITS.applicableRules,
  );
  const errorCount = boundedInteger(
    evaluation.errorCount,
    `${path}.errorCount`,
    KAFKA_LIVE_RULE_LIMITS.applicableRules,
  );
  const evaluatedRules = boundedInteger(
    evaluation.evaluatedRules,
    `${path}.evaluatedRules`,
    KAFKA_LIVE_RULE_LIMITS.applicableRules,
  );
  const omittedRules = boundedInteger(
    evaluation.omittedRules,
    `${path}.omittedRules`,
    KAFKA_RULE_LIMITS.rules,
  );
  const omittedEvidence = boundedInteger(
    evaluation.omittedEvidence,
    `${path}.omittedEvidence`,
    KAFKA_LIVE_RULE_LIMITS.applicableRules,
  );
  const durationMicros = boundedInteger(
    evaluation.durationMicros,
    `${path}.durationMicros`,
    KAFKA_LIVE_RULE_LIMITS.durationMicros,
  );
  const state = declaredValue(evaluation.state, KAFKA_LIVE_RULE_EVALUATION_STATES, `${path}.state`);
  const hasHighestActiveSeverity = Object.hasOwn(evaluation, "highestActiveSeverity");
  const highestActiveSeverity = hasHighestActiveSeverity
    ? declaredValue(
        evaluation.highestActiveSeverity,
        KAFKA_RULE_SEVERITIES,
        `${path}.highestActiveSeverity`,
      )
    : undefined;
  const hasReason = Object.hasOwn(evaluation, "reason");
  const reason = hasReason
    ? declaredValue(evaluation.reason, KAFKA_LIVE_RULE_UNAVAILABLE_REASONS, `${path}.reason`)
    : undefined;

  if (
    activeMatches.length > activeMatchCount ||
    suppressedMatches.length > suppressedMatchCount ||
    errors.length > errorCount
  ) {
    throw new HostContractValidationError(path, "retained entries cannot exceed declared counts");
  }
  const retainedHighestSeverity = maximumKafkaRuleSeverity(
    activeMatches.map((match) => match.level),
  );
  if (
    (activeMatchCount === 0 && hasHighestActiveSeverity) ||
    (activeMatchCount > 0 && highestActiveSeverity === undefined)
  ) {
    throw new HostContractValidationError(
      `${path}.highestActiveSeverity`,
      "must exist exactly when active matches exist",
    );
  }
  if (
    highestActiveSeverity !== undefined &&
    maximumKafkaRuleSeverity(
      retainedHighestSeverity === null
        ? [highestActiveSeverity]
        : [retainedHighestSeverity, highestActiveSeverity],
    ) !== highestActiveSeverity
  ) {
    throw new HostContractValidationError(
      `${path}.highestActiveSeverity`,
      "cannot be lower than retained active-match severity",
    );
  }
  if (
    activeMatchCount > 0 &&
    activeMatches.length === activeMatchCount &&
    highestActiveSeverity !== retainedHighestSeverity
  ) {
    throw new HostContractValidationError(
      `${path}.highestActiveSeverity`,
      "must equal retained active-match severity when no active evidence is omitted",
    );
  }
  if (
    activeMatchCount + suppressedMatchCount + errorCount > evaluatedRules ||
    evaluatedRules + omittedRules > KAFKA_RULE_LIMITS.rules
  ) {
    throw new HostContractValidationError(
      path,
      "rule outcome counts exceed evaluated catalog bounds",
    );
  }
  if (omittedRules > 0 && evaluatedRules !== KAFKA_LIVE_RULE_LIMITS.applicableRules) {
    throw new HostContractValidationError(
      `${path}.evaluatedRules`,
      "must equal live evaluation capacity when applicable rules are omitted",
    );
  }
  const expectedOmittedEvidence =
    activeMatchCount -
    activeMatches.length +
    (suppressedMatchCount - suppressedMatches.length) +
    (errorCount - errors.length);
  if (omittedEvidence !== expectedOmittedEvidence) {
    throw new HostContractValidationError(
      `${path}.omittedEvidence`,
      "must equal the declared outcomes omitted from retained evidence",
    );
  }
  assertUniqueEvidenceNames(activeMatches, suppressedMatches, errors, path);

  const parsed: KafkaLiveRuleEvaluation = {
    activeMatchCount,
    activeMatches,
    durationMicros,
    errorCount,
    errors,
    evaluatedRules,
    ...(highestActiveSeverity === undefined ? {} : { highestActiveSeverity }),
    omittedEvidence,
    omittedRules,
    ...(reason === undefined ? {} : { reason }),
    state,
    suppressedMatchCount,
    suppressedMatches,
  };
  assertAvailableState(parsed, hasReason, path);
  if (kafkaLiveRuleEvidenceBytes(parsed) > KAFKA_LIVE_RULE_LIMITS.evidenceBytes) {
    throw new HostContractValidationError(
      path,
      `retained rule evidence must be no larger than ${String(
        KAFKA_LIVE_RULE_LIMITS.evidenceBytes,
      )} UTF-8 bytes`,
    );
  }
  return parsed;
}

export function parseKafkaRuleNotification(value: unknown, path: string): KafkaRuleNotification {
  const notification = record(value, path);
  exactKeys(
    notification,
    ["activeMatchCount", "highestSeverity", "matches", "omittedMatches", "topic"],
    path,
  );
  if (
    !Array.isArray(notification.matches) ||
    notification.matches.length > KAFKA_RULE_NOTIFICATION_LIMITS.matches
  ) {
    throw new HostContractValidationError(
      `${path}.matches`,
      `must contain at most ${String(KAFKA_RULE_NOTIFICATION_LIMITS.matches)} entries`,
    );
  }
  const matches = notification.matches.map((entry, index) =>
    parseNotificationMatch(entry, `${path}.matches[${String(index)}]`),
  );
  const activeMatchCount = boundedInteger(
    notification.activeMatchCount,
    `${path}.activeMatchCount`,
    KAFKA_RULE_NOTIFICATION_LIMITS.activeMatches,
  );
  const omittedMatches = boundedInteger(
    notification.omittedMatches,
    `${path}.omittedMatches`,
    KAFKA_RULE_NOTIFICATION_LIMITS.activeMatches,
  );
  const highestSeverity = declaredValue(
    notification.highestSeverity,
    KAFKA_RULE_SEVERITIES,
    `${path}.highestSeverity`,
  );
  const retainedCount = matches.reduce((count, match) => count + match.count, 0);
  const identities = matches.map(
    (match) => `${match.level}\u0000${match.name.toLocaleLowerCase("en-US")}`,
  );
  const retainedHighest = maximumKafkaRuleSeverity(matches.map((match) => match.level));
  if (
    activeMatchCount === 0 ||
    activeMatchCount !== retainedCount + omittedMatches ||
    new Set(identities).size !== identities.length ||
    (retainedHighest !== null &&
      (maximumKafkaRuleSeverity([retainedHighest, highestSeverity]) !== highestSeverity ||
        (omittedMatches === 0 && retainedHighest !== highestSeverity)))
  ) {
    throw new HostContractValidationError(path, "contains inconsistent rule-notification evidence");
  }
  return {
    activeMatchCount,
    highestSeverity,
    matches,
    omittedMatches,
    topic: text(notification.topic, `${path}.topic`, KAFKA_RULE_LIMITS.topicCharacters),
  };
}

export function parseKafkaLiveRuleCapability(
  value: unknown,
  path: string,
): KafkaLiveRuleCapability {
  const capability = record(value, path);
  exactKeys(capability, ["applicableRules", "omittedRules", "recovery", "state"], path);
  const applicableRules = boundedInteger(
    capability.applicableRules,
    `${path}.applicableRules`,
    KAFKA_RULE_LIMITS.rules,
  );
  const omittedRules = boundedInteger(
    capability.omittedRules,
    `${path}.omittedRules`,
    KAFKA_RULE_LIMITS.rules,
  );
  const state = declaredValue(capability.state, KAFKA_LIVE_RULE_CAPABILITY_STATES, `${path}.state`);
  const hasRecovery = Object.hasOwn(capability, "recovery");
  const recovery = hasRecovery
    ? text(capability.recovery, `${path}.recovery`, KAFKA_RULE_LIMITS.diagnosticCharacters)
    : undefined;

  if (omittedRules > applicableRules) {
    throw new HostContractValidationError(`${path}.omittedRules`, "cannot exceed applicable rules");
  }
  switch (state) {
    case "idle":
      if (applicableRules !== 0 || omittedRules !== 0 || hasRecovery) {
        throw new HostContractValidationError(path, "an idle capability must contain zero counts");
      }
      break;
    case "ready":
      if (
        applicableRules > KAFKA_LIVE_RULE_LIMITS.applicableRules ||
        omittedRules !== 0 ||
        hasRecovery
      ) {
        throw new HostContractValidationError(
          path,
          "a ready capability must fit live capacity without omissions or recovery",
        );
      }
      break;
    case "partial":
      if (
        omittedRules === 0 ||
        omittedRules !== applicableRules - KAFKA_LIVE_RULE_LIMITS.applicableRules ||
        hasRecovery
      ) {
        throw new HostContractValidationError(
          path,
          "a partial capability must identify exactly the rules beyond live capacity and no recovery",
        );
      }
      break;
    case "unavailable":
      if (applicableRules !== 0 || omittedRules !== 0 || recovery === undefined) {
        throw new HostContractValidationError(
          path,
          "an unavailable capability must contain recovery and zero counts",
        );
      }
      break;
  }

  return {
    applicableRules,
    omittedRules,
    ...(recovery === undefined ? {} : { recovery }),
    state,
  };
}
