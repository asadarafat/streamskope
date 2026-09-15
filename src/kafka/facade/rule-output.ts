import {
  HOST_PROTOCOL_VERSION,
  KAFKA_RULE_NOTIFICATION_LIMITS,
  KAFKA_RULE_SEVERITIES,
  type ActivitySeverity,
  type HostEvent,
  type KafkaOperationalPreferenceLogLevel,
  type KafkaRuleNotification,
  type KafkaRuleNotificationMatch,
  type KafkaRuleSeverity,
} from "../contracts";

import type { QueuedFacadeMessage } from "./facade-support";

interface RetainedMatch {
  count: number;
  readonly level: KafkaRuleSeverity;
  readonly name: string;
}

interface MatchAccumulator {
  highestSeverity: KafkaRuleSeverity | null;
  readonly matches: Map<string, RetainedMatch>;
  total: number;
}

type MatchIdentityCache = Record<KafkaRuleSeverity, Map<string, string>>;

export interface FacadeRuleOutputAggregate {
  readonly activity:
    | {
        readonly detail: string;
        readonly severity: ActivitySeverity;
      }
    | undefined;
  readonly notification: KafkaRuleNotification | undefined;
}

function accumulator(): MatchAccumulator {
  return {
    highestSeverity: null,
    matches: new Map(),
    total: 0,
  };
}

function identity(identities: MatchIdentityCache, name: string, level: KafkaRuleSeverity): string {
  const cache = identities[level];
  let key = cache.get(name);
  if (key === undefined) {
    key = `${level}\u0000${name.toLocaleLowerCase("en-US")}`;
    cache.set(name, key);
  }
  return key;
}

function retain(
  aggregate: MatchAccumulator,
  key: string,
  name: string,
  level: KafkaRuleSeverity,
): void {
  const existing = aggregate.matches.get(key);
  if (existing !== undefined) {
    existing.count += 1;
    return;
  }
  if (aggregate.matches.size < KAFKA_RULE_NOTIFICATION_LIMITS.matches) {
    aggregate.matches.set(key, { count: 1, level, name });
  }
}

function includesSeverity(
  severity: KafkaRuleSeverity,
  threshold: KafkaOperationalPreferenceLogLevel,
): boolean {
  switch (threshold) {
    case "debug":
    case "info":
      return true;
    case "warn":
      return severity === "warn" || severity === "error";
    case "error":
      return severity === "error";
  }
}

function mergeHighestSeverity(
  current: KafkaRuleSeverity | null,
  candidate: KafkaRuleSeverity | undefined,
): KafkaRuleSeverity | null {
  if (candidate === undefined) {
    return current;
  }
  return current === null ||
    KAFKA_RULE_SEVERITIES.indexOf(candidate) > KAFKA_RULE_SEVERITIES.indexOf(current)
    ? candidate
    : current;
}

function retainedMatches(aggregate: MatchAccumulator): readonly KafkaRuleNotificationMatch[] {
  return [...aggregate.matches.values()].map(({ count, level, name }) => ({
    count,
    level,
    name,
  }));
}

function retainedCount(matches: readonly KafkaRuleNotificationMatch[]): number {
  return matches.reduce((count, match) => count + match.count, 0);
}

function notification(
  topic: string,
  aggregate: MatchAccumulator,
): KafkaRuleNotification | undefined {
  const matches = retainedMatches(aggregate);
  return aggregate.total === 0 || aggregate.highestSeverity === null
    ? undefined
    : {
        activeMatchCount: aggregate.total,
        highestSeverity: aggregate.highestSeverity,
        matches,
        omittedMatches: aggregate.total - retainedCount(matches),
        topic,
      };
}

function activitySeverity(severity: KafkaRuleSeverity): ActivitySeverity {
  switch (severity) {
    case "info":
      return "info";
    case "warn":
      return "warning";
    case "error":
      return "error";
  }
}

function activityDetail(
  aggregate: MatchAccumulator,
  matches: readonly KafkaRuleNotificationMatch[],
): string {
  const entries = matches
    .map(
      (match) =>
        `${match.name} (${match.level}, ${String(match.count)} match${
          match.count === 1 ? "" : "es"
        })`,
    )
    .join("; ");
  const omitted = aggregate.total - retainedCount(matches);
  return `${String(aggregate.total)} active rule match${
    aggregate.total === 1 ? "" : "es"
  } met the captured logging threshold. ${entries}.${
    omitted === 0
      ? ""
      : ` ${String(omitted)} additional match${omitted === 1 ? "" : "es"} omitted from bounded Activity evidence.`
  }`;
}

export function aggregateFacadeRuleOutputs(
  topic: string,
  queued: readonly QueuedFacadeMessage[],
): FacadeRuleOutputAggregate {
  const notices = accumulator();
  const logs = accumulator();
  const identities: MatchIdentityCache = {
    error: new Map(),
    info: new Map(),
    warn: new Map(),
  };
  for (const item of queued) {
    const { ruleEvaluation } = item.message;
    if (ruleEvaluation.activeMatchCount === 0) {
      continue;
    }
    if (item.ruleOutput.notificationsEnabled) {
      notices.total += ruleEvaluation.activeMatchCount;
      notices.highestSeverity = mergeHighestSeverity(
        notices.highestSeverity,
        ruleEvaluation.highestActiveSeverity,
      );
    }
    for (const match of ruleEvaluation.activeMatches) {
      const key = identity(identities, match.name, match.level);
      if (item.ruleOutput.notificationsEnabled) {
        retain(notices, key, match.name, match.level);
      }
      if (
        item.ruleOutput.loggingEnabled &&
        includesSeverity(match.level, item.ruleOutput.logLevel)
      ) {
        logs.total += 1;
        logs.highestSeverity = mergeHighestSeverity(logs.highestSeverity, match.level);
        retain(logs, key, match.name, match.level);
      }
    }
  }
  const logMatches = retainedMatches(logs);
  return {
    activity:
      logs.total === 0 || logs.highestSeverity === null
        ? undefined
        : {
            detail: activityDetail(logs, logMatches),
            severity: activitySeverity(logs.highestSeverity),
          },
    notification: notification(topic, notices),
  };
}

export function ruleNotificationEvent(
  payload: KafkaRuleNotification,
  sequence: number,
): Extract<HostEvent, { readonly event: "rules.notification" }> {
  return {
    event: "rules.notification",
    payload,
    sequence,
    version: HOST_PROTOCOL_VERSION,
  };
}
