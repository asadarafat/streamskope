import type {
  HostEvent,
  KafkaRuleDefinition,
  KafkaRuleEvaluationReport,
  KafkaRuleSnapshot,
} from "../contracts";

export type KafkaRuleUiOperation =
  "create" | "delete" | "evaluate" | "list" | "update" | "validate";

export interface KafkaRulePendingOperation {
  readonly accepted: boolean;
  readonly evidenceObserved: boolean;
  readonly nextName?: string;
  readonly operation: KafkaRuleUiOperation;
  readonly originalName?: string;
  readonly requestId: string;
}

export interface KafkaRuleCompletion {
  readonly operation: KafkaRuleUiOperation;
  readonly requestId: string;
}

export interface KafkaRuleUiState {
  readonly completion: KafkaRuleCompletion | null;
  readonly evaluation: KafkaRuleEvaluationReport | null;
  readonly filter: string;
  readonly lastSequence: number;
  readonly pending: KafkaRulePendingOperation | null;
  readonly requestError: string | null;
  readonly selectedName: string | null;
  readonly snapshot: KafkaRuleSnapshot | null;
  readonly stale: boolean;
}

export type KafkaRuleUiAction =
  | {
      readonly event: HostEvent;
      readonly type: "host.event";
    }
  | {
      readonly query: string;
      readonly type: "filter.changed";
    }
  | {
      readonly name: string | null;
      readonly type: "selection.changed";
    }
  | {
      readonly nextName?: string;
      readonly operation: KafkaRuleUiOperation;
      readonly originalName?: string;
      readonly requestId: string;
      readonly type: "operation.started";
    }
  | {
      readonly requestId: string;
      readonly type: "operation.accepted";
    }
  | {
      readonly message: string;
      readonly requestId: string;
      readonly type: "operation.failed";
    };

export const initialKafkaRuleUiState: KafkaRuleUiState = {
  completion: null,
  evaluation: null,
  filter: "",
  lastSequence: -1,
  pending: null,
  requestError: null,
  selectedName: null,
  snapshot: null,
  stale: false,
};

function matchingRule(
  rules: readonly KafkaRuleDefinition[],
  name: string | undefined,
): KafkaRuleDefinition | undefined {
  return name === undefined ? undefined : rules.find((rule) => rule.name === name);
}

function snapshotIsEvidence(
  pending: KafkaRulePendingOperation,
  snapshot: KafkaRuleSnapshot,
): boolean {
  switch (pending.operation) {
    case "create":
      return (
        pending.nextName === undefined ||
        matchingRule(snapshot.rules, pending.nextName) !== undefined
      );
    case "delete":
      return (
        pending.originalName === undefined ||
        matchingRule(snapshot.rules, pending.originalName) === undefined
      );
    case "list":
      return true;
    case "update":
      return (
        pending.nextName !== undefined &&
        matchingRule(snapshot.rules, pending.nextName) !== undefined &&
        (pending.originalName === pending.nextName ||
          matchingRule(snapshot.rules, pending.originalName) === undefined)
      );
    case "evaluate":
    case "validate":
      return false;
  }
}

function evaluationIsEvidence(
  pending: KafkaRulePendingOperation,
  report: KafkaRuleEvaluationReport,
): boolean {
  return (
    pending.requestId === report.requestId &&
    ((pending.operation === "evaluate" && report.kind === "evaluation") ||
      (pending.operation === "validate" && report.kind === "validation"))
  );
}

function completion(pending: KafkaRulePendingOperation): KafkaRuleCompletion {
  return {
    operation: pending.operation,
    requestId: pending.requestId,
  };
}

function observedPending(pending: KafkaRulePendingOperation): KafkaRulePendingOperation {
  const observed = { ...pending, evidenceObserved: true };
  return observed;
}

function acceptedPending(pending: KafkaRulePendingOperation): KafkaRulePendingOperation {
  const accepted = { ...pending, accepted: true };
  return accepted;
}

function reconciledSelection(
  state: KafkaRuleUiState,
  rules: readonly KafkaRuleDefinition[],
): string | null {
  if (rules.length === 0) {
    return null;
  }
  if (
    state.pending?.operation === "create" &&
    matchingRule(rules, state.pending.nextName) !== undefined
  ) {
    return state.pending.nextName ?? null;
  }
  if (
    state.pending?.operation === "update" &&
    state.selectedName === state.pending.originalName &&
    matchingRule(rules, state.pending.nextName) !== undefined
  ) {
    return state.pending.nextName ?? null;
  }
  if (matchingRule(rules, state.selectedName ?? undefined) !== undefined) {
    return state.selectedName;
  }
  const previousIndex =
    state.snapshot?.rules.findIndex((rule) => rule.name === state.selectedName) ?? -1;
  const replacementIndex = previousIndex < 0 ? 0 : Math.min(previousIndex, rules.length - 1);
  return rules[replacementIndex]?.name ?? null;
}

function reduceHostEvent(state: KafkaRuleUiState, event: HostEvent): KafkaRuleUiState {
  if (event.sequence <= state.lastSequence) {
    return state;
  }
  const sequenced = { ...state, lastSequence: event.sequence };
  switch (event.event) {
    case "backend.availability":
      return event.payload.state === "unavailable"
        ? {
            ...sequenced,
            completion: null,
            pending: null,
            requestError:
              event.payload.recovery ?? "Restart StreamSkope to restore rule operations.",
            stale: state.snapshot !== null,
          }
        : {
            ...sequenced,
            requestError: null,
          };
    case "rules.changed": {
      const observed =
        state.pending !== null && snapshotIsEvidence(state.pending, event.payload)
          ? observedPending(state.pending)
          : state.pending;
      const completed = observed?.accepted === true && observed.evidenceObserved;
      return {
        ...sequenced,
        completion: completed ? completion(observed) : state.completion,
        pending: completed ? null : observed,
        selectedName: reconciledSelection(state, event.payload.rules),
        snapshot: event.payload,
        stale: false,
      };
    }
    case "rules.evaluation": {
      if (state.pending === null || !evaluationIsEvidence(state.pending, event.payload)) {
        return sequenced;
      }
      const observed = observedPending(state.pending);
      const completed = observed.accepted;
      return {
        ...sequenced,
        completion: completed ? completion(observed) : state.completion,
        evaluation: event.payload,
        pending: completed ? null : observed,
        requestError: null,
      };
    }
    case "rules.notification":
    case "activity.recorded":
    case "connection.state":
    case "clusterDetails.changed":
    case "consumerGroups.changed":
    case "consumerGroup.changed":
    case "schemas.changed":
    case "schema.changed":
    case "schemaCompatibility.changed":
    case "acls.changed":
    case "transforms.changed":
    case "transform.changed":
    case "transformLogs.changed":
    case "latency.changed":
    case "latency.history.changed":
    case "streamMetrics.changed":
    case "consumption.state":
    case "messages.batch":
    case "profiles.changed":
    case "preferences.changed":
    case "templates.changed":
    case "recipes.changed":
    case "topicConfiguration.changed":
    case "topicConfiguration.history":
    case "topics.changed":
      return sequenced;
  }
}

export function reduceKafkaRuleUiState(
  state: KafkaRuleUiState,
  action: KafkaRuleUiAction,
): KafkaRuleUiState {
  switch (action.type) {
    case "filter.changed":
      return { ...state, filter: action.query };
    case "host.event":
      return reduceHostEvent(state, action.event);
    case "operation.accepted":
      if (state.pending?.requestId !== action.requestId) {
        return state;
      }
      {
        const accepted = acceptedPending(state.pending);
        return accepted.evidenceObserved
          ? {
              ...state,
              completion: completion(accepted),
              pending: null,
            }
          : { ...state, pending: accepted };
      }
    case "operation.failed":
      return state.pending?.requestId === action.requestId
        ? {
            ...state,
            completion: null,
            pending: null,
            requestError: action.message,
          }
        : state;
    case "operation.started":
      return {
        ...state,
        completion: null,
        evaluation:
          action.operation === "evaluate" || action.operation === "validate"
            ? null
            : state.evaluation,
        pending: {
          accepted: false,
          evidenceObserved: false,
          ...(action.nextName === undefined ? {} : { nextName: action.nextName }),
          operation: action.operation,
          ...(action.originalName === undefined ? {} : { originalName: action.originalName }),
          requestId: action.requestId,
        },
        requestError: null,
      };
    case "selection.changed":
      return action.name === null ||
        matchingRule(state.snapshot?.rules ?? [], action.name) !== undefined
        ? { ...state, selectedName: action.name }
        : state;
  }
}

export type KafkaRuleSelector = (state: KafkaRuleUiState) => readonly KafkaRuleDefinition[];

export function createKafkaRuleSelector(): KafkaRuleSelector {
  let previousFilter: string | undefined;
  let previousRules: readonly KafkaRuleDefinition[] | undefined;
  let previousResult: readonly KafkaRuleDefinition[] = [];

  return (state): readonly KafkaRuleDefinition[] => {
    const rules = state.snapshot?.rules ?? [];
    const filter = state.filter.trim().toLocaleLowerCase("en-US");
    if (rules === previousRules && filter === previousFilter) {
      return previousResult;
    }
    previousRules = rules;
    previousFilter = filter;
    previousResult =
      filter.length === 0
        ? rules
        : rules.filter((rule) =>
            [rule.name, rule.expression, rule.topic ?? "", rule.level, rule.description ?? ""].some(
              (value) => value.toLocaleLowerCase("en-US").includes(filter),
            ),
          );
    return previousResult;
  };
}
