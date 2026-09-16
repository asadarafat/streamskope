import { describe, expect, it } from "vitest";

import {
  HOST_PROTOCOL_VERSION,
  type HostEvent,
  type KafkaRuleDefinition,
} from "../../src/features/kafka/contracts";
import {
  createKafkaRuleSelector,
  initialKafkaRuleUiState,
  reduceKafkaRuleUiState,
  type KafkaRuleUiAction,
} from "../../src/features/kafka/ui";

const first: KafkaRuleDefinition = {
  cooldownMs: 5_000,
  description: "Detect critical orders.",
  enabled: true,
  expression: '$.priority == "high"',
  level: "warn",
  name: "High priority",
  topic: "orders",
};

const second: KafkaRuleDefinition = {
  cooldownMs: 0,
  description: "Payment latency threshold.",
  enabled: true,
  expression: "$.latency > 500",
  level: "error",
  name: "Slow payment",
  topic: "payments",
};

function host(event: HostEvent): KafkaRuleUiAction {
  return { event, type: "host.event" };
}

function snapshot(rules: readonly KafkaRuleDefinition[], sequence: number): HostEvent {
  return {
    event: "rules.changed",
    payload: {
      rules,
      store: { durability: "session", state: "ready" },
    },
    sequence,
    version: HOST_PROTOCOL_VERSION,
  };
}

describe("Kafka rule renderer state", () => {
  it("selects the exact rule confirmed by a create snapshot", () => {
    let state = reduceKafkaRuleUiState(initialKafkaRuleUiState, host(snapshot([first], 1)));
    state = reduceKafkaRuleUiState(state, {
      nextName: "Slow payment",
      operation: "create",
      requestId: "create-1",
      type: "operation.started",
    });
    state = reduceKafkaRuleUiState(state, host(snapshot([first, second], 2)));

    expect(state.selectedName).toBe("Slow payment");
    expect(state.pending).toMatchObject({
      evidenceObserved: true,
      requestId: "create-1",
    });
  });

  it("reconciles selection through a confirmed rename and exact deletion", () => {
    let state = reduceKafkaRuleUiState(initialKafkaRuleUiState, host(snapshot([first, second], 1)));
    expect(state.selectedName).toBe("High priority");
    state = reduceKafkaRuleUiState(state, {
      name: "Slow payment",
      type: "selection.changed",
    });
    state = reduceKafkaRuleUiState(state, {
      nextName: "Payment latency",
      operation: "update",
      originalName: "Slow payment",
      requestId: "update-1",
      type: "operation.started",
    });
    state = reduceKafkaRuleUiState(
      state,
      host(snapshot([first, { ...second, name: "Payment latency" }], 2)),
    );

    expect(state.selectedName).toBe("Payment latency");
    expect(state.pending).toMatchObject({
      evidenceObserved: true,
      requestId: "update-1",
    });
    state = reduceKafkaRuleUiState(state, {
      requestId: "update-1",
      type: "operation.accepted",
    });
    expect(state.pending).toBeNull();
    expect(state.completion).toEqual({
      operation: "update",
      requestId: "update-1",
    });

    state = reduceKafkaRuleUiState(state, {
      operation: "delete",
      originalName: "Payment latency",
      requestId: "delete-1",
      type: "operation.started",
    });
    expect(state.completion).toBeNull();
    state = reduceKafkaRuleUiState(state, host(snapshot([first], 3)));
    state = reduceKafkaRuleUiState(state, {
      requestId: "delete-1",
      type: "operation.accepted",
    });

    expect(state.selectedName).toBe("High priority");
    expect(state.pending).toBeNull();
    expect(state.completion).toEqual({
      operation: "delete",
      requestId: "delete-1",
    });
  });

  it("keeps a confirmed catalog visibly stale until unavailable storage is explicit", () => {
    let state = reduceKafkaRuleUiState(initialKafkaRuleUiState, host(snapshot([first], 1)));
    state = reduceKafkaRuleUiState(
      state,
      host({
        event: "backend.availability",
        payload: {
          recovery: "Restart StreamSkope.",
          state: "unavailable",
        },
        sequence: 2,
        version: HOST_PROTOCOL_VERSION,
      }),
    );

    expect(state.snapshot?.rules).toEqual([first]);
    expect(state.stale).toBe(true);

    state = reduceKafkaRuleUiState(
      state,
      host({
        event: "rules.changed",
        payload: {
          rules: [],
          store: {
            durability: "session",
            recovery: "Restore the rule document and restart StreamSkope.",
            state: "unavailable",
          },
        },
        sequence: 3,
        version: HOST_PROTOCOL_VERSION,
      }),
    );
    expect(state).toMatchObject({
      selectedName: null,
      stale: false,
      snapshot: {
        rules: [],
        store: { state: "unavailable" },
      },
    });
  });

  it("clears stale recovery text after a ready catalog is confirmed", () => {
    let state = reduceKafkaRuleUiState(initialKafkaRuleUiState, host(snapshot([first], 1)));
    state = reduceKafkaRuleUiState(
      state,
      host({
        event: "backend.availability",
        payload: {
          recovery: "Restart StreamSkope.",
          state: "unavailable",
        },
        sequence: 2,
        version: HOST_PROTOCOL_VERSION,
      }),
    );
    state = reduceKafkaRuleUiState(
      state,
      host({
        event: "backend.availability",
        payload: { state: "ready" },
        sequence: 3,
        version: HOST_PROTOCOL_VERSION,
      }),
    );
    state = reduceKafkaRuleUiState(state, host(snapshot([first], 4)));

    expect(state.stale).toBe(false);
    expect(state.requestError).toBeNull();
  });

  it("accepts only the active request's evaluation evidence in either transport order", () => {
    let state = reduceKafkaRuleUiState(initialKafkaRuleUiState, {
      operation: "evaluate",
      requestId: "evaluate-old",
      type: "operation.started",
    });
    state = reduceKafkaRuleUiState(state, {
      operation: "evaluate",
      requestId: "evaluate-current",
      type: "operation.started",
    });
    state = reduceKafkaRuleUiState(
      state,
      host({
        event: "rules.evaluation",
        payload: {
          kind: "evaluation",
          requestId: "evaluate-old",
          results: [{ name: "High priority", outcome: "not-matched" }],
        },
        sequence: 1,
        version: HOST_PROTOCOL_VERSION,
      }),
    );
    expect(state.evaluation).toBeNull();

    state = reduceKafkaRuleUiState(
      state,
      host({
        event: "rules.evaluation",
        payload: {
          kind: "evaluation",
          requestId: "evaluate-current",
          results: [{ name: "High priority", outcome: "matched" }],
        },
        sequence: 2,
        version: HOST_PROTOCOL_VERSION,
      }),
    );
    expect(state.evaluation).toMatchObject({
      requestId: "evaluate-current",
      results: [{ outcome: "matched" }],
    });
    expect(state.pending).toMatchObject({ evidenceObserved: true });

    state = reduceKafkaRuleUiState(state, {
      requestId: "evaluate-current",
      type: "operation.accepted",
    });
    expect(state.pending).toBeNull();

    state = reduceKafkaRuleUiState(state, {
      operation: "validate",
      requestId: "validate-current",
      type: "operation.started",
    });
    state = reduceKafkaRuleUiState(state, {
      requestId: "validate-current",
      type: "operation.accepted",
    });
    expect(state.pending).toMatchObject({ accepted: true, evidenceObserved: false });
    state = reduceKafkaRuleUiState(
      state,
      host({
        event: "rules.evaluation",
        payload: {
          kind: "validation",
          requestId: "validate-current",
          results: [{ name: "High priority", outcome: "valid" }],
        },
        sequence: 3,
        version: HOST_PROTOCOL_VERSION,
      }),
    );
    expect(state.pending).toBeNull();
    expect(state.evaluation?.kind).toBe("validation");
  });

  it("retains explicit failure and ignores later evidence for the failed request", () => {
    let state = reduceKafkaRuleUiState(initialKafkaRuleUiState, {
      operation: "evaluate",
      requestId: "evaluate-failed",
      type: "operation.started",
    });
    state = reduceKafkaRuleUiState(state, {
      message: "Sample must be valid JSON.",
      requestId: "evaluate-failed",
      type: "operation.failed",
    });
    state = reduceKafkaRuleUiState(
      state,
      host({
        event: "rules.evaluation",
        payload: {
          kind: "evaluation",
          requestId: "evaluate-failed",
          results: [{ name: "High priority", outcome: "matched" }],
        },
        sequence: 1,
        version: HOST_PROTOCOL_VERSION,
      }),
    );

    expect(state.pending).toBeNull();
    expect(state.requestError).toBe("Sample must be valid JSON.");
    expect(state.evaluation).toBeNull();
  });

  it("filters every searchable field without mutating the authoritative catalog", () => {
    let state = reduceKafkaRuleUiState(initialKafkaRuleUiState, host(snapshot([first, second], 1)));
    const select = createKafkaRuleSelector();

    for (const query of ["slow", "latency >", "payments", "error", "threshold"]) {
      state = reduceKafkaRuleUiState(state, { query, type: "filter.changed" });
      expect(select(state).map((rule) => rule.name)).toEqual(["Slow payment"]);
      expect(select(state)).toBe(select(state));
    }
    expect(state.snapshot?.rules).toEqual([first, second]);
  });

  it("rejects late host sequences without disturbing pending or selected state", () => {
    let state = reduceKafkaRuleUiState(initialKafkaRuleUiState, host(snapshot([first, second], 2)));
    state = reduceKafkaRuleUiState(state, {
      name: "Slow payment",
      type: "selection.changed",
    });
    state = reduceKafkaRuleUiState(state, {
      operation: "list",
      requestId: "list-1",
      type: "operation.started",
    });

    const late = reduceKafkaRuleUiState(state, host(snapshot([], 1)));

    expect(late).toBe(state);
    expect(late.selectedName).toBe("Slow payment");
    expect(late.pending?.requestId).toBe("list-1");
  });
});
