import { useCallback, useEffect, useRef, useState } from "react";

import {
  HOST_PROTOCOL_VERSION,
  HostContractValidationError,
  type KafkaExploredMessage,
  type KafkaMessage,
  type StreamSkopeHost,
} from "../contracts";
import {
  createEmptyKafkaSavedRecordContext,
  kafkaRecordLocator,
  parseKafkaSavedRecordContext,
  sameKafkaRecordLocator,
  type KafkaRecordLocator,
  type KafkaRecordLocatorOutcome,
  type KafkaSavedRecordContext,
} from "../contracts/record-locator";

import type { KafkaUiState } from "./state";
import { isKafkaConsumptionActive, kafkaConsumptionStopLabel } from "./workbench-status";

export type SavedRecordSlot = "selected" | "comparison";
export type SavedRecordLoadStatus = {
  readonly state: KafkaRecordLocatorOutcome["state"];
  readonly detail?: string;
};
type Authority = {
  readonly host: StreamSkopeHost;
  readonly key: string;
  readonly generation: number;
};
type LoadedRecord = { readonly authority: Authority; readonly message: KafkaExploredMessage };
type Attempt = {
  readonly host: StreamSkopeHost;
  readonly authority: Authority;
  readonly requestId: string;
  readonly locator: KafkaRecordLocator;
  readonly slot: SavedRecordSlot;
  cancelling: boolean;
  stopping: Promise<void> | null;
};
export interface InvestigationRecordsController {
  readonly references: KafkaSavedRecordContext;
  readonly selected: KafkaExploredMessage | null;
  readonly baseline: KafkaExploredMessage | null;
  readonly outcomes: Partial<Record<SavedRecordSlot, SavedRecordLoadStatus>>;
  readonly busy: boolean;
  readonly cleanupPending: boolean;
  readonly error: string | undefined;
  capture(this: void, selected: KafkaExploredMessage | null): KafkaSavedRecordContext;
  restore(this: void, references: KafkaSavedRecordContext): void;
  clear(this: void): void;
  selectGrid(this: void, message: KafkaExploredMessage | null): void;
  pin(this: void, message: KafkaExploredMessage | null): void;
  choose(this: void, locator: KafkaRecordLocator, slot: SavedRecordSlot): void;
  reload(this: void, slot: SavedRecordSlot): Promise<void>;
  cancel(this: void): Promise<void>;
}

export function workbenchRecordScope(
  state: Pick<
    KafkaUiState,
    | "backend"
    | "connectionState"
    | "connectionName"
    | "profiles"
    | "preferenceSnapshot"
    | "consumptionState"
    | "consumptionRequest"
    | "consumptionError"
    | "latency"
  >,
  stopping: boolean,
): Omit<Parameters<typeof useInvestigationRecords>[0], "host"> {
  return {
    connected: state.connectionState === "connected" && state.backend === "ready",
    authorityKey: JSON.stringify([
      state.connectionState,
      state.profiles.find((profile) => profile.active)?.id,
      state.connectionName,
      state.backend,
    ]),
    settingsKey: JSON.stringify([
      state.preferenceSnapshot?.preferences.protection,
      state.preferenceSnapshot?.preferences.codecs,
    ]),
    readBlocked:
      stopping ||
      isKafkaConsumptionActive(state.consumptionState, state.consumptionRequest) ||
      kafkaConsumptionStopLabel(
        state.consumptionState,
        state.consumptionRequest,
        state.consumptionError,
      ) !== null ||
      state.latency.state === "running",
  };
}

function unexplored(message: KafkaMessage): KafkaExploredMessage {
  return {
    ...message,
    ruleEvaluation: {
      state: "unavailable",
      reason: "not-evaluated",
      activeMatchCount: 0,
      activeMatches: [],
      suppressedMatchCount: 0,
      suppressedMatches: [],
      evaluatedRules: 0,
      omittedRules: 0,
      omittedEvidence: 0,
      durationMicros: 0,
      errorCount: 0,
      errors: [],
    },
  };
}

/** One transient reader owner; only validated positions cross the saved-view boundary. */
export function useInvestigationRecords({
  host,
  connected,
  authorityKey,
  settingsKey,
  readBlocked,
}: {
  readonly host: StreamSkopeHost;
  readonly connected: boolean;
  readonly authorityKey: string;
  readonly settingsKey: string;
  readonly readBlocked: boolean;
}): InvestigationRecordsController {
  const key = JSON.stringify([authorityKey, settingsKey, connected]);
  const authority = useRef<Authority>({ host, key, generation: 0 });
  if (authority.current.host !== host || authority.current.key !== key) {
    authority.current = { host, key, generation: authority.current.generation + 1 };
  }
  const currentAuthority = authority.current;
  const [references, setReferences] = useState(createEmptyKafkaSavedRecordContext);
  const [loaded, setLoaded] = useState<Partial<Record<SavedRecordSlot, LoadedRecord>>>({});
  const [outcomes, setOutcomes] = useState<Partial<Record<SavedRecordSlot, SavedRecordLoadStatus>>>(
    {},
  );
  const [outcomeAuthority, setOutcomeAuthority] = useState(currentAuthority);
  const [phase, setPhase] = useState<"idle" | "loading" | "stopping" | "cleanup">("idle");
  const [error, setError] = useState<string>();
  const pending = useRef<Attempt | null>(null);
  const mounted = useRef(true);
  const currentReferences = useRef(references);
  const currentSettings = useRef(settingsKey);
  currentSettings.current = settingsKey;
  currentReferences.current = references;

  const cancel = useCallback(async (): Promise<void> => {
    const attempt = pending.current;
    if (attempt === null) return;
    if (attempt.stopping !== null) return attempt.stopping;
    attempt.cancelling = true;
    if (mounted.current) {
      setPhase("stopping");
      setError(undefined);
    }
    const operation = (async (): Promise<void> => {
      try {
        const response = await attempt.host.execute({
          command: "records.locator.cancel",
          payload: { requestId: attempt.requestId },
          id: crypto.randomUUID(),
          version: HOST_PROTOCOL_VERSION,
        });
        if (!response.ok) throw new Error("Stop was not confirmed");
        if (response.result.requestId !== attempt.requestId || response.result.stopped !== true)
          throw new Error("Stop identity did not match");
        if (pending.current !== attempt) return;
        pending.current = null;
        if (mounted.current) {
          setPhase("idle");
          setOutcomeAuthority(attempt.authority);
          setOutcomes((previous) => ({
            ...previous,
            [attempt.slot]: {
              state: "cancelled",
              detail: "Reload stopped. No record was restored.",
            },
          }));
        }
      } catch {
        if (mounted.current && pending.current === attempt) {
          setPhase("cleanup");
          setError(
            "The host did not confirm reload cleanup. Retry stop before reloading or opening another view.",
          );
        }
      }
    })();
    attempt.stopping = operation;
    try {
      await operation;
    } finally {
      if (attempt.stopping === operation) attempt.stopping = null;
    }
  }, []);

  useEffect(() => {
    mounted.current = true;
    return (): void => {
      mounted.current = false;
      void cancel();
    };
  }, [cancel]);
  useEffect(() => {
    // Render already hides records from the previous stamp; cancellation retains its original owner.
    setLoaded({});
    setOutcomes({});
    void cancel();
  }, [currentAuthority, cancel]);
  useEffect(
    () =>
      host.subscribe((event) => {
        if (event.event === "preferences.changed") {
          const nextSettings = JSON.stringify([
            event.payload.preferences.protection,
            event.payload.preferences.codecs,
          ]);
          if (nextSettings === currentSettings.current) return;
          currentSettings.current = nextSettings;
        } else if (event.event !== "connection.state" && event.event !== "backend.availability")
          return;
        authority.current = { ...authority.current, generation: authority.current.generation + 1 };
        setLoaded({});
        setOutcomes({});
        void cancel();
      }),
    [host, cancel],
  );

  const restore = useCallback((input: KafkaSavedRecordContext): void => {
    if (pending.current !== null)
      throw new HostContractValidationError(
        "Saved records",
        "stop reload and wait for confirmation first",
      );
    const next = parseKafkaSavedRecordContext(input);
    currentReferences.current = next;
    setReferences(next);
    setLoaded({});
    setOutcomes({});
    setError(undefined);
  }, []);
  const choose = useCallback((locator: KafkaRecordLocator, slot: SavedRecordSlot): void => {
    if (pending.current !== null) return;
    try {
      const next = parseKafkaSavedRecordContext({ ...currentReferences.current, [slot]: locator });
      currentReferences.current = next;
      setReferences(next);
      setLoaded((previous) => ({ ...previous, [slot]: undefined }));
      setOutcomes((previous) => ({ ...previous, [slot]: undefined }));
      setError(undefined);
    } catch {
      setError(
        "These positions belong to another Kafka cluster. Open or create a view for this cluster.",
      );
    }
  }, []);
  const setFromMessage = useCallback(
    (slot: SavedRecordSlot, message: KafkaExploredMessage | null): void => {
      // Explicit selection must never fall back to a previous saved locator when provenance is absent.
      const locator = message === null ? null : kafkaRecordLocator(message);
      let next: KafkaSavedRecordContext;
      try {
        next = parseKafkaSavedRecordContext({ ...currentReferences.current, [slot]: locator });
      } catch {
        setError(
          "These positions belong to another Kafka cluster. Open or create a view for this cluster.",
        );
        return;
      }
      currentReferences.current = next;
      setReferences(next);
      setLoaded((previous) => ({
        ...previous,
        [slot]:
          slot === "comparison" && message !== null
            ? { authority: authority.current, message }
            : undefined,
      }));
      setOutcomes((previous) => ({ ...previous, [slot]: undefined }));
      setError(undefined);
    },
    [],
  );

  const reload = useCallback(
    async (slot: SavedRecordSlot): Promise<void> => {
      const locator = currentReferences.current[slot];
      if (!connected || readBlocked || pending.current !== null || locator === null) return;
      const attempt: Attempt = {
        host,
        authority: authority.current,
        requestId: crypto.randomUUID(),
        locator,
        slot,
        cancelling: false,
        stopping: null,
      };
      pending.current = attempt;
      setPhase("loading");
      setError(undefined);
      setLoaded((previous) => ({ ...previous, [slot]: undefined }));
      setOutcomes((previous) => ({ ...previous, [slot]: undefined }));
      try {
        const response = await host.execute({
          command: "records.locator.load",
          payload: { requestId: attempt.requestId, locator },
          id: crypto.randomUUID(),
          version: HOST_PROTOCOL_VERSION,
        });
        if (pending.current !== attempt) return;
        if (!response.ok) {
          if (attempt.cancelling) return;
          setPhase("cleanup");
          setError(
            `${response.error.summary} ${response.error.recovery} Retry stop before another reload.`,
          );
          return;
        }
        const outcome = response.result.outcome;
        if (
          outcome.requestId !== attempt.requestId ||
          !sameKafkaRecordLocator(outcome.locator, locator)
        )
          throw new Error("Reload identity did not match");
        pending.current = null;
        if (!mounted.current) return;
        setPhase("idle");
        setError(undefined);
        if (authority.current !== attempt.authority) return;
        if (attempt.cancelling) {
          // A correlated successful outcome proves cleanup even if Stop raced with admission.
          // The user's stop intent still prevents displaying any returned record.
          setOutcomeAuthority(attempt.authority);
          setOutcomes((previous) => ({
            ...previous,
            [slot]: { state: "cancelled", detail: "Reload stopped. No record was restored." },
          }));
          return;
        }
        const current = currentReferences.current[slot];
        if (current === null || !sameKafkaRecordLocator(current, locator)) return;
        setOutcomeAuthority(attempt.authority);
        setOutcomes((previous) => ({
          ...previous,
          [slot]:
            outcome.state === "loaded"
              ? { state: "loaded" }
              : { state: outcome.state, detail: outcome.detail },
        }));
        if (outcome.state === "loaded")
          setLoaded((previous) => ({
            ...previous,
            [slot]: {
              authority: attempt.authority,
              message: unexplored(outcome.message),
            },
          }));
      } catch {
        if (mounted.current && pending.current === attempt && !attempt.cancelling) {
          setPhase("cleanup");
          setError("The host did not confirm the reload. Retry stop before starting another read.");
        }
      }
    },
    [connected, host, readBlocked],
  );
  const selected =
    connected && loaded.selected?.authority === currentAuthority ? loaded.selected.message : null;
  const baseline =
    connected && loaded.comparison?.authority === currentAuthority
      ? loaded.comparison.message
      : null;
  const capture = useCallback(
    (gridSelection: KafkaExploredMessage | null): KafkaSavedRecordContext =>
      parseKafkaSavedRecordContext({
        ...currentReferences.current,
        selected:
          gridSelection === null
            ? currentReferences.current.selected
            : kafkaRecordLocator(gridSelection),
      }),
    [],
  );
  return {
    references,
    selected,
    baseline,
    outcomes: outcomeAuthority === currentAuthority ? outcomes : {},
    busy: phase !== "idle",
    cleanupPending: phase === "cleanup",
    error,
    capture,
    restore,
    clear: () => restore(createEmptyKafkaSavedRecordContext()),
    choose,
    reload,
    cancel,
    selectGrid: (message) => setFromMessage("selected", message),
    pin: (message) => setFromMessage("comparison", message),
  };
}
