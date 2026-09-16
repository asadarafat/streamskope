import { useEffect, useMemo, useRef, useState } from "react";
import { AlertTitle, Box, Stack, Typography } from "@mui/material";

import {
  HOST_PROTOCOL_VERSION,
  KAFKA_FETCH_MODE_LABELS,
  KAFKA_OPERATIONAL_PREFERENCE_DEFAULTS,
  KAFKA_OPERATIONAL_PREFERENCE_LIMITS,
  KAFKA_OPERATIONAL_PREFERENCE_LOG_LEVELS,
  type HostCommand,
  type KafkaOperationalPreferenceSnapshot,
  type KafkaOperationalPreferences,
  type StreamSkopeHost,
} from "../contracts";
import {
  StudioAlert as Alert,
  StudioButton as Button,
  StudioDialog as Dialog,
  StudioDialogActions as DialogActions,
  StudioDialogContent as DialogContent,
  StudioDialogTitle as DialogTitle,
  StudioLabeledControl as FormControlLabel,
  StudioMenuItem as MenuItem,
  StudioSwitch as Switch,
  StudioTextField as TextField,
} from "../../../platform/ui/controls";

import {
  changedKafkaOperationalPreferenceGroups,
  createKafkaOperationalPreferenceDraft,
  rebaseKafkaOperationalPreferenceDraft,
  sameKafkaOperationalPreferenceDraft,
  validateKafkaOperationalPreferenceDraft,
  type KafkaOperationalPreferenceDraft,
  type KafkaOperationalPreferenceDraftField,
} from "./operational-preference-draft";

export interface OperationalPreferencesDialogProperties {
  readonly host: StreamSkopeHost;
  readonly loadError?: string;
  readonly onClose: () => void;
  readonly onOpenActivity: () => void;
  readonly open: boolean;
  readonly snapshot: KafkaOperationalPreferenceSnapshot | null;
}

type PendingOperation = "reset" | "save";

interface NumberPreferenceFieldProperties {
  readonly confirmed: number | null;
  readonly disabled: boolean;
  readonly field: KafkaOperationalPreferenceDraftField;
  readonly issue: string | undefined;
  readonly label: string;
  readonly limits: { readonly maximum: number; readonly minimum: number };
  readonly onChange: (field: KafkaOperationalPreferenceDraftField, value: string) => void;
  readonly scope: string;
  readonly unit: string;
  readonly value: string;
}

function NumberPreferenceField({
  confirmed,
  disabled,
  field,
  issue,
  label,
  limits,
  onChange,
  scope,
  unit,
  value,
}: NumberPreferenceFieldProperties): React.JSX.Element {
  return (
    <TextField
      disabled={disabled}
      error={issue !== undefined}
      fullWidth
      helperText={
        issue ??
        `${scope} ${limits.minimum.toLocaleString()}–${limits.maximum.toLocaleString()} ${unit} · ${
          confirmed === null
            ? "Awaiting host confirmation."
            : `Confirmed ${confirmed.toLocaleString()} ${unit}.`
        }`
      }
      label={label}
      onChange={(event) => {
        onChange(field, event.target.value);
      }}
      slotProps={{
        htmlInput: {
          max: limits.maximum,
          min: limits.minimum,
          step: 1,
        },
      }}
      type="number"
      value={value}
    />
  );
}

const SECTION_GRID = {
  display: "grid",
  gap: 2,
  gridTemplateColumns: "repeat(auto-fit, minmax(220px, 1fr))",
} as const;

function preferenceCommand(
  command: "preferences.reset",
): Extract<HostCommand, { readonly command: "preferences.reset" }>;
function preferenceCommand(
  command: "preferences.update",
  payload: Extract<HostCommand, { readonly command: "preferences.update" }>["payload"],
): Extract<HostCommand, { readonly command: "preferences.update" }>;
function preferenceCommand(
  command: "preferences.reset" | "preferences.update",
  payload?: Extract<HostCommand, { readonly command: "preferences.update" }>["payload"],
): HostCommand {
  return command === "preferences.reset"
    ? {
        command,
        id: globalThis.crypto.randomUUID(),
        payload: {},
        version: HOST_PROTOCOL_VERSION,
      }
    : {
        command,
        id: globalThis.crypto.randomUUID(),
        payload: payload ?? { patch: {} },
        version: HOST_PROTOCOL_VERSION,
      };
}

function storageLabel(store: KafkaOperationalPreferenceSnapshot["store"] | null): string {
  if (store === null) {
    return "Preference storage is being loaded.";
  }
  if (store.state === "unavailable") {
    return "Preference storage unavailable";
  }
  return `${store.durability === "durable" ? "Durable" : "Session-only"} storage`;
}

function saveConfirmation(snapshot: KafkaOperationalPreferenceSnapshot): string {
  return snapshot.store.durability === "durable"
    ? "Saved durable workbench preferences."
    : "Confirmed workbench preferences for this browser-development session.";
}

export function OperationalPreferencesDialog({
  host,
  loadError,
  onClose,
  onOpenActivity,
  open,
  snapshot,
}: OperationalPreferencesDialogProperties): React.JSX.Element {
  const initialConfirmed = snapshot?.preferences ?? KAFKA_OPERATIONAL_PREFERENCE_DEFAULTS;
  const confirmedReference = useRef<KafkaOperationalPreferences>(initialConfirmed);
  const [confirmed, setConfirmed] = useState<KafkaOperationalPreferences>(initialConfirmed);
  const [draft, setDraft] = useState<KafkaOperationalPreferenceDraft>(() =>
    createKafkaOperationalPreferenceDraft(initialConfirmed),
  );
  const [failure, setFailure] = useState<string>();
  const [pending, setPending] = useState<PendingOperation>();
  const [resetConfirmationOpen, setResetConfirmationOpen] = useState(false);
  const [store, setStore] = useState<KafkaOperationalPreferenceSnapshot["store"] | null>(
    snapshot?.store ?? null,
  );
  const [status, setStatus] = useState("");
  const resetButton = useRef<HTMLButtonElement>(null);

  useEffect(() => {
    if (snapshot === null) {
      return;
    }
    const previousDraft = createKafkaOperationalPreferenceDraft(confirmedReference.current);
    const nextDraft = createKafkaOperationalPreferenceDraft(snapshot.preferences);
    setDraft((current) => rebaseKafkaOperationalPreferenceDraft(previousDraft, nextDraft, current));
    confirmedReference.current = snapshot.preferences;
    setConfirmed(snapshot.preferences);
    setStore(snapshot.store);
  }, [snapshot]);

  const validation = useMemo(() => validateKafkaOperationalPreferenceDraft(draft), [draft]);
  const confirmedDraft = useMemo(
    () => createKafkaOperationalPreferenceDraft(confirmed),
    [confirmed],
  );
  const dirty = !sameKafkaOperationalPreferenceDraft(draft, confirmedDraft);
  const patch = useMemo(
    () =>
      validation.preferences === null
        ? {}
        : changedKafkaOperationalPreferenceGroups(confirmed, validation.preferences),
    [confirmed, validation.preferences],
  );
  const hasValidChanges = validation.preferences !== null && Object.keys(patch).length > 0;
  const storageReady = store?.state === "ready";
  const controlsDisabled = pending !== undefined;

  function updateDraft<K extends keyof KafkaOperationalPreferenceDraft>(
    field: K,
    value: KafkaOperationalPreferenceDraft[K],
  ): void {
    setDraft((current) => ({ ...current, [field]: value }));
    setFailure(undefined);
    setStatus("");
  }

  function updateText(field: KafkaOperationalPreferenceDraftField, value: string): void {
    updateDraft(field, value);
  }

  function acceptSnapshot(next: KafkaOperationalPreferenceSnapshot, confirmation: string): void {
    confirmedReference.current = next.preferences;
    setConfirmed(next.preferences);
    setDraft(createKafkaOperationalPreferenceDraft(next.preferences));
    setStore(next.store);
    setFailure(undefined);
    setStatus(confirmation);
  }

  async function save(): Promise<void> {
    if (!storageReady || !hasValidChanges) {
      return;
    }
    setPending("save");
    setFailure(undefined);
    setStatus("");
    try {
      const response = await host.execute(preferenceCommand("preferences.update", { patch }));
      if (!response.ok) {
        setFailure(`${response.error.summary} ${response.error.recovery}`);
        return;
      }
      if (!("snapshot" in response.result)) {
        setFailure(
          "The host returned no confirmed preference snapshot. Open Activity for diagnostics.",
        );
        return;
      }
      acceptSnapshot(response.result.snapshot, saveConfirmation(response.result.snapshot));
    } catch {
      setFailure(
        "The application host did not confirm the preference save. The unsaved draft is retained. Open Activity for diagnostics.",
      );
    } finally {
      setPending((current) => (current === "save" ? undefined : current));
    }
  }

  async function reset(): Promise<void> {
    setPending("reset");
    setFailure(undefined);
    setStatus("");
    try {
      const response = await host.execute(preferenceCommand("preferences.reset"));
      if (!response.ok) {
        setFailure(`${response.error.summary} ${response.error.recovery}`);
        return;
      }
      if (!("snapshot" in response.result)) {
        setFailure(
          "The host returned no confirmed factory snapshot. Open Activity for diagnostics.",
        );
        return;
      }
      acceptSnapshot(response.result.snapshot, "Factory workbench preferences confirmed.");
      setResetConfirmationOpen(false);
    } catch {
      setFailure(
        "The application host did not confirm the preference reset. Existing preferences remain authoritative. Open Activity for diagnostics.",
      );
    } finally {
      setPending((current) => (current === "reset" ? undefined : current));
    }
  }

  const stateDescription =
    pending === "save"
      ? "Saving workbench preferences."
      : pending === "reset"
        ? "Resetting workbench preferences."
        : status.length > 0
          ? status
          : failure !== undefined
            ? "Preference operation failed. Unsaved changes remain."
            : dirty
              ? "Unsaved workbench preference changes."
              : store === null
                ? "Loading workbench preferences."
                : store.state === "unavailable"
                  ? "Factory preferences are shown because preference storage is unavailable."
                  : `Confirmed ${
                      store.durability === "durable" ? "durable" : "session-only"
                    } workbench preferences.`;

  return (
    <>
      <Dialog
        aria-labelledby="kafka-operational-preferences-title"
        fullWidth
        maxWidth="md"
        onClose={controlsDisabled ? undefined : onClose}
        open={open}
      >
        <DialogTitle id="kafka-operational-preferences-title">Workbench Preferences</DialogTitle>
        <DialogContent dividers>
          <Stack spacing={3}>
            <Box>
              <Typography color="text.secondary" sx={{ mb: 1.5 }} variant="body2">
                Defaults and limits for message reads, monitoring, latency probes, and rule output.
              </Typography>
              <Typography component="p" variant="subtitle2">
                {storageLabel(store)}
              </Typography>
              <Typography color="text.secondary" variant="body2">
                {store === null
                  ? "Awaiting host confirmation. Factory values are shown as a non-durable fallback and cannot be saved."
                  : store.state === "unavailable"
                    ? (store.recovery ??
                      "Reset Workbench Preferences or restore application storage.")
                    : store.durability === "durable"
                      ? "Confirmed changes are stored in the Electron application-data directory."
                      : "Browser-development changes last only for this host process."}
              </Typography>
            </Box>

            {loadError === undefined ? null : (
              <Alert
                action={
                  <Button color="inherit" onClick={onOpenActivity} variant="text">
                    Open activity
                  </Button>
                }
                severity="error"
              >
                <AlertTitle>Preferences could not be loaded</AlertTitle>
                {loadError}
              </Alert>
            )}
            {failure === undefined ? null : (
              <Alert
                action={
                  <Button color="inherit" onClick={onOpenActivity} variant="text">
                    Open activity
                  </Button>
                }
                severity="error"
              >
                <AlertTitle>Preference operation failed</AlertTitle>
                {failure}
              </Alert>
            )}
            {dirty ? (
              <Box
                sx={{
                  borderLeft: 3,
                  borderColor: "warning.main",
                  pl: 1.5,
                }}
              >
                <Typography component="p" variant="subtitle2">
                  Unsaved changes
                </Typography>
                <Typography color="text.secondary" variant="body2">
                  The last confirmed snapshot remains active until Save succeeds.
                </Typography>
              </Box>
            ) : null}

            <Typography aria-live="polite" role="status" variant="body2">
              {stateDescription}
            </Typography>

            <Box component="section">
              <Typography component="h3" variant="subtitle1">
                Fetch and stream
              </Typography>
              <Typography color="text.secondary" sx={{ mb: 2 }} variant="body2">
                Defaults initialize the next request. Active operations keep their original limits.
              </Typography>
              <Box sx={SECTION_GRID}>
                <TextField
                  disabled={controlsDisabled}
                  fullWidth
                  helperText={`Next request · ${
                    store === null
                      ? "Awaiting host confirmation."
                      : `Confirmed ${KAFKA_FETCH_MODE_LABELS[confirmed.fetch.mode]}.`
                  }`}
                  label="Default fetch mode"
                  onChange={(event) => {
                    updateDraft("fetchMode", event.target.value as typeof draft.fetchMode);
                  }}
                  select
                  value={draft.fetchMode}
                >
                  {(["tail", "newest", "earliest", "time-window"] as const).map((mode) => (
                    <MenuItem key={mode} value={mode}>
                      {KAFKA_FETCH_MODE_LABELS[mode]}
                    </MenuItem>
                  ))}
                </TextField>
                <NumberPreferenceField
                  confirmed={store === null ? null : confirmed.fetch.maxMessages}
                  disabled={controlsDisabled}
                  field="fetchMaxMessages"
                  issue={validation.issues.fetchMaxMessages}
                  label="Default maximum results"
                  limits={KAFKA_OPERATIONAL_PREFERENCE_LIMITS.fetchMessages}
                  onChange={updateText}
                  scope="Topic-wide maximum for the next request."
                  unit="messages"
                  value={draft.fetchMaxMessages}
                />
                <NumberPreferenceField
                  confirmed={store === null ? null : confirmed.stream.queueDepth}
                  disabled={controlsDisabled}
                  field="streamQueueDepth"
                  issue={validation.issues.streamQueueDepth}
                  label="Stream queue depth"
                  limits={KAFKA_OPERATIONAL_PREFERENCE_LIMITS.queueDepth}
                  onChange={updateText}
                  scope="Maximum records awaiting renderer delivery."
                  unit="records"
                  value={draft.streamQueueDepth}
                />
                <NumberPreferenceField
                  confirmed={store === null ? null : confirmed.stream.batchSize}
                  disabled={controlsDisabled}
                  field="streamBatchSize"
                  issue={validation.issues.streamBatchSize}
                  label="Stream batch size"
                  limits={KAFKA_OPERATIONAL_PREFERENCE_LIMITS.batchSize}
                  onChange={updateText}
                  scope="Maximum records in one renderer delivery."
                  unit="records"
                  value={draft.streamBatchSize}
                />
                <NumberPreferenceField
                  confirmed={store === null ? null : confirmed.stream.intervalMs}
                  disabled={controlsDisabled}
                  field="streamIntervalMs"
                  issue={validation.issues.streamIntervalMs}
                  label="Stream delivery interval"
                  limits={KAFKA_OPERATIONAL_PREFERENCE_LIMITS.intervalMs}
                  onChange={updateText}
                  scope="Maximum shaping interval between deliveries."
                  unit="ms"
                  value={draft.streamIntervalMs}
                />
                <NumberPreferenceField
                  confirmed={store === null ? null : confirmed.stream.historySamples}
                  disabled={controlsDisabled}
                  field="streamHistorySamples"
                  issue={validation.issues.streamHistorySamples}
                  label="Monitor history samples"
                  limits={KAFKA_OPERATIONAL_PREFERENCE_LIMITS.historySamples}
                  onChange={updateText}
                  scope="Retained Stream Monitor samples for the active request."
                  unit="samples"
                  value={draft.streamHistorySamples}
                />
              </Box>
            </Box>

            <Box component="section">
              <Typography component="h3" variant="subtitle1">
                Latency
              </Typography>
              <Typography color="text.secondary" sx={{ mb: 2 }} variant="body2">
                Defaults initialize the next explicit probe. Running and completed probes never
                change.
              </Typography>
              <Box sx={SECTION_GRID}>
                <NumberPreferenceField
                  confirmed={store === null ? null : confirmed.latency.messageCount}
                  disabled={controlsDisabled}
                  field="latencyMessageCount"
                  issue={validation.issues.latencyMessageCount}
                  label="Default probe records"
                  limits={KAFKA_OPERATIONAL_PREFERENCE_LIMITS.latencyMessages}
                  onChange={updateText}
                  scope="Synthetic records proposed by the next probe."
                  unit="records"
                  value={draft.latencyMessageCount}
                />
                <TextField
                  disabled={controlsDisabled}
                  fullWidth
                  helperText="Broker response required by the next explicit probe."
                  label="Default acknowledgements"
                  onChange={(event) => {
                    updateDraft(
                      "latencyAcknowledgements",
                      Number(event.target.value) as typeof draft.latencyAcknowledgements,
                    );
                  }}
                  select
                  value={draft.latencyAcknowledgements}
                >
                  <MenuItem value={-1}>All in-sync replicas</MenuItem>
                  <MenuItem value={1}>Leader</MenuItem>
                  <MenuItem value={0}>No broker response</MenuItem>
                </TextField>
                <NumberPreferenceField
                  confirmed={store === null ? null : confirmed.latency.timeoutMs}
                  disabled={controlsDisabled}
                  field="latencyTimeoutMs"
                  issue={validation.issues.latencyTimeoutMs}
                  label="Default probe timeout"
                  limits={KAFKA_OPERATIONAL_PREFERENCE_LIMITS.latencyTimeoutMs}
                  onChange={updateText}
                  scope="Overall timeout for the next explicit probe."
                  unit="ms"
                  value={draft.latencyTimeoutMs}
                />
                <TextField
                  disabled={controlsDisabled}
                  error={validation.issues.latencyRunbookUrl !== undefined}
                  fullWidth
                  helperText={
                    validation.issues.latencyRunbookUrl ??
                    "Credential-free HTTPS recovery URL · Maximum 2,048 characters."
                  }
                  label="Latency runbook URL"
                  onChange={(event) => {
                    updateDraft("latencyRunbookUrl", event.target.value);
                  }}
                  slotProps={{
                    htmlInput: {
                      maxLength: KAFKA_OPERATIONAL_PREFERENCE_LIMITS.runbookCharacters,
                    },
                  }}
                  type="url"
                  value={draft.latencyRunbookUrl}
                />
              </Box>
            </Box>

            <Box component="section">
              <Typography component="h3" variant="subtitle1">
                Rules
              </Typography>
              <Typography color="text.secondary" sx={{ mb: 1 }} variant="body2">
                Optional output controls apply only to later active matches. Required evaluation
                failures and retained message evidence are never suppressed.
              </Typography>
              <Stack spacing={1}>
                <FormControlLabel
                  control={
                    <Switch
                      checked={draft.ruleNotificationsEnabled}
                      disabled={controlsDisabled}
                      onChange={(event) => {
                        updateDraft("ruleNotificationsEnabled", event.target.checked);
                      }}
                    />
                  }
                  label="Show rule match notifications"
                />
                <FormControlLabel
                  control={
                    <Switch
                      checked={draft.ruleLoggingEnabled}
                      disabled={controlsDisabled}
                      onChange={(event) => {
                        updateDraft("ruleLoggingEnabled", event.target.checked);
                      }}
                    />
                  }
                  label="Record successful rule matches in Activity"
                />
                <TextField
                  disabled={controlsDisabled || !draft.ruleLoggingEnabled}
                  helperText={
                    draft.ruleLoggingEnabled
                      ? "Used for successful match Activity entries. Mandatory failures always bypass this threshold."
                      : "Enable successful rule-match Activity recording to choose a threshold. Mandatory failures are always recorded."
                  }
                  label="Rule Activity threshold"
                  onChange={(event) => {
                    updateDraft("ruleLogLevel", event.target.value as typeof draft.ruleLogLevel);
                  }}
                  select
                  value={draft.ruleLogLevel}
                >
                  {KAFKA_OPERATIONAL_PREFERENCE_LOG_LEVELS.map((level) => (
                    <MenuItem key={level} value={level}>
                      {level}
                    </MenuItem>
                  ))}
                </TextField>
              </Stack>
            </Box>
          </Stack>
        </DialogContent>
        <DialogActions sx={{ flexWrap: "wrap", gap: 1 }}>
          <Button
            color="error"
            disabled={controlsDisabled || store === null}
            onClick={() => {
              setResetConfirmationOpen(true);
            }}
            ref={resetButton}
            variant="text"
          >
            Reset workbench preferences
          </Button>
          <Box sx={{ flex: 1 }} />
          <Button disabled={controlsDisabled} onClick={onClose} variant="text">
            Close
          </Button>
          <Button
            disabled={!storageReady || !hasValidChanges || controlsDisabled}
            onClick={() => {
              void save();
            }}
            variant="contained"
          >
            Save preferences
          </Button>
        </DialogActions>
      </Dialog>

      <Dialog
        aria-labelledby="reset-kafka-operational-preferences-title"
        onClose={
          pending === "reset"
            ? undefined
            : (): void => {
                setResetConfirmationOpen(false);
              }
        }
        open={resetConfirmationOpen}
      >
        <DialogTitle id="reset-kafka-operational-preferences-title">
          Reset Workbench Preferences?
        </DialogTitle>
        <DialogContent dividers>
          <Typography variant="body2">
            Replace only workbench preferences with the factory snapshot. Profiles, rules,
            templates, topic history, and Kafka data are unaffected.
          </Typography>
        </DialogContent>
        <DialogActions>
          <Button
            disabled={pending === "reset"}
            onClick={() => {
              setResetConfirmationOpen(false);
              globalThis.setTimeout(() => resetButton.current?.focus(), 0);
            }}
            variant="text"
          >
            Cancel
          </Button>
          <Button
            color="error"
            disabled={pending === "reset"}
            onClick={() => {
              void reset();
            }}
            variant="contained"
          >
            Reset workbench preferences
          </Button>
        </DialogActions>
      </Dialog>
    </>
  );
}
