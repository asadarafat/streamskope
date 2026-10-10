import { useEffect, useRef, useState } from "react";
import {
  Box,
  Stack,
  Typography,
  Table,
  TableHead,
  TableBody,
  TableRow,
  TableCell,
} from "@mui/material";

import { HOST_PROTOCOL_VERSION, type StreamSkopeHost } from "../contracts";
import {
  CLIENT_QUOTA_KEYS,
  parseClientQuotaEntity,
  parseClientQuotaInput,
  clientQuotaLabel,
  type ClientQuotaKey,
  type ClientQuotaComponent,
  type ClientQuotaSnapshot,
  type ClientQuotaReview,
  type ClientQuotaOutcome,
} from "../contracts/client-quotas";
import {
  StudioButton as Button,
  StudioDialog as Dialog,
  StudioDialogTitle as DialogTitle,
  StudioDialogContent as DialogContent,
  StudioDialogActions as DialogActions,
  StudioTextField as TextField,
  StudioAlert as Alert,
  StudioCheckbox as Checkbox,
  StudioFormControl as FormControl,
  StudioInputLabel as InputLabel,
  StudioSelect as Select,
  StudioMenuItem as MenuItem,
  StudioLabeledControl as FormControlLabel,
} from "../../../platform/ui/controls";

const units: Record<ClientQuotaKey, string> = {
  producer_byte_rate: "bytes/s per broker",
  consumer_byte_rate: "bytes/s per broker",
  request_percentage: "network/handler-thread % per broker",
  controller_mutation_rate: "mutations/s",
};
type Mode = "keep" | "set" | "remove";
type Dimension = ClientQuotaComponent["type"];
const dimensions: readonly Dimension[] = ["user", "client-id"];

export function ClientQuotaAction({
  host,
  connected,
  connectionName,
  canWrite,
}: {
  readonly host: StreamSkopeHost;
  readonly connected: boolean;
  readonly connectionName: string | null;
  readonly canWrite: boolean;
}): React.JSX.Element {
  const [open, setOpen] = useState(false),
    [busy, setBusy] = useState(false),
    [attempted, setAttempted] = useState(false);
  const [included, setIncluded] = useState<Record<Dimension, boolean>>({
      user: true,
      "client-id": false,
    }),
    [defaults, setDefaults] = useState<Record<Dimension, boolean>>({
      user: false,
      "client-id": false,
    }),
    [names, setNames] = useState<Record<Dimension, string>>({ user: "", "client-id": "" });
  const [changes, setChanges] = useState<
    Partial<Record<ClientQuotaKey, { mode: Mode; value: string }>>
  >({});
  const [snapshot, setSnapshot] = useState<ClientQuotaSnapshot>(),
    [review, setReview] = useState<ClientQuotaReview>(),
    [outcome, setOutcome] = useState<ClientQuotaOutcome>();
  const [confirmation, setConfirmation] = useState(""),
    [error, setError] = useState<string>();
  const generation = useRef(0);
  useEffect(() => {
    generation.current++;
    setOpen(false);
    setBusy(false);
    setAttempted(false);
    setSnapshot(undefined);
    setReview(undefined);
    setOutcome(undefined);
    setConfirmation("");
    setError(undefined);
    return (): void => {
      generation.current++;
    };
  }, [host, connected, connectionName, canWrite]);
  const clear = (entityChanged = false): void => {
    generation.current++;
    setReview(undefined);
    setOutcome(undefined);
    setConfirmation("");
    setError(undefined);
    setAttempted(false);
    if (entityChanged) setSnapshot(undefined);
  };
  const entity = (): ReturnType<typeof parseClientQuotaEntity> =>
    parseClientQuotaEntity(
      dimensions
        .filter((type) => included[type])
        .map((type) => ({ type, name: defaults[type] ? null : names[type] })),
    );
  const prepare = async (kind: "inspect" | "review"): Promise<void> => {
    const current = generation.current;
    setBusy(true);
    setReview(undefined);
    setOutcome(undefined);
    setConfirmation("");
    setError(undefined);
    try {
      const selected = entity();
      if (kind === "inspect") {
        setSnapshot(undefined);
        const response = await host.execute({
          command: "quotas.inspect",
          id: crypto.randomUUID(),
          version: HOST_PROTOCOL_VERSION,
          payload: { entity: selected },
        });
        if (generation.current !== current) return;
        if (response.ok) setSnapshot(response.result.snapshot);
        else setError(response.error.summary + " " + response.error.recovery);
      } else {
        const input = parseClientQuotaInput({
          entity: selected,
          changes: CLIENT_QUOTA_KEYS.filter(
            (key) => changes[key]?.mode && changes[key]?.mode !== "keep",
          ).map((key) => {
            const change = changes[key]!;
            if (change.mode === "set" && !change.value.trim())
              throw new Error("Enter a quota value.");
            return { key, value: change.mode === "remove" ? null : Number(change.value) };
          }),
        });
        const response = await host.execute({
          command: "quotas.change.review",
          id: crypto.randomUUID(),
          version: HOST_PROTOCOL_VERSION,
          payload: input,
        });
        if (generation.current !== current) return;
        if (response.ok) {
          setReview(response.result.review);
          setSnapshot(response.result.review.baseline);
        } else setError(response.error.summary + " " + response.error.recovery);
      }
    } catch {
      if (generation.current === current)
        setError(
          "Select at least one exact user/client-ID or its explicit default entry. Set requires a finite positive number; byte rates require a safe whole number; remove is a separate action. Inspect and review again.",
        );
    } finally {
      if (generation.current === current) setBusy(false);
    }
  };
  const apply = async (): Promise<void> => {
    if (!review || busy || attempted || !canWrite || confirmation !== review.confirmation) return;
    const current = generation.current;
    setBusy(true);
    setAttempted(true);
    setError(undefined);
    try {
      const response = await host.execute({
        command: "quotas.change.apply",
        id: crypto.randomUUID(),
        version: HOST_PROTOCOL_VERSION,
        payload: { planId: review.planId, confirmation },
      });
      if (generation.current !== current) return;
      if (response.ok) setOutcome(response.result.outcome);
      else setError(response.error.summary + " " + response.error.recovery);
    } catch {
      if (generation.current === current)
        setError(
          "The result did not reach this view. Inspect Activity and the exact entity before another attempt; this review cannot be resent.",
        );
    } finally {
      if (generation.current === current) setBusy(false);
    }
  };
  const close = (): void => {
    if (!busy) {
      generation.current++;
      setOpen(false);
    }
  };
  const selectedChanges = CLIENT_QUOTA_KEYS.some(
    (key) => changes[key]?.mode && changes[key]?.mode !== "keep",
  );
  return (
    <>
      <Button
        variant="outlined"
        disabled={!connected}
        onClick={() => {
          clear(true);
          setChanges({});
          setOpen(true);
        }}
      >
        Client quotas…
      </Button>
      <Dialog open={open} onClose={close} maxWidth="md" fullWidth>
        <DialogTitle>Inspect and review client quotas</DialogTitle>
        <DialogContent>
          <Stack spacing={2} sx={{ pt: 1 }}>
            <Typography>Connection: {connectionName}</Typography>
            <Alert severity="info">
              Inspect one exact entity. Values are explicit entries, not effective or inherited
              quotas. Successful inspection does not establish permission to change quotas.
            </Alert>
            <Stack direction={{ xs: "column", sm: "row" }} spacing={2}>
              {dimensions.map((type) => (
                <Stack spacing={1} key={type} sx={{ flex: 1 }}>
                  <FormControlLabel
                    control={
                      <Checkbox
                        checked={included[type]}
                        disabled={busy || attempted}
                        onChange={(_, checked) => {
                          clear(true);
                          setIncluded({ ...included, [type]: checked });
                        }}
                      />
                    }
                    label={type === "user" ? "Include user" : "Include client ID"}
                  />
                  <FormControlLabel
                    control={
                      <Checkbox
                        checked={defaults[type]}
                        disabled={busy || attempted || !included[type]}
                        onChange={(_, checked) => {
                          clear(true);
                          setDefaults({ ...defaults, [type]: checked });
                        }}
                      />
                    }
                    label={type === "user" ? "Default user entry" : "Default client-ID entry"}
                  />
                  <TextField
                    label={type === "user" ? "Exact Kafka user" : "Exact Kafka client ID"}
                    value={names[type]}
                    disabled={busy || attempted || !included[type] || defaults[type]}
                    onChange={(e) => {
                      clear(true);
                      setNames({ ...names, [type]: e.target.value });
                    }}
                    fullWidth
                  />
                </Stack>
              ))}
            </Stack>
            {dimensions.some((type) => included[type] && defaults[type]) && (
              <Alert severity="warning">
                Default entries can affect many clients. Removing an explicit key restores Kafka's
                quota resolution; the inherited result is not measured here.
              </Alert>
            )}
            <Button disabled={busy || attempted} onClick={() => void prepare("inspect")}>
              Inspect exact quotas
            </Button>
            {snapshot && (
              <>
                <Typography>
                  {clientQuotaLabel(snapshot.entity)} · cluster {snapshot.clusterId}
                </Typography>
                {!snapshot.alterSupported && (
                  <Alert severity="warning">
                    AlterClientQuotas is unavailable. Inspection remains separate from modification
                    support.
                  </Alert>
                )}
                <Box sx={{ overflowX: "auto" }}>
                  <Table size="small" aria-label="Explicit client quotas">
                    <TableHead>
                      <TableRow>
                        <TableCell>Key</TableCell>
                        <TableCell>Explicit value</TableCell>
                        <TableCell>Units</TableCell>
                      </TableRow>
                    </TableHead>
                    <TableBody>
                      {snapshot.values.map((value) => (
                        <TableRow key={value.key}>
                          <TableCell>{value.key}</TableCell>
                          <TableCell>{value.value}</TableCell>
                          <TableCell>
                            {units[value.key as ClientQuotaKey] ?? "Broker-defined"}
                          </TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                </Box>
                {!snapshot.values.length && (
                  <Typography>
                    No explicit quotas were returned for exactly this entity. Effective or inherited
                    limits remain unknown.
                  </Typography>
                )}
                <Alert severity="warning">
                  Kafka has no atomic quota compare-and-set. The host rechecks the complete explicit
                  baseline immediately before admission, but another operator can still race it.
                  Review the actual receipt and readback before another change.
                </Alert>
                <Box sx={{ overflowX: "auto" }}>
                  <Table size="small" aria-label="Select client quota changes">
                    <TableHead>
                      <TableRow>
                        <TableCell>Key / units</TableCell>
                        <TableCell>Change</TableCell>
                        <TableCell>Set value</TableCell>
                      </TableRow>
                    </TableHead>
                    <TableBody>
                      {CLIENT_QUOTA_KEYS.map((key) => (
                        <TableRow key={key}>
                          <TableCell>
                            {key}
                            <Typography variant="caption" sx={{ display: "block" }}>
                              {units[key]}
                            </Typography>
                          </TableCell>
                          <TableCell>
                            <FormControl fullWidth size="small">
                              <InputLabel id={"quota-mode-" + key}>Change {key}</InputLabel>
                              <Select
                                labelId={"quota-mode-" + key}
                                label={"Change " + key}
                                value={changes[key]?.mode ?? "keep"}
                                disabled={
                                  busy || attempted || !canWrite || !snapshot.alterSupported
                                }
                                onChange={(e) => {
                                  clear();
                                  setChanges({
                                    ...changes,
                                    [key]: {
                                      mode: e.target.value,
                                      value: changes[key]?.value ?? "",
                                    },
                                  });
                                }}
                              >
                                <MenuItem value="keep">Keep unchanged</MenuItem>
                                <MenuItem value="set">Set explicit value</MenuItem>
                                <MenuItem value="remove">Remove explicit key</MenuItem>
                              </Select>
                            </FormControl>
                          </TableCell>
                          <TableCell>
                            <TextField
                              label={"Value " + key}
                              value={changes[key]?.value ?? ""}
                              disabled={
                                busy || attempted || !canWrite || changes[key]?.mode !== "set"
                              }
                              onChange={(e) => {
                                clear();
                                setChanges({
                                  ...changes,
                                  [key]: { mode: "set", value: e.target.value },
                                });
                              }}
                            />
                          </TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                </Box>
                <Typography variant="body2">
                  Zero is refused as a set value; removal is explicit. Every unselected explicit key
                  is preserved. Read-only mode permits inspection and blocks apply.
                </Typography>
                <Button
                  disabled={
                    busy || attempted || !canWrite || !snapshot.alterSupported || !selectedChanges
                  }
                  onClick={() => void prepare("review")}
                >
                  Review quota changes
                </Button>
              </>
            )}
            {review && (
              <>
                <Typography>
                  Reviewed on {review.connectionName} · expires {review.expiresAt}
                </Typography>
                <Box sx={{ overflowX: "auto" }}>
                  <Table size="small" aria-label="Reviewed client quota changes">
                    <TableHead>
                      <TableRow>
                        <TableCell>Key</TableCell>
                        <TableCell>Before</TableCell>
                        <TableCell>After</TableCell>
                      </TableRow>
                    </TableHead>
                    <TableBody>
                      {review.input.changes.map((change) => (
                        <TableRow key={change.key}>
                          <TableCell>{change.key}</TableCell>
                          <TableCell>
                            {review.baseline.values.find((v) => v.key === change.key)?.value ??
                              "No explicit key"}
                          </TableCell>
                          <TableCell>
                            {change.value === null
                              ? "Remove; inherited result unknown"
                              : change.value}
                          </TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                </Box>
                <TextField
                  label="Confirm exact quota change"
                  helperText={review.confirmation}
                  value={confirmation}
                  disabled={busy || attempted}
                  onChange={(e) => setConfirmation(e.target.value)}
                  fullWidth
                />
              </>
            )}
            {outcome && (
              <Alert
                severity={
                  outcome.state === "acknowledged" &&
                  outcome.verification === "verified" &&
                  outcome.cleanup === "confirmed"
                    ? "success"
                    : "warning"
                }
              >
                {outcome.state} · readback {outcome.verification} · cleanup {outcome.cleanup}.{" "}
                {outcome.detail}
                {outcome.observed !== null && (
                  <Typography variant="body2">
                    Observed explicit keys:{" "}
                    {outcome.observed.map((v) => v.key + "=" + String(v.value)).join(", ") ||
                      "none"}
                    . Effective or inherited quotas remain unknown.
                  </Typography>
                )}
              </Alert>
            )}
            {error && <Alert severity="error">{error}</Alert>}
          </Stack>
        </DialogContent>
        <DialogActions>
          <Button disabled={busy} onClick={close}>
            Close
          </Button>
          <Button
            variant="contained"
            disabled={
              !review ||
              busy ||
              attempted ||
              !canWrite ||
              confirmation !== review.confirmation ||
              Date.now() >= Date.parse(review.expiresAt)
            }
            onClick={() => void apply()}
          >
            Apply reviewed quota changes
          </Button>
        </DialogActions>
      </Dialog>
    </>
  );
}
