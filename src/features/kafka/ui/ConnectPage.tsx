import { useEffect, useRef, useState } from "react";
import { Stack, Typography, Table, TableHead, TableBody, TableRow, TableCell } from "@mui/material";

import {
  StudioAlert as Alert,
  StudioButton as Button,
  StudioTextField as TextField,
  StudioMenuItem as MenuItem,
} from "../../../platform/ui/controls";
import { StudioCodeBlock } from "../../../platform/ui/StudioCodeBlock";
import { HOST_PROTOCOL_VERSION, type StreamSkopeHost } from "../contracts";
import {
  CONNECT_ACTIONS,
  parseConnectInput,
  type ConnectInput,
  type ConnectInventory,
  type ConnectDetail,
  type ConnectReview,
  type ConnectValidation,
  type ConnectOutcome,
} from "../contracts/connect";
export function ConnectPage({
  host,
  canWrite,
  onOpenTopic,
}: {
  readonly host: StreamSkopeHost;
  readonly canWrite: boolean;
  readonly onOpenTopic: (topic: string) => void;
}): React.JSX.Element {
  const [inventory, setInventory] = useState<ConnectInventory>({ names: [], plugins: [] }),
    [detail, setDetail] = useState<ConnectDetail>();
  const [name, setName] = useState(""),
    [action, setAction] = useState<ConnectInput["action"]>("create"),
    [config, setConfig] = useState('{"connector.class":"","tasks.max":"1"}');
  const [review, setReview] = useState<ConnectReview>(),
    [validation, setValidation] = useState<ConnectValidation>(),
    [outcome, setOutcome] = useState<ConnectOutcome>(),
    [confirmation, setConfirmation] = useState(""),
    [error, setError] = useState(""),
    [busy, setBusy] = useState(false),
    [attempted, setAttempted] = useState(false);
  const mounted = useRef(true);
  useEffect(() => {
    mounted.current = true;
    return (): void => {
      mounted.current = false;
    };
  }, []);
  const invalidate = (): void => {
    setReview(undefined);
    setOutcome(undefined);
    setValidation(undefined);
    setConfirmation("");
    setAttempted(false);
  };
  const run = async (fn: () => Promise<void>): Promise<void> => {
    if (busy) return;
    setBusy(true);
    setError("");
    try {
      await fn();
    } catch {
      if (mounted.current)
        setError(
          "Check the configured Connect endpoint, permissions and JSON configuration. Refresh status and validate before reviewing an action.",
        );
    } finally {
      if (mounted.current) setBusy(false);
    }
  };
  const refresh = async (): Promise<void> => {
    const r = await host.execute({
      command: "connect.list",
      id: crypto.randomUUID(),
      version: HOST_PROTOCOL_VERSION,
      payload: {},
    });
    if (!r.ok) throw new Error();
    if (mounted.current) setInventory(r.result.inventory);
  };
  useEffect(() => {
    void run(refresh);
  }, [host]);
  const load = async (n: string): Promise<void> => {
    invalidate();
    const r = await host.execute({
      command: "connect.load",
      id: crypto.randomUUID(),
      version: HOST_PROTOCOL_VERSION,
      payload: { name: n },
    });
    if (!r.ok) throw new Error();
    if (mounted.current) {
      setDetail(r.result.detail);
      setName(n);
      setAction("update");
      setConfig("{}");
    }
  };
  const input = (): ConnectInput =>
    parseConnectInput({
      name,
      action,
      config: action === "create" || action === "update" ? (JSON.parse(config) as unknown) : {},
    });
  return (
    <Stack
      component="main"
      aria-label="Kafka Connect page"
      spacing={2}
      sx={{ p: 3, overflow: "auto", height: "100%" }}
    >
      <Typography variant="h5" component="h1">
        Kafka Connect
      </Typography>
      <Typography>
        Manage connectors through the REST endpoint saved in your connection profile. Worker
        plugins, tasks and supported DLQs are separate from Redpanda transforms.
      </Typography>
      <Stack direction="row" spacing={1}>
        <Button
          disabled={busy}
          onClick={() => {
            void run(refresh);
          }}
        >
          Refresh connectors
        </Button>
        <Button
          disabled={busy}
          onClick={() => {
            invalidate();
            setDetail(undefined);
            setName("");
            setAction("create");
            setConfig('{"connector.class":"","tasks.max":"1"}');
          }}
        >
          New connector
        </Button>
      </Stack>
      <TextField
        select
        label="Existing connector"
        value={detail?.name ?? ""}
        disabled={busy}
        onChange={(e) => {
          void run(() => load(e.target.value));
        }}
      >
        <MenuItem value="">Select a connector</MenuItem>
        {inventory.names.map((n) => (
          <MenuItem key={n} value={n}>
            {n}
          </MenuItem>
        ))}
      </TextField>
      <Typography variant="body2">
        Installed classes: {inventory.plugins.join(", ") || "No plugins loaded"}
      </Typography>
      {detail && (
        <>
          <Typography>
            Observed {detail.observedAt} · Connector {detail.name}: {detail.state}
          </Typography>
          <Table size="small" aria-label="Connect tasks">
            <TableHead>
              <TableRow>
                <TableCell>Task</TableCell>
                <TableCell>State</TableCell>
                <TableCell>Next step</TableCell>
              </TableRow>
            </TableHead>
            <TableBody>
              {detail.tasks.map((t) => (
                <TableRow key={t.id}>
                  <TableCell>{t.id}</TableCell>
                  <TableCell>{t.state}</TableCell>
                  <TableCell>{t.failure || "No failure reported"}</TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
          <StudioCodeBlock>{JSON.stringify(detail.config, null, 2)}</StudioCodeBlock>
          {detail.dlq ? (
            <Button onClick={() => onOpenTopic(detail.dlq!)}>Browse DLQ: {detail.dlq}</Button>
          ) : (
            <Typography variant="body2">
              No supported DLQ topic is configured. A failed task does not imply a record can be
              skipped.
            </Typography>
          )}
        </>
      )}
      <Stack direction={{ xs: "column", md: "row" }} spacing={2}>
        <TextField
          label="Connector name"
          value={name}
          disabled={busy}
          onChange={(e) => {
            invalidate();
            setName(e.target.value);
          }}
        />
        <TextField
          select
          label="Action"
          value={action}
          disabled={busy}
          onChange={(e) => {
            invalidate();
            setAction(e.target.value as ConnectInput["action"]);
          }}
        >
          {CONNECT_ACTIONS.map((a) => (
            <MenuItem key={a} value={a}>
              {a}
            </MenuItem>
          ))}
        </TextField>
      </Stack>
      {(action === "create" || action === "update") && (
        <TextField
          label={
            action === "create"
              ? "Connector configuration (JSON string map)"
              : "Configuration changes (JSON string map)"
          }
          multiline
          minRows={5}
          value={config}
          disabled={busy}
          onChange={(e) => {
            invalidate();
            setConfig(e.target.value);
          }}
          helperText="For updates, omitted fields retain their existing values, including secrets. Supply replacement values explicitly. Remote secrets and worker traces are never shown."
        />
      )}
      <Stack direction="row" spacing={1}>
        <Button
          disabled={busy}
          onClick={() => {
            void run(async () => {
              invalidate();
              const r = await host.execute({
                command: "connect.validate",
                id: crypto.randomUUID(),
                version: HOST_PROTOCOL_VERSION,
                payload: input(),
              });
              if (!r.ok) throw new Error();
              if (mounted.current) setValidation(r.result.validation);
            });
          }}
        >
          Validate configuration
        </Button>
        <Button
          disabled={busy}
          onClick={() => {
            void run(async () => {
              invalidate();
              const r = await host.execute({
                command: "connect.review",
                id: crypto.randomUUID(),
                version: HOST_PROTOCOL_VERSION,
                payload: input(),
              });
              if (!r.ok) throw new Error();
              if (mounted.current) setReview(r.result.review);
            });
          }}
        >
          Review action
        </Button>
      </Stack>
      {validation &&
        (validation.issues.length ? (
          <Alert severity="error">
            {validation.issues.map((i) => (
              <div key={i.field}>
                {i.field}: {i.message}
              </div>
            ))}
          </Alert>
        ) : (
          <Alert severity="success">Validation passed. No connector change was made.</Alert>
        ))}
      {review && (
        <>
          <Alert severity="warning">
            {review.action} {review.name}. Changed fields: {review.fields.join(", ") || "None"}.
            Review expires {review.expiresAt}. Connect actions can interrupt delivery; deletion does
            not delete Kafka topics. Restart targets failed connector/tasks only.
          </Alert>
          <TextField
            label={`Type ${review.confirmation} to confirm`}
            value={confirmation}
            disabled={busy || attempted}
            onChange={(e) => setConfirmation(e.target.value)}
          />
          <Button
            variant="contained"
            disabled={busy || attempted || !canWrite || confirmation !== review.confirmation}
            onClick={() => {
              setAttempted(true);
              void run(async () => {
                const r = await host.execute({
                  command: "connect.apply",
                  id: crypto.randomUUID(),
                  version: HOST_PROTOCOL_VERSION,
                  payload: { planId: review.planId, confirmation },
                });
                if (!r.ok) throw new Error();
                if (mounted.current) {
                  setOutcome(r.result.outcome);
                  setDetail(r.result.outcome.observed ?? undefined);
                }
                await refresh();
              });
            }}
          >
            Apply reviewed action
          </Button>
        </>
      )}
      {outcome && (
        <Alert severity={outcome.state === "acknowledged" ? "success" : "warning"}>
          {outcome.state}: {outcome.detail}
        </Alert>
      )}
      {!canWrite && <Alert severity="info">Read-only mode blocks Connect changes.</Alert>}
      {error && <Alert severity="error">{error}</Alert>}
    </Stack>
  );
}
