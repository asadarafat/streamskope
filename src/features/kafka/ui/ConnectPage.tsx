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
  connectionName,
  onOpenTopic,
}: {
  readonly host: StreamSkopeHost;
  readonly canWrite: boolean;
  readonly connectionName: string | null;
  readonly onOpenTopic: (topic: string) => void;
}): React.JSX.Element {
  const [inventory, setInventory] = useState<ConnectInventory>({ names: [], plugins: [] }),
    [detail, setDetail] = useState<ConnectDetail>();
  const [name, setName] = useState(""),
    [action, setAction] = useState<ConnectInput["action"]>("create"),
    [remove, setRemove] = useState("[]"),
    [config, setConfig] = useState('{"connector.class":"","tasks.max":"1"}');
  const [review, setReview] = useState<ConnectReview>(),
    [validation, setValidation] = useState<ConnectValidation>(),
    [outcome, setOutcome] = useState<ConnectOutcome>(),
    [confirmation, setConfirmation] = useState(""),
    [error, setError] = useState(""),
    [busy, setBusy] = useState(false),
    [attempted, setAttempted] = useState(false);
  const mounted = useRef(true),
    generation = useRef(0),
    running = useRef(false);
  useEffect(() => {
    mounted.current = true;
    return (): void => {
      mounted.current = false;
      generation.current += 1;
    };
  }, []);
  const invalidate = (): void => {
    setReview(undefined);
    setOutcome(undefined);
    setValidation(undefined);
    setConfirmation("");
    setAttempted(false);
  };
  const run = async (fn: (current: () => boolean) => Promise<void>): Promise<void> => {
    if (running.current) return;
    running.current = true;
    const epoch = generation.current;
    const current = (): boolean => mounted.current && generation.current === epoch;
    setBusy(true);
    setError("");
    try {
      await fn(current);
    } catch {
      if (current())
        setError(
          "Check the configured Connect endpoint, permissions and JSON configuration. Refresh status and validate before reviewing an action.",
        );
    } finally {
      if (current()) {
        running.current = false;
        setBusy(false);
      }
    }
  };
  const refresh = async (current: () => boolean): Promise<void> => {
    const r = await host.execute({
      command: "connect.list",
      id: crypto.randomUUID(),
      version: HOST_PROTOCOL_VERSION,
      payload: {},
    });
    if (!r.ok) throw new Error();
    if (current()) setInventory(r.result.inventory);
  };
  useEffect(() => {
    const reset = (): void => {
      generation.current += 1;
      running.current = false;
      setInventory({ names: [], plugins: [] });
      setDetail(undefined);
      setName("");
      setAction("create");
      setConfig('{"connector.class":"","tasks.max":"1"}');
      setRemove("[]");
      invalidate();
      setBusy(false);
    };
    reset();
    void run(refresh);
    let sequence = -1;
    const unsubscribe = host.subscribe((event) => {
      if (event.event !== "connection.state" || event.sequence <= sequence) return;
      sequence = event.sequence;
      // Invalidate synchronously, including same-name reconnect events batched by React.
      reset();
      if (event.payload.state === "connected") void run(refresh);
    });
    return (): void => {
      generation.current += 1;
      unsubscribe();
    };
  }, [host, connectionName]);
  const load = async (n: string, current: () => boolean): Promise<void> => {
    invalidate();
    const r = await host.execute({
      command: "connect.load",
      id: crypto.randomUUID(),
      version: HOST_PROTOCOL_VERSION,
      payload: { name: n },
    });
    if (!r.ok) throw new Error();
    if (current()) {
      setDetail(r.result.detail);
      setName(n);
      setAction("update");
      setConfig("{}");
      setRemove("[]");
    }
  };
  const input = (): ConnectInput =>
    parseConnectInput({
      name,
      action,
      config: action === "create" || action === "update" ? (JSON.parse(config) as unknown) : {},
      remove: action === "update" ? (JSON.parse(remove) as unknown) : [],
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
      <Typography variant="body2">Connection: {connectionName ?? "Disconnected"}</Typography>
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
          disabled={busy || attempted}
          onClick={() => {
            invalidate();
            setDetail(undefined);
            setName("");
            setAction("create");
            setRemove("[]");
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
        disabled={busy || attempted}
        onChange={(e) => {
          void run((current) => load(e.target.value, current));
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
          disabled={busy || attempted}
          onChange={(e) => {
            invalidate();
            setName(e.target.value);
          }}
        />
        <TextField
          select
          label="Action"
          value={action}
          disabled={busy || attempted}
          onChange={(e) => {
            invalidate();
            setAction(e.target.value as ConnectInput["action"]);
            setRemove("[]");
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
          disabled={busy || attempted}
          onChange={(e) => {
            invalidate();
            setConfig(e.target.value);
          }}
          helperText="Set only replacement string values. Omitted fields retain their actual values, including secrets. Protected display placeholders cannot be submitted."
        />
      )}
      {action === "update" && (
        <TextField
          label="Fields to remove (JSON string array)"
          multiline
          minRows={2}
          value={remove}
          disabled={busy || attempted}
          onChange={(event) => {
            invalidate();
            setRemove(event.target.value);
          }}
          helperText='Example: ["errors.tolerance"]. Removed fields may revert to worker or connector defaults. A field cannot also be set.'
        />
      )}
      <Stack direction="row" spacing={1}>
        <Button
          disabled={busy || attempted}
          onClick={() => {
            void run(async (current) => {
              invalidate();
              const r = await host.execute({
                command: "connect.validate",
                id: crypto.randomUUID(),
                version: HOST_PROTOCOL_VERSION,
                payload: input(),
              });
              if (!r.ok) throw new Error();
              if (current()) setValidation(r.result.validation);
            });
          }}
        >
          Validate configuration
        </Button>
        <Button
          disabled={busy || attempted}
          onClick={() => {
            void run(async (current) => {
              invalidate();
              const r = await host.execute({
                command: "connect.review",
                id: crypto.randomUUID(),
                version: HOST_PROTOCOL_VERSION,
                payload: input(),
              });
              if (!r.ok) throw new Error();
              if (current()) setReview(r.result.review);
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
            Connection {review.connectionName}: {review.action} {review.name}. Set fields:{" "}
            {review.fields.join(", ") || "None"}. Remove fields:{" "}
            {review.removedFields.join(", ") || "None"}. Review expires {review.expiresAt}. Connect
            actions can interrupt delivery; deletion does not delete Kafka topics. Restart targets
            failed connector/tasks only.
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
              void run(async (current) => {
                const r = await host.execute({
                  command: "connect.apply",
                  id: crypto.randomUUID(),
                  version: HOST_PROTOCOL_VERSION,
                  payload: { planId: review.planId, confirmation },
                });
                if (!r.ok) throw new Error();
                if (current()) {
                  setOutcome(r.result.outcome);
                  setDetail(r.result.outcome.observed ?? undefined);
                }
                if (current()) await refresh(current);
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
          <Typography variant="body2">
            Readback: {outcome.verification}. Original request cleanup: {outcome.cleanup}.
          </Typography>
          {outcome.cleanup === "unresolved" && (
            <Typography>
              New actions are blocked until original cleanup is resolved. Disconnect and inspect
              host diagnostics.
            </Typography>
          )}
        </Alert>
      )}
      {attempted && !busy && (
        <Button disabled={outcome?.cleanup === "unresolved"} onClick={invalidate}>
          Dismiss receipt and start another review
        </Button>
      )}
      {!canWrite && <Alert severity="info">Read-only mode blocks Connect changes.</Alert>}
      {error && <Alert severity="error">{error}</Alert>}
    </Stack>
  );
}
