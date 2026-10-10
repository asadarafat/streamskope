import { useEffect, useRef, useState } from "react";
import { Stack, Typography, Table, TableHead, TableBody, TableRow, TableCell } from "@mui/material";

import {
  StudioAlert as Alert,
  StudioButton as Button,
  StudioTextField as TextField,
  StudioMenuItem as MenuItem,
} from "../../../platform/ui/controls";
import { HOST_PROTOCOL_VERSION, type StreamSkopeHost } from "../contracts";
import type {
  ConnectOffsetsInput,
  ConnectOffsetsSnapshot,
  ConnectOffsetsReview,
  ConnectOffsetsOutcome,
} from "../contracts/connect-offsets";

export function ConnectOffsetsPanel({
  host,
  name,
  connectionName,
  canWrite,
}: {
  readonly host: StreamSkopeHost;
  readonly name: string;
  readonly connectionName: string | null;
  readonly canWrite: boolean;
}): React.JSX.Element {
  const [snapshot, setSnapshot] = useState<ConnectOffsetsSnapshot>(),
    [review, setReview] = useState<ConnectOffsetsReview>(),
    [outcome, setOutcome] = useState<ConnectOffsetsOutcome>();
  const [action, setAction] = useState<ConnectOffsetsInput["action"]>("set"),
    [partitionRef, setPartitionRef] = useState(""),
    [position, setPosition] = useState(""),
    [confirmation, setConfirmation] = useState(""),
    [error, setError] = useState(""),
    [busy, setBusy] = useState(false),
    [attempted, setAttempted] = useState(false);
  const mounted = useRef(true),
    generation = useRef(0),
    running = useRef(false);
  const invalidate = (): void => {
    setReview(undefined);
    setOutcome(undefined);
    setConfirmation("");
    setAttempted(false);
  };
  useEffect(() => {
    mounted.current = true;
    const reset = (): void => {
      generation.current += 1;
      running.current = false;
      setBusy(false);
      setSnapshot(undefined);
      setAction("set");
      setPartitionRef("");
      setPosition("");
      setError("");
      invalidate();
    };
    reset();
    let sequence = -1;
    const unsubscribe = host.subscribe((event) => {
      if (event.event === "connection.state" && event.sequence > sequence) {
        sequence = event.sequence;
        reset();
      }
    });
    return (): void => {
      mounted.current = false;
      generation.current += 1;
      unsubscribe();
    };
  }, [host, name, connectionName]);
  const run = async (
    fn: (current: () => boolean) => Promise<void>,
    applying = false,
  ): Promise<void> => {
    if (running.current) return;
    running.current = true;
    setBusy(true);
    setError("");
    const epoch = generation.current;
    const current = (): boolean => mounted.current && generation.current === epoch;
    try {
      await fn(current);
    } catch {
      if (current())
        setError(
          applying
            ? "The offset action result is unavailable; it may have been sent. Do not resend. Resolve the original connection's cleanup and inspect Connect before another review."
            : "Offsets could not be checked. Stop the connector, verify the profile's Connect endpoint and broker identity, then inspect and review again.",
        );
    } finally {
      if (current()) {
        running.current = false;
        setBusy(false);
      }
    }
  };
  const locked = busy || attempted,
    writable =
      canWrite &&
      snapshot?.status === "available" &&
      snapshot.connectorState === "STOPPED" &&
      snapshot.positions.length > 0;
  return (
    <Stack
      component="section"
      aria-label={`Connector offsets ${name}`}
      spacing={2}
      sx={{ minWidth: 0, overflowWrap: "anywhere" }}
    >
      <Typography variant="h6" component="h2">
        Connector offsets
      </Typography>
      <Typography variant="body2">
        {name} · {connectionName ?? "Disconnected"}. Inspect Connect's saved positions. Stop the
        connector through a reviewed lifecycle action before editing; pausing retains tasks and does
        not qualify.
      </Typography>
      <Button
        disabled={locked}
        onClick={(): void => {
          void run(async (current) => {
            const reply = await host.execute({
              command: "connect.offsets.inspect",
              id: crypto.randomUUID(),
              version: HOST_PROTOCOL_VERSION,
              payload: { name },
            });
            if (!reply.ok) throw new Error();
            if (current()) {
              setSnapshot(reply.result.snapshot);
              setPartitionRef(reply.result.snapshot.positions[0]?.partitionRef ?? "");
              invalidate();
            }
          });
        }}
      >
        Inspect connector offsets
      </Button>
      {error && <Alert severity="error">{error}</Alert>}
      {snapshot && (
        <>
          <Alert severity={snapshot.status === "available" ? "info" : "warning"}>
            {snapshot.status} · {snapshot.detail}
          </Alert>
          <Typography variant="body2">
            Worker {snapshot.workerVersion ?? "unverified"} · cluster{" "}
            {snapshot.clusterId ?? "unverified"} · connector {snapshot.connectorState} · observed{" "}
            {snapshot.observedAt}
          </Typography>
          {snapshot.status === "available" && (
            <>
              <Typography variant="body2">
                {snapshot.mapping === "file-source"
                  ? "Source position is the FileStream connector's saved position, not a Kafka offset. Source paths remain protected in the host."
                  : "Sink position is the next Kafka offset for that topic partition."}{" "}
                Changes can reprocess or omit records; no exactly-once or atomic compare-and-set
                guarantee.
              </Typography>
              {snapshot.positions.length ? (
                <Stack sx={{ overflowX: "auto" }}>
                  <Table size="small" aria-label="Observed connector offsets">
                    <TableHead>
                      <TableRow>
                        <TableCell>Partition</TableCell>
                        <TableCell>Position</TableCell>
                      </TableRow>
                    </TableHead>
                    <TableBody>
                      {snapshot.positions.map((item) => (
                        <TableRow key={item.partitionRef}>
                          <TableCell>{item.label}</TableCell>
                          <TableCell>{item.position}</TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                </Stack>
              ) : (
                <Typography>
                  No saved offsets were observed. An empty set is not position zero.
                </Typography>
              )}
              <TextField
                select
                label="Offset action"
                value={action}
                disabled={locked || !writable}
                onChange={(event): void => {
                  setAction(event.target.value as ConnectOffsetsInput["action"]);
                  invalidate();
                }}
              >
                <MenuItem value="set">Set one position</MenuItem>
                <MenuItem value="remove">Remove one position</MenuItem>
                <MenuItem value="reset">Reset all positions</MenuItem>
              </TextField>
              {action !== "reset" && (
                <TextField
                  select
                  label="Offset partition"
                  value={partitionRef}
                  disabled={locked || !writable}
                  onChange={(event): void => {
                    setPartitionRef(event.target.value);
                    invalidate();
                  }}
                >
                  {snapshot.positions.map((item) => (
                    <MenuItem key={item.partitionRef} value={item.partitionRef}>
                      {item.label}
                    </MenuItem>
                  ))}
                </TextField>
              )}
              {action === "set" && (
                <TextField
                  label="New offset position"
                  value={position}
                  disabled={locked || !writable}
                  onChange={(event): void => {
                    setPosition(event.target.value);
                    invalidate();
                  }}
                  helperText="Nonnegative safe integer; the host rechecks the complete baseline before dispatch."
                />
              )}
              {action !== "set" && (
                <Alert severity="warning">
                  {action === "reset"
                    ? "Reset removes every saved position for this connector."
                    : "Removal clears only this observed partition's saved position."}{" "}
                  Resume behavior depends on the connector and its offset reset policy; no records
                  are deleted from Kafka by this offset request.
                </Alert>
              )}
              <Button
                disabled={
                  locked ||
                  !writable ||
                  (action === "set" &&
                    (!/^(0|[1-9][0-9]*)$/u.test(position) ||
                      !Number.isSafeInteger(Number(position))))
                }
                onClick={(): void => {
                  void run(async (current) => {
                    const reply = await host.execute({
                      command: "connect.offsets.review",
                      id: crypto.randomUUID(),
                      version: HOST_PROTOCOL_VERSION,
                      payload: {
                        snapshotId: snapshot.snapshotId!,
                        action,
                        partitionRef: action === "reset" ? null : partitionRef,
                        position: action === "set" ? Number(position) : null,
                      },
                    });
                    if (!reply.ok) throw new Error();
                    if (current()) {
                      setReview(reply.result.review);
                      setConfirmation("");
                    }
                  });
                }}
              >
                Review offset change
              </Button>
            </>
          )}
        </>
      )}
      {review && (
        <Stack spacing={1}>
          <Typography>
            Review {review.input.action} offsets · {review.name} · {review.connectionName}
          </Typography>
          <Typography variant="body2">
            Cluster {review.clusterId} · expires {review.expiresAt}. The host rechecks stopped state
            and all saved offsets.
          </Typography>
          {review.changes.map((item, index) => (
            <Typography key={index} variant="body2">
              {item.label}: {item.before} → {item.after === null ? "removed" : item.after}
            </Typography>
          ))}
          <TextField
            label="Confirm exact offset change"
            value={confirmation}
            disabled={locked || !canWrite}
            onChange={(event): void => setConfirmation(event.target.value)}
            helperText={`Type ${review.confirmation}`}
          />
          <Button
            disabled={locked || !canWrite || confirmation !== review.confirmation}
            onClick={(): void => {
              void run(async (current) => {
                setAttempted(true);
                const reply = await host.execute({
                  command: "connect.offsets.apply",
                  id: crypto.randomUUID(),
                  version: HOST_PROTOCOL_VERSION,
                  payload: { planId: review.planId, confirmation },
                });
                if (!reply.ok) throw new Error();
                if (current()) setOutcome(reply.result.outcome);
              }, true);
            }}
          >
            Apply reviewed offset change
          </Button>
        </Stack>
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
          {outcome.state} · dispatch {outcome.dispatch} · readback {outcome.verification} · cleanup{" "}
          {outcome.cleanup}. {outcome.detail}
          {outcome.cleanup === "unresolved" &&
            " Resolve original request cleanup before another action; this receipt cannot be dismissed."}
        </Alert>
      )}
      {attempted && (
        <Button
          disabled={busy || outcome?.cleanup === "unresolved" || !outcome}
          onClick={(): void => {
            invalidate();
            setSnapshot(undefined);
          }}
        >
          Dismiss offset receipt
        </Button>
      )}
    </Stack>
  );
}
