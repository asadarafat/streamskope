import { useEffect, useRef, useState } from "react";
import { Stack, Typography, Table, TableHead, TableBody, TableRow, TableCell } from "@mui/material";

import { HOST_PROTOCOL_VERSION, type StreamSkopeHost } from "../contracts";
import type { RepairJobSummary } from "../contracts/repair-jobs";
import {
  StudioButton as Button,
  StudioAlert as Alert,
  StudioDialog as Dialog,
  StudioDialogTitle as DialogTitle,
  StudioDialogContent as DialogContent,
  StudioDialogActions as DialogActions,
} from "../../../platform/ui/controls";

export function RepairJobHistory({ host }: { readonly host: StreamSkopeHost }): React.JSX.Element {
  const [open, setOpen] = useState(false),
    [busy, setBusy] = useState(false),
    [jobs, setJobs] = useState<readonly RepairJobSummary[]>([]),
    [durability, setDurability] = useState<string>(),
    [error, setError] = useState<string>();
  const generation = useRef(0);
  useEffect(() => {
    generation.current++;
    setOpen(false);
    setJobs([]);
    setDurability(undefined);
    setError(undefined);
    return (): void => {
      generation.current++;
    };
  }, [host]);
  const load = async (): Promise<void> => {
    const current = ++generation.current;
    setOpen(true);
    setBusy(true);
    setError(undefined);
    try {
      const response = await host.execute({
        command: "records.repair.list",
        id: crypto.randomUUID(),
        version: HOST_PROTOCOL_VERSION,
        payload: {},
      });
      if (current !== generation.current) return;
      if (!response.ok) {
        setError(`${response.error.summary} ${response.error.recovery}`);
        return;
      }
      setJobs(response.result.jobs);
      setDurability(response.result.durability);
    } catch {
      if (current === generation.current)
        setError(
          "Repair history could not be loaded. Preserve the application data and inspect Activity.",
        );
    } finally {
      if (current === generation.current) setBusy(false);
    }
  };
  return (
    <>
      <Button size="small" variant="outlined" onClick={() => void load()}>
        Repair history
      </Button>
      <Dialog
        open={open}
        onClose={() => {
          generation.current++;
          setOpen(false);
        }}
        fullWidth
        maxWidth="lg"
      >
        <DialogTitle>Repair jobs and receipts</DialogTitle>
        <DialogContent>
          <Stack spacing={2}>
            <Alert severity="info">
              A recorded dispatch intent without a receipt is uncertain. Inspect the destination
              before any new attempt; repeating writes can create duplicates.
            </Alert>
            {durability && (
              <Typography>
                Storage:{" "}
                {durability === "durable"
                  ? "Protected, durable host storage"
                  : durability === "session"
                    ? "Session only — lost when this development host stops"
                    : "Unavailable"}
                .
              </Typography>
            )}
            {error && <Alert severity="error">{error}</Alert>}
            {busy && <Typography>Loading repair history…</Typography>}
            {!busy && !error && jobs.length === 0 && (
              <Typography>No recorded repair jobs.</Typography>
            )}
            {jobs
              .slice()
              .reverse()
              .map((job) => (
                <Stack key={job.id} spacing={1}>
                  <Typography component="h3" variant="subtitle1">
                    {job.targetName} / {job.topic} / {job.partition}
                  </Typography>
                  <Typography variant="body2">
                    Job {job.id} · {job.updatedAt} · {job.status} · cleanup {job.cleanup} ·{" "}
                    {job.unsent} definitely unsent
                    {job.uncertainIndex === null
                      ? ""
                      : ` · record ${job.uncertainIndex + 1} uncertain after interruption`}
                    .
                  </Typography>
                  <Table size="small" aria-label={`Receipts for ${job.id}`}>
                    <TableHead>
                      <TableRow>
                        <TableCell>Record</TableCell>
                        <TableCell>Outcome</TableCell>
                        <TableCell>Broker receipt</TableCell>
                        <TableCell>Read-back</TableCell>
                      </TableRow>
                    </TableHead>
                    <TableBody>
                      {job.outcomes.map((outcome, index) => (
                        <TableRow key={index}>
                          <TableCell>{index + 1}</TableCell>
                          <TableCell>{outcome.state}</TableCell>
                          <TableCell>
                            {outcome.receipt
                              ? `${outcome.receipt.topic}/${outcome.receipt.partition}@${outcome.receipt.offset}`
                              : "None"}
                          </TableCell>
                          <TableCell>{outcome.verification}</TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                </Stack>
              ))}
          </Stack>
        </DialogContent>
        <DialogActions>
          <Button disabled={busy} onClick={() => void load()}>
            Refresh
          </Button>
          <Button
            onClick={() => {
              generation.current++;
              setOpen(false);
            }}
          >
            Close
          </Button>
        </DialogActions>
      </Dialog>
    </>
  );
}
