import { useState, useRef, useEffect } from "react";
import { Stack, Typography, Table, TableHead, TableBody, TableRow, TableCell } from "@mui/material";

import {
  StudioButton as Button,
  StudioAlert as Alert,
  StudioTextField as TextField,
  StudioMenuItem as MenuItem,
  StudioCheckbox as Checkbox,
} from "../../../platform/ui/controls";
import { HOST_PROTOCOL_VERSION, type StreamSkopeHost, type ProfileSummary } from "../contracts";
import {
  parseEnvironmentSnapshot,
  environmentDiff,
  exportEnvironment,
  type EnvironmentSnapshot,
  type EnvironmentReview,
  type EnvironmentOutcome,
  type EnvironmentSelection,
} from "../contracts/environment-snapshot";

import type { TextDocumentTransferPort } from "./text-document-transfer";
export function EnvironmentPage({
  host,
  profiles,
  transfer,
  canWrite,
}: {
  readonly host: StreamSkopeHost;
  readonly profiles: readonly ProfileSummary[];
  readonly transfer: TextDocumentTransferPort;
  readonly canWrite: boolean;
}): React.JSX.Element {
  const [topics, setTopics] = useState(""),
    [profile, setProfile] = useState(""),
    [imported, setImported] = useState("");
  const [source, setSource] = useState<EnvironmentSnapshot>(),
    [target, setTarget] = useState<EnvironmentSnapshot>(),
    [selected, setSelected] = useState<readonly EnvironmentSelection[]>([]),
    [review, setReview] = useState<EnvironmentReview>(),
    [outcome, setOutcome] = useState<EnvironmentOutcome>();
  const [confirmation, setConfirmation] = useState(""),
    [busy, setBusy] = useState(false),
    [error, setError] = useState(""),
    [attempted, setAttempted] = useState(false),
    [notice, setNotice] = useState("");
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
    setConfirmation("");
    setAttempted(false);
  };
  const run = async (fn: () => Promise<void>): Promise<void> => {
    if (busy) return;
    setBusy(true);
    setError("");
    setNotice("");
    try {
      await fn();
    } catch {
      if (mounted.current)
        setError(
          "The operation could not complete. Use 1–20 existing topics and a valid versioned snapshot. Refresh the target and review supported changes again.",
        );
    } finally {
      if (mounted.current) setBusy(false);
    }
  };
  const destination = (): { id: string; revision: number } | null => {
    const p = profiles.find((p) => p.id === profile);
    if (profile && !p) throw new Error("Missing profile.");
    return p ? { id: p.id, revision: p.revision ?? 1 } : null;
  };
  const capture = async (which: "source" | "target"): Promise<void> => {
    invalidate();
    const names =
      which === "source"
        ? topics
            .split(",")
            .map((t) => t.trim())
            .filter(Boolean)
        : (source?.topics.map((t) => t.name) ?? []);
    const r = await host.execute({
      command: "environments.capture",
      id: crypto.randomUUID(),
      version: HOST_PROTOCOL_VERSION,
      payload: { topics: names, profile: which === "source" ? null : destination() },
    });
    if (!r.ok) throw new Error();
    if (mounted.current) {
      if (which === "source") {
        setSource(r.result.snapshot);
        setTarget(undefined);
      } else setTarget(r.result.snapshot);
      setSelected([]);
    }
  };
  const diff = source && target ? environmentDiff(source, target) : [];
  return (
    <Stack
      component="main"
      aria-label="Environment comparison page"
      spacing={2}
      sx={{ p: 3, overflow: "auto", height: "100%" }}
    >
      <Typography component="h1" variant="h5">
        Compare environments
      </Typography>
      <Typography>
        Capture selected topic settings, save a reproducible JSON snapshot in Git and compare
        against another cluster. Only supported existing topic settings can be promoted. Broker
        settings, ACLs, schemas, connectors, secrets, topic creation/deletion and partition changes
        are excluded.
      </Typography>
      <TextField
        label="Source topics (comma-separated, at most 20)"
        value={topics}
        disabled={busy}
        onChange={(e) => {
          invalidate();
          setTopics(e.target.value);
        }}
      />
      <Button
        disabled={busy}
        onClick={() => {
          void run(() => capture("source"));
        }}
      >
        Capture active source
      </Button>
      <TextField
        label="Import source snapshot JSON"
        value={imported}
        disabled={busy}
        multiline
        minRows={3}
        onChange={(e) => setImported(e.target.value)}
        helperText="Paste a reviewed streamskope.topic-config/v1 export. Import only changes this comparison; it does not contact or change a cluster."
      />
      <Button
        disabled={busy || !imported}
        onClick={() => {
          void run(() => {
            if (imported.length > 262144) throw new Error("Snapshot too large.");
            const parsed = parseEnvironmentSnapshot(JSON.parse(imported) as unknown);
            invalidate();
            setSource(parsed);
            setTarget(undefined);
            setSelected([]);
            return Promise.resolve();
          });
        }}
      >
        Use imported source
      </Button>
      {source && (
        <>
          <Typography>
            Source cluster {source.clusterId} · observed {source.observedAt}. Snapshot age matters:
            source values are historical intent and are not refreshed during promotion.
          </Typography>
          <Button
            disabled={busy}
            onClick={() => {
              void run(async () => {
                const content = exportEnvironment(source);
                const result = await transfer.download({
                  content,
                  byteSize: new TextEncoder().encode(content).byteLength,
                  fileName: "streamskope-environment.json",
                  mediaType: "application/json",
                });
                setNotice(
                  result === "cancelled" ? "Export cancelled." : "Snapshot export requested.",
                );
              });
            }}
          >
            Export source snapshot
          </Button>
          <TextField
            select
            label="Destination profile"
            value={profile}
            disabled={busy}
            onChange={(e) => {
              invalidate();
              setProfile(e.target.value);
              setTarget(undefined);
              setSelected([]);
            }}
          >
            <MenuItem value="">Active connection</MenuItem>
            {profiles.map((p) => (
              <MenuItem key={p.id} value={p.id}>
                {p.name}
              </MenuItem>
            ))}
          </TextField>
          <Button
            disabled={busy}
            onClick={() => {
              void run(() => capture("target"));
            }}
          >
            Capture destination and compare
          </Button>
        </>
      )}
      {target && (
        <Typography>
          Destination cluster {target.clusterId} · observed {target.observedAt}. {diff.length}{" "}
          differences. Topic identities are pinned in each snapshot.
        </Typography>
      )}
      {!!target && (
        <Table size="small" aria-label="Environment differences">
          <TableHead>
            <TableRow>
              <TableCell>Select</TableCell>
              <TableCell>Topic / setting</TableCell>
              <TableCell>Source</TableCell>
              <TableCell>Destination</TableCell>
              <TableCell>Support</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {diff.map((d) => (
              <TableRow key={`${d.topic}/${d.key}`}>
                <TableCell>
                  <Checkbox
                    aria-label={`Promote ${d.topic} ${d.key}`}
                    disabled={busy || !d.supported}
                    checked={selected.some((s) => s.topic === d.topic && s.key === d.key)}
                    onChange={(_, checked) => {
                      invalidate();
                      setSelected((old) =>
                        checked
                          ? [...old, { topic: d.topic, key: d.key }]
                          : old.filter((s) => s.topic !== d.topic || s.key !== d.key),
                      );
                    }}
                  />
                </TableCell>
                <TableCell>
                  {d.topic} / {d.key}
                </TableCell>
                <TableCell>{d.source ?? "Unavailable"}</TableCell>
                <TableCell>{d.target ?? "Unavailable"}</TableCell>
                <TableCell>{d.reason}</TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      )}
      <Button
        disabled={busy || !selected.length || !source || !target}
        onClick={() => {
          void run(async () => {
            invalidate();
            const r = await host.execute({
              command: "environments.review",
              id: crypto.randomUUID(),
              version: HOST_PROTOCOL_VERSION,
              payload: { source: source!, target: target!, targetProfile: destination(), selected },
            });
            if (!r.ok) throw new Error();
            if (mounted.current) setReview(r.result.review);
          });
        }}
      >
        Review selected promotion
      </Button>
      {review && (
        <>
          <Alert severity="warning">
            {review.changes.length} settings will be changed on {review.target.clusterId}. Changes
            can affect retention, durability and consumers. Expires {review.expiresAt}. There is no
            cross-topic transaction or automatic rollback.
          </Alert>
          <TextField
            label={`Type ${review.confirmation} to confirm`}
            value={confirmation}
            disabled={busy || attempted}
            onChange={(e) => setConfirmation(e.target.value)}
          />
          <Button
            disabled={busy || attempted || !canWrite || confirmation !== review.confirmation}
            onClick={() => {
              setAttempted(true);
              void run(async () => {
                const r = await host.execute({
                  command: "environments.apply",
                  id: crypto.randomUUID(),
                  version: HOST_PROTOCOL_VERSION,
                  payload: { planId: review.planId, confirmation },
                });
                if (!r.ok) throw new Error();
                if (mounted.current) setOutcome(r.result.outcome);
              });
            }}
          >
            Apply reviewed promotion
          </Button>
        </>
      )}
      {outcome && (
        <Alert
          severity={
            outcome.results.every((r) => r.state === "acknowledged" && r.verified)
              ? "success"
              : "warning"
          }
        >
          {outcome.results.map((r) => (
            <div key={r.topic}>
              {r.topic}: {r.state}; read-back {r.verified ? "verified" : "unavailable"}
            </div>
          ))}
          {outcome.detail}
        </Alert>
      )}
      {!canWrite && <Alert severity="info">Read-only mode blocks promotion.</Alert>}
      {error && <Alert severity="error">{error}</Alert>}
      {notice && <Alert severity="info">{notice}</Alert>}
    </Stack>
  );
}
