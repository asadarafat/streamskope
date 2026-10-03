import { useEffect, useRef, useState } from "react";
import { Stack, Typography, Table, TableHead, TableBody, TableRow, TableCell } from "@mui/material";

import {
  StudioButton as Button,
  StudioAlert as Alert,
  StudioTextField as TextField,
  StudioCheckbox as Checkbox,
  StudioLabeledControl as FormControlLabel,
} from "../../../platform/ui/controls";
import { HOST_PROTOCOL_VERSION, type StreamSkopeHost } from "../contracts";
import {
  RELATIONSHIP_LIMITS as limits,
  schemaImpact,
  type RelationshipGraph,
} from "../contracts/relationships";

import { RelationshipGraphView } from "./RelationshipGraphView";

export function RelationshipsPage({ host }: { readonly host: StreamSkopeHost }): React.JSX.Element {
  const [topics, setTopics] = useState(""),
    [subject, setSubject] = useState(""),
    [version, setVersion] = useState(""),
    [sampleRecords, setSampleRecords] = useState(false);
  const [graph, setGraph] = useState<RelationshipGraph | null>(null),
    [error, setError] = useState(""),
    [busy, setBusy] = useState(false),
    [selected, setSelected] = useState("");
  const [now, setNow] = useState(Date.now());
  const alive = useRef(true),
    generation = useRef(0),
    pending = useRef(false);
  const observedAt = graph?.observedAt;
  useEffect(() => {
    alive.current = true;
    return (): void => {
      alive.current = false;
      generation.current++;
      void host
        .execute({
          command: "relationships.cancel",
          id: crypto.randomUUID(),
          version: HOST_PROTOCOL_VERSION,
          payload: {},
        })
        .catch(() => undefined);
    };
  }, [host]);
  useEffect(() => {
    const current = Date.now();
    setNow(current);
    if (observedAt === undefined) return;
    const remaining = observedAt + limits.staleMs + 1 - current;
    if (remaining <= 0) return;
    const expiry = setTimeout(() => setNow(Date.now()), remaining);
    return (): void => clearTimeout(expiry);
  }, [observedAt]);
  const capture = async (): Promise<void> => {
    if (pending.current) return;
    pending.current = true;
    setBusy(true);
    setError("");
    setGraph(null);
    setSelected("");
    const owner = generation.current;
    try {
      const result = await host.execute({
        command: "relationships.capture",
        id: crypto.randomUUID(),
        version: HOST_PROTOCOL_VERSION,
        payload: {
          topics: topics
            .split(",")
            .map((s) => s.trim())
            .filter(Boolean),
          subject: subject.trim() || null,
          version: version.trim() ? Number(version) : null,
          sampleRecords,
        },
      });
      if (!alive.current || owner !== generation.current) return;
      if (result.ok) {
        setGraph(result.result.graph);
        setNow(Date.now());
      } else setError(result.error.summary + " " + result.error.recovery);
    } catch {
      if (alive.current && owner === generation.current)
        setError("Relationship evidence is unavailable. Check the selection and endpoint access.");
    } finally {
      pending.current = false;
      if (alive.current) setBusy(false);
    }
  };
  const cancel = (): void => {
    generation.current++;
    void host
      .execute({
        command: "relationships.cancel",
        id: crypto.randomUUID(),
        version: HOST_PROTOCOL_VERSION,
        payload: {},
      })
      .catch(() => undefined);
  };
  const stale =
    graph !== null && (now < graph.observedAt || now - graph.observedAt > limits.staleMs);
  const name = (id: string): string => {
    const n = graph?.nodes.find((n) => n.id === id);
    return n ? n.label + (n.version === null ? "" : ` v${n.version}`) : id;
  };
  const impact = graph ? schemaImpact(graph) : [];
  return (
    <Stack
      component="main"
      aria-label="Relationships page"
      spacing={2}
      sx={{ p: 3, overflow: "auto", height: "100%" }}
    >
      <Typography component="h1" variant="h5">
        Relationships
      </Typography>
      <Typography>
        Discover selected-topic lineage and potential schema impact from this connection. Each edge
        records its source, time and whether it is observed, declared or inferred. This is a bounded
        snapshot; unknown producers, consumers and dependencies remain unknown.
      </Typography>
      <Stack direction={{ xs: "column", md: "row" }} spacing={1}>
        <TextField
          label="Topics (up to three, comma-separated)"
          value={topics}
          onChange={(e) => setTopics(e.target.value)}
          disabled={busy}
        />
        <TextField
          label="Impact subject (optional)"
          value={subject}
          onChange={(e) => setSubject(e.target.value)}
          disabled={busy}
        />
        <TextField
          label="Exact subject version"
          value={version}
          onChange={(e) => setVersion(e.target.value)}
          disabled={busy}
        />
      </Stack>
      <FormControlLabel
        control={
          <Checkbox
            checked={sampleRecords}
            onChange={(e) => setSampleRecords(e.target.checked)}
            disabled={busy}
          />
        }
        label="Inspect protected record headers for possible schema IDs"
      />
      <Typography variant="body2">
        Optional reads: preceding minute per topic, up to 200 records / 2 MiB / five seconds each.
        Confluent magic-byte/ID framing is assumed, without decoding the payload. No record bytes or
        connector credentials are returned or saved.
      </Typography>
      <Stack direction="row" spacing={1}>
        <Button
          disabled={busy || !topics.trim() || Boolean(subject.trim()) !== Boolean(version.trim())}
          onClick={() => {
            void capture();
          }}
        >
          {busy ? "Discovering…" : "Discover relationships"}
        </Button>
        {busy && <Button onClick={cancel}>Cancel discovery</Button>}
      </Stack>
      {error && <Alert severity="error">{error}</Alert>}
      {graph && (
        <>
          <Alert severity={stale ? "warning" : "info"}>
            Cluster {graph.clusterId} · observed {new Date(graph.observedAt).toISOString()} ·{" "}
            {stale
              ? "Stale snapshot — discover again before planning a change."
              : "Recent snapshot; observations are sequential, not atomic."}
          </Alert>
          <Typography component="h2" variant="h6">
            Coverage and unknowns
          </Typography>
          <Table size="small" aria-label="Relationship coverage">
            <TableHead>
              <TableRow>
                {["Source", "Coverage", "Inspected / omitted", "Limits"].map((s) => (
                  <TableCell key={s}>{s}</TableCell>
                ))}
              </TableRow>
            </TableHead>
            <TableBody>
              {graph.coverage.map((c, i) => (
                <TableRow key={i}>
                  <TableCell>{c.source}</TableCell>
                  <TableCell>{c.state}</TableCell>
                  <TableCell>
                    {c.inspected} / {c.omitted ?? "unknown"}
                  </TableCell>
                  <TableCell>{c.detail}</TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
          <Typography component="h2" variant="h6">
            Observed lineage
          </Typography>
          <Typography variant="body2">
            Solid: observed API relation. Dashed: declared configuration/reference. Dotted: inferred
            mapping. Arrows follow the relation described in the evidence table; they do not all
            represent record flow. Select a node to filter its evidence.
          </Typography>
          <RelationshipGraphView
            graph={graph}
            selected={selected}
            onSelect={(id) => setSelected((value) => (value === id ? "" : id))}
          />
          {selected && <Button onClick={() => setSelected("")}>Show all relationships</Button>}
          <Table size="small" aria-label="Relationship evidence">
            <TableHead>
              <TableRow>
                {["From", "Relation", "To", "Evidence / source / time", "Meaning"].map((s) => (
                  <TableCell key={s}>{s}</TableCell>
                ))}
              </TableRow>
            </TableHead>
            <TableBody>
              {graph.edges
                .filter((e) => !selected || [e.from, e.to].includes(selected))
                .map((e, i) => (
                  <TableRow key={i}>
                    <TableCell>{name(e.from)}</TableCell>
                    <TableCell>{e.relation}</TableCell>
                    <TableCell>{name(e.to)}</TableCell>
                    <TableCell>
                      {e.evidence} · {e.source} · {new Date(e.observedAt).toISOString()}
                    </TableCell>
                    <TableCell>{e.detail}</TableCell>
                  </TableRow>
                ))}
            </TableBody>
          </Table>
          {graph.edges.length === 0 && (
            <Typography>
              No relationships found in this scope. This does not establish that the topics are
              unused.
            </Typography>
          )}
          <Typography component="h2" variant="h6">
            Potential schema impact
          </Typography>
          <Alert severity="warning">
            {graph.target
              ? `Requested: ${name(graph.target)}. `
              : "Choose an exact subject/version to trace potential impact. "}
            This is a discovery aid, not a compatibility check or approval to change/delete a
            schema. External consumers, historical dependent versions, reader schemas and custom
            naming strategies can be missing. Check Schema Registry compatibility and downstream
            owners separately.
          </Alert>
          {graph.target &&
            (stale ? (
              <Typography>
                Impact assessment is withheld for this stale snapshot. Discover again.
              </Typography>
            ) : (
              <>
                <Table size="small" aria-label="Potential schema impact">
                  <TableHead>
                    <TableRow>
                      <TableCell>Potentially affected resource</TableCell>
                      <TableCell>Evidence path</TableCell>
                    </TableRow>
                  </TableHead>
                  <TableBody>
                    {impact.map((entry) => (
                      <TableRow key={entry.nodeId}>
                        <TableCell>{name(entry.nodeId)}</TableCell>
                        <TableCell>
                          {entry.path
                            .map((i) => {
                              const e = graph.edges[i]!;
                              return `${name(e.from)} → ${e.relation} → ${name(e.to)} (${e.evidence}, ${e.source})`;
                            })
                            .join("; ")}
                        </TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
                {impact.length === 0 && (
                  <Typography>
                    No dependent resource found within the inspected scope. Unknown impact is not
                    zero impact.
                  </Typography>
                )}
              </>
            ))}
        </>
      )}
    </Stack>
  );
}
