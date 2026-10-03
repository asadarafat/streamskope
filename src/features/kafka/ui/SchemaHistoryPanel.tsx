import { useEffect, useRef, useState } from "react";
import { Box, Stack, Typography } from "@mui/material";

import {
  HOST_PROTOCOL_VERSION,
  type SchemaVersionDetail,
  type StreamSkopeHost,
} from "../contracts";
import type { SchemaInspection } from "../contracts/schema-inspection";
import {
  StudioAlert as Alert,
  StudioButton as Button,
  StudioTextField as TextField,
} from "../../../platform/ui/controls";

import { DocumentDiff } from "./DocumentDiff";

export function SchemaHistoryPanel({
  schema,
  versions,
  host,
  enabled,
  onSelect,
}: {
  readonly schema: SchemaVersionDetail;
  readonly versions: readonly number[];
  readonly host: StreamSkopeHost;
  readonly enabled: boolean;
  readonly onSelect: (subject: string, version: number) => void;
}): React.JSX.Element {
  const [graph, setGraph] = useState<SchemaInspection>();
  const [baseline, setBaseline] = useState<SchemaVersionDetail>();
  const [version, setVersion] = useState<number | "">("");
  const [source, setSource] = useState(false);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string>();
  const generation = useRef(0);
  useEffect(() => {
    generation.current++;
    setGraph(undefined);
    setBaseline(undefined);
    setVersion("");
    setBusy(false);
    setError(undefined);
    return (): void => {
      generation.current++;
    };
  }, [schema, enabled]);
  const inspect = async (target: number, comparison: boolean): Promise<void> => {
    const request = ++generation.current;
    setBusy(true);
    setError(undefined);
    try {
      const response = await host.execute({
        command: "schemas.inspect",
        id: globalThis.crypto.randomUUID(),
        version: HOST_PROTOCOL_VERSION,
        payload: { subject: schema.subject, version: target },
      });
      if (!response.ok) throw new Error(`${response.error.summary} ${response.error.recovery}`);
      if (request === generation.current) {
        if (comparison) setBaseline(response.result.inspection.root);
        else setGraph(response.result.inspection);
      }
    } catch (error) {
      if (request === generation.current)
        setError(error instanceof Error ? error.message : "Schema inspection failed.");
    } finally {
      if (request === generation.current) setBusy(false);
    }
  };
  const asJson =
    baseline && !source && schema.schemaType !== "PROTOBUF" && baseline.schemaType !== "PROTOBUF";
  const document = (value: SchemaVersionDetail): string =>
    asJson
      ? `{"schemaType":${JSON.stringify(value.schemaType)},"schema":${value.schema},"references":${JSON.stringify(value.references)}}`
      : `${value.schemaType}\nReferences: ${JSON.stringify(value.references)}\n${value.schema}`;
  return (
    <Stack spacing={2} component="section" aria-label="Schema history and references">
      <Typography component="h3" variant="subtitle2">
        Version history
      </Typography>
      <Stack direction="row" sx={{ flexWrap: "wrap", gap: 1, maxHeight: 180, overflow: "auto" }}>
        {versions
          .slice(-100)
          .reverse()
          .map((value) => (
            <Button
              key={value}
              size="small"
              variant={value === schema.version ? "contained" : "outlined"}
              disabled={!enabled || busy}
              onClick={() => onSelect(schema.subject, value)}
              aria-label={`Inspect version ${String(value)}`}
            >
              v{value}
            </Button>
          ))}
      </Stack>
      {versions.length > 100 ? (
        <Typography variant="caption">
          Showing the latest 100 versions. The Version selector above can open older versions.
        </Typography>
      ) : null}
      <TextField
        type="number"
        label="Compare from version"
        value={version}
        helperText="Enter an exact version listed in this subject's history."
        disabled={!enabled || busy}
        onChange={(event) => {
          setVersion(event.target.value === "" ? "" : Number(event.target.value));
          setBaseline(undefined);
        }}
      />
      <Stack direction="row" spacing={1}>
        <Button
          disabled={!enabled || busy || version === "" || !versions.includes(version)}
          onClick={() => {
            if (version !== "") void inspect(version, true);
          }}
        >
          Compare schema versions
        </Button>
        <Button
          disabled={!enabled || busy}
          onClick={() => {
            void inspect(schema.version, false);
          }}
        >
          Show reference tree
        </Button>
      </Stack>
      {error ? <Alert severity="error">{error}</Alert> : null}
      {busy ? <Typography role="status">Loading exact schema versions…</Typography> : null}
      {baseline ? (
        <Stack spacing={1}>
          <Typography variant="body2">
            Before: {baseline.subject}@{baseline.version} (ID {baseline.id}) → After:{" "}
            {schema.subject}@{schema.version} (ID {schema.id})
          </Typography>
          <Button onClick={() => setSource(!source)}>
            {source ? "Compare structure" : "Compare source text"}
          </Button>
          <DocumentDiff
            before={document(baseline)}
            after={document(schema)}
            mode={asJson ? "json" : "text"}
          />
        </Stack>
      ) : null}
      {graph ? (
        <Stack spacing={1}>
          <Typography component="h3" variant="subtitle2">
            Declared reference tree
          </Typography>
          <Typography variant="body2">
            {graph.root.subject}@{graph.root.version} · ID {graph.root.id}
          </Typography>
          <Typography variant="caption">
            Schema dependencies only. This does not establish producer, consumer or event-flow
            lineage. Unavailable references may be missing or denied.
          </Typography>
          {graph.limited ? (
            <Alert severity="warning">
              Partial graph: inspection reached its 32-node, 64-edge, eight-level or 1 MiB limit.
            </Alert>
          ) : null}
          {graph.edges.length === 0 ? (
            <Typography>No declared references.</Typography>
          ) : (
            <Box
              component="ol"
              aria-label="Schema reference relationships"
              sx={{ m: 0, p: 0, listStyle: "none", overflowX: "auto" }}
            >
              {graph.edges.map((edge, index) => (
                <Box
                  component="li"
                  key={index}
                  sx={{
                    ml: Math.min(edge.depth - 1, 8) * 2,
                    pl: 2,
                    py: 1,
                    borderLeft: 2,
                    borderColor: edge.state === "resolved" ? "primary.main" : "warning.main",
                  }}
                >
                  <Typography variant="caption">
                    {edge.from.subject}@{edge.from.version} — {edge.name}
                  </Typography>
                  <Typography variant="body2">
                    ↳ {edge.to.subject}@{edge.to.version} · {edge.state}
                  </Typography>
                  {edge.state === "resolved" ? (
                    <Button size="small" onClick={() => onSelect(edge.to.subject, edge.to.version)}>
                      Inspect referenced version
                    </Button>
                  ) : null}
                </Box>
              ))}
            </Box>
          )}
        </Stack>
      ) : null}
    </Stack>
  );
}
