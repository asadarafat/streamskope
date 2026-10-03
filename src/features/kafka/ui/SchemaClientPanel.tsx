import { useState, useRef, useEffect } from "react";
import { Stack, Typography } from "@mui/material";

import { StudioButton as Button, StudioAlert as Alert } from "../../../platform/ui/controls";
import { StudioCodeBlock } from "../../../platform/ui/StudioCodeBlock";
import {
  HOST_PROTOCOL_VERSION,
  type SchemaVersionDetail,
  type StreamSkopeHost,
} from "../contracts";
import type { SchemaClient } from "../contracts/schema-client";

import { browserTextDocumentTransfer } from "./text-document-transfer";
export function SchemaClientPanel({
  schema,
  host,
  enabled,
}: {
  readonly schema: SchemaVersionDetail;
  readonly host: StreamSkopeHost;
  readonly enabled: boolean;
}): React.JSX.Element {
  const [client, setClient] = useState<SchemaClient>(),
    [busy, setBusy] = useState(false),
    [error, setError] = useState(""),
    [copied, setCopied] = useState(false);
  const generation = useRef(0);
  useEffect(() => {
    generation.current++;
    setClient(undefined);
    setError("");
    setBusy(false);
    setCopied(false);
    return (): void => {
      generation.current++;
    };
  }, [schema.subject, schema.version, host, enabled]);
  return (
    <Stack spacing={1}>
      <Typography component="h3" variant="subtitle2">
        Generate a client
      </Typography>
      <Typography variant="body2">
        Node 24 JavaScript CommonJS client for self-contained JSON Schema draft-07. Includes schema
        validation, Registry framing, encode/decode and a producer helper. References, regexes,
        custom keywords, Avro and Protobuf clients are unsupported.
      </Typography>
      <Button
        disabled={!enabled || busy || schema.schemaType !== "JSON"}
        onClick={() => {
          const current = generation.current;
          setBusy(true);
          setError("");
          setClient(undefined);
          setCopied(false);
          void host
            .execute({
              command: "schemas.client",
              id: crypto.randomUUID(),
              version: HOST_PROTOCOL_VERSION,
              payload: { subject: schema.subject, version: schema.version },
            })
            .then((r) => {
              if (current !== generation.current) return;
              if (r.ok) setClient(r.result.client);
              else setError(r.error.summary);
            })
            .catch(() => {
              if (current === generation.current)
                setError("Client generation failed or exceeded supported schema bounds.");
            })
            .finally(() => {
              if (current === generation.current) setBusy(false);
            });
        }}
      >
        Generate JavaScript client
      </Button>
      {client && (
        <>
          <Typography variant="body2">
            {client.generator} · schema ID {client.schemaId} · SHA-256 {client.sha256}. Save as
            client.cjs and install the exact Ajv runtime stated in its header. Registry IDs are
            cluster-specific.
          </Typography>
          <Button
            onClick={() => {
              const current = generation.current;
              void browserTextDocumentTransfer
                .copy(client.source)
                .then(() => {
                  if (current === generation.current) setCopied(true);
                })
                .catch(() => {
                  if (current === generation.current) setError("Clipboard copy failed.");
                });
            }}
          >
            Copy client source
          </Button>
          <StudioCodeBlock sx={{ maxHeight: 320, overflow: "auto" }}>
            {client.source}
          </StudioCodeBlock>
        </>
      )}
      {copied && <Alert severity="success">Client source copied.</Alert>}
      {error && <Alert severity="error">{error}</Alert>}
    </Stack>
  );
}
