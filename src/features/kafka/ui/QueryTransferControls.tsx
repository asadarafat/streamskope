import { useEffect, useRef, useState } from "react";
import { Stack, Typography } from "@mui/material";

import {
  createKafkaQueryLink,
  parseKafkaQueryTransfer,
  serializeKafkaQuery,
  KAFKA_QUERY_TRANSFER_LIMITS,
  HostContractValidationError,
  type KafkaInvestigationQuery,
  type ProfileSummary,
} from "../contracts";
import {
  StudioAlert as Alert,
  StudioButton as Button,
  StudioMenuItem as MenuItem,
  StudioTextField as TextField,
} from "../../../platform/ui/controls";

import type { TextDocumentTransferPort } from "./text-document-transfer";

export function QueryTransferControls({
  transfer,
  captureExport,
  exportAvailable = true,
  profiles,
  readActive,
  onRestore,
  initialImport,
}: {
  readonly transfer: TextDocumentTransferPort;
  readonly captureExport: () => KafkaInvestigationQuery | null;
  readonly exportAvailable?: boolean;
  readonly profiles: readonly ProfileSummary[];
  readonly readActive: boolean;
  readonly onRestore: (query: KafkaInvestigationQuery, profileId: string | undefined) => void;
  readonly initialImport?: string | undefined;
}): React.JSX.Element {
  const [input, setInput] = useState(initialImport ?? "");
  const [preview, setPreview] = useState<KafkaInvestigationQuery>();
  const [profileId, setProfileId] = useState("");
  const [notice, setNotice] = useState("");
  const [error, setError] = useState("");
  const [busy, setBusy] = useState(false);
  const fileInput = useRef<HTMLInputElement>(null);
  const reader = useRef<FileReader | undefined>(undefined);
  const alive = useRef(true);
  useEffect(() => {
    alive.current = true;
    return (): void => {
      alive.current = false;
      if (reader.current?.readyState === FileReader.LOADING) reader.current.abort();
    };
  }, []);

  function cancelFileRead(): void {
    const pending = reader.current;
    reader.current = undefined;
    if (pending?.readyState === FileReader.LOADING) pending.abort();
  }

  function review(text: string): void {
    setError("");
    setNotice("");
    setPreview(undefined);
    setProfileId("");
    try {
      setPreview(parseKafkaQueryTransfer(text));
    } catch (failure) {
      setError(
        failure instanceof HostContractValidationError
          ? failure.message
          : "The query could not be imported.",
      );
    }
  }
  async function exportQuery(link: boolean): Promise<void> {
    setBusy(true);
    setError("");
    setNotice("");
    try {
      const query = captureExport();
      if (query === null) {
        setError("This group-only view has no query settings to export.");
        return;
      }
      if (link) {
        const location = globalThis.location;
        const base =
          location !== undefined && ["http:", "https:"].includes(location.protocol)
            ? location.origin + location.pathname
            : "streamskope://app/";
        await transfer.copy(createKafkaQueryLink(query, base));
        if (alive.current)
          setNotice(
            "Query link copied. Recipients can paste it below; browser links also open a review step.",
          );
      } else {
        const content = serializeKafkaQuery(query);
        const outcome = await transfer.download({
          fileName: "streamskope-query.json",
          mediaType: "application/json",
          content,
          byteSize: new TextEncoder().encode(content).length,
        });
        if (alive.current)
          setNotice(
            outcome === "cancelled"
              ? "Query export cancelled."
              : outcome === "saved"
                ? "Query file saved."
                : "Query download started.",
          );
      }
    } catch {
      if (alive.current)
        setError(
          "Query sharing failed. Choose valid query settings and check the save or clipboard permission, then retry.",
        );
    } finally {
      if (alive.current) setBusy(false);
    }
  }

  return (
    <Stack spacing={1.5} component="section" aria-label="Portable query">
      <Typography variant="subtitle1">Query settings only</Typography>
      <Typography variant="body2">
        Export the selected view's query, or the current query if none is selected. Layout and
        resource selections are not included. Review filter text before sharing: it can contain
        sensitive values. Files and links omit local profiles, credentials and message records.
      </Typography>
      <Stack direction="row" spacing={1} sx={{ flexWrap: "wrap", rowGap: 1 }}>
        <Button disabled={busy || !exportAvailable} onClick={() => void exportQuery(false)}>
          Export query JSON
        </Button>
        <Button disabled={busy || !exportAvailable} onClick={() => void exportQuery(true)}>
          Copy query link
        </Button>
        <Button disabled={busy} onClick={() => fileInput.current?.click()}>
          Import query file
        </Button>
      </Stack>
      {!exportAvailable && (
        <Typography variant="caption">
          This view has no query settings to export. Query files can still be imported below.
        </Typography>
      )}
      <input
        ref={fileInput}
        type="file"
        accept=".json,application/json"
        hidden
        aria-label="Query file"
        onChange={(event) => {
          const file = event.target.files?.[0];
          event.target.value = "";
          if (file === undefined) return;
          cancelFileRead();
          setPreview(undefined);
          setError("");
          setNotice("");
          if (file.size > KAFKA_QUERY_TRANSFER_LIMITS.documentBytes) {
            setError("Query files are limited to 32 KiB.");
            return;
          }
          const pending = new FileReader();
          reader.current = pending;
          pending.onload = (): void => {
            if (alive.current && reader.current === pending) {
              const text = typeof pending.result === "string" ? pending.result : "";
              reader.current = undefined;
              setInput(text);
              review(text);
            }
          };
          pending.onerror = (): void => {
            if (alive.current && reader.current === pending)
              setError("The query file could not be read. Select a readable JSON file and retry.");
          };
          pending.readAsText(file);
        }}
      />
      <TextField
        label="Query JSON or link"
        multiline
        minRows={2}
        maxRows={5}
        value={input}
        slotProps={{ htmlInput: { maxLength: KAFKA_QUERY_TRANSFER_LIMITS.linkCharacters } }}
        onChange={(event) => {
          cancelFileRead();
          setInput(event.target.value);
          setPreview(undefined);
          setError("");
        }}
      />
      <Button
        disabled={busy || input.trim().length === 0}
        onClick={() => {
          cancelFileRead();
          review(input);
        }}
      >
        Review import
      </Button>
      {error.length > 0 ? <Alert severity="error">{error}</Alert> : null}
      {notice.length > 0 ? <Typography role="status">{notice}</Typography> : null}
      {preview === undefined ? null : (
        <>
          <Alert severity="info">
            Review topic, bounds and filters below. Opening clears old results and restores controls
            only; connect and run explicitly. To keep this query, open it and save it in the
            library.
          </Alert>
          <Typography
            component="pre"
            variant="body2"
            aria-label="Imported query preview"
            sx={{
              whiteSpace: "pre-wrap",
              overflowWrap: "anywhere",
              maxHeight: 220,
              overflow: "auto",
            }}
          >
            {serializeKafkaQuery(preview)}
          </Typography>
          <TextField
            select
            label="Connection for imported query"
            value={profileId}
            onChange={(event) => setProfileId(event.target.value)}
          >
            <MenuItem value="">Choose a connection after opening</MenuItem>
            {profiles.map((profile) => (
              <MenuItem value={profile.id} key={profile.id}>
                {profile.name}
              </MenuItem>
            ))}
          </TextField>
          <Button
            disabled={
              readActive ||
              (profileId !== "" && !profiles.some((profile) => profile.id === profileId))
            }
            onClick={() => onRestore(preview, profileId || undefined)}
          >
            Open imported query
          </Button>
        </>
      )}
    </Stack>
  );
}
