import { useEffect, useRef, useState } from "react";
import { Stack, Typography } from "@mui/material";

import {
  createKafkaQueryLink,
  serializeKafkaQuery,
  HostContractValidationError,
  type ProfileSummary,
} from "../contracts";
import {
  StudioAlert as Alert,
  StudioButton as Button,
  StudioMenuItem as MenuItem,
  StudioTextField as TextField,
} from "../../../platform/ui/controls";
import {
  createKafkaPortableView,
  parseKafkaInvestigationTransfer,
  serializeKafkaPortableView,
  KAFKA_VIEW_TRANSFER_LIMITS,
  type KafkaInvestigationTransfer,
} from "../contracts/view-transfer";

import {
  portableViewSettings,
  queryViewSettings,
  type KafkaViewSettings,
} from "./investigation-view-settings";
import type { TextDocumentTransferPort } from "./text-document-transfer";

export function InvestigationTransferControls({
  transfer,
  captureExport,
  viewExportAvailable,
  queryExportAvailable,
  profiles,
  readActive,
  onRestore,
  initialImport,
}: {
  readonly transfer: TextDocumentTransferPort;
  readonly captureExport: () => {
    readonly settings: KafkaViewSettings;
    readonly suggestedName: string | null;
  };
  readonly viewExportAvailable: boolean;
  readonly queryExportAvailable: boolean;
  readonly profiles: readonly ProfileSummary[];
  readonly readActive: boolean;
  readonly onRestore: (settings: KafkaViewSettings, profileId: string | undefined) => void;
  readonly initialImport?: string | undefined;
}): React.JSX.Element {
  const [input, setInput] = useState(initialImport ?? "");
  const [preview, setPreview] = useState<KafkaInvestigationTransfer>();
  const [profileId, setProfileId] = useState("");
  const [notice, setNotice] = useState("");
  const [error, setError] = useState("");
  const [busy, setBusy] = useState(false);
  const profileMissing = profileId !== "" && !profiles.some((profile) => profile.id === profileId);
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
      setPreview(parseKafkaInvestigationTransfer(text));
    } catch (failure) {
      setError(
        failure instanceof HostContractValidationError
          ? failure.message
          : "The document could not be imported.",
      );
    }
  }
  async function exportDocument(kind: "view" | "query" | "link"): Promise<void> {
    setBusy(true);
    setError("");
    setNotice("");
    try {
      const { settings, suggestedName } = captureExport();
      const query = settings.configuration;
      if (kind !== "view" && query === null) {
        setError("This group-only view has no query settings to export.");
        return;
      }
      if (kind === "link" && query !== null) {
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
        const content =
          kind === "view"
            ? serializeKafkaPortableView(createKafkaPortableView(settings, suggestedName))
            : serializeKafkaQuery(query!);
        const label = kind === "view" ? "View" : "Query";
        const outcome = await transfer.download({
          fileName: kind === "view" ? "streamskope-view.json" : "streamskope-query.json",
          mediaType: "application/json",
          content,
          byteSize: new TextEncoder().encode(content).length,
        });
        if (alive.current)
          setNotice(
            outcome === "cancelled"
              ? `${label} export cancelled.`
              : outcome === "saved"
                ? `${label} file saved.`
                : `${label} download started.`,
          );
      }
    } catch (failure) {
      if (alive.current)
        setError(
          failure instanceof HostContractValidationError && failure.message.includes("128 KiB")
            ? "This view exceeds the 128 KiB sharing limit. Reduce its query or saved positions and retry."
            : "Sharing failed. Choose valid view or query settings and check the save or clipboard permission, then retry.",
        );
    } finally {
      if (alive.current) setBusy(false);
    }
  }

  return (
    <Stack spacing={1.5} component="section" aria-label="Portable investigation">
      <Typography variant="subtitle1">Share an investigation view</Typography>
      <Typography variant="body2">
        Export the selected saved view, or the current workspace. View files include its query,
        layout and unloaded record positions. They omit local profile and bookmark IDs, credentials,
        record contents and local topic notes. Review names, filters and cluster/topic identities
        before sharing: they can reveal sensitive incident details.
      </Typography>
      <Stack direction="row" spacing={1} sx={{ flexWrap: "wrap", rowGap: 1 }}>
        <Button disabled={busy || !viewExportAvailable} onClick={() => void exportDocument("view")}>
          Export view JSON
        </Button>
        <Button disabled={busy} onClick={() => fileInput.current?.click()}>
          Import file
        </Button>
      </Stack>
      <details>
        <Typography component="summary" variant="body2">
          Query settings only
        </Typography>
        <Typography variant="caption">
          Query files and links omit layout and saved record positions.
        </Typography>
        <Stack direction="row" spacing={1} sx={{ flexWrap: "wrap", rowGap: 1 }}>
          <Button
            disabled={busy || !queryExportAvailable}
            onClick={() => void exportDocument("query")}
          >
            Export query JSON
          </Button>
          <Button
            disabled={busy || !queryExportAvailable}
            onClick={() => void exportDocument("link")}
          >
            Copy query link
          </Button>
        </Stack>
      </details>
      {!queryExportAvailable && (
        <Typography variant="caption">
          This view has no query settings to export. Query files can still be imported below.
        </Typography>
      )}
      <input
        ref={fileInput}
        type="file"
        accept=".json,application/json"
        hidden
        aria-label="View or query file"
        onChange={(event) => {
          const file = event.target.files?.[0];
          event.target.value = "";
          if (file === undefined) return;
          cancelFileRead();
          setPreview(undefined);
          setProfileId("");
          setInput("");
          setError("");
          setNotice("");
          if (file.size > KAFKA_VIEW_TRANSFER_LIMITS.documentBytes) {
            setError("View files are limited to 128 KiB; query files remain limited to 32 KiB.");
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
              setError("The file could not be read. Select a readable JSON file and retry.");
          };
          pending.readAsText(file);
        }}
      />
      <TextField
        label="View or query JSON/link"
        multiline
        minRows={2}
        maxRows={5}
        value={input}
        slotProps={{ htmlInput: { maxLength: KAFKA_VIEW_TRANSFER_LIMITS.documentBytes } }}
        onChange={(event) => {
          cancelFileRead();
          setInput(event.target.value);
          setPreview(undefined);
          setProfileId("");
          setNotice("");
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
            Review the destination, query, layout and record positions below. Opening clears old
            results and restores unloaded settings only; connect and read explicitly. To keep the
            investigation, open it and save a view in the library.
          </Alert>
          <Typography
            component="pre"
            variant="body2"
            aria-label={
              preview.kind === "view" ? "Imported view preview" : "Imported query preview"
            }
            sx={{
              whiteSpace: "pre-wrap",
              overflowWrap: "anywhere",
              maxHeight: 220,
              overflow: "auto",
            }}
          >
            {JSON.stringify(preview.kind === "view" ? preview.view : preview.query, null, 2)}
          </Typography>
          <TextField
            select
            label="Connection for imported investigation"
            value={profileId}
            onChange={(event) => setProfileId(event.target.value)}
          >
            <MenuItem value="">Choose a connection after opening</MenuItem>
            {profileMissing ? <MenuItem value={profileId}>Profile unavailable</MenuItem> : null}
            {profiles.map((profile) => (
              <MenuItem value={profile.id} key={profile.id}>
                {profile.name}
              </MenuItem>
            ))}
          </TextField>
          {profileMissing ? (
            <Typography variant="caption">
              The selected local profile is unavailable. Choose another profile or choose a
              connection after opening.
            </Typography>
          ) : null}
          <Button
            disabled={readActive || profileMissing}
            onClick={() => {
              try {
                const settings =
                  preview.kind === "view"
                    ? portableViewSettings(preview.view)
                    : queryViewSettings(preview.query);
                onRestore(settings, profileId || undefined);
              } catch {
                setError("The investigation could not be opened. Review its settings and retry.");
              }
            }}
          >
            {preview.kind === "view" ? "Open imported view" : "Open imported query"}
          </Button>
        </>
      )}
    </Stack>
  );
}
