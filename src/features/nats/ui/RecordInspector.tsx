import { useEffect, useRef, useState } from "react";
import Box from "@mui/material/Box";
import Typography from "@mui/material/Typography";

import {
  StudioAlert,
  StudioButton,
  StudioToggleButton,
  StudioToggleButtonGroup,
} from "../../../platform/ui/controls";
import { StudioPanelHeader } from "../../../platform/ui/StudioPanel";
import { StudioDetailRow } from "../../../platform/ui/StudioPropertyRow";

import type { NatsRecord } from "../contracts";
import {
  prepareNatsJsonPresentation,
  type NatsJsonPresentation,
} from "./bounded-json-presentation";
/** Original bytes and receive provenance remain distinct from optional JSON presentation. */
export function RecordInspector({
  record,
  onClose,
}: {
  readonly record: NatsRecord;
  readonly onClose: () => void;
}): React.JSX.Element {
  const close = useRef<HTMLButtonElement>(null);
  const [presentation, setPresentation] = useState<"original" | "json">("original");
  const [prepared, setPrepared] = useState<{
    readonly recordId: string;
    readonly result: NatsJsonPresentation;
  } | null>(null);
  const json = prepared?.recordId === record.id ? prepared.result : null;
  const showingJson = presentation === "json" && json?.state === "ready";
  useEffect(() => {
    setPresentation("original");
    setPrepared(null);
    close.current?.focus();
  }, [record.id]);
  return (
    <Box
      component="aside"
      aria-label="Record inspector"
      onKeyDown={(event): void => {
        if (event.key === "Escape") {
          event.preventDefault();
          event.stopPropagation();
          onClose();
        }
      }}
      sx={{
        bgcolor: "background.paper",
        borderLeft: 1,
        borderColor: "divider",
        containerName: "studio-workspace",
        containerType: "inline-size",
        display: "flex",
        flexDirection: "column",
        height: "100%",
        minHeight: 0,
        minWidth: 0,
      }}
    >
      <StudioPanelHeader
        title="Record inspector"
        actions={
          <StudioButton
            aria-label="Close record inspector"
            onClick={onClose}
            ref={close}
            variant="text"
          >
            Close
          </StudioButton>
        }
      />
      <Box sx={{ minHeight: 0, overflow: "auto" }}>
        <Box component="dl" sx={{ m: 0 }}>
          <StudioDetailRow label="Subject" value={record.subject} />
          <StudioDetailRow label="Reply" value={record.reply ?? "Not supplied"} />
          <StudioDetailRow label="Payload bytes" value={String(record.payloadBytes)} />
          <StudioDetailRow
            label="Encoding"
            value={record.payload.encoding === "utf8" ? "UTF-8" : "Base64 (binary)"}
          />
          <StudioDetailRow label="Received at" value={record.receivedAt} />
          <StudioDetailRow
            label="Timestamp provenance"
            value="Host received (UTC); publisher timestamp unavailable"
          />
        </Box>
        <Box sx={{ p: 2, minWidth: 0 }}>
          <Typography component="h3" variant="subtitle2">
            Payload
          </Typography>
          {record.payload.encoding !== "utf8" || record.payloadBytes === 0 ? null : (
            <StudioToggleButtonGroup
              aria-label="Payload presentation"
              onChange={(_event, value: "original" | "json" | null): void => {
                if (value === "original") setPresentation("original");
                else if (value === "json") {
                  const result = json ?? prepareNatsJsonPresentation(record.payload.data);
                  setPrepared({ recordId: record.id, result });
                  setPresentation(result.state === "ready" ? "json" : "original");
                }
              }}
              value={showingJson ? "json" : "original"}
              sx={{ my: 1 }}
            >
              <StudioToggleButton value="original">Original</StudioToggleButton>
              <StudioToggleButton value="json">Pretty JSON</StudioToggleButton>
            </StudioToggleButtonGroup>
          )}
          {record.payloadBytes === 0 ? (
            <Typography color="text.secondary" variant="body2">
              Empty payload (0 bytes); the payload is present.
            </Typography>
          ) : null}
          {json?.state === "invalid" ? (
            <StudioAlert severity="info" sx={{ my: 1 }}>
              Pretty JSON is unavailable because the payload is not valid JSON. The original payload
              is unchanged.
            </StudioAlert>
          ) : json?.state === "limited" ? (
            <StudioAlert severity="info" sx={{ my: 1 }}>
              Pretty JSON is unavailable because it exceeds the 32-level or 1 MiB presentation
              limit. The original payload is unchanged.
            </StudioAlert>
          ) : null}
          <Box
            component="pre"
            aria-label={showingJson ? "Pretty JSON payload" : "Original payload"}
            sx={{
              bgcolor: "action.hover",
              borderRadius: 1,
              fontFamily: "monospace",
              fontSize: "0.8125rem",
              m: 0,
              my: 1,
              overflowWrap: "anywhere",
              p: 1.5,
              whiteSpace: "pre-wrap",
              wordBreak: "break-word",
            }}
          >
            {showingJson ? json.text : record.payload.data}
          </Box>
          <Typography component="h3" sx={{ mt: 2 }} variant="subtitle2">
            Headers
          </Typography>
          {record.headersTruncated ? (
            <StudioAlert severity="warning" sx={{ my: 1 }}>
              Header limits were reached; displayed headers are incomplete.
            </StudioAlert>
          ) : null}
          {record.headers.length === 0 ? (
            <Typography color="text.secondary" variant="body2">
              No headers supplied.
            </Typography>
          ) : (
            <Box component="dl" aria-label="Record headers" sx={{ m: 0 }}>
              {record.headers.map((header, index): React.JSX.Element => (
                <Box key={`${String(index)}-${header.name}`} sx={{ mt: 1 }}>
                  <Typography
                    component="dt"
                    sx={{ fontFamily: "monospace", overflowWrap: "anywhere" }}
                    variant="body2"
                  >
                    {header.name}
                  </Typography>
                  {header.values.map((value, valueIndex): React.JSX.Element => (
                    <Typography
                      component="dd"
                      key={valueIndex}
                      sx={{ m: 0, overflowWrap: "anywhere", whiteSpace: "pre-wrap" }}
                      variant="body2"
                    >
                      {value || "(empty value)"}
                    </Typography>
                  ))}
                </Box>
              ))}
            </Box>
          )}
        </Box>
      </Box>
    </Box>
  );
}
