import { useEffect, useId, useMemo, useState, type ReactNode } from "react";
import { Box, Stack, Typography } from "@mui/material";

import {
  KAFKA_MESSAGE_LIMITS,
  type KafkaExploredMessage,
  type KafkaLiveRuleMatch,
  type KafkaLiveRuleUnavailableReason,
} from "../contracts";
import { StudioDetailRow } from "../../../platform/ui/StudioPropertyRow";
import { StudioCodeBlock } from "../../../platform/ui/StudioCodeBlock";
import { StudioPanelHeader } from "../../../platform/ui/StudioPanel";
import {
  StudioAlert as Alert,
  StudioButton as Button,
  StudioDialog as Dialog,
  StudioDialogActions as DialogActions,
  StudioDialogContent as DialogContent,
  StudioDialogTitle as DialogTitle,
  StudioTab as Tab,
  StudioTabs as Tabs,
  StudioTextField as TextField,
} from "../../../platform/ui/controls";
import { streamSkopeMuiMonospaceTypography } from "../../../platform/ui/createStreamSkopeTheme";
import { studioSpace } from "../../../platform/ui/muiSpacing";

import {
  browserTextDocumentTransfer,
  type TextDocumentTransferPort,
} from "./text-document-transfer";
import { formatUtcTimestamp } from "./timestamp-presentation";

export interface MessageInspectorProperties {
  readonly message: KafkaExploredMessage;
  readonly onClose: () => void;
  readonly transfer?: TextDocumentTransferPort;
}

function formattedJson(payload: string | null): string | null {
  if (payload === null) {
    return null;
  }
  try {
    return JSON.stringify(JSON.parse(payload) as unknown, null, 2);
  } catch {
    return null;
  }
}

function TechnicalText({ children }: { readonly children: string }): React.JSX.Element {
  return (
    <StudioCodeBlock
      sx={{
        m: 0,
        overflow: "auto",
        overflowWrap: "anywhere",
        whiteSpace: "pre-wrap",
      }}
    >
      {children}
    </StudioCodeBlock>
  );
}

function MessageEvidenceText({ children }: { readonly children: ReactNode }): React.JSX.Element {
  return (
    <Box component="span" sx={streamSkopeMuiMonospaceTypography}>
      {children}
    </Box>
  );
}

function InspectorEvidenceSection({
  children,
  title,
}: {
  readonly children: ReactNode;
  readonly title: string;
}): React.JSX.Element {
  const headingId = useId();
  return (
    <Box
      aria-labelledby={headingId}
      component="section"
      sx={{ borderBottom: 1, borderColor: "divider", minWidth: 0, py: studioSpace.space8 }}
    >
      <Typography
        component="h3"
        id={headingId}
        sx={{ px: studioSpace.space12, pb: studioSpace.space8 }}
        variant="subtitle2"
      >
        {title}
      </Typography>
      <Box
        sx={{
          minWidth: 0,
          overflow: "hidden",
        }}
      >
        {children}
      </Box>
    </Box>
  );
}

function ruleStateLabel(state: KafkaExploredMessage["ruleEvaluation"]["state"]): string {
  switch (state) {
    case "evaluated":
      return "Evaluated";
    case "partial":
      return "Partial";
    case "unavailable":
      return "Unavailable";
  }
}

function countLabel(count: number, singular: string, plural: string): string {
  return `${count.toLocaleString()} ${count === 1 ? singular : plural}`;
}

function severityLabel(severity: KafkaLiveRuleMatch["level"]): string {
  return `${severity.slice(0, 1).toUpperCase()}${severity.slice(1)}`;
}

function unavailableRuleExplanation(reason: KafkaLiveRuleUnavailableReason): {
  readonly detail: string;
  readonly title: string;
} {
  switch (reason) {
    case "catalog-unavailable":
      return {
        detail:
          "The Kafka record remains available. Open Activity, repair the rule catalog, and restart consumption.",
        title: "Rule catalog unavailable",
      };
    case "payload-null":
      return {
        detail: "Rules require a complete JSON payload. The Kafka metadata remains available.",
        title: "Kafka null payload",
      };
    case "payload-truncated":
      return {
        detail:
          "Rules do not evaluate retained payload fragments. Inspect the preview or consume a smaller record.",
        title: "Payload truncated",
      };
    case "payload-malformed":
      return {
        detail:
          "Rules require valid JSON. The raw payload remains available for diagnosis and correction.",
        title: "Malformed JSON payload",
      };
    case "payload-limit-exceeded":
      return {
        detail:
          "The complete payload remains available for inspection, but it exceeds the bounded live-evaluation limit. Reduce the payload size to use live rules.",
        title: "Live evaluation limit exceeded",
      };
    case "internal":
      return {
        detail:
          "The Kafka record remains available. Open Activity for diagnostics and retry consumption.",
        title: "Internal live evaluation failure",
      };
  }
}

function RuleMatchList({
  matches,
  title,
}: {
  readonly matches: readonly KafkaLiveRuleMatch[];
  readonly title: string;
}): React.JSX.Element | null {
  if (matches.length === 0) {
    return null;
  }
  return (
    <Box>
      <Typography component="h4" variant="caption">
        {title}
      </Typography>
      <Stack component="ul" spacing={0.5} sx={{ mb: 0, mt: 0.5, pl: 2.5 }}>
        {matches.map((match) => (
          <Typography component="li" key={`${match.name}:${match.level}`} variant="body2">
            {match.name} · {severityLabel(match.level)}
          </Typography>
        ))}
      </Stack>
    </Box>
  );
}

function MessageScratchEditor({
  content,
  incomplete,
  onClose,
  open,
}: {
  readonly content: string;
  readonly incomplete: boolean;
  readonly onClose: () => void;
  readonly open: boolean;
}): React.JSX.Element {
  const title = incomplete ? "Message preview editor" : "Message value editor";
  const contentType = formattedJson(content) === null ? "Plain text" : "JSON";
  const titleId = "kafka-message-scratch-editor-title";
  return (
    <Dialog
      aria-labelledby={titleId}
      fullWidth
      maxWidth="md"
      onClose={() => {
        onClose();
      }}
      open={open}
    >
      <DialogTitle id={titleId}>{title}</DialogTitle>
      <DialogContent dividers>
        <Stack spacing={1.5}>
          <Typography color="text.secondary" variant="body2">
            Scratch edits do not change or publish the Kafka record. Closing this editor discards
            them.
          </Typography>
          {incomplete ? (
            <Alert severity="warning">
              Only the retained preview is available. The original Kafka value is incomplete.
            </Alert>
          ) : null}
          <Typography color="text.secondary" variant="caption">
            {contentType}
          </Typography>
          <TextField
            defaultValue={content}
            fullWidth
            maxRows={24}
            minRows={14}
            multiline
            slotProps={{
              htmlInput: {
                "aria-label": "Scratch message value",
                spellCheck: false,
              },
            }}
            sx={{ "& textarea": streamSkopeMuiMonospaceTypography }}
          />
        </Stack>
      </DialogContent>
      <DialogActions>
        <Button onClick={onClose}>Close editor</Button>
      </DialogActions>
    </Dialog>
  );
}

export function MessageInspector({
  message,
  onClose,
  transfer = browserTextDocumentTransfer,
}: MessageInspectorProperties): React.JSX.Element {
  const formatted = useMemo(() => formattedJson(message.payload), [message.payload]);
  const payloadTruncated =
    message.truncated && message.originalByteSize > KAFKA_MESSAGE_LIMITS.messageBytes;
  const selectedContent =
    message.payload !== null ? message.payload : payloadTruncated ? message.preview : null;
  const [payloadView, setPayloadView] = useState<"formatted" | "raw">(
    formatted === null ? "raw" : "formatted",
  );
  const [section, setSection] = useState<"key" | "metadata" | "rules" | "value">("metadata");
  const [copying, setCopying] = useState(false);
  const [editorOpen, setEditorOpen] = useState(false);
  const [operationError, setOperationError] = useState<string>();
  const [operationStatus, setOperationStatus] = useState("");
  const evaluation = message.ruleEvaluation;
  const unavailable =
    evaluation.state === "unavailable" && evaluation.reason !== undefined
      ? unavailableRuleExplanation(evaluation.reason)
      : null;

  useEffect(() => {
    setCopying(false);
    setEditorOpen(false);
    setOperationError(undefined);
    setOperationStatus("");
  }, [formatted, message.id]);

  async function copySelectedContent(): Promise<void> {
    if (selectedContent === null) {
      return;
    }
    setCopying(true);
    setOperationError(undefined);
    setOperationStatus("");
    try {
      await transfer.copy(selectedContent);
      setOperationStatus(
        payloadTruncated ? "Retained preview copied to clipboard." : "Value copied to clipboard.",
      );
    } catch {
      setOperationError(
        "The selected value could not be copied. Check clipboard permission and retry.",
      );
    } finally {
      setCopying(false);
    }
  }

  const headerEntries = Object.entries(message.headers);
  const valueKind = payloadTruncated
    ? "Retained preview"
    : message.payload === null
      ? "Kafka null"
      : formatted === null
        ? "Plain text"
        : "JSON";

  return (
    <Box
      aria-label="Message inspector"
      component="aside"
      sx={{
        bgcolor: "background.paper",
        borderLeft: 1,
        borderColor: "divider",
        containerName: "studio-workspace",
        containerType: "inline-size",
        display: "grid",
        gridTemplateRows: "auto auto auto minmax(0, 1fr)",
        minHeight: 0,
        minWidth: 0,
        overflow: "hidden",
      }}
    >
      <StudioPanelHeader
        collapseLabel="Close inspector"
        density="toolbar"
        onCollapse={onClose}
        title="Message details"
      />
      <Box
        aria-label="Selected message summary"
        component="section"
        sx={{
          alignContent: "center",
          borderBottom: 1,
          borderColor: "divider",
          display: "grid",
          minHeight: 48,
          minWidth: 0,
          px: studioSpace.space12,
          py: studioSpace.space6,
        }}
      >
        <Typography
          component="p"
          noWrap
          title={`Partition ${String(message.partition)} · Offset ${message.offset}`}
          variant="subtitle2"
        >
          Partition {message.partition} · Offset {message.offset}
        </Typography>
        <Typography
          color="text.secondary"
          component="p"
          noWrap
          sx={streamSkopeMuiMonospaceTypography}
          title={`${message.topic} · ${message.timestamp}`}
          variant="caption"
        >
          {message.topic} ·{" "}
          <time dateTime={message.timestamp}>{formatUtcTimestamp(message.timestamp)}</time>
        </Typography>
      </Box>
      <Tabs
        aria-label="Message evidence"
        onChange={(_event, value: "key" | "metadata" | "rules" | "value") => {
          setSection(value);
        }}
        sx={{
          borderBottom: 1,
          borderColor: "divider",
          "& .MuiTab-root": { minWidth: 0, px: 0.5 },
        }}
        value={section}
        variant="fullWidth"
      >
        <Tab label="Metadata" value="metadata" />
        <Tab label="Key" value="key" />
        <Tab label="Value" value="value" />
        <Tab label="Rules" value="rules" />
      </Tabs>
      <Box
        aria-label="Message evidence content"
        component="section"
        sx={{ minHeight: 0, overflow: "auto" }}
        tabIndex={0}
      >
        {section === "metadata" ? (
          <Box aria-label="Metadata evidence" component="section">
            <InspectorEvidenceSection title="Record">
              <Box component="dl" sx={{ m: 0 }}>
                <StudioDetailRow
                  label="Topic"
                  value={<MessageEvidenceText>{message.topic}</MessageEvidenceText>}
                />
                <StudioDetailRow
                  label="Timestamp"
                  value={
                    <time dateTime={message.timestamp} title={message.timestamp}>
                      {formatUtcTimestamp(message.timestamp)}
                    </time>
                  }
                />
                <StudioDetailRow label="Partition" value={String(message.partition)} />
                <StudioDetailRow label="Offset" value={message.offset} />
                <StudioDetailRow
                  label="Original bytes"
                  value={message.originalByteSize.toLocaleString()}
                />
                <StudioDetailRow
                  label="Retention"
                  value={message.truncated ? "Incomplete" : "Complete"}
                />
              </Box>
            </InspectorEvidenceSection>
            <InspectorEvidenceSection title="Headers">
              {message.truncated && !payloadTruncated ? (
                <Alert severity="warning" sx={{ m: studioSpace.space8 }}>
                  One or more headers were truncated to remain within the retained-data bounds.
                </Alert>
              ) : null}
              {headerEntries.length === 0 ? (
                <Typography color="text.secondary" sx={{ p: studioSpace.space10 }} variant="body2">
                  This record has no retained headers.
                </Typography>
              ) : (
                <Box aria-label="Message headers" component="dl" sx={{ m: 0 }}>
                  {headerEntries.map(([name, value]) => (
                    <StudioDetailRow
                      key={name}
                      label={name}
                      value={<MessageEvidenceText>{value}</MessageEvidenceText>}
                    />
                  ))}
                </Box>
              )}
            </InspectorEvidenceSection>
          </Box>
        ) : null}

        {section === "key" ? (
          <Box aria-label="Key evidence" component="section">
            <InspectorEvidenceSection title="Kafka key">
              <Box sx={{ p: studioSpace.space8 }}>
                {message.key === null ? (
                  <Typography color="text.secondary" variant="body2">
                    Kafka supplied a null key.
                  </Typography>
                ) : (
                  <TechnicalText>{message.key}</TechnicalText>
                )}
              </Box>
            </InspectorEvidenceSection>
          </Box>
        ) : null}

        {section === "value" ? (
          <Box aria-label="Value evidence" component="section">
            <Box
              sx={{
                alignItems: "center",
                borderBottom: 1,
                borderColor: "divider",
                display: "flex",
                flexWrap: "wrap",
                gap: studioSpace.space8,
                justifyContent: "space-between",
                px: studioSpace.space12,
                py: studioSpace.space8,
              }}
            >
              <Box sx={{ flex: "1 1 96px", minWidth: 0 }}>
                <Typography component="h3" variant="subtitle2">
                  Message value
                </Typography>
                <Typography color="text.secondary" component="p" variant="caption">
                  {valueKind} · {message.originalByteSize.toLocaleString()} bytes
                </Typography>
              </Box>
              <Stack direction="row" spacing={studioSpace.space6}>
                <Button
                  aria-label={payloadTruncated ? "Copy retained preview" : "Copy value"}
                  disabled={selectedContent === null || copying}
                  onClick={() => {
                    void copySelectedContent();
                  }}
                  variant="outlined"
                >
                  Copy
                </Button>
                <Button
                  aria-label={payloadTruncated ? "Open preview in editor" : "Open in editor"}
                  disabled={selectedContent === null}
                  onClick={() => {
                    setOperationError(undefined);
                    setOperationStatus("");
                    setEditorOpen(true);
                  }}
                  variant="outlined"
                >
                  Open in editor
                </Button>
              </Stack>
            </Box>
            {operationStatus.length === 0 ? null : (
              <Typography
                aria-live="polite"
                color="text.secondary"
                role="status"
                sx={{ mx: studioSpace.space12, mt: studioSpace.space8 }}
                variant="body2"
              >
                {operationStatus}
              </Typography>
            )}
            {operationError === undefined ? null : (
              <Alert severity="error" sx={{ mx: studioSpace.space12, mt: studioSpace.space8 }}>
                {operationError}
              </Alert>
            )}
            {payloadTruncated ? (
              <Stack spacing={studioSpace.space8} sx={{ p: studioSpace.space12 }}>
                <Alert severity="warning">
                  Payload truncated. The complete value is not retained. Original size:{" "}
                  {message.originalByteSize.toLocaleString()} bytes.
                </Alert>
                <Typography color="text.secondary" variant="caption">
                  Retained preview (incomplete)
                </Typography>
                <TechnicalText>{message.preview}</TechnicalText>
              </Stack>
            ) : message.payload === null ? (
              <Typography color="text.secondary" sx={{ p: studioSpace.space12 }} variant="body2">
                Kafka supplied a null value.
              </Typography>
            ) : (
              <Box>
                {formatted === null ? null : (
                  <Tabs
                    aria-label="Value representation"
                    onChange={(_event, value: "formatted" | "raw") => {
                      setPayloadView(value);
                    }}
                    value={payloadView}
                    variant="fullWidth"
                  >
                    <Tab label="Formatted JSON" value="formatted" />
                    <Tab label="Raw" value="raw" />
                  </Tabs>
                )}
                <Box
                  sx={{
                    borderColor: "divider",
                    borderTop: formatted === null ? 0 : 1,
                    p: studioSpace.space12,
                  }}
                >
                  <TechnicalText>
                    {formatted !== null && payloadView === "formatted"
                      ? formatted
                      : message.payload}
                  </TechnicalText>
                </Box>
              </Box>
            )}
          </Box>
        ) : null}

        {section === "rules" ? (
          <Box aria-label="Rules evidence" component="section">
            <InspectorEvidenceSection title="Rule evaluation">
              <Box
                sx={{
                  borderBottom: 1,
                  borderColor: "divider",
                  px: studioSpace.space10,
                  py: studioSpace.space6,
                }}
              >
                <Typography color="text.secondary" variant="caption">
                  {countLabel(evaluation.activeMatchCount, "active match", "active matches")} ·{" "}
                  {countLabel(
                    evaluation.suppressedMatchCount,
                    "suppressed match",
                    "suppressed matches",
                  )}
                </Typography>
              </Box>
              <Box component="dl" sx={{ m: 0 }}>
                <StudioDetailRow label="State" value={ruleStateLabel(evaluation.state)} />
                <StudioDetailRow
                  label="Evaluated rules"
                  value={evaluation.evaluatedRules.toLocaleString()}
                />
                <StudioDetailRow
                  label="Duration"
                  value={`${evaluation.durationMicros.toLocaleString()} µs`}
                />
                <StudioDetailRow
                  label="Omitted rules"
                  value={evaluation.omittedRules.toLocaleString()}
                />
                <StudioDetailRow
                  label="Omitted evidence"
                  value={evaluation.omittedEvidence.toLocaleString()}
                />
              </Box>
            </InspectorEvidenceSection>
            <Stack spacing={studioSpace.space12} sx={{ p: studioSpace.space12 }}>
              {evaluation.state === "partial" ? (
                <Alert severity="warning">
                  Partial evaluation. Some applicable rules or evidence could not be evaluated or
                  retained.
                </Alert>
              ) : null}
              {unavailable === null ? null : (
                <Alert severity="warning">
                  <Typography component="p" variant="subtitle2">
                    {unavailable.title}
                  </Typography>
                  <Typography component="p" variant="body2">
                    {unavailable.detail}
                  </Typography>
                </Alert>
              )}
              <RuleMatchList matches={evaluation.activeMatches} title="Active matches" />
              <RuleMatchList
                matches={evaluation.suppressedMatches}
                title="Cooldown-suppressed matches"
              />
              {evaluation.errors.length === 0 ? null : (
                <Box>
                  <Typography component="h4" variant="caption">
                    Evaluation errors
                  </Typography>
                  <Stack component="ul" spacing={0.75} sx={{ mb: 0, mt: 0.5, pl: 2.5 }}>
                    {evaluation.errors.map((error) => (
                      <Typography component="li" key={error.name} variant="body2">
                        {error.name}: {error.diagnostic}
                      </Typography>
                    ))}
                  </Stack>
                </Box>
              )}
            </Stack>
          </Box>
        ) : null}
      </Box>
      {selectedContent === null ? null : (
        <MessageScratchEditor
          content={selectedContent}
          incomplete={payloadTruncated}
          onClose={() => {
            setEditorOpen(false);
          }}
          open={editorOpen}
        />
      )}
    </Box>
  );
}

export default MessageInspector;
