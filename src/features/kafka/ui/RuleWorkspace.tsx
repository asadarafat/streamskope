import { useCallback, useEffect, useRef, useState, type RefObject } from "react";
import { Box, DialogContentText, Stack, Typography } from "@mui/material";

import {
  HOST_PROTOCOL_VERSION,
  KAFKA_RULE_LIMITS,
  KAFKA_RULE_SEVERITIES,
  type HostCommand,
  type KafkaRuleDefinition,
  type KafkaRuleEvaluationReport,
  type StreamSkopeHost,
} from "../contracts";
import { streamSkopeLayout } from "../../../platform/ui/createStreamSkopeTheme";
import { StudioDetailRow as DetailRow } from "../../../platform/ui/StudioPropertyRow";
import {
  StudioAlert as Alert,
  StudioButton as Button,
  StudioCheckbox as Checkbox,
  StudioDialog as Dialog,
  StudioDialogActions as DialogActions,
  StudioDialogContent as DialogContent,
  StudioDialogTitle as DialogTitle,
  StudioFormControl as FormControl,
  StudioInputLabel as InputLabel,
  StudioLabeledControl as FormControlLabel,
  StudioMenuItem as MenuItem,
  StudioSelect as Select,
  StudioTab as Tab,
  StudioTabs as Tabs,
  StudioTextField as TextField,
} from "../../../platform/ui/controls";

import {
  emptyKafkaRuleDraft,
  kafkaRuleDefinitionFromDraft,
  kafkaRuleDraftFromDefinition,
  type KafkaRuleDraft,
} from "./rule-draft";
import { RuleCatalogPanel } from "./RuleCatalogPanel";
import { TopicWorkspaceToolbar } from "./TopicWorkspaceToolbar";
import type { KafkaRuleUiAction, KafkaRuleUiOperation, KafkaRuleUiState } from "./rule-state";
import { StatusIndicator } from "./StatusIndicator";

export type KafkaRuleEditorMode =
  { readonly kind: "create" } | { readonly kind: "edit"; readonly originalName: string } | null;

interface RuleWorkspaceProperties {
  readonly component?: "main" | "section";
  readonly createButtonRef: RefObject<HTMLButtonElement | null>;
  readonly editorMode: KafkaRuleEditorMode;
  readonly host: StreamSkopeHost;
  readonly onEditorModeChange: (mode: KafkaRuleEditorMode) => void;
  readonly onRuleAction: (action: KafkaRuleUiAction) => void;
  readonly state: KafkaRuleUiState;
}

interface OperationIdentity {
  readonly nextName?: string;
  readonly originalName?: string;
}

type WorkspaceTab = "details" | "syntax" | "test";

function outcomeText(report: KafkaRuleEvaluationReport | null): string | null {
  const result = report?.results[0];
  if (result === undefined) {
    return null;
  }
  switch (result.outcome) {
    case "valid":
      return "Rule is valid";
    case "invalid":
      return result.diagnostic === undefined
        ? "Rule is invalid"
        : `Rule is invalid: ${result.diagnostic}`;
    case "matched":
      return "Matched";
    case "not-matched":
      return "Not matched";
    case "skipped":
      return result.reason === "disabled"
        ? "Skipped because the rule is disabled"
        : "Skipped because the topic does not match";
  }
}

function operationLabel(operation: KafkaRuleUiOperation): string {
  switch (operation) {
    case "create":
      return "Creating rule";
    case "delete":
      return "Deleting rule";
    case "evaluate":
      return "Evaluating sample";
    case "list":
      return "Loading rules";
    case "update":
      return "Updating rule";
    case "validate":
      return "Validating rule";
  }
}

function RuleDetails({ rule }: { readonly rule: KafkaRuleDefinition }): React.JSX.Element {
  return (
    <Box component="dl" sx={{ m: 0 }}>
      <DetailRow label="Expression" value={rule.expression} />
      <DetailRow label="Topic filter" value={rule.topic ?? "All topics"} />
      <DetailRow label="Severity" value={rule.level} />
      <DetailRow label="Cooldown" value={`${rule.cooldownMs.toLocaleString()} ms`} />
      <DetailRow label="Description" value={rule.description ?? "No description"} />
    </Box>
  );
}

function RuleEditor({
  draft,
  onChange,
}: {
  readonly draft: KafkaRuleDraft;
  readonly onChange: (draft: KafkaRuleDraft) => void;
}): React.JSX.Element {
  return (
    <Stack spacing={1.25}>
      <TextField
        autoFocus
        fullWidth
        label="Rule name"
        onChange={(event) => {
          onChange({ ...draft, name: event.target.value });
        }}
        required
        slotProps={{
          htmlInput: { maxLength: KAFKA_RULE_LIMITS.nameCharacters },
        }}
        value={draft.name}
      />
      <TextField
        fullWidth
        label="JSONPath expression"
        multiline
        onChange={(event) => {
          onChange({ ...draft, expression: event.target.value });
        }}
        required
        rows={3}
        slotProps={{
          htmlInput: { maxLength: KAFKA_RULE_LIMITS.expressionCharacters },
        }}
        value={draft.expression}
      />
      <Box
        sx={{
          display: "grid",
          gap: 2,
          gridTemplateColumns: "minmax(0, 1fr) minmax(140px, 0.5fr)",
        }}
      >
        <TextField
          fullWidth
          label="Topic filter"
          onChange={(event) => {
            onChange({ ...draft, topic: event.target.value });
          }}
          slotProps={{
            htmlInput: { maxLength: KAFKA_RULE_LIMITS.topicCharacters },
          }}
          value={draft.topic}
        />
        <FormControl fullWidth>
          <InputLabel id="rule-severity-label">Severity</InputLabel>
          <Select
            id="rule-severity"
            label="Severity"
            labelId="rule-severity-label"
            onChange={(event) => {
              const level = event.target.value;
              if (KAFKA_RULE_SEVERITIES.includes(level)) {
                onChange({ ...draft, level });
              }
            }}
            value={draft.level}
          >
            {KAFKA_RULE_SEVERITIES.map((level) => (
              <MenuItem key={level} value={level}>
                {level}
              </MenuItem>
            ))}
          </Select>
        </FormControl>
      </Box>
      <TextField
        fullWidth
        label="Cooldown (milliseconds)"
        onChange={(event) => {
          onChange({ ...draft, cooldownMs: event.target.value });
        }}
        slotProps={{
          htmlInput: {
            max: KAFKA_RULE_LIMITS.cooldownMs,
            min: 0,
            step: 1,
          },
        }}
        type="number"
        value={draft.cooldownMs}
      />
      <TextField
        fullWidth
        label="Description"
        multiline
        onChange={(event) => {
          onChange({ ...draft, description: event.target.value });
        }}
        rows={2}
        slotProps={{
          htmlInput: { maxLength: KAFKA_RULE_LIMITS.descriptionCharacters },
        }}
        value={draft.description}
      />
      <FormControlLabel
        control={
          <Checkbox
            checked={draft.enabled}
            onChange={(event) => {
              onChange({ ...draft, enabled: event.target.checked });
            }}
          />
        }
        label="Enabled"
      />
    </Stack>
  );
}

function RuleResult({
  report,
  stale,
  provenanceUnavailable,
}: {
  readonly report: KafkaRuleEvaluationReport | null;
  readonly stale: boolean;
  readonly provenanceUnavailable: boolean;
}): React.JSX.Element | null {
  const text =
    report !== null && provenanceUnavailable
      ? "Previous inputs unavailable—validate or evaluate again to obtain a result for these inputs."
      : report !== null && stale
        ? "Inputs changed—validate or evaluate again. The previous result does not describe these inputs."
        : outcomeText(report);
  return text === null ? null : (
    <Alert aria-label="Rule result" role="status" severity="info">
      {text}
    </Alert>
  );
}

export function RuleWorkspace({
  component = "main",
  createButtonRef,
  editorMode,
  host,
  onEditorModeChange,
  onRuleAction,
  state,
}: RuleWorkspaceProperties): React.JSX.Element {
  const [deleteName, setDeleteName] = useState<string | null>(null);
  const [draft, setDraft] = useState<KafkaRuleDraft>(emptyKafkaRuleDraft);
  const [editorRequestId, setEditorRequestId] = useState<string | null>(null);
  const [localIssue, setLocalIssue] = useState<string>();
  const [sample, setSample] = useState("{}");
  const [submittedEvidence, setSubmittedEvidence] = useState<{
    requestId: string;
    rule: string;
    sample: string | null;
  } | null>(null);
  const [tab, setTab] = useState<WorkspaceTab>("details");
  const deleteButtonRef = useRef<HTMLButtonElement>(null);
  const editButtonRef = useRef<HTMLButtonElement>(null);
  const initializedEditorKeyRef = useRef<string | null>(null);
  const selectedRule =
    state.snapshot?.rules.find((rule) => rule.name === state.selectedName) ?? null;
  const mutationAvailable =
    state.snapshot?.store.state === "ready" && !state.stale && state.pending === null;
  const submittedRule = kafkaRuleDefinitionFromDraft(draft);
  const activeRule = editorMode === null ? selectedRule : submittedRule;
  const evidenceStale =
    submittedEvidence === null ||
    state.evaluation?.requestId !== submittedEvidence.requestId ||
    JSON.stringify(activeRule) !== submittedEvidence.rule ||
    (submittedEvidence.sample !== null && submittedEvidence.sample !== sample);

  useEffect(() => {
    if (editorMode === null) {
      initializedEditorKeyRef.current = null;
      return;
    }
    const key = editorMode.kind === "create" ? "create" : `edit:${editorMode.originalName}`;
    if (initializedEditorKeyRef.current === key) {
      return;
    }
    const editingRule =
      editorMode.kind === "edit"
        ? state.snapshot?.rules.find((rule) => rule.name === editorMode.originalName)
        : undefined;
    if (editorMode.kind === "edit" && editingRule === undefined) {
      return;
    }
    initializedEditorKeyRef.current = key;
    setDraft(
      editingRule === undefined ? emptyKafkaRuleDraft : kafkaRuleDraftFromDefinition(editingRule),
    );
    setEditorRequestId(null);
    setLocalIssue(undefined);
    setSample("{}");
    setTab("details");
  }, [editorMode, state.snapshot]);

  useEffect(() => {
    if (
      editorRequestId === null ||
      state.completion?.requestId !== editorRequestId ||
      (state.completion.operation !== "create" && state.completion.operation !== "update")
    ) {
      return;
    }
    const returnTarget = editorMode?.kind === "create" ? createButtonRef : editButtonRef;
    setEditorRequestId(null);
    onEditorModeChange(null);
    globalThis.queueMicrotask(() => {
      returnTarget.current?.focus();
    });
  }, [createButtonRef, editorMode, editorRequestId, onEditorModeChange, state.completion]);

  const executeRuleCommand = useCallback(
    async (
      command: HostCommand,
      operation: KafkaRuleUiOperation,
      identity: OperationIdentity = {},
    ): Promise<void> => {
      onRuleAction({
        ...(identity.nextName === undefined ? {} : { nextName: identity.nextName }),
        operation,
        ...(identity.originalName === undefined ? {} : { originalName: identity.originalName }),
        requestId: command.id,
        type: "operation.started",
      });
      try {
        const response = await host.execute(command);
        if (response.ok) {
          onRuleAction({
            requestId: command.id,
            type: "operation.accepted",
          });
        } else {
          onRuleAction({
            message: `${response.error.summary} ${response.error.recovery}`,
            requestId: command.id,
            type: "operation.failed",
          });
        }
      } catch {
        onRuleAction({
          message:
            "The application host did not accept the rule request. Open Activity for diagnostics.",
          requestId: command.id,
          type: "operation.failed",
        });
      }
    },
    [host, onRuleAction],
  );

  function cancelEditing(): void {
    const returnTarget = editorMode?.kind === "create" ? createButtonRef : editButtonRef;
    onEditorModeChange(null);
    setLocalIssue(undefined);
    globalThis.queueMicrotask(() => {
      returnTarget.current?.focus();
    });
  }

  function saveRule(): void {
    const rule = kafkaRuleDefinitionFromDraft(draft);
    if (rule === null || editorMode === null) {
      setLocalIssue(
        "Enter a rule name, expression and cooldown from 0 through 86,400,000 milliseconds.",
      );
      return;
    }
    const id = globalThis.crypto.randomUUID();
    setEditorRequestId(id);
    if (editorMode.kind === "create") {
      void executeRuleCommand(
        {
          command: "rules.create",
          id,
          payload: { rule },
          version: HOST_PROTOCOL_VERSION,
        },
        "create",
        { nextName: rule.name },
      );
      return;
    }
    void executeRuleCommand(
      {
        command: "rules.update",
        id,
        payload: { originalName: editorMode.originalName, rule },
        version: HOST_PROTOCOL_VERSION,
      },
      "update",
      { nextName: rule.name, originalName: editorMode.originalName },
    );
  }

  function validateRule(): void {
    const rule = kafkaRuleDefinitionFromDraft(draft);
    if (rule === null) {
      setLocalIssue(
        "Enter a rule name, expression and cooldown from 0 through 86,400,000 milliseconds.",
      );
      return;
    }
    setLocalIssue(undefined);
    const id = globalThis.crypto.randomUUID();
    setSubmittedEvidence({ requestId: id, rule: JSON.stringify(rule), sample: null });
    void executeRuleCommand(
      {
        command: "rules.validate",
        id,
        payload: { rule },
        version: HOST_PROTOCOL_VERSION,
      },
      "validate",
    );
  }

  function evaluateRule(): void {
    if (activeRule === null) {
      setLocalIssue("Select or complete a rule before evaluating a sample.");
      return;
    }
    setLocalIssue(undefined);
    const id = globalThis.crypto.randomUUID();
    setSubmittedEvidence({ requestId: id, rule: JSON.stringify(activeRule), sample });
    void executeRuleCommand(
      {
        command: "rules.evaluate",
        id,
        payload: {
          rule: activeRule,
          sample,
          scope: "single",
          ...(activeRule.topic === undefined ? {} : { topic: activeRule.topic }),
        },
        version: HOST_PROTOCOL_VERSION,
      },
      "evaluate",
    );
  }

  function toggleRule(rule: KafkaRuleDefinition): void {
    const next = { ...rule, enabled: !rule.enabled };
    const id = globalThis.crypto.randomUUID();
    void executeRuleCommand(
      {
        command: "rules.update",
        id,
        payload: { originalName: rule.name, rule: next },
        version: HOST_PROTOCOL_VERSION,
      },
      "update",
      { nextName: next.name, originalName: rule.name },
    );
  }

  function deleteRule(name: string): void {
    const id = globalThis.crypto.randomUUID();
    setDeleteName(null);
    void executeRuleCommand(
      {
        command: "rules.delete",
        id,
        payload: { name },
        version: HOST_PROTOCOL_VERSION,
      },
      "delete",
      { originalName: name },
    );
  }

  const staleOrUnavailable = state.stale || state.snapshot?.store.state === "unavailable";
  const statusTitle = state.stale
    ? "Rule data is stale"
    : state.snapshot?.store.state === "unavailable"
      ? "Rule storage unavailable"
      : null;
  const statusRecovery =
    state.snapshot?.store.state === "unavailable"
      ? state.snapshot.store.recovery
      : state.requestError;

  return (
    <Box
      aria-label="Rule workspace"
      component={component}
      sx={{
        bgcolor: "background.paper",
        display: "grid",
        gridTemplateColumns: "minmax(200px, 220px) minmax(0, 1fr)",
        height: "100%",
        minHeight: 0,
        minWidth: 0,
        overflow: "hidden",
      }}
    >
      <Box
        aria-label="Rule catalog"
        component="section"
        role="region"
        sx={{
          borderRight: 1,
          borderColor: "divider",
          minHeight: 0,
          overflow: "hidden",
        }}
      >
        <RuleCatalogPanel
          createButtonRef={createButtonRef}
          onCreate={() => {
            onEditorModeChange({ kind: "create" });
          }}
          onRuleAction={onRuleAction}
          state={state}
        />
      </Box>
      <Box
        sx={{
          display: "grid",
          gridTemplateRows: "auto minmax(0, 1fr)",
          containerName: "studio-workspace",
          containerType: "inline-size",
          minHeight: 0,
          minWidth: 0,
          overflow: "hidden",
        }}
      >
        <TopicWorkspaceToolbar label="Rule controls">
          <Box sx={{ flex: 1, minWidth: 0 }}>
            {editorMode !== null || selectedRule !== null ? (
              <Typography component="h2" noWrap variant="subtitle2">
                {editorMode?.kind === "create"
                  ? "Create rule"
                  : editorMode?.kind === "edit"
                    ? "Edit rule"
                    : selectedRule?.name}
              </Typography>
            ) : null}
            {editorMode !== null ? (
              <Typography color="text.secondary" noWrap variant="caption">
                Changes are confirmed only after the host publishes the catalog.
              </Typography>
            ) : null}
          </Box>
          <Tabs
            aria-label="Rule workspace sections"
            onChange={(_event, value: WorkspaceTab) => {
              setTab(value);
            }}
            sx={{
              alignSelf: "stretch",
              minHeight: streamSkopeLayout.workspaceBarHeight,
              "& .MuiTab-root": {
                minHeight: streamSkopeLayout.workspaceBarHeight,
                minWidth: 64,
              },
            }}
            value={tab}
          >
            <Tab label="Details" value="details" />
            <Tab label="Test" value="test" />
            <Tab label="Syntax" value="syntax" />
          </Tabs>
          {state.pending === null || state.pending.operation === "list" ? null : (
            <Typography
              aria-label="Rule operation status"
              aria-live="polite"
              color="text.secondary"
              role="status"
              variant="body2"
            >
              {operationLabel(state.pending.operation)}…
            </Typography>
          )}
          {selectedRule === null || editorMode !== null ? null : (
            <StatusIndicator
              ariaLabel="Rule state"
              label={selectedRule.enabled ? "Enabled" : "Disabled"}
              tone={selectedRule.enabled ? "success" : "neutral"}
            />
          )}
        </TopicWorkspaceToolbar>
        <Box sx={{ minHeight: 0, overflow: "auto", p: 2 }}>
          <Stack spacing={2}>
            {statusTitle === null ? null : (
              <Alert severity={state.stale ? "warning" : "error"}>
                <Typography component="p" variant="subtitle2">
                  {statusTitle}
                </Typography>
                <Typography component="p" variant="body2">
                  {statusRecovery ?? "Open Activity for diagnostics and restore the rule backend."}
                </Typography>
              </Alert>
            )}
            {!staleOrUnavailable && state.requestError !== null ? (
              <Alert severity="error">{state.requestError}</Alert>
            ) : null}
            {localIssue === undefined ? null : <Alert severity="warning">{localIssue}</Alert>}
            {tab === "details" && editorMode !== null ? (
              <>
                <RuleEditor draft={draft} onChange={setDraft} />
                <RuleResult
                  report={state.evaluation}
                  stale={evidenceStale}
                  provenanceUnavailable={submittedEvidence === null}
                />
                <Stack direction="row" spacing={1} sx={{ justifyContent: "flex-end" }}>
                  <Button disabled={state.pending !== null} onClick={cancelEditing} variant="text">
                    Cancel editing
                  </Button>
                  <Button
                    disabled={state.pending !== null || submittedRule === null}
                    onClick={validateRule}
                    variant="outlined"
                  >
                    Validate rule
                  </Button>
                  <Button
                    disabled={
                      !mutationAvailable || submittedRule === null || editorRequestId !== null
                    }
                    onClick={saveRule}
                    variant="contained"
                  >
                    Save rule
                  </Button>
                </Stack>
              </>
            ) : null}
            {tab === "details" && editorMode === null && selectedRule !== null ? (
              <>
                <RuleDetails rule={selectedRule} />
                <Stack direction="row" spacing={1}>
                  <Button
                    disabled={!mutationAvailable}
                    onClick={() => {
                      onEditorModeChange({
                        kind: "edit",
                        originalName: selectedRule.name,
                      });
                    }}
                    ref={editButtonRef}
                    variant="contained"
                  >
                    Edit rule
                  </Button>
                  <Button
                    disabled={!mutationAvailable}
                    onClick={() => {
                      toggleRule(selectedRule);
                    }}
                    variant="outlined"
                  >
                    {selectedRule.enabled ? "Disable rule" : "Enable rule"}
                  </Button>
                  <Button
                    color="error"
                    disabled={!mutationAvailable}
                    onClick={() => {
                      setDeleteName(selectedRule.name);
                    }}
                    ref={deleteButtonRef}
                    variant="text"
                  >
                    Delete rule
                  </Button>
                </Stack>
              </>
            ) : null}
            {tab === "details" &&
            editorMode === null &&
            selectedRule === null &&
            state.snapshot?.store.state === "ready" ? (
              <Typography color="text.secondary" variant="body2">
                Select a rule to inspect it, or create the first rule.
              </Typography>
            ) : null}
            {tab === "test" ? (
              <>
                <Typography component="h3" variant="subtitle2">
                  Offline sample test
                </Typography>
                <Typography color="text.secondary" variant="body2">
                  Evaluation is local to the host. It does not connect to Kafka or change the rule
                  catalog.
                </Typography>
                <TextField
                  fullWidth
                  label="Sample JSON"
                  multiline
                  onChange={(event) => {
                    setSample(event.target.value);
                  }}
                  rows={12}
                  slotProps={{
                    htmlInput: { maxLength: KAFKA_RULE_LIMITS.sampleBytes },
                  }}
                  value={sample}
                />
                <Box>
                  <Button
                    disabled={state.pending !== null || activeRule === null}
                    onClick={evaluateRule}
                    variant="contained"
                  >
                    Evaluate selected rule
                  </Button>
                </Box>
                <RuleResult
                  report={state.evaluation}
                  stale={evidenceStale}
                  provenanceUnavailable={submittedEvidence === null}
                />
              </>
            ) : null}
            {tab === "syntax" ? (
              <>
                <Typography component="h3" variant="subtitle2">
                  Supported rule syntax
                </Typography>
                <Typography color="text.secondary" variant="body2">
                  Use root-based JSONPath properties, indexes, wildcards and constrained filters.
                  Combine comparisons with &amp;&amp;, || and parentheses. User-supplied JavaScript
                  is never executed.
                </Typography>
                <Typography
                  component="code"
                  sx={{
                    bgcolor: "action.hover",
                    border: 1,
                    borderColor: "divider",
                    display: "block",
                    overflowWrap: "anywhere",
                    p: 1.5,
                  }}
                  variant="body2"
                >
                  $.status == &quot;ready&quot; &amp;&amp; $.latency &lt; 500
                </Typography>
              </>
            ) : null}
          </Stack>
        </Box>
      </Box>
      {deleteName === null ? null : (
        <Dialog
          aria-labelledby="delete-rule-dialog-title"
          fullWidth
          maxWidth="xs"
          onClose={() => {
            setDeleteName(null);
          }}
          open
        >
          <DialogTitle id="delete-rule-dialog-title">Delete {deleteName}?</DialogTitle>
          <DialogContent dividers>
            <DialogContentText>
              Delete rule {deleteName}? This action cannot be undone.
            </DialogContentText>
          </DialogContent>
          <DialogActions>
            <Button
              onClick={() => {
                setDeleteName(null);
              }}
            >
              Cancel
            </Button>
            <Button
              color="error"
              onClick={() => {
                deleteRule(deleteName);
              }}
              variant="contained"
            >
              Delete rule
            </Button>
          </DialogActions>
        </Dialog>
      )}
    </Box>
  );
}
