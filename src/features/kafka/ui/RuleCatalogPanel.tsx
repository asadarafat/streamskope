import { useMemo, type Ref } from "react";
import { Box, Chip, List, ListItem, ListItemText, Stack, Typography } from "@mui/material";

import {
  StudioButton as Button,
  StudioListItemButton as ListItemButton,
  StudioTextField as TextField,
} from "../../../platform/ui/controls";

import {
  createKafkaRuleSelector,
  type KafkaRuleUiAction,
  type KafkaRuleUiState,
} from "./rule-state";
import { TopicWorkspaceToolbar } from "./TopicWorkspaceToolbar";

interface RuleCatalogPanelProperties {
  readonly createButtonRef: Ref<HTMLButtonElement>;
  readonly onCreate: () => void;
  readonly onRuleAction: (action: KafkaRuleUiAction) => void;
  readonly state: KafkaRuleUiState;
}

function storageStatus(state: KafkaRuleUiState): string {
  if (state.snapshot === null) {
    return "Loading rules";
  }
  if (state.snapshot.store.state === "unavailable") {
    return "Rule storage unavailable";
  }
  const label =
    state.snapshot.store.durability === "durable" ? "Durable rules" : "Session-only rules";
  return state.stale ? `${label} · stale` : label;
}

function ruleCount(count: number): string {
  return `${String(count)} ${count === 1 ? "rule" : "rules"}`;
}

export function RuleCatalogPanel({
  createButtonRef,
  onCreate,
  onRuleAction,
  state,
}: RuleCatalogPanelProperties): React.JSX.Element {
  const selectRules = useMemo(createKafkaRuleSelector, []);
  const rules = selectRules(state);
  const snapshotReady = state.snapshot?.store.state === "ready";
  const mutationAvailable = snapshotReady && !state.stale && state.pending === null;
  const healthyEmpty = snapshotReady && !state.stale && state.snapshot?.rules.length === 0;
  const noMatches =
    snapshotReady && !state.stale && (state.snapshot?.rules.length ?? 0) > 0 && rules.length === 0;

  return (
    <Stack sx={{ height: "100%", minHeight: 0, overflow: "auto" }}>
      <TopicWorkspaceToolbar label="Rule catalog controls">
        <Typography component="h2" variant="subtitle2">
          Saved rules
        </Typography>
      </TopicWorkspaceToolbar>
      <Stack spacing={1.5} sx={{ p: 1.5 }}>
        <Button
          disabled={!mutationAvailable}
          fullWidth
          onClick={onCreate}
          ref={createButtonRef}
          variant={healthyEmpty ? "contained" : "outlined"}
        >
          Create rule
        </Button>
        <TextField
          disabled={state.snapshot === null}
          fullWidth
          onChange={(event) => {
            onRuleAction({ query: event.target.value, type: "filter.changed" });
          }}
          placeholder="Search rules"
          slotProps={{ htmlInput: { "aria-label": "Search rules", type: "search" } }}
          value={state.filter}
        />
        <Stack
          direction="row"
          spacing={1}
          sx={{ alignItems: "center", justifyContent: "space-between" }}
        >
          <Typography
            aria-label="Rule storage status"
            aria-live="polite"
            color={state.stale ? "warning.main" : "text.secondary"}
            role="status"
            variant="body2"
          >
            {storageStatus(state)}
          </Typography>
          {state.snapshot === null ? null : (
            <Typography color="text.secondary" variant="caption">
              {ruleCount(state.snapshot.rules.length)}
            </Typography>
          )}
        </Stack>
        {healthyEmpty ? (
          <Typography color="text.secondary" variant="body2">
            No rules configured.
          </Typography>
        ) : null}
        {noMatches ? (
          <Box>
            <Typography color="text.secondary" variant="body2">
              No rules match &quot;{state.filter.trim()}&quot;.
            </Typography>
            <Button
              onClick={() => {
                onRuleAction({ query: "", type: "filter.changed" });
              }}
              variant="text"
            >
              Clear rule filter
            </Button>
          </Box>
        ) : null}
        {snapshotReady && rules.length > 0 ? (
          <List aria-label="Kafka rules" dense disablePadding>
            {rules.map((rule) => (
              <ListItem disablePadding key={rule.name}>
                <ListItemButton
                  aria-current={state.selectedName === rule.name ? "true" : undefined}
                  aria-label={`${rule.name}, ${rule.level}, ${rule.enabled ? "enabled" : "disabled"}`}
                  onClick={() => {
                    onRuleAction({ name: rule.name, type: "selection.changed" });
                  }}
                  selected={state.selectedName === rule.name}
                >
                  <ListItemText
                    primary={rule.name}
                    secondary={rule.topic ?? "All topics"}
                    slotProps={{
                      primary: { noWrap: true, variant: "body2" },
                      secondary: {
                        noWrap: true,
                        variant: "caption",
                      },
                    }}
                  />
                  <Chip
                    label={rule.level}
                    size="small"
                    sx={{ ml: 1, textTransform: "uppercase" }}
                    variant="outlined"
                  />
                </ListItemButton>
              </ListItem>
            ))}
          </List>
        ) : null}
      </Stack>
    </Stack>
  );
}
