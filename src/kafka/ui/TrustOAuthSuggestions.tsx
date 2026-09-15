import { Box, Stack, Typography } from "@mui/material";

import type { TrustRecipeOAuth } from "../contracts/trust-recipe-types";
import { StudioCheckbox, StudioLabeledControl } from "../../ui/controls";
import { StudioDetailRow } from "../../ui/StudioPropertyRow";

export type OAuthSuggestionField = keyof TrustRecipeOAuth;

export function TrustOAuthSuggestions({
  current,
  proposed,
  selected,
  disabled,
  onChange,
}: {
  readonly current: TrustRecipeOAuth | undefined;
  readonly proposed: TrustRecipeOAuth;
  readonly selected: readonly OAuthSuggestionField[];
  readonly disabled: boolean;
  readonly onChange: (fields: readonly OAuthSuggestionField[]) => void;
}): React.JSX.Element {
  return (
    <Stack spacing={1} role="region" aria-label="Review OAuth suggestions">
      <Typography component="h4" variant="subtitle2">
        Optional OAuth settings
      </Typography>
      <Typography variant="body2" color="text.secondary">
        Choose which retrieved OAuth settings to apply. Your OAuth client secret stays unchanged.
      </Typography>
      {(
        [
          ["endpoint", "OAuth token endpoint"],
          ["clientId", "OAuth client ID"],
          ["scope", "OAuth scope"],
        ] as const
      ).map(([key, label]) => (
        <Box key={key}>
          <StudioLabeledControl
            label={`Use suggested ${label}`}
            control={
              <StudioCheckbox
                disabled={disabled}
                checked={selected.includes(key)}
                onChange={(_event, checked) =>
                  onChange(checked ? [...selected, key] : selected.filter((field) => field !== key))
                }
              />
            }
          />
          <Box component="dl" sx={{ m: 0 }}>
            <StudioDetailRow label="Current" value={current?.[key] || "Not set"} />
            <StudioDetailRow label="Proposed" value={proposed[key] || "Clear value"} />
          </Box>
        </Box>
      ))}
    </Stack>
  );
}
