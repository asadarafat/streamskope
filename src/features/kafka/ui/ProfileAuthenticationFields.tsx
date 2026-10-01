import { Box, InputAdornment, Stack, Typography } from "@mui/material";

import {
  StudioButton as Button,
  StudioLabeledControl as FormControlLabel,
  StudioSwitch as Switch,
  StudioTextField as TextField,
} from "../../../platform/ui/controls";

import type { ProfileForm, ProfileFormIssues } from "./profile-dialog-model";

type AuthenticationForm = Pick<
  ProfileForm,
  "oauthEnabled" | "tokenEndpoint" | "clientId" | "scope" | "clientSecret"
>;

interface ProfileAuthenticationFieldsProperties {
  readonly disabled: boolean;
  readonly form: AuthenticationForm;
  readonly issues: Pick<ProfileFormIssues, "tokenEndpoint" | "clientId" | "scope" | "clientSecret">;
  readonly onChange: <K extends keyof AuthenticationForm>(field: K, value: ProfileForm[K]) => void;
  readonly savedSecretPresent: boolean;
  readonly secretVisible: boolean;
  readonly onSecretVisibilityChange: (visible: boolean) => void;
}

export function ProfileAuthenticationFields({
  disabled,
  form,
  issues,
  onChange,
  savedSecretPresent,
  secretVisible,
  onSecretVisibilityChange,
}: ProfileAuthenticationFieldsProperties): React.JSX.Element {
  const retainedOauthSecret =
    savedSecretPresent && form.oauthEnabled && form.clientSecret.length === 0;
  return (
    <>
      <Stack
        direction={{ sm: "row", xs: "column" }}
        spacing={1}
        sx={{
          alignItems: { sm: "center", xs: "stretch" },
          borderTop: 1,
          borderColor: "divider",
          pt: 2,
        }}
      >
        <Box sx={{ flex: 1 }}>
          <Typography component="h3" variant="subtitle2">
            Authentication
          </Typography>
          <Typography color="text.secondary" variant="body2">
            Enable OAuth 2.0 only when the broker requires OAUTHBEARER.
          </Typography>
        </Box>
        <FormControlLabel
          control={
            <Switch
              checked={form.oauthEnabled}
              disabled={disabled}
              onChange={(event) => {
                onChange("oauthEnabled", event.target.checked);
              }}
              slotProps={{ input: { "aria-label": "Use OAuth OAUTHBEARER" } }}
            />
          }
          label="OAuth 2.0"
          labelPlacement="start"
          sx={{ m: 0 }}
        />
      </Stack>
      {form.oauthEnabled ? (
        <>
          <Stack spacing={0.75}>
            <TextField
              disabled={disabled}
              error={issues.tokenEndpoint !== undefined}
              fullWidth
              helperText={issues.tokenEndpoint}
              label="OAuth token endpoint"
              onChange={(event) => {
                onChange("tokenEndpoint", event.target.value);
              }}
              value={form.tokenEndpoint}
            />
          </Stack>
          <Stack direction={{ sm: "row", xs: "column" }} spacing={2}>
            <TextField
              disabled={disabled}
              error={issues.clientId !== undefined}
              fullWidth
              helperText={issues.clientId}
              label="OAuth client ID"
              onChange={(event) => {
                onChange("clientId", event.target.value);
              }}
              value={form.clientId}
            />
            <TextField
              disabled={disabled}
              error={issues.scope !== undefined}
              fullWidth
              helperText={
                issues.scope ?? "Optional. Leave empty to use the authorization server default."
              }
              label="OAuth scope"
              onChange={(event) => {
                onChange("scope", event.target.value);
              }}
              value={form.scope}
            />
          </Stack>
          <TextField
            disabled={disabled}
            error={issues.clientSecret !== undefined}
            fullWidth
            helperText={
              issues.clientSecret ??
              (retainedOauthSecret ? "Saved client secret will be retained." : undefined)
            }
            label="OAuth client secret"
            onChange={(event) => {
              onChange("clientSecret", event.target.value);
            }}
            slotProps={{
              htmlInput: { autoComplete: "new-password" },
              input: {
                endAdornment: (
                  <InputAdornment position="end">
                    <Button
                      aria-label={`${secretVisible ? "Hide" : "Show"} OAuth client secret`}
                      aria-pressed={secretVisible}
                      disabled={disabled}
                      onClick={() => {
                        onSecretVisibilityChange(!secretVisible);
                      }}
                      type="button"
                      variant="text"
                    >
                      {secretVisible ? "Hide" : "Show"}
                    </Button>
                  </InputAdornment>
                ),
              },
            }}
            type={secretVisible ? "text" : "password"}
            value={form.clientSecret}
          />
          {savedSecretPresent && form.clientSecret.length > 0 ? (
            <Button
              disabled={disabled}
              onClick={() => {
                onChange("clientSecret", "");
                onSecretVisibilityChange(false);
              }}
              sx={{ alignSelf: "flex-start" }}
              variant="text"
            >
              Retain saved client secret
            </Button>
          ) : null}
        </>
      ) : null}
    </>
  );
}
