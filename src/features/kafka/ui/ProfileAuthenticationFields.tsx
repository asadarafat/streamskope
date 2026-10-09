import { Box, InputAdornment, Stack, Typography } from "@mui/material";

import {
  StudioButton as Button,
  StudioFormControl as FormControl,
  StudioInputLabel as InputLabel,
  StudioMenuItem as MenuItem,
  StudioSelect as Select,
  StudioTextField as TextField,
} from "../../../platform/ui/controls";

import { ProfileProtectedTextField } from "./ProfileProtectedFields";
import type { ProfileForm, ProfileFormIssues } from "./profile-dialog-model";

type AuthenticationForm = Pick<
  ProfileForm,
  | "authentication"
  | "saslUsername"
  | "saslPassword"
  | "tokenEndpoint"
  | "clientId"
  | "scope"
  | "clientSecret"
>;

interface ProfileAuthenticationFieldsProperties {
  readonly disabled: boolean;
  readonly form: AuthenticationForm;
  readonly issues: Pick<
    ProfileFormIssues,
    "tokenEndpoint" | "clientId" | "scope" | "clientSecret" | "saslUsername" | "saslPassword"
  >;
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
    savedSecretPresent && form.authentication === "oauth" && form.clientSecret.length === 0;
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
            Choose the SASL mechanism configured on your Kafka listener. Mutual TLS is configured
            separately.
          </Typography>
        </Box>
      </Stack>
      <FormControl disabled={disabled} fullWidth>
        <InputLabel id="broker-authentication-label">Broker authentication</InputLabel>
        <Select
          label="Broker authentication"
          labelId="broker-authentication-label"
          value={form.authentication}
          onChange={(event) => onChange("authentication", event.target.value)}
        >
          <MenuItem value="none">No SASL authentication</MenuItem>
          <MenuItem value="oauth">OAuth 2.0 (OAUTHBEARER)</MenuItem>
          <MenuItem value="PLAIN">SASL PLAIN</MenuItem>
          <MenuItem value="SCRAM-SHA-256">SASL SCRAM-SHA-256</MenuItem>
          <MenuItem value="SCRAM-SHA-512">SASL SCRAM-SHA-512</MenuItem>
        </Select>
      </FormControl>
      {form.authentication !== "none" && form.authentication !== "oauth" ? (
        <Stack spacing={1.5}>
          <TextField
            disabled={disabled}
            error={issues.saslUsername !== undefined}
            fullWidth
            helperText={issues.saslUsername}
            label="SASL username"
            onChange={(event) => onChange("saslUsername", event.target.value)}
            value={form.saslUsername}
          />
          <ProfileProtectedTextField
            disabled={disabled}
            label="SASL password"
            onChange={(value) => onChange("saslPassword", value)}
            value={form.saslPassword}
          />
          {issues.saslPassword === undefined ? null : (
            <Typography color="error" role="alert" variant="body2">
              {issues.saslPassword}
            </Typography>
          )}
        </Stack>
      ) : null}
      {form.authentication === "oauth" ? (
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
