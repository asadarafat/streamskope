import { Box, Stack, Typography } from "@mui/material";

import {
  StudioFormControl as FormControl,
  StudioInputLabel as InputLabel,
  StudioMenuItem as MenuItem,
  StudioSelect as Select,
  StudioTextField as TextField,
} from "../../../platform/ui/controls";

import type { ProfileForm, ProfileFormIssues } from "./profile-dialog-model";

type ServiceForm = Pick<
  ProfileForm,
  | "schemaRegistryUrl"
  | "schemaRegistryAuthentication"
  | "redpandaAdminUrl"
  | "redpandaAdminAuthentication"
>;

interface ProfileServiceFieldsProperties {
  readonly form: ServiceForm;
  readonly issues: Pick<ProfileFormIssues, "schemaRegistryUrl" | "redpandaAdminUrl">;
  readonly onChange: <K extends keyof ServiceForm>(field: K, value: ProfileForm[K]) => void;
}

export function ProfileServiceFields({
  form,
  issues,
  onChange,
}: ProfileServiceFieldsProperties): React.JSX.Element {
  return (
    <>
      <Box sx={{ borderTop: 1, borderColor: "divider", pt: 2 }}>
        <Typography component="h3" variant="subtitle2">
          Cluster services
        </Typography>
        <Typography color="text.secondary" variant="body2">
          Optional endpoints enable Schema Registry and Redpanda transform workflows. OAuth reuses
          this profile&apos;s protected token configuration.
        </Typography>
      </Box>
      <Stack direction={{ sm: "row", xs: "column" }} spacing={2}>
        <TextField
          error={issues.schemaRegistryUrl !== undefined}
          fullWidth
          helperText={issues.schemaRegistryUrl ?? "Confluent-compatible Schema Registry URL."}
          label="Schema Registry URL"
          onChange={(event) => {
            onChange("schemaRegistryUrl", event.target.value);
          }}
          placeholder="https://schema.example:8081"
          value={form.schemaRegistryUrl}
        />
        <FormControl fullWidth>
          <InputLabel id="schema-registry-authentication-label">
            Schema Registry authentication
          </InputLabel>
          <Select
            label="Schema Registry authentication"
            labelId="schema-registry-authentication-label"
            onChange={(event) => {
              onChange("schemaRegistryAuthentication", event.target.value);
            }}
            value={form.schemaRegistryAuthentication}
          >
            <MenuItem value="none">No HTTP authorization</MenuItem>
            <MenuItem value="oauth">Profile OAuth bearer token</MenuItem>
          </Select>
        </FormControl>
      </Stack>
      <Stack direction={{ sm: "row", xs: "column" }} spacing={2}>
        <TextField
          error={issues.redpandaAdminUrl !== undefined}
          fullWidth
          helperText={issues.redpandaAdminUrl ?? "Redpanda Admin API URL for transforms."}
          label="Redpanda Admin URL"
          onChange={(event) => {
            onChange("redpandaAdminUrl", event.target.value);
          }}
          placeholder="https://redpanda.example:9644"
          value={form.redpandaAdminUrl}
        />
        <FormControl fullWidth>
          <InputLabel id="redpanda-admin-authentication-label">
            Redpanda Admin authentication
          </InputLabel>
          <Select
            label="Redpanda Admin authentication"
            labelId="redpanda-admin-authentication-label"
            onChange={(event) => {
              onChange("redpandaAdminAuthentication", event.target.value);
            }}
            value={form.redpandaAdminAuthentication}
          >
            <MenuItem value="none">No HTTP authorization</MenuItem>
            <MenuItem value="oauth">Profile OAuth bearer token</MenuItem>
          </Select>
        </FormControl>
      </Stack>
    </>
  );
}
