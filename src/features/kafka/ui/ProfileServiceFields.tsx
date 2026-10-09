import { Box, Stack, Typography } from "@mui/material";

import type { ClusterServiceAuthenticationMode } from "../contracts";
import {
  StudioAlert as Alert,
  StudioFormControl as FormControl,
  StudioInputLabel as InputLabel,
  StudioMenuItem as MenuItem,
  StudioSelect as Select,
  StudioTextField as TextField,
} from "../../../platform/ui/controls";

import { ProfileClientIdentityFields } from "./ProfileClientIdentityFields";
import { ProfileProtectedFileField, ProfileProtectedTextField } from "./ProfileProtectedFields";
import { initialProtectedField, type ServiceSecurityForm } from "./profile-security-form";
import type { ProfileForm, ProfileFormIssues } from "./profile-dialog-model";

type ServiceForm = Pick<
  ProfileForm,
  | "connectUrl"
  | "connectAuthentication"
  | "connectSecurity"
  | "schemaRegistryUrl"
  | "schemaRegistryAuthentication"
  | "schemaRegistrySecurity"
  | "redpandaAdminUrl"
  | "redpandaAdminAuthentication"
  | "redpandaAdminSecurity"
>;

interface ProfileServiceFieldsProperties {
  readonly disabled: boolean;
  readonly form: ServiceForm;
  readonly issues: Pick<ProfileFormIssues, "connectUrl" | "schemaRegistryUrl" | "redpandaAdminUrl">;
  readonly onChange: <K extends keyof ServiceForm>(field: K, value: ProfileForm[K]) => void;
}

function ServiceSecurityFields({
  disabled,
  label,
  authentication,
  form,
  https,
  onChange,
}: {
  readonly disabled: boolean;
  readonly label: string;
  readonly authentication: ClusterServiceAuthenticationMode;
  readonly form: ServiceSecurityForm;
  readonly https: boolean;
  readonly onChange: (value: ServiceSecurityForm) => void;
}): React.JSX.Element {
  const fieldId = label.toLowerCase().replaceAll(" ", "-");
  function change<K extends keyof ServiceSecurityForm>(
    key: K,
    value: ServiceSecurityForm[K],
  ): void {
    onChange({ ...form, [key]: value });
  }
  return (
    <Stack spacing={1.5}>
      {!https && authentication !== "none" ? (
        <Alert severity="warning">
          HTTP sends service credentials without TLS. Use HTTPS outside an isolated test
          environment.
        </Alert>
      ) : null}
      {authentication === "basic" ? (
        <>
          <TextField
            disabled={disabled}
            fullWidth
            label={`${label} HTTP Basic username`}
            onChange={(event) => change("username", event.target.value)}
            value={form.username}
          />
          <ProfileProtectedTextField
            disabled={disabled}
            label={`${label} HTTP Basic password`}
            onChange={(value) => change("password", value)}
            value={form.password}
          />
        </>
      ) : null}
      {authentication === "bearer" ? (
        <ProfileProtectedTextField
          disabled={disabled}
          label={`${label} bearer token`}
          onChange={(value) => change("bearer", value)}
          value={form.bearer}
        />
      ) : null}
      {authentication === "oauth-client" ? (
        <>
          <TextField
            disabled={disabled}
            fullWidth
            label={`${label} OAuth token endpoint`}
            onChange={(event) => change("tokenEndpoint", event.target.value)}
            value={form.tokenEndpoint}
          />
          <Stack direction={{ sm: "row", xs: "column" }} spacing={2}>
            <TextField
              disabled={disabled}
              fullWidth
              label={`${label} OAuth client ID`}
              onChange={(event) => change("clientId", event.target.value)}
              value={form.clientId}
            />
            <TextField
              disabled={disabled}
              fullWidth
              label={`${label} OAuth scope`}
              onChange={(event) => change("scope", event.target.value)}
              value={form.scope}
            />
          </Stack>
          <ProfileProtectedTextField
            disabled={disabled}
            label={`${label} OAuth client secret`}
            onChange={(value) => change("clientSecret", value)}
            value={form.clientSecret}
          />
        </>
      ) : null}
      {https ? (
        <>
          <FormControl disabled={disabled} fullWidth>
            <InputLabel id={`${fieldId}-trust-label`}>{label} certificate trust</InputLabel>
            <Select
              label={`${label} certificate trust`}
              labelId={`${fieldId}-trust-label`}
              onChange={(event) => change("trustMode", event.target.value)}
              value={form.trustMode}
            >
              <MenuItem value="system">System certificate authorities</MenuItem>
              <MenuItem value="broker">Reuse broker certificate trust</MenuItem>
              <MenuItem value="custom">Separate CA or truststore</MenuItem>
            </Select>
          </FormControl>
          {form.trustMode === "custom" ? (
            <>
              <FormControl disabled={disabled} fullWidth>
                <InputLabel id={`${fieldId}-trust-kind-label`}>
                  {label} trust material format
                </InputLabel>
                <Select
                  label={`${label} trust material format`}
                  labelId={`${fieldId}-trust-kind-label`}
                  onChange={(event) => {
                    const trustKind = event.target.value;
                    onChange({
                      ...form,
                      trustKind,
                      trustLabel:
                        trustKind === "pem"
                          ? "ca.pem"
                          : `truststore.${trustKind === "jks" ? "jks" : "p12"}`,
                      trustMaterial: initialProtectedField(),
                      trustPassword: initialProtectedField(),
                    });
                  }}
                  value={form.trustKind}
                >
                  <MenuItem value="pem">PEM certificate</MenuItem>
                  <MenuItem value="jks">JKS truststore</MenuItem>
                  <MenuItem value="pkcs12">PKCS12 truststore</MenuItem>
                </Select>
              </FormControl>
              <ProfileProtectedFileField
                disabled={disabled}
                kind={form.trustKind}
                label={`${label} trust material`}
                onChange={(trustMaterial, filename) =>
                  onChange({ ...form, trustMaterial, trustLabel: filename || form.trustLabel })
                }
                value={form.trustMaterial}
              />
              {form.trustKind === "pem" ? null : (
                <ProfileProtectedTextField
                  disabled={disabled}
                  label={`${label} truststore password`}
                  onChange={(value) => change("trustPassword", value)}
                  value={form.trustPassword}
                />
              )}
            </>
          ) : null}
          <ProfileClientIdentityFields
            disabled={disabled}
            label={label}
            form={form.clientIdentity}
            onChange={(value) => change("clientIdentity", value)}
          />
        </>
      ) : null}
    </Stack>
  );
}

export function ProfileServiceFields({
  disabled,
  form,
  issues,
  onChange,
}: ProfileServiceFieldsProperties): React.JSX.Element {
  const services = [
    {
      label: "Connect",
      urlLabel: "Kafka Connect URL",
      url: "connectUrl",
      authentication: "connectAuthentication",
      security: "connectSecurity",
      placeholder: "https://connect.example:8083",
    },
    {
      label: "Schema Registry",
      urlLabel: "Schema Registry URL",
      url: "schemaRegistryUrl",
      authentication: "schemaRegistryAuthentication",
      security: "schemaRegistrySecurity",
      placeholder: "https://schema.example:8081",
    },
    {
      label: "Redpanda Admin",
      urlLabel: "Redpanda Admin URL",
      url: "redpandaAdminUrl",
      authentication: "redpandaAdminAuthentication",
      security: "redpandaAdminSecurity",
      placeholder: "https://redpanda.example:9644",
    },
  ] as const;
  return (
    <>
      <Box sx={{ borderTop: 1, borderColor: "divider", pt: 2 }}>
        <Typography component="h3" variant="subtitle2">
          Cluster services
        </Typography>
        <Typography color="text.secondary" variant="body2">
          Optional endpoints use their own authentication and certificate trust. Choose profile
          OAuth explicitly to reuse the broker token configuration. Removing an endpoint removes its
          saved credentials.
        </Typography>
      </Box>
      {services.map((service) => (
        <Stack key={service.url} spacing={1.5}>
          <Stack direction={{ sm: "row", xs: "column" }} spacing={2}>
            <TextField
              label={service.urlLabel}
              fullWidth
              disabled={disabled}
              value={form[service.url]}
              error={issues[service.url] !== undefined}
              helperText={issues[service.url]}
              onChange={(event) => onChange(service.url, event.target.value)}
              placeholder={service.placeholder}
            />
            <FormControl fullWidth disabled={disabled}>
              <InputLabel id={`${service.url}-auth-label`}>
                {service.label} authentication
              </InputLabel>
              <Select
                label={`${service.label} authentication`}
                labelId={`${service.url}-auth-label`}
                value={form[service.authentication]}
                onChange={(event) => onChange(service.authentication, event.target.value)}
              >
                <MenuItem value="none">No HTTP authorization</MenuItem>
                <MenuItem value="oauth">Profile OAuth bearer token</MenuItem>
                <MenuItem value="basic">HTTP Basic</MenuItem>
                <MenuItem value="bearer">Bearer token</MenuItem>
                <MenuItem value="oauth-client">Separate OAuth client</MenuItem>
              </Select>
            </FormControl>
          </Stack>
          {form[service.url].trim().length === 0 ? null : (
            <ServiceSecurityFields
              disabled={disabled}
              label={service.label}
              authentication={form[service.authentication]}
              form={form[service.security]}
              https={form[service.url].trim().startsWith("https:")}
              onChange={(value) => onChange(service.security, value)}
            />
          )}
        </Stack>
      ))}
    </>
  );
}
