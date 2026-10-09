import { Stack, Typography } from "@mui/material";

import {
  StudioLabeledControl as FormControlLabel,
  StudioSwitch as Switch,
} from "../../../platform/ui/controls";

import { ProfileProtectedFileField, ProfileProtectedTextField } from "./ProfileProtectedFields";
import type { ClientIdentityForm } from "./profile-security-form";

export function ProfileClientIdentityFields({
  disabled,
  label,
  form,
  onChange,
  issue,
}: {
  readonly disabled: boolean;
  readonly label: string;
  readonly form: ClientIdentityForm;
  readonly onChange: (value: ClientIdentityForm) => void;
  readonly issue?: string | undefined;
}): React.JSX.Element {
  return (
    <Stack spacing={1.5}>
      <FormControlLabel
        control={
          <Switch
            checked={form.enabled}
            disabled={disabled}
            onChange={(event) => onChange({ ...form, enabled: event.target.checked })}
            slotProps={{ input: { "aria-label": `${label} mutual TLS` } }}
          />
        }
        label={`${label} client certificate (mutual TLS)`}
      />
      {form.enabled ? (
        <>
          <ProfileProtectedFileField
            disabled={disabled}
            label={`${label} client certificate`}
            onChange={(certificatePem) => onChange({ ...form, certificatePem })}
            value={form.certificatePem}
          />
          <ProfileProtectedFileField
            disabled={disabled}
            label={`${label} client private key`}
            onChange={(privateKeyPem) => onChange({ ...form, privateKeyPem })}
            value={form.privateKeyPem}
          />
          <ProfileProtectedTextField
            disabled={disabled}
            label={`${label} private key passphrase`}
            onChange={(passphrase) => onChange({ ...form, passphrase })}
            optional
            value={form.passphrase}
          />
          <Typography color="text.secondary" variant="body2">
            Use a PEM certificate chain and PEM private key. The passphrase is optional for an
            unencrypted key. Saved key material is never returned to this form.
          </Typography>
        </>
      ) : null}
      {issue === undefined ? null : (
        <Typography color="error" role="alert" variant="body2">
          {issue}
        </Typography>
      )}
    </Stack>
  );
}
