import { useMemo, useRef, useState } from "react";
import { Box, InputAdornment, Stack, Typography } from "@mui/material";
import ExpandMoreIcon from "@mui/icons-material/ExpandMore";

import {
  HOST_PROTOCOL_VERSION,
  type ConnectionTemplateSnapshot,
  type HostCommand,
  type ProfileSummary,
  type ProfileTestInput,
  type RemoteTrustAcquisitionSummary,
  type StreamSkopeHost,
} from "../contracts";
import type { ProfileBindingInput } from "../contracts/profile-binding";
import type { TrustRecipeOAuth } from "../contracts/trust-recipe-types";
import {
  StudioAccordion as Accordion,
  StudioAccordionSummary as AccordionSummary,
  StudioAccordionDetails as AccordionDetails,
  StudioAlert as Alert,
  StudioButton as Button,
  StudioDialog as Dialog,
  StudioDialogActions as DialogActions,
  StudioDialogContent as DialogContent,
  StudioDialogTitle as DialogTitle,
  StudioFormControl as FormControl,
  StudioInputLabel as InputLabel,
  StudioMenuItem as MenuItem,
  StudioSelect as Select,
  StudioTextField as TextField,
} from "../../../platform/ui/controls";

import { ProfileAuthenticationFields } from "./ProfileAuthenticationFields";
import { ProfileServiceFields } from "./ProfileServiceFields";
import { buildCreateInput, buildUpdateInput } from "./profile-dialog-input";
import { TrustRecipeManagementButton } from "./TrustRecipeManagementButton";
import {
  ProfileTrustRecipeSelector,
  type ProfileTrustRecipeSelection,
} from "./ProfileTrustRecipeSelector";
import type { TextDocumentTransferPort } from "./text-document-transfer";
import {
  derivedTrustLabel,
  initialProfileForm,
  readTrustFile,
  validateProfileForm,
  type ProfileForm,
  type ProfileFormIssues,
} from "./profile-dialog-model";
import { RemoteTrustAcquisitionPanel } from "./RemoteTrustAcquisitionPanel";

interface ProfileDialogProperties {
  readonly transfer?: TextDocumentTransferPort | undefined;
  readonly host: StreamSkopeHost;
  readonly onClose: () => void;
  readonly onOpenActivity: (correlationId?: string) => void;
  readonly open: boolean;
  readonly profile?: ProfileSummary;
  readonly templateLoading: boolean;
  readonly templateRequestError?: string;
  readonly templateSnapshot: ConnectionTemplateSnapshot | null;
}

export function ProfileDialog({
  transfer,
  host,
  onClose,
  onOpenActivity,
  open,
  profile,
  templateSnapshot,
}: ProfileDialogProperties): React.JSX.Element {
  const [retrievalOpen, setRetrievalOpen] = useState(false);
  const [retrievalStarted, setRetrievalStarted] = useState(false);
  const [acquisition, setAcquisition] = useState<RemoteTrustAcquisitionSummary | null>(null);
  const [recipeSelection, setRecipeSelection] = useState<ProfileTrustRecipeSelection | null>();
  const binding: ProfileBindingInput | undefined =
    recipeSelection === undefined
      ? undefined
      : recipeSelection === null
        ? { mode: "clear" }
        : {
            ...recipeSelection.reference,
            ...(acquisition?.editor === undefined ||
            acquisition.target.origin !== undefined ||
            recipeSelection.reference.identity?.mode === "reset"
              ? {}
              : {
                  identity: {
                    mode: "acquired",
                    acquisitionId: acquisition.id,
                    editorId: acquisition.editor.id,
                  },
                }),
          };
  const [form, setForm] = useState<ProfileForm>(() => initialProfileForm(profile));
  const [expectedRevision] = useState(() => profile?.revision ?? 1);
  const [issues, setIssues] = useState<ProfileFormIssues>({});
  const [oauthSecretVisible, setOauthSecretVisible] = useState(false);
  const [submissionError, setSubmissionError] = useState<string>();
  const [submitting, setSubmitting] = useState(false);
  const [testConfirmation, setTestConfirmation] = useState<string>();
  const [testing, setTesting] = useState(false);
  const [trustPasswordVisible, setTrustPasswordVisible] = useState(false);
  const trustFileInput = useRef<HTMLInputElement>(null);
  const editing = profile !== undefined;
  const busy = submitting || testing;
  const title = editing ? `Edit Kafka profile ${profile.name}` : "Add Kafka profile";
  const retainedTrust = editing && form.trustMaterialMode === "retain";
  const retainedTrustPassword = editing && form.trustPasswordMode === "retain";
  const issueCount = useMemo(
    () => Object.values(issues).filter((issue) => issue !== undefined).length,
    [issues],
  );

  function update<K extends keyof ProfileForm>(field: K, value: ProfileForm[K]): void {
    setForm((current) => ({ ...current, [field]: value }));
    setIssues({});
    setSubmissionError(undefined);
    setTestConfirmation(undefined);
  }

  async function selectTrustFile(file: File | undefined): Promise<void> {
    if (file === undefined) return;
    try {
      const material = await readTrustFile(file, form.trustKind);
      update("trustMaterial", material);
      update("trustMaterialMode", "replace");
      update("trustLabel", file.name);
      update("trustSource", "local");
    } catch (error) {
      setIssues({
        trustMaterial:
          error instanceof Error ? error.message : "The trust material could not be read.",
      });
    }
  }

  function changeAcquisition(
    next: RemoteTrustAcquisitionSummary | null,
    oauth?: Partial<TrustRecipeOAuth>,
  ): void {
    setAcquisition(next);
    setTrustPasswordVisible(false);
    setForm((current) => {
      if (next === null) {
        return {
          ...current,
          trustMaterialMode:
            current.trustMaterialMode === "acquired"
              ? profile?.trust.materialPresent === true
                ? "retain"
                : "clear"
              : current.trustMaterialMode,
          trustPasswordMode:
            current.trustPasswordMode === "acquired"
              ? profile?.trust.passwordPresent === true && current.trustKind !== "pem"
                ? "retain"
                : "clear"
              : current.trustPasswordMode,
          trustLabel:
            profile?.trust.materialPresent === true
              ? profile.trust.label
              : derivedTrustLabel(current.trustKind),
        };
      }
      return {
        ...current,
        ...(oauth === undefined
          ? {}
          : {
              oauthEnabled: true,
              ...(oauth.endpoint === undefined ? {} : { tokenEndpoint: oauth.endpoint }),
              ...(oauth.clientId === undefined ? {} : { clientId: oauth.clientId }),
              ...(oauth.scope === undefined ? {} : { scope: oauth.scope }),
            }),
        ...(next.material === null
          ? {}
          : {
              trustKind: next.material.kind,
              trustLabel: next.material.label,
              trustMaterial: "",
              trustMaterialMode: "acquired" as const,
            }),
        ...(next.password.present
          ? {
              trustPassword: "",
              trustPasswordMode: "acquired" as const,
            }
          : {}),
      };
    });
    setIssues({});
    setSubmissionError(undefined);
    setTestConfirmation(undefined);
  }

  function testPayload(): ProfileTestInput {
    return profile === undefined
      ? {
          mode: "create",
          profile: buildCreateInput(form, acquisition, binding),
        }
      : {
          mode: "update",
          profile: buildUpdateInput(form, profile, acquisition, expectedRevision, binding),
          profileId: profile.id,
        };
  }

  async function testConnection(): Promise<void> {
    const nextIssues = validateProfileForm(form, acquisition, profile);
    setIssues(nextIssues);
    setOauthSecretVisible(false);
    setTrustPasswordVisible(false);
    setSubmissionError(undefined);
    setTestConfirmation(undefined);
    if (Object.keys(nextIssues).length > 0) {
      return;
    }

    const command: HostCommand = {
      command: "profiles.test",
      id: globalThis.crypto.randomUUID(),
      payload: testPayload(),
      version: HOST_PROTOCOL_VERSION,
    };
    setTesting(true);
    try {
      const response = await host.execute(command);
      if (!response.ok) {
        setSubmissionError(`${response.error.summary} ${response.error.recovery}`);
        return;
      }
      setTestConfirmation(
        "Connection test passed. No profile was saved and the active connection was unchanged.",
      );
    } catch {
      setSubmissionError(
        "The application host did not accept the profile connection test. Open Activity for diagnostics.",
      );
    } finally {
      setTesting(false);
    }
  }

  async function submit(): Promise<void> {
    const nextIssues = validateProfileForm(form, acquisition, profile);
    setIssues(nextIssues);
    setOauthSecretVisible(false);
    setTrustPasswordVisible(false);
    setSubmissionError(undefined);
    setTestConfirmation(undefined);
    if (Object.keys(nextIssues).length > 0) {
      return;
    }

    const command: HostCommand =
      profile === undefined
        ? {
            command: "profiles.create",
            id: globalThis.crypto.randomUUID(),
            payload: { profile: buildCreateInput(form, acquisition, binding) },
            version: HOST_PROTOCOL_VERSION,
          }
        : {
            command: "profiles.update",
            id: globalThis.crypto.randomUUID(),
            payload: {
              profile: buildUpdateInput(form, profile, acquisition, expectedRevision, binding),
              profileId: profile.id,
            },
            version: HOST_PROTOCOL_VERSION,
          };
    setSubmitting(true);
    try {
      const response = await host.execute(command);
      if (!response.ok) {
        setSubmissionError(`${response.error.summary} ${response.error.recovery}`);
        return;
      }
      setAcquisition(null);
      onClose();
    } catch {
      setSubmissionError(
        "The application host did not accept the profile request. Open Activity for diagnostics.",
      );
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <Dialog
      aria-labelledby="profile-dialog-title"
      fullWidth
      keepMounted
      maxWidth="md"
      onClose={busy ? undefined : onClose}
      open={open}
      slotProps={{
        paper: {
          sx: {
            maxHeight: "calc(100% - 32px)",
            maxWidth: 760,
          },
        },
      }}
    >
      <DialogTitle id="profile-dialog-title">{title}</DialogTitle>
      <DialogContent dividers>
        <Stack spacing={2}>
          <Box>
            <Typography component="h3" variant="subtitle2">
              Cluster
            </Typography>
            <Typography color="text.secondary" variant="body2">
              Stored values remain in the application host and are never returned to this form.
            </Typography>
          </Box>
          <Stack direction={{ sm: "row", xs: "column" }} spacing={2}>
            <TextField
              autoFocus
              error={issues.name !== undefined}
              fullWidth
              helperText={issues.name}
              label="Profile name"
              onChange={(event) => {
                update("name", event.target.value);
              }}
              required
              value={form.name}
            />
            <TextField
              error={issues.brokers !== undefined}
              fullWidth
              helperText={issues.brokers ?? "Separate multiple host:port endpoints with commas."}
              label="Bootstrap brokers"
              onChange={(event) => {
                update("brokers", event.target.value);
              }}
              placeholder="127.0.0.1:9093"
              required
              value={form.brokers}
            />
          </Stack>

          <Box>
            <Stack
              direction={{ sm: "row", xs: "column" }}
              spacing={1}
              sx={{ alignItems: { sm: "center" } }}
            >
              <Box sx={{ flex: 1 }}>
                <Typography component="h3" variant="subtitle2">
                  TLS trust
                </Typography>
                <Typography color="text.secondary" variant="body2">
                  Enter certificates and credentials manually, or use optional retrieval.
                  Certificate verification is always enabled.
                </Typography>
              </Box>
            </Stack>
          </Box>
          <FormControl fullWidth>
            <InputLabel id="trust-kind-label">Trust material format</InputLabel>
            <Select
              aria-label="Trust material format"
              label="Trust material format"
              labelId="trust-kind-label"
              onChange={(event) => {
                const kind = event.target.value;
                const kindChanged = kind !== form.trustKind;
                update("trustKind", kind);
                if (kindChanged) {
                  setAcquisition(null);
                  update("trustMaterial", "");
                  update("trustMaterialMode", "clear");
                  update("trustLabel", derivedTrustLabel(kind));
                  update("trustPassword", "");
                  update("trustPasswordMode", "clear");
                }
              }}
              value={form.trustKind}
            >
              <MenuItem value="pem">PEM certificate</MenuItem>
              <MenuItem value="jks">JKS truststore</MenuItem>
              <MenuItem value="pkcs12">PKCS12 truststore</MenuItem>
            </Select>
          </FormControl>
          <Stack spacing={0.75}>
            <Stack direction="row" spacing={1.5} sx={{ alignItems: "center" }}>
              <Button
                onClick={() => {
                  trustFileInput.current?.click();
                }}
                variant="outlined"
              >
                Select trust material
              </Button>
              <input
                accept={
                  form.trustKind === "pem"
                    ? ".pem,.crt,application/x-pem-file,application/x-x509-ca-cert"
                    : ".jks,.p12,.pfx,application/octet-stream"
                }
                aria-label="Trust material file"
                onChange={(event) => {
                  void selectTrustFile(event.currentTarget.files?.[0]);
                }}
                ref={trustFileInput}
                style={{ display: "none" }}
                type="file"
              />
              <Typography
                color={issues.trustMaterial === undefined ? "text.secondary" : "error"}
                noWrap
                variant="body2"
              >
                {issues.trustMaterial ??
                  (form.trustMaterialMode === "replace"
                    ? form.trustLabel
                    : form.trustMaterialMode === "acquired"
                      ? "Retrieved certificate applied to this connection draft."
                      : retainedTrust
                        ? "Saved trust material will be retained."
                        : "No trust material selected.")}
              </Typography>
            </Stack>
          </Stack>
          <Accordion
            expanded={retrievalOpen}
            onChange={(_, expanded) => {
              setRetrievalStarted(true);
              setRetrievalOpen(expanded);
            }}
          >
            <AccordionSummary
              id="profile-secret-retrieval-heading"
              aria-controls="profile-secret-retrieval"
              aria-label="Secret Retrieval Profile"
              aria-describedby="profile-secret-retrieval-description"
              expandIcon={<ExpandMoreIcon />}
              disabled={busy}
            >
              <Stack spacing={0.5}>
                <Typography component="span" variant="subtitle2">
                  Secret Retrieval Profile
                </Typography>
                <Typography
                  id="profile-secret-retrieval-description"
                  variant="body2"
                  color="text.secondary"
                >
                  Optional: use saved retrieval instructions to fill certificate and secret fields.
                  You can also configure these fields manually.
                </Typography>
              </Stack>
            </AccordionSummary>
            <AccordionDetails>
              {retrievalStarted ? (
                <Stack spacing={2}>
                  <ProfileTrustRecipeSelector
                    host={host}
                    managementAction={
                      <TrustRecipeManagementButton host={host} transfer={transfer} />
                    }
                    profile={
                      profile === undefined
                        ? undefined
                        : { id: profile.id, revision: expectedRevision }
                    }
                    disabled={busy}
                    onChange={(next) => {
                      setRecipeSelection((current) =>
                        next === null
                          ? null
                          : {
                              ...next,
                              reference: {
                                ...next.reference,
                                ...(current?.reference.access === undefined
                                  ? {}
                                  : { access: current.reference.access }),
                                ...(current?.reference.apiAccess === undefined
                                  ? {}
                                  : { apiAccess: current.reference.apiAccess }),
                              },
                            },
                      );
                      setTestConfirmation(undefined);
                    }}
                  />
                  <Box hidden={recipeSelection == null}>
                    <RemoteTrustAcquisitionPanel
                      acquisition={acquisition}
                      disabled={busy || recipeSelection?.reference.identity?.mode === "reset"}
                      host={host}
                      kind={recipeSelection?.recipe.kind ?? form.trustKind}
                      label={derivedTrustLabel(recipeSelection?.recipe.kind ?? form.trustKind)}
                      recipeSelection={recipeSelection ?? null}
                      profile={
                        profile === undefined
                          ? undefined
                          : { id: profile.id, revision: expectedRevision }
                      }
                      onAcquisitionChange={changeAcquisition}
                      onApiCaChange={(apiCa) => {
                        setForm((current) => ({ ...current, apiCa }));
                        setTestConfirmation(undefined);
                      }}
                      onApiAccessChange={(apiAccess) =>
                        setRecipeSelection((current) =>
                          current == null
                            ? current
                            : { ...current, reference: { ...current.reference, apiAccess } },
                        )
                      }
                      onAccessChange={(access) =>
                        setRecipeSelection((current) =>
                          current == null
                            ? current
                            : { ...current, reference: { ...current.reference, access } },
                        )
                      }
                      currentOAuth={{
                        endpoint: form.tokenEndpoint,
                        clientId: form.clientId,
                        scope: form.scope,
                      }}
                      onOpenActivity={onOpenActivity}
                      templateSnapshot={templateSnapshot}
                    />
                  </Box>
                </Stack>
              ) : null}
            </AccordionDetails>
          </Accordion>
          {form.trustKind === "pem" ? null : (
            <TextField
              disabled={busy}
              error={issues.trustPassword !== undefined}
              fullWidth
              helperText={
                issues.trustPassword ??
                (form.trustPasswordMode === "acquired"
                  ? "Retrieved successfully. Stored securely in the host; type to replace it."
                  : retainedTrustPassword
                    ? "Saved truststore password will be retained. Type here to replace it."
                    : undefined)
              }
              label="Truststore password"
              placeholder={
                form.trustPasswordMode === "acquired" || retainedTrustPassword
                  ? "••••••••"
                  : undefined
              }
              onChange={(event) => {
                const value = event.target.value;
                update("trustPassword", value);
                update("trustPasswordMode", value.length === 0 ? "clear" : "replace");
              }}
              slotProps={{
                inputLabel: { shrink: true },
                htmlInput: { autoComplete: "new-password" },
                input: {
                  endAdornment: (
                    <InputAdornment position="end">
                      {form.trustPasswordMode === "acquired" || retainedTrustPassword ? (
                        <Typography variant="body2" color="text.secondary">
                          {form.trustPasswordMode === "acquired" ? "Retrieved" : "Saved"}
                        </Typography>
                      ) : (
                        <Button
                          aria-label={`${trustPasswordVisible ? "Hide" : "Show"} truststore password`}
                          aria-pressed={trustPasswordVisible}
                          onClick={() => {
                            setTrustPasswordVisible((visible) => !visible);
                          }}
                          type="button"
                          variant="text"
                        >
                          {trustPasswordVisible ? "Hide" : "Show"}
                        </Button>
                      )}
                    </InputAdornment>
                  ),
                },
              }}
              type={trustPasswordVisible ? "text" : "password"}
              value={form.trustPassword}
            />
          )}
          {editing && !retainedTrust && profile.trust.materialPresent ? (
            <Button
              onClick={() => {
                update("trustKind", profile.trust.kind);
                update("trustLabel", profile.trust.label);
                update("trustMaterial", "");
                update("trustMaterialMode", "retain");
              }}
              sx={{ alignSelf: "flex-start" }}
              variant="text"
            >
              Retain saved trust material
            </Button>
          ) : null}
          {editing &&
          form.trustKind !== "pem" &&
          profile.trust.passwordPresent &&
          !retainedTrustPassword ? (
            <Button
              onClick={() => {
                update("trustPassword", "");
                update("trustPasswordMode", "retain");
              }}
              sx={{ alignSelf: "flex-start" }}
              variant="text"
            >
              Retain saved truststore password
            </Button>
          ) : null}

          <ProfileAuthenticationFields
            form={form}
            issues={issues}
            onChange={update}
            savedSecretPresent={profile?.oauth?.clientSecretPresent === true}
            secretVisible={oauthSecretVisible}
            onSecretVisibilityChange={setOauthSecretVisible}
          />
          <ProfileServiceFields form={form} issues={issues} onChange={update} />
          {issueCount === 0 ? null : (
            <Alert severity="error">
              Correct {issueCount} identified {issueCount === 1 ? "field" : "fields"} before saving.
            </Alert>
          )}
          {submissionError === undefined ? null : (
            <Alert
              action={
                <Button color="inherit" onClick={() => onOpenActivity()} variant="text">
                  Open activity log
                </Button>
              }
              severity="error"
            >
              {submissionError}
            </Alert>
          )}
          {testConfirmation === undefined ? null : (
            <Alert role="status" severity="success">
              {testConfirmation}
            </Alert>
          )}
        </Stack>
      </DialogContent>
      <DialogActions aria-label="Profile actions" role="group">
        <Button disabled={busy} onClick={onClose}>
          Cancel
        </Button>
        <Button
          disabled={busy}
          onClick={() => {
            void testConnection();
          }}
          variant="outlined"
        >
          {testing ? "Testing connection…" : "Test connection"}
        </Button>
        <Button
          disabled={busy}
          onClick={() => {
            void submit();
          }}
          variant="contained"
        >
          {editing ? "Update profile" : "Save profile"}
        </Button>
      </DialogActions>
    </Dialog>
  );
}
