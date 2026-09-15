import { Box, Stack, Typography } from "@mui/material";

import { previewCommandTemplate, type ProfileTrustKind } from "../contracts";
import type { RemoteSshAuthentication } from "../contracts/remote-trust-types";
import {
  StudioAccordion as Accordion,
  StudioAccordionDetails as AccordionDetails,
  StudioAccordionSummary as AccordionSummary,
  StudioTextField as TextField,
  StudioButton as Button,
} from "../../ui/controls";

import { RemoteSshCredentials } from "./RemoteSshCredentials";
import type { ProfileTrustRecipeSelection } from "./ProfileTrustRecipeSelector";
import type { SshTargetDraft, TargetField } from "./remote-trust-panel-model";
import { formatUtcTimestamp } from "./timestamp-presentation";

export function SshIdentityReview({
  host,
  port,
  fingerprint,
  expiresAt,
  onAccept,
}: {
  readonly host: string;
  readonly port: string;
  readonly fingerprint: string;
  readonly expiresAt: string;
  readonly onAccept: () => void;
}): React.JSX.Element {
  return (
    <Stack spacing={1}>
      <Typography variant="body2">
        First connection to {host}:{port}. Verify this SSH identity independently before sending
        credentials. Discovery alone does not prove who owns the server.
      </Typography>
      <Typography variant="body2" sx={{ overflowWrap: "anywhere" }}>
        {fingerprint}
      </Typography>
      <Typography variant="body2">
        Acceptance is for this acquisition. The identity is retained only when you save the profile.
        This review expires {formatUtcTimestamp(expiresAt)}.
      </Typography>
      <Button onClick={onAccept}>Accept identity and acquire</Button>
    </Stack>
  );
}

export function SshAccessFields({
  inputsLocked,
  issues,
  target,
  update,
  agentStatus,
  authentication,
  onAuthenticationChange,
}: {
  readonly inputsLocked: boolean;
  readonly issues: Partial<Record<TargetField, string>>;
  readonly target: SshTargetDraft;
  readonly update: (field: TargetField, value: string) => void;
  readonly agentStatus: "checking" | "unknown" | "configured" | "unavailable";
  readonly authentication: RemoteSshAuthentication;
  readonly onAuthenticationChange: (value: RemoteSshAuthentication) => void;
}): React.JSX.Element {
  return (
    <>
      {" "}
      <Stack direction={{ sm: "row", xs: "column" }} spacing={2}>
        <TextField
          disabled={inputsLocked}
          error={issues.host !== undefined}
          fullWidth
          helperText={issues.host}
          label="SSH host"
          onChange={(event) => {
            update("host", event.target.value);
          }}
          value={target.host}
        />
        <TextField
          disabled={inputsLocked}
          error={issues.port !== undefined}
          helperText={issues.port}
          label="SSH port"
          onChange={(event) => {
            update("port", event.target.value);
          }}
          slotProps={{ htmlInput: { max: 65_535, min: 1 } }}
          sx={{ width: { sm: 144, xs: "100%" } }}
          type="number"
          value={target.port}
        />
      </Stack>
      <TextField
        disabled={inputsLocked}
        error={issues.username !== undefined}
        fullWidth
        helperText={
          issues.username ??
          "Server login used to retrieve certificates and passwords; not your Kafka login."
        }
        label="SSH username"
        onChange={(event) => {
          update("username", event.target.value);
        }}
        value={target.username}
      />
      <RemoteSshCredentials
        agentStatus={agentStatus}
        authentication={authentication}
        disabled={inputsLocked}
        error={issues.password}
        onChange={onAuthenticationChange}
      />
    </>
  );
}

export function SshAcquisitionDetails({
  targetComplete,
  target,
  targetPort,
  recipeSelection,
  kind,
  passwordTemplate,
  materialTemplate,
}: {
  readonly targetComplete: boolean;
  readonly target: SshTargetDraft;
  readonly targetPort: number;
  readonly recipeSelection?: ProfileTrustRecipeSelection | null | undefined;
  readonly kind: ProfileTrustKind;
  readonly passwordTemplate: { readonly name: string; readonly template: string } | undefined;
  readonly materialTemplate: { readonly name: string; readonly template: string } | undefined;
}): React.JSX.Element {
  return (
    <>
      {" "}
      <Box>
        <Typography color="text.secondary" variant="caption">
          Target
        </Typography>
        <Typography variant="body2">
          {targetComplete
            ? `${target.host.trim()}:${targetPort} as ${target.username.trim()}`
            : "Complete the SSH target and credentials to acquire trust."}
        </Typography>
        <Typography color="text.secondary" sx={{ mt: 0.75 }} variant="body2">
          New SSH identities require your approval before credentials are sent. Identity is retained
          only when you save the profile.{" "}
          {recipeSelection?.recipe.ssh?.source === "file"
            ? "The selected certificate file is read without changing it."
            : recipeSelection?.recipe.ssh?.source === "stdout"
              ? "The selected command returns the certificate contents."
              : "The selected command creates a temporary trust file; StreamSkope removes only that owned file."}
        </Typography>
      </Box>
      <Accordion
        disableGutters
        elevation={0}
        slotProps={{ transition: { unmountOnExit: true } }}
        sx={{
          "&::before": { display: "none" },
          borderBlock: 1,
          borderColor: "divider",
        }}
      >
        <AccordionSummary>
          <Typography variant="body2">Advanced acquisition details</Typography>
        </AccordionSummary>
        <AccordionDetails>
          <Stack spacing={1.5}>
            <Typography color="text.secondary" variant="body2">
              Host identity is automatically discovered and pinned before credentials are sent.
              First-use discovery confirms key continuity, not independent ownership.
            </Typography>
            {kind === "pem" ? null : (
              <>
                <Typography variant="body2">
                  Password template: {passwordTemplate?.name ?? "No selected template"}
                </Typography>
                {passwordTemplate === undefined ? null : (
                  <Typography
                    aria-label="Remote password template preview"
                    component="pre"
                    sx={{
                      bgcolor: "action.hover",
                      m: 0,
                      overflow: "auto",
                      p: 1.5,
                      whiteSpace: "pre-wrap",
                      wordBreak: "break-word",
                    }}
                    variant="body2"
                  >
                    {previewCommandTemplate("truststore-password", passwordTemplate.template)}
                  </Typography>
                )}
              </>
            )}
            <Typography variant="body2">
              Trust template: {materialTemplate?.name ?? "No selected template"}
            </Typography>
            {materialTemplate === undefined ? null : (
              <Typography
                aria-label="Remote trust template preview"
                component="pre"
                sx={{
                  bgcolor: "action.hover",
                  m: 0,
                  overflow: "auto",
                  p: 1.5,
                  whiteSpace: "pre-wrap",
                  wordBreak: "break-word",
                }}
                variant="body2"
              >
                {previewCommandTemplate("truststore-fetch", materialTemplate.template)}
              </Typography>
            )}
          </Stack>
        </AccordionDetails>
      </Accordion>
    </>
  );
}
