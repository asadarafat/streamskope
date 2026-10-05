import { useEffect, useId, useRef, useState } from "react";
import Box from "@mui/material/Box";
import RadioGroup from "@mui/material/RadioGroup";
import Stack from "@mui/material/Stack";
import Typography from "@mui/material/Typography";

import {
  NATS_LIMITS,
  natsUtf8Bytes,
  parseNatsCaPem,
  parseNatsName,
  parseNatsServers,
  parseNatsToken,
  type NatsProfileCreateInput,
  type NatsProfileSummary,
  type NatsProfileUpdateInput,
} from "../contracts";
import {
  StudioAlert as Alert,
  StudioButton as Button,
  StudioDialog as Dialog,
  StudioDialogActions as DialogActions,
  StudioDialogContent as DialogContent,
  StudioDialogTitle as DialogTitle,
  StudioFormControl as FormControl,
  StudioFormLabel as FormLabel,
  StudioInputLabel as InputLabel,
  StudioLabeledControl as FormControlLabel,
  StudioMenuItem as MenuItem,
  StudioRadio as Radio,
  StudioSelect as Select,
  StudioTextField as TextField,
} from "../../../platform/ui/controls";
import { streamSkopeMuiMonospaceTypography } from "../../../platform/ui/createStreamSkopeTheme";

export interface ProfileEditorProperties {
  readonly profile: NatsProfileSummary | null;
  readonly available: boolean;
  readonly isInteractive: () => boolean;
  readonly failure: { readonly summary: string; readonly recovery?: string } | null;
  readonly onCreate: (input: NatsProfileCreateInput) => Promise<boolean>;
  readonly onUpdate: (
    profile: NatsProfileSummary,
    input: NatsProfileUpdateInput,
  ) => Promise<boolean>;
  readonly onClose: () => void;
}

type Issues = Partial<Record<"name" | "servers" | "token" | "ca", string>>;

/** Only the safe summary is captured; credentials are never rehydrated from the host. */
export function ProfileEditor({
  profile,
  available,
  isInteractive,
  failure,
  onCreate,
  onUpdate,
  onClose,
}: ProfileEditorProperties): React.JSX.Element {
  const [original] = useState(() => profile);
  const [name, setName] = useState(original?.name ?? "");
  const [servers, setServers] = useState(original?.servers.join("\n") ?? "");
  const [authentication, setAuthentication] = useState<"none" | "token">(
    original?.authentication.mode ?? "none",
  );
  const [transport, setTransport] = useState<"plaintext" | "tls">(original?.tls.mode ?? "tls");
  const savedToken =
    original?.authentication.mode === "token" && original.authentication.tokenPresent;
  const savedCa = original?.tls.mode === "tls" && original.tls.caPresent;
  const [token, setToken] = useState("");
  const [ca, setCa] = useState("");
  const [caMode, setCaMode] = useState<"retain" | "replace" | "clear">(
    savedCa ? "retain" : "clear",
  );
  const [issues, setIssues] = useState<Issues>({});
  const [submitting, setSubmitting] = useState(false);
  const [submissionFailed, setSubmissionFailed] = useState(false);
  const mounted = useRef(true);
  const nameInput = useRef<HTMLInputElement>(null);
  const id = useId();
  const disabled = submitting || !available || !isInteractive();

  useEffect(() => {
    mounted.current = true;
    return (): void => {
      mounted.current = false;
    };
  }, []);

  function dismiss(): void {
    setToken("");
    setCa("");
    onClose();
  }

  async function submit(): Promise<void> {
    if (!isInteractive() || !available || submitting) return;
    const nextIssues: Issues = {};
    let parsedName = "";
    let parsedServers: readonly string[] = [];
    try {
      parsedName = parseNatsName(name);
    } catch {
      nextIssues.name = "Enter a profile name of at most 256 UTF-8 bytes.";
    }
    try {
      parsedServers = parseNatsServers(
        servers
          .split(/[\n,]/u)
          .map((entry) => entry.trim())
          .filter(Boolean),
        transport,
      );
    } catch {
      nextIssues.servers =
        "Enter up to eight distinct nats:// or tls:// server URLs without credentials, paths, or query parameters.";
    }
    if (authentication === "token" && (token.length > 0 || !savedToken)) {
      try {
        if (token.length === 0) throw new Error();
        parseNatsToken(token);
      } catch {
        nextIssues.token = "Enter a token of at most 16 KiB without control characters.";
      }
    }
    let parsedCa = "";
    if (transport === "tls" && caMode === "replace") {
      try {
        parsedCa = parseNatsCaPem(ca);
      } catch {
        nextIssues.ca =
          "Enter a valid certificate PEM bundle of at most 256 KiB, or select system trust.";
      }
    }
    setIssues(nextIssues);
    if (Object.keys(nextIssues).length > 0) return;
    // Check the current activation immediately before admitting a host write.
    if (!isInteractive()) return;
    setSubmissionFailed(false);
    setSubmitting(true);
    const auth =
      authentication === "none"
        ? { mode: "none" as const }
        : {
            mode: "token" as const,
            token:
              token.length > 0
                ? { mode: "replace" as const, value: token }
                : { mode: "retain" as const },
          };
    const tls =
      transport === "plaintext"
        ? { mode: "plaintext" as const }
        : {
            mode: "tls" as const,
            caPem:
              caMode === "replace"
                ? { mode: "replace" as const, value: parsedCa }
                : { mode: caMode },
          };
    try {
      // Create has no retain mode. Validation above guarantees a replacement token;
      // the create CA can only be custom or system trust.
      const accepted =
        original === null
          ? await onCreate({
              name: parsedName,
              servers: parsedServers,
              authentication:
                authentication === "none"
                  ? { mode: "none" }
                  : { mode: "token", token: { mode: "replace", value: token } },
              tls:
                transport === "plaintext"
                  ? { mode: "plaintext" }
                  : {
                      mode: "tls",
                      caPem:
                        caMode === "replace"
                          ? { mode: "replace", value: parsedCa }
                          : { mode: "clear" },
                    },
            })
          : await onUpdate(original, {
              name: parsedName,
              servers: parsedServers,
              authentication: auth,
              tls,
            });
      if (!mounted.current) return;
      if (accepted) dismiss();
      else setSubmissionFailed(true);
    } catch {
      if (mounted.current) setSubmissionFailed(true);
    } finally {
      if (mounted.current) setSubmitting(false);
    }
  }

  return (
    <Dialog
      aria-labelledby={`${id}-title`}
      initialFocusRef={nameInput}
      onClose={submitting ? undefined : dismiss}
      open
    >
      <DialogTitle id={`${id}-title`}>
        {original === null ? "Create NATS profile" : "Edit NATS profile"}
      </DialogTitle>
      <DialogContent>
        <Box
          component="form"
          id={`${id}-form`}
          onSubmit={(event) => {
            event.preventDefault();
            void submit();
          }}
        >
          <Stack spacing={2}>
            <Typography color="text.secondary" variant="body2">
              {original === null
                ? "Credentials are sent only to the NATS host when you save."
                : `Editing revision ${String(original.revision)}. Blank saved token and CA fields retain their host-held values.`}
            </Typography>
            <TextField
              autoFocus
              disabled={disabled}
              error={issues.name !== undefined}
              helperText={issues.name}
              inputRef={nameInput}
              label="Profile name"
              onChange={(event) => {
                if (isInteractive()) setName(event.target.value);
              }}
              required
              value={name}
            />
            <TextField
              disabled={disabled}
              error={issues.servers !== undefined}
              helperText={
                issues.servers ?? "One server URL per line, for example tls://nats.example:4222."
              }
              label="NATS servers"
              minRows={2}
              multiline
              onChange={(event) => {
                if (isInteractive()) setServers(event.target.value);
              }}
              required
              slotProps={{ input: { sx: streamSkopeMuiMonospaceTypography } }}
              value={servers}
            />
            <FormControl disabled={disabled}>
              <InputLabel id={`${id}-authentication`}>Authentication</InputLabel>
              <Select
                label="Authentication"
                labelId={`${id}-authentication`}
                onChange={(event) => {
                  if (!isInteractive()) return;
                  const next = event.target.value === "token" ? "token" : "none";
                  setAuthentication(next);
                  if (next === "none") setToken("");
                }}
                value={authentication}
              >
                <MenuItem value="none">None</MenuItem>
                <MenuItem value="token">Token</MenuItem>
              </Select>
            </FormControl>
            {authentication === "token" ? (
              <TextField
                disabled={disabled}
                error={issues.token !== undefined}
                helperText={
                  issues.token ??
                  (savedToken
                    ? "Saved token is retained when blank. Type a replacement, or select None to remove authentication."
                    : "A token is required for token authentication.")
                }
                label="Token"
                onChange={(event) => {
                  if (isInteractive()) setToken(event.target.value);
                }}
                slotProps={{
                  htmlInput: { autoComplete: "new-password", maxLength: NATS_LIMITS.tokenBytes },
                }}
                type="password"
                value={token}
              />
            ) : null}
            <FormControl disabled={disabled}>
              <FormLabel component="legend">Transport security</FormLabel>
              <RadioGroup
                aria-label="Transport security"
                onChange={(event) => {
                  if (!isInteractive()) return;
                  const next = event.target.value === "plaintext" ? "plaintext" : "tls";
                  setTransport(next);
                  if (next === "plaintext") {
                    setCa("");
                    setCaMode("clear");
                  }
                }}
                row
                value={transport}
              >
                <FormControlLabel control={<Radio />} label="Verified TLS" value="tls" />
                <FormControlLabel control={<Radio />} label="Plaintext" value="plaintext" />
              </RadioGroup>
            </FormControl>
            {transport === "tls" ? (
              <>
                <FormControl disabled={disabled}>
                  <InputLabel id={`${id}-ca-mode`}>CA handling</InputLabel>
                  <Select
                    label="CA handling"
                    labelId={`${id}-ca-mode`}
                    onChange={(event) => {
                      if (!isInteractive()) return;
                      const next = event.target.value;
                      if (next !== "retain" && next !== "replace" && next !== "clear") return;
                      setCaMode(next);
                      if (next !== "replace") setCa("");
                    }}
                    value={caMode}
                  >
                    {savedCa ? <MenuItem value="retain">Retain saved CA</MenuItem> : null}
                    <MenuItem value="replace">Replace CA</MenuItem>
                    <MenuItem value="clear">Use system trust</MenuItem>
                  </Select>
                </FormControl>
                <TextField
                  disabled={disabled}
                  error={issues.ca !== undefined}
                  helperText={
                    issues.ca ??
                    (caMode === "retain"
                      ? "Saved CA is retained when blank. Enter PEM to replace it, or select system trust to clear it."
                      : caMode === "clear"
                        ? "System trust is used with certificate and hostname verification. Enter PEM to use a custom CA."
                        : "Certificate and hostname verification remain enabled.")
                  }
                  label="CA certificate PEM"
                  maxRows={5}
                  minRows={3}
                  multiline
                  onChange={(event) => {
                    if (!isInteractive()) return;
                    const next = event.target.value;
                    if (natsUtf8Bytes(next) > NATS_LIMITS.caPemBytes) {
                      setIssues((current) => ({
                        ...current,
                        ca: "CA certificate PEM must be at most 256 KiB.",
                      }));
                      return;
                    }
                    setCa(next);
                    setCaMode(next.length === 0 ? (savedCa ? "retain" : "clear") : "replace");
                  }}
                  slotProps={{ input: { sx: streamSkopeMuiMonospaceTypography } }}
                  value={ca}
                />
              </>
            ) : (
              <Alert severity="warning">
                Plaintext sends NATS traffic and tokens without transport encryption. Use it only in
                an isolated environment you trust.
              </Alert>
            )}
            {submissionFailed ? (
              <Alert severity="error">
                {failure?.summary ?? "The NATS host did not accept the profile."}{" "}
                {failure?.recovery ?? "Review the fields and retry."}
              </Alert>
            ) : null}
            {!available ? (
              <Alert severity="warning">
                NATS host unavailable. Saving is disabled until the host is available.
              </Alert>
            ) : null}
          </Stack>
        </Box>
      </DialogContent>
      <DialogActions>
        <Button disabled={submitting} onClick={dismiss}>
          Cancel
        </Button>
        <Button disabled={disabled} form={`${id}-form`} type="submit" variant="contained">
          {submitting ? "Saving profile…" : "Save profile"}
        </Button>
      </DialogActions>
    </Dialog>
  );
}
