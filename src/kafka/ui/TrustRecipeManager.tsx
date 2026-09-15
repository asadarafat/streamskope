import { useEffect, useRef, useState } from "react";
import { Box, List, Stack, Typography } from "@mui/material";

import {
  BUILT_IN_TRUST_RECIPES,
  HOST_PROTOCOL_VERSION,
  TRUST_RECIPE_LIMITS,
  HostContractValidationError,
  parseTrustAcquisitionRecipeInput,
  trustAcquisitionRecipeDefinition,
  trustRecipeComparableName,
  type HostCommand,
  type HostCommandResponse,
  type StreamSkopeHost,
  type TrustAcquisitionRecipe,
  type TrustAcquisitionRecipeInput,
  type TrustAcquisitionRecipeSnapshot,
} from "../contracts";
import {
  StudioAlert as Alert,
  StudioButton as Button,
  StudioDialog as Dialog,
  StudioDialogActions as DialogActions,
  StudioDialogContent as DialogContent,
  StudioDialogTitle as DialogTitle,
  StudioListItemButton as ListItemButton,
  StudioMenu as Menu,
  StudioMenuItem as MenuItem,
  StudioTextField as TextField,
} from "../../ui/controls";

import { TrustRecipeFields } from "./TrustRecipeFields";
import {
  browserTextDocumentTransfer,
  type TextDocumentTransferPort,
} from "./text-document-transfer";

interface Properties {
  readonly host: StreamSkopeHost;
  readonly onClose: () => void;
  readonly transfer?: TextDocumentTransferPort | undefined;
}

function blankRecipe(): TrustAcquisitionRecipeInput {
  return {
    name: "",
    kind: "pem",
    syntax: "named-v1",
    method: "ssh",
    ssh: { source: "file", value: "", password: { source: "none" } },
    parameters: [],
    timeoutSeconds: TRUST_RECIPE_LIMITS.defaultTimeoutSeconds,
  };
}

function readImport(file: File): Promise<string> {
  if (file.size > TRUST_RECIPE_LIMITS.exchangeBytes)
    return Promise.reject(
      new Error(`Template file exceeds the ${TRUST_RECIPE_LIMITS.exchangeBytes}-byte limit.`),
    );
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onerror = (): void =>
      reject(
        new Error("The template file could not be read. Select a readable JSON file and retry."),
      );
    reader.onabort = (): void => reject(new Error("Template file reading was cancelled."));
    reader.onload = (): void =>
      typeof reader.result === "string"
        ? resolve(reader.result)
        : reject(new Error("The template file could not be read as text."));
    reader.readAsText(file);
  });
}

export function TrustRecipeManager({
  host,
  onClose,
  transfer = browserTextDocumentTransfer,
}: Properties): React.JSX.Element {
  const [snapshot, setSnapshot] = useState<TrustAcquisitionRecipeSnapshot | null>(null);
  const latest = useRef(snapshot);
  const mounted = useRef(true);
  const commitWaiter = useRef<((cancelled?: boolean) => void) | undefined>(undefined);
  const [loading, setLoading] = useState(true);
  const [busy, setBusy] = useState(false);
  const [query, setQuery] = useState("");
  const [builtInAnchor, setBuiltInAnchor] = useState<HTMLElement | null>(null);
  const [selected, setSelected] = useState<TrustAcquisitionRecipe>();
  const [draft, setDraft] = useState<TrustAcquisitionRecipeInput>();
  const [saved, setSaved] = useState("");
  const [error, setError] = useState<string>();
  const [issue, setIssue] = useState<{ path: string; message: string }>();
  const [notice, setNotice] = useState<string>();
  const [pendingNavigation, setPendingNavigation] = useState<() => void>();
  const [deleting, setDeleting] = useState(false);
  const [usage, setUsage] = useState<readonly { id: string; name: string; revision: number }[]>([]);
  const fileInput = useRef<HTMLInputElement>(null);
  const dirty = draft !== undefined && JSON.stringify(draft) !== saved;
  const unavailable = snapshot?.store.state !== "ready";

  async function execute(command: HostCommand): Promise<HostCommandResponse> {
    const response = await host.execute(command);
    if (!response.ok) throw new Error(`${response.error.summary} ${response.error.recovery}`);
    return response;
  }

  useEffect(() => {
    mounted.current = true;
    const unsubscribe = host.subscribe((event) => {
      if (event.event !== "recipes.changed") return;
      latest.current = event.payload;
      commitWaiter.current?.();
      setSnapshot(event.payload);
      setLoading(false);
    });
    void host
      .execute({
        command: "recipes.list",
        payload: {},
        id: crypto.randomUUID(),
        version: HOST_PROTOCOL_VERSION,
      })
      .then((response) => {
        if (!mounted.current) return;
        if (!response.ok) setError(`${response.error.summary} ${response.error.recovery}`);
        setLoading(false);
      })
      .catch(() => {
        if (mounted.current) {
          setError(
            "Templates could not be loaded. Close and reopen the manager to retry. Manual profile configuration remains available.",
          );
          setLoading(false);
        }
      });
    return (): void => {
      mounted.current = false;
      commitWaiter.current?.(true);
      unsubscribe();
    };
  }, [host]);

  function open(recipe?: TrustAcquisitionRecipe): void {
    const next = recipe === undefined ? blankRecipe() : trustAcquisitionRecipeDefinition(recipe);
    setSelected(recipe);
    setDraft(next);
    setSaved(JSON.stringify(next));
    setError(undefined);
    setIssue(undefined);
    setNotice(undefined);
  }
  function navigate(action: () => void): void {
    if (dirty) setPendingNavigation(() => action);
    else action();
  }
  function openBuiltIn(recipe: TrustAcquisitionRecipeInput): void {
    const existing = latest.current?.recipes.find(
      (entry) => trustRecipeComparableName(entry.name) === trustRecipeComparableName(recipe.name),
    );
    if (existing !== undefined) {
      open(existing);
      return;
    }
    open();
    setDraft(parseTrustAcquisitionRecipeInput(recipe));
    setSaved("");
    setNotice(
      "Review before saving. Commands run on the SSH host and require its configured tools and permissions. {{host}} in OAuth suggestions uses the SSH host; edit the endpoint if Kafka authentication uses another address. Nothing has been retrieved.",
    );
  }
  async function run(action: () => Promise<void>): Promise<void> {
    setBusy(true);
    setError(undefined);
    setNotice(undefined);
    try {
      await action();
    } catch (failure: unknown) {
      if (mounted.current)
        setError(
          failure instanceof Error
            ? failure.message
            : "Template operation failed. Open Raw logs for diagnostics and retry.",
        );
    } finally {
      if (mounted.current) setBusy(false);
    }
  }
  async function save(): Promise<void> {
    if (draft === undefined) return;
    let validated: TrustAcquisitionRecipeInput;
    try {
      validated = parseTrustAcquisitionRecipeInput(draft);
      setIssue(undefined);
    } catch (failure: unknown) {
      if (failure instanceof HostContractValidationError) {
        setIssue({ path: failure.path, message: failure.message });
        setError(failure.message);
      }
      return;
    }
    await run(async () => {
      const common = { id: crypto.randomUUID(), version: HOST_PROTOCOL_VERSION };
      await execute(
        selected === undefined
          ? { ...common, command: "recipes.create", payload: validated }
          : {
              ...common,
              command: "recipes.update",
              payload: { id: selected.id, revision: selected.revision, recipe: validated },
            },
      );
      if (!mounted.current) return;
      const committed = await new Promise<TrustAcquisitionRecipe>((resolve, reject) => {
        const timeout = setTimeout(() => {
          commitWaiter.current = undefined;
          reject(
            new Error(
              "The host accepted the operation but no committed template was received. Reopen the library before retrying.",
            ),
          );
        }, 5_000);
        const check = (cancelled = false): void => {
          const recipe = latest.current?.recipes.find(
            (entry) =>
              (selected === undefined
                ? trustRecipeComparableName(entry.name) ===
                  trustRecipeComparableName(validated.name)
                : entry.id === selected.id && entry.revision >= selected.revision) &&
              JSON.stringify(trustAcquisitionRecipeDefinition(entry)) === JSON.stringify(validated),
          );
          if (!cancelled && recipe === undefined) return;
          clearTimeout(timeout);
          commitWaiter.current = undefined;
          if (recipe !== undefined && !cancelled) resolve(recipe);
          else reject(new Error("Template confirmation cancelled."));
        };
        commitWaiter.current = check;
        check();
      });
      if (!mounted.current) return;
      open(committed);
      setNotice("Retrieval profile saved.");
    });
  }
  async function importFile(file: File): Promise<void> {
    await run(async () => {
      const contents = await readImport(file);
      if (!mounted.current) return;
      const response = await execute({
        command: "recipes.import.preview",
        payload: { contents },
        id: crypto.randomUUID(),
        version: HOST_PROTOCOL_VERSION,
      });
      if (!mounted.current) return;
      if (!response.ok || !("draft" in response.result))
        throw new Error("The host did not return a template review. Nothing was imported.");
      setSelected(undefined);
      setDraft(response.result.draft);
      setSaved("");
      setIssue(undefined);
      setNotice(
        "Review imported commands before saving. Import review does not execute or save the template.",
      );
    });
  }
  const visible =
    snapshot?.recipes.filter((recipe) =>
      recipe.name.toLocaleLowerCase().includes(query.trim().toLocaleLowerCase()),
    ) ?? [];

  return (
    <>
      <Dialog
        open
        fullWidth
        maxWidth="md"
        aria-labelledby="trust-recipes-title"
        onClose={busy ? undefined : (): void => navigate(onClose)}
      >
        <DialogTitle id="trust-recipes-title">Secret Retrieval Profiles</DialogTitle>
        <DialogContent dividers>
          <Stack spacing={2}>
            <Typography variant="body2" color="text.secondary">
              Reusable SSH or HTTPS instructions to fill connection security fields. Create one for
              your environment, then select it under Secret Retrieval Profile in a connection.
              Editing these instructions never retrieves secrets or connects to Kafka.
            </Typography>
            {snapshot?.store.durability === "session" ? (
              <Alert severity="info">
                Templates are session-only in this host and will be lost when it stops.
              </Alert>
            ) : null}
            {snapshot?.store.state === "unavailable" ? (
              <Alert severity="error">
                {snapshot.store.recovery ??
                  "Template storage is unavailable. Existing profiles and manual trust remain available."}
              </Alert>
            ) : null}
            {error === undefined || snapshot?.store.state === "unavailable" ? null : (
              <Alert severity="error">{error}</Alert>
            )}
            {notice === undefined ? null : (
              <Typography role="status" variant="body2">
                {notice}
              </Typography>
            )}
            <Box
              sx={{
                display: "grid",
                gridTemplateColumns: {
                  xs: "minmax(0, 1fr)",
                  md: "minmax(160px, 1fr) minmax(0, 2fr)",
                },
                gap: 2,
              }}
            >
              <Stack spacing={1}>
                <TextField
                  label="Search retrieval profiles"
                  value={query}
                  onChange={(event) => setQuery(event.target.value)}
                />
                <Stack direction="row" spacing={1} useFlexGap sx={{ flexWrap: "wrap" }}>
                  <Button
                    disabled={busy || loading || unavailable}
                    onClick={() => navigate(() => open())}
                  >
                    New retrieval profile
                  </Button>
                  <Button
                    disabled={busy || loading || unavailable}
                    aria-haspopup="menu"
                    aria-expanded={builtInAnchor !== null}
                    aria-controls={
                      builtInAnchor === null ? undefined : "built-in-retrieval-profiles"
                    }
                    onClick={(event) => setBuiltInAnchor(event.currentTarget)}
                  >
                    Built-in profiles
                  </Button>
                  <Menu
                    id="built-in-retrieval-profiles"
                    anchorEl={builtInAnchor}
                    open={builtInAnchor !== null}
                    onClose={() => setBuiltInAnchor(null)}
                  >
                    {BUILT_IN_TRUST_RECIPES.map((recipe) => (
                      <MenuItem
                        key={recipe.name}
                        disabled={busy || loading || unavailable}
                        onClick={() => {
                          setBuiltInAnchor(null);
                          navigate(() => openBuiltIn(recipe));
                        }}
                      >
                        {recipe.name}
                      </MenuItem>
                    ))}
                  </Menu>
                  <Button
                    disabled={busy || loading || unavailable}
                    onClick={() => fileInput.current?.click()}
                  >
                    Import
                  </Button>
                </Stack>
                <input
                  ref={fileInput}
                  aria-label="Import template file"
                  type="file"
                  accept=".json,application/json"
                  hidden
                  onChange={(event) => {
                    const file = event.target.files?.[0];
                    event.target.value = "";
                    if (file !== undefined)
                      navigate(() => {
                        void importFile(file);
                      });
                  }}
                />
                {loading ? (
                  <Typography role="status" variant="body2">
                    Loading templates…
                  </Typography>
                ) : visible.length === 0 && !unavailable ? (
                  <Typography variant="body2">
                    {query.trim() === "" ? "No templates configured." : "No matching templates."}
                  </Typography>
                ) : null}
                <List
                  aria-label="Trust acquisition template library"
                  disablePadding
                  sx={{ maxHeight: { xs: 160, md: 400 }, overflow: "auto" }}
                >
                  {visible.map((recipe) => (
                    <Box component="li" key={recipe.id}>
                      <ListItemButton
                        disabled={busy}
                        selected={recipe.id === selected?.id}
                        onClick={() => navigate(() => open(recipe))}
                      >
                        <Box sx={{ minWidth: 0 }}>
                          <Typography variant="body2" sx={{ overflowWrap: "anywhere" }}>
                            {recipe.name}
                          </Typography>
                          <Typography variant="caption" color="text.secondary">
                            {recipe.kind.toUpperCase()} · {recipe.method.toUpperCase()} · Revision{" "}
                            {recipe.revision}
                          </Typography>
                        </Box>
                      </ListItemButton>
                    </Box>
                  ))}
                </List>
              </Stack>
              {draft === undefined ? (
                <Typography variant="body2" color="text.secondary">
                  Select a template to edit, or create one.
                </Typography>
              ) : (
                <Box
                  component="fieldset"
                  disabled={busy || unavailable}
                  sx={{ minWidth: 0, border: 0, p: 0, m: 0 }}
                >
                  <TrustRecipeFields
                    value={draft}
                    issue={issue}
                    onChange={(next) => {
                      setDraft(next);
                      setIssue(undefined);
                      setNotice(undefined);
                    }}
                  />
                  {selected === undefined ? null : (
                    <Typography variant="body2" color="text.secondary" sx={{ mt: 2 }}>
                      Export omits parameter defaults and profile credentials. Review literal
                      commands and URLs for embedded secrets before sharing.
                    </Typography>
                  )}
                  {selected === undefined ? null : (
                    <Stack direction="row" spacing={1} useFlexGap sx={{ mt: 2, flexWrap: "wrap" }}>
                      <Button
                        onClick={() =>
                          navigate(() => {
                            setSelected(undefined);
                            setDraft({
                              ...trustAcquisitionRecipeDefinition(selected),
                              name: `${selected.name} copy`,
                            });
                            setSaved("");
                            setNotice("Review the copy and save it with a unique name.");
                          })
                        }
                      >
                        Duplicate
                      </Button>
                      <Button
                        disabled={dirty}
                        onClick={() => {
                          void run(async () => {
                            const response = await execute({
                              command: "recipes.export",
                              payload: { id: selected.id, revision: selected.revision },
                              id: crypto.randomUUID(),
                              version: HOST_PROTOCOL_VERSION,
                            });
                            if (
                              !response.ok ||
                              !("document" in response.result) ||
                              !("warning" in response.result)
                            )
                              throw new Error("The host did not return a safe template export.");
                            if (!mounted.current) return;
                            const result = await transfer.download(response.result.document);
                            setNotice(
                              result === "cancelled"
                                ? "Export cancelled. No template changed."
                                : `Export ${result === "saved" ? "saved" : "started"}. ${response.result.warning}`,
                            );
                          });
                        }}
                      >
                        Export
                      </Button>
                      <Button
                        color="error"
                        disabled={dirty || busy}
                        onClick={() => {
                          void run(async () => {
                            const response = await execute({
                              command: "recipes.usage",
                              id: crypto.randomUUID(),
                              version: HOST_PROTOCOL_VERSION,
                              payload: { id: selected.id, revision: selected.revision },
                            });
                            if (!mounted.current) return;
                            if (!response.ok || !("usage" in response.result))
                              throw new Error(
                                "Template usage could not be verified. Retry before deleting.",
                              );
                            setUsage(response.result.usage);
                            setDeleting(true);
                          });
                        }}
                      >
                        Delete retrieval profile
                      </Button>
                    </Stack>
                  )}
                </Box>
              )}
            </Box>
          </Stack>
        </DialogContent>
        <DialogActions>
          <Button disabled={busy} onClick={() => navigate(onClose)}>
            Close retrieval profiles
          </Button>
          <Button
            variant="contained"
            disabled={busy || unavailable || draft === undefined || !dirty}
            onClick={() => {
              void save();
            }}
          >
            {busy ? "Working…" : "Save retrieval profile"}
          </Button>
        </DialogActions>
      </Dialog>
      <Dialog
        open={pendingNavigation !== undefined}
        aria-labelledby="discard-template-title"
        onClose={() => setPendingNavigation(undefined)}
      >
        <DialogTitle id="discard-template-title">Discard template changes?</DialogTitle>
        <DialogContent>
          Your unsaved template edits will be lost. Saved profiles and trust will not change.
        </DialogContent>
        <DialogActions>
          <Button autoFocus onClick={() => setPendingNavigation(undefined)}>
            Keep editing
          </Button>
          <Button
            onClick={() => {
              const action = pendingNavigation;
              setPendingNavigation(undefined);
              action?.();
            }}
          >
            Discard changes
          </Button>
        </DialogActions>
      </Dialog>
      <Dialog
        open={deleting}
        aria-labelledby="delete-template-title"
        onClose={busy ? undefined : (): void => setDeleting(false)}
      >
        <DialogTitle id="delete-template-title">
          Delete retrieval profile {selected?.name}?
        </DialogTitle>
        <DialogContent>
          Remove revision {selected?.revision} from the library? This cannot be undone. Existing
          stored Kafka trust will not be removed.
          {usage.length === 0 ? (
            <Typography>No profiles use this template.</Typography>
          ) : (
            <Box>
              <Typography>
                {usage.length} profiles retain their pinned recipe and existing trust:
              </Typography>
              <Box component="ul">
                {usage.map((profile) => (
                  <li key={profile.id}>{profile.name}</li>
                ))}
              </Box>
            </Box>
          )}
        </DialogContent>
        <DialogActions>
          <Button disabled={busy} onClick={() => setDeleting(false)}>
            Cancel
          </Button>
          <Button
            color="error"
            disabled={busy}
            onClick={() => {
              if (selected !== undefined)
                void run(async () => {
                  await execute({
                    command: "recipes.delete",
                    payload: {
                      id: selected.id,
                      revision: selected.revision,
                      confirmedProfileIds: usage.map((profile) => profile.id),
                    },
                    id: crypto.randomUUID(),
                    version: HOST_PROTOCOL_VERSION,
                  });
                  if (!mounted.current) return;
                  setDeleting(false);
                  setSelected(undefined);
                  setDraft(undefined);
                  setSaved("");
                  setNotice("Template deleted. Stored Kafka trust was not changed.");
                });
            }}
          >
            Confirm deletion
          </Button>
        </DialogActions>
      </Dialog>
    </>
  );
}
