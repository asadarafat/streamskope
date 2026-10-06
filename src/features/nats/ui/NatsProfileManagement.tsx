import { useId, useLayoutEffect, useMemo, useRef, useState, useSyncExternalStore } from "react";
import Box from "@mui/material/Box";
import Stack from "@mui/material/Stack";
import Typography from "@mui/material/Typography";

import type {
  ProviderProfileManagementAction,
  ProviderProfileManagementControls,
} from "../../../platform/ui/provider-workspaces";
import {
  StudioAlert as Alert,
  StudioButton as Button,
  StudioDialog as Dialog,
  StudioDialogActions as DialogActions,
  StudioDialogContent as DialogContent,
  StudioDialogTitle as DialogTitle,
} from "../../../platform/ui/controls";
import type { NatsProfileSummary } from "../contracts";

import { ProfileEditor } from "./ProfileEditor";
import { captureNatsProfile, natsProfileStorageLabel } from "./profile-presentation";
import type { NatsProfileManagementStore } from "./profiles-facet";
import type { NatsWorkspaceSnapshot } from "./workspace-state";

type Action = Exclude<ProviderProfileManagementAction, null>;

/** Only a selected safe summary and profile mutation callbacks enter this dialog. */
function ProfileAction({
  action,
  controls,
  state,
  store,
}: {
  readonly action: Action;
  readonly controls: ProviderProfileManagementControls;
  readonly state: NatsWorkspaceSnapshot;
  readonly store: NatsProfileManagementStore;
}): React.JSX.Element {
  const [capturedProfile] = useState<NatsProfileSummary | null>(() => {
    if (action.kind === "create") return null;
    const selected = state.profiles?.profiles.find((entry) => entry.id === action.profileId);
    return selected === undefined ? null : captureNatsProfile(selected);
  });
  const profile =
    action.kind === "inspect"
      ? (state.profiles?.profiles.find((entry) => entry.id === action.profileId) ?? null)
      : capturedProfile;
  const [pending, setPending] = useState(false);
  const [failure, setFailure] = useState<string | null>(null);
  const title = useId();
  const storageReady = state.profiles?.capability.state === "ready";
  const inUse =
    profile !== null &&
    state.connection.state !== "disconnected" &&
    state.connection.profile?.id === profile.id;
  const available = state.available && storageReady;
  const disabled = !available || pending || !controls.isInteractive();
  const safeFailure = state.failure ?? state.connection.failure ?? null;

  if (action.kind === "edit" && inUse) {
    return (
      <Dialog aria-labelledby={title} onClose={controls.onClose} open>
        <DialogTitle id={title}>Edit NATS profile</DialogTitle>
        <DialogContent>
          <Alert severity="warning">
            Disconnect this NATS profile before editing its settings or credentials.
          </Alert>
        </DialogContent>
        <DialogActions>
          <Button onClick={controls.onClose}>Close</Button>
        </DialogActions>
      </Dialog>
    );
  }

  if (
    (action.kind === "create" && action.actionId === "new") ||
    (action.kind === "edit" && profile !== null)
  ) {
    return (
      <ProfileEditor
        available={available && !inUse}
        failure={safeFailure}
        isInteractive={controls.isInteractive}
        onClose={controls.onClose}
        onCreate={async (input): Promise<boolean> => {
          if (!controls.isInteractive() || !available || action.kind !== "create") return false;
          const owner = store.owner();
          if (owner === undefined) return false;
          const before = new Set(store.getSnapshot().profiles?.profiles.map((entry) => entry.id));
          const accepted = await owner.createProfile(input);
          if (accepted) {
            const created = store
              .getSnapshot()
              .profiles?.profiles.find(
                (entry) => !before.has(entry.id) && entry.name === input.name,
              );
            if (created !== undefined) controls.onProfileReady(created.id);
          }
          return accepted;
        }}
        onUpdate={async (captured, input): Promise<boolean> => {
          if (!controls.isInteractive() || !available || inUse || action.kind !== "edit")
            return false;
          const accepted = await store.owner()?.updateProfile(captured, input);
          if (accepted) controls.onProfileReady(captured.id);
          return accepted === true;
        }}
        profile={profile}
      />
    );
  }

  async function remove(): Promise<void> {
    if (profile === null || disabled || inUse || !controls.isInteractive()) return;
    setPending(true);
    setFailure(null);
    try {
      const accepted = await store.owner()?.deleteProfile(profile);
      if (!controls.isInteractive()) return;
      if (accepted) controls.onClose();
      else setFailure("The NATS host did not delete this profile.");
    } finally {
      if (controls.isInteractive()) setPending(false);
    }
  }
  async function connect(): Promise<void> {
    if (profile === null || disabled || !controls.isInteractive()) return;
    setPending(true);
    setFailure(null);
    try {
      // The application coordinator confirms the current connection's cleanup first.
      const outcome = await controls.onConnect({ id: profile.id, revision: profile.revision });
      if (!controls.isInteractive()) return;
      if (outcome.ok) controls.onClose();
      else setFailure(`${outcome.summary} ${outcome.recovery}`);
    } catch {
      if (controls.isInteractive())
        setFailure(
          "The connection request could not be completed. Refresh profiles before retrying.",
        );
    } finally {
      if (controls.isInteractive()) setPending(false);
    }
  }

  if (action.kind === "inspect") {
    return (
      <Box aria-label="NATS profile details" component="section" sx={{ p: 2, minWidth: 0 }}>
        <Stack spacing={1.5}>
          {profile === null ? (
            <Alert severity="warning">
              This NATS profile is no longer available. Refresh profiles and select it again.
            </Alert>
          ) : (
            <>
              <Typography component="h2" variant="h6">
                {profile.name}
              </Typography>
              <Typography sx={{ overflowWrap: "anywhere" }} variant="body2">
                Servers: {profile.servers.join(", ")}
              </Typography>
              <Typography variant="body2">
                Authentication: {profile.authentication.mode === "token" ? "Token" : "None"}
              </Typography>
              <Typography variant="body2">
                Transport: {profile.tls.mode === "tls" ? "Verified TLS" : "Plaintext"}
              </Typography>
              <Typography color="text.secondary" variant="body2">
                Live subscriptions only. JetStream history and management are unavailable.
              </Typography>
              <Typography color="text.secondary" variant="body2">
                {natsProfileStorageLabel(state.profiles?.capability)}
              </Typography>
            </>
          )}
          {failure === null ? null : <Alert severity="error">{failure}</Alert>}
          <Stack direction="row" spacing={1}>
            <Button disabled={pending} onClick={controls.onClose}>
              Close details
            </Button>
            <Button
              disabled={disabled || inUse || profile === null}
              onClick={() => void connect()}
              variant="contained"
            >
              {pending ? "Connecting…" : "Connect"}
            </Button>
          </Stack>
        </Stack>
      </Box>
    );
  }

  return (
    <Dialog
      aria-labelledby={title}
      maxWidth="sm"
      onClose={pending ? undefined : controls.onClose}
      open
    >
      <DialogTitle id={title}>
        {action.kind === "delete" ? "Delete NATS profile" : "NATS connection profile"}
      </DialogTitle>
      <DialogContent>
        <Stack spacing={1.5}>
          {profile === null ? (
            <Alert severity="warning">
              This NATS profile is no longer available. Refresh profiles and select it again.
            </Alert>
          ) : action.kind === "delete" ? (
            <Typography variant="body2">
              Delete profile <strong>{profile.name}</strong> and its host-held credentials? This
              does not stop or remove the external NATS server.
            </Typography>
          ) : null}
          {inUse && action.kind === "delete" ? (
            <Alert severity="warning">Disconnect the profile before deleting it.</Alert>
          ) : null}
          {failure !== null ? (
            <Alert severity="error">
              {safeFailure?.summary ?? failure} {safeFailure?.recovery}
            </Alert>
          ) : null}
        </Stack>
      </DialogContent>
      <DialogActions>
        <Button disabled={pending} onClick={controls.onClose}>
          {action.kind === "delete" ? "Cancel" : "Close"}
        </Button>
        {profile === null ? null : action.kind === "delete" ? (
          <Button
            color="error"
            disabled={disabled || inUse}
            onClick={() => void remove()}
            variant="contained"
          >
            {pending ? "Deleting profile…" : "Delete profile"}
          </Button>
        ) : (
          <Button disabled={disabled || inUse} onClick={() => void connect()} variant="contained">
            {pending ? "Connecting…" : "Connect"}
          </Button>
        )}
      </DialogActions>
    </Dialog>
  );
}

/** Mounted with no action so catalog observation never depends on an open editor. */
export function NatsProfileManagement({
  store,
  controls,
}: {
  readonly store: NatsProfileManagementStore;
  readonly controls: ProviderProfileManagementControls;
}): React.JSX.Element | null {
  const state = useSyncExternalStore(store.subscribe, store.getSnapshot, store.getSnapshot);
  const current = useRef(controls);
  current.current = controls;
  const mounted = useRef(true);
  useLayoutEffect(() => {
    mounted.current = true;
    return (): void => {
      mounted.current = false;
    };
  }, []);
  const action = controls.action;
  const ownsAction = useMemo(
    () => (): boolean =>
      mounted.current && current.current.action === action && current.current.isInteractive(),
    [action],
  );
  const ownedControls: ProviderProfileManagementControls = {
    ...controls,
    isInteractive: ownsAction,
    onClose: (): void => {
      if (ownsAction()) controls.onClose();
    },
    onProfileReady: (profileId): void => {
      if (ownsAction()) controls.onProfileReady(profileId);
    },
    onConnect: (profile) =>
      ownsAction()
        ? controls.onConnect(profile)
        : Promise.resolve({
            ok: false,
            summary: "This NATS profile management view is no longer active.",
            recovery: "Select the profile again before connecting.",
          }),
  };
  if (action === null) return null;
  const key =
    action.kind === "create" ? `create:${action.actionId}` : `${action.kind}:${action.profileId}`;
  return (
    <ProfileAction key={key} action={action} controls={ownedControls} state={state} store={store} />
  );
}
