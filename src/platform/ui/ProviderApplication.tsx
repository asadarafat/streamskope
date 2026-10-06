import { useLayoutEffect, useRef, useState, useSyncExternalStore } from "react";
import { Box } from "@mui/material";

import {
  StudioAlert as Alert,
  StudioButton as Button,
  StudioDialog as Dialog,
  StudioDialogActions as DialogActions,
  StudioDialogContent as DialogContent,
  StudioDialogTitle as DialogTitle,
} from "./controls";
import {
  ConnectionProfilesCatalog,
  catalogProfileId,
  type CatalogProfile,
} from "./ConnectionProfilesCatalog";
import type {
  ProviderConnectionOutcome,
  ProviderProfileManagementAction,
  ProviderProfileReference,
  ProviderProfilesSnapshot,
  ProviderWorkspaceRegistration,
} from "./provider-workspaces";

interface Activation {
  readonly key: number;
  readonly workspace: ProviderWorkspaceRegistration;
  readonly isInteractive: () => boolean;
  interactive: boolean;
  retired: boolean;
}

type ConnectionRequest =
  | {
      readonly kind: "connect";
      readonly providerId: string;
      readonly profile: ProviderProfileReference;
    }
  | { readonly kind: "disconnect" };
interface FailedConnection {
  readonly request: ConnectionRequest;
  readonly summary: string;
  readonly recovery: string;
}
interface ManagementOwner {
  readonly providerId: string;
  readonly action: ProviderProfileManagementAction;
}

export interface ProviderApplicationProperties {
  readonly workspaces: readonly ProviderWorkspaceRegistration[];
  readonly initialProviderId?: string;
}

function createCatalogStore(workspaces: readonly ProviderWorkspaceRegistration[]): {
  readonly getSnapshot: () => readonly ProviderProfilesSnapshot[];
  readonly subscribe: (listener: () => void) => () => void;
} {
  let snapshot = workspaces.map((workspace) => workspace.profiles.getSnapshot());
  return {
    getSnapshot: (): readonly ProviderProfilesSnapshot[] => {
      const next = workspaces.map((workspace) => workspace.profiles.getSnapshot());
      if (next.some((item, index) => item !== snapshot[index])) snapshot = next;
      return snapshot;
    },
    subscribe: (listener) => {
      const subscriptions = workspaces.map((workspace) => workspace.profiles.subscribe(listener));
      return (): void => {
        for (const unsubscribe of subscriptions) unsubscribe();
      };
    },
  };
}

/** The catalog is provider-neutral; only Connect replaces the active session. */
export function ProviderApplication({
  workspaces,
  initialProviderId,
}: ProviderApplicationProperties): React.JSX.Element {
  const [registry] = useState(() => {
    const registered = new Map<string, ProviderWorkspaceRegistration>();
    for (const workspace of workspaces) {
      if (!/^[a-z][a-z0-9-]{0,31}$/u.test(workspace.id) || registered.has(workspace.id)) {
        throw new Error("Messaging workspaces must have distinct valid provider IDs.");
      }
      registered.set(workspace.id, workspace);
    }
    if (registered.size === 0) throw new Error("At least one messaging workspace is required.");
    return registered;
  });
  if (
    workspaces.length !== registry.size ||
    workspaces.some((workspace) => registry.get(workspace.id) !== workspace)
  ) {
    throw new Error("Messaging workspace registration is fixed for this application lifetime.");
  }
  const [store] = useState(() => createCatalogStore(workspaces));
  const snapshots = useSyncExternalStore(store.subscribe, store.getSnapshot);
  const mounted = useRef(true);
  const current = useRef<Activation | undefined>(undefined);
  const nextKey = useRef(0);
  const switching = useRef(false);
  const pending = useRef<Promise<ProviderConnectionOutcome> | undefined>(undefined);
  const createActivation = (workspace: ProviderWorkspaceRegistration): Activation => {
    const next: Activation = {
      key: ++nextKey.current,
      workspace,
      interactive: true,
      retired: false,
      isInteractive: () =>
        mounted.current && current.current === next && next.interactive && !next.retired,
    };
    return next;
  };
  const [activation, setActivation] = useState(() => {
    const initial =
      initialProviderId === undefined ? workspaces[0] : registry.get(initialProviderId);
    if (initial === undefined)
      throw new Error("The initial messaging workspace is not registered.");
    return createActivation(initial);
  });
  current.current ??= activation;
  const [busy, setBusy] = useState(false);
  const [filter, setFilter] = useState("");
  const [selection, setSelection] = useState<{ providerId: string; profileId: string } | null>(
    null,
  );
  const selected = useRef(selection);
  const publishSelection = (next: typeof selection): void => {
    selected.current = next;
    setSelection(next);
  };
  const [management, setManagement] = useState<ManagementOwner | null>(null);
  const managementOwner = useRef<ManagementOwner | null>(null);
  const [failure, setFailure] = useState<FailedConnection>();
  const currentFailure = useRef<FailedConnection | undefined>(undefined);
  const publishFailure = (next: FailedConnection | undefined): void => {
    currentFailure.current = next;
    setFailure(next);
  };
  const publishManagement = (next: ManagementOwner | null): void => {
    managementOwner.current = next;
    setManagement(next);
  };
  const inactive: ProviderConnectionOutcome = {
    ok: false,
    summary: "This connection action is no longer active.",
    recovery: "Select the profile again and retry.",
  };

  useLayoutEffect(() => {
    mounted.current = true;
    // StrictMode replay starts a new ownership lifetime; old callbacks never regain authority.
    if (current.current?.retired === true) {
      const replacement = createActivation(current.current.workspace);
      current.current = replacement;
      setActivation(replacement);
    }
    return (): void => {
      mounted.current = false;
      const owned = current.current;
      if (owned !== undefined) {
        owned.interactive = false;
        owned.retired = true;
      }
      managementOwner.current = null;
    };
  }, []);

  const runConnection = (request: ConnectionRequest): Promise<ProviderConnectionOutcome> => {
    if (switching.current) return pending.current ?? Promise.resolve(inactive);
    const source = current.current;
    const target =
      request.kind === "connect" ? registry.get(request.providerId) : source?.workspace;
    if (!mounted.current || source === undefined || source.retired || target === undefined)
      return Promise.resolve(inactive);
    // Close admission synchronously, including portalled, keyboard and native callbacks.
    switching.current = true;
    source.interactive = false;
    setBusy(true);
    publishFailure(undefined);
    const fail = (
      outcome: Extract<ProviderConnectionOutcome, { ok: false }>,
    ): ProviderConnectionOutcome => {
      if (mounted.current)
        publishFailure({ request, summary: outcome.summary, recovery: outcome.recovery });
      return outcome;
    };
    const operation = Promise.resolve()
      .then(async (): Promise<ProviderConnectionOutcome> => {
        const cleanup = await source.workspace.deactivate().catch(() => ({
          state: "blocked" as const,
          summary: "The messaging provider could not finish connection cleanup.",
          recovery: "Keep this workspace open, resolve its connection cleanup, then retry.",
        }));
        if (!mounted.current || current.current !== source || source.retired) return inactive;
        if (cleanup.state === "blocked") {
          source.interactive = true;
          return fail({ ok: false, summary: cleanup.summary, recovery: cleanup.recovery });
        }
        source.retired = true;
        const outcome =
          request.kind === "disconnect"
            ? { ok: true as const }
            : await target.profiles.connect(request.profile).catch(() => ({
                ok: false as const,
                summary: "The selected profile could not connect.",
                recovery: "Inspect its connection settings, test it, then retry Connect.",
              }));
        if (!mounted.current || current.current !== source) return outcome;
        // Show the destination even on connect failure, so its own recovery remains available.
        const replacement = createActivation(target);
        current.current = replacement;
        setActivation(replacement);
        if (request.kind === "connect") {
          publishSelection({ providerId: target.id, profileId: request.profile.id });
          publishManagement({
            providerId: target.id,
            action: { kind: "inspect", profileId: request.profile.id },
          });
        }
        return outcome.ok ? outcome : fail(outcome);
      })
      .finally(() => {
        if (pending.current !== operation) return;
        pending.current = undefined;
        switching.current = false;
        if (mounted.current) setBusy(false);
      });
    pending.current = operation;
    return operation;
  };
  const refresh = (): void => {
    if (!activation.isInteractive()) return;
    // A failed provider store cannot prevent its sibling inventory from refreshing.
    void Promise.allSettled(workspaces.map((workspace) => workspace.profiles.refresh()));
  };
  const inspect = (providerId: string, profileId: string): void => {
    publishSelection({ providerId, profileId });
    publishManagement({ providerId, action: { kind: "inspect", profileId } });
  };
  const managementViews = workspaces.map((workspace) => {
    const owner = management?.providerId === workspace.id ? management : null;
    const owns = (): boolean =>
      mounted.current && !switching.current && owner !== null && managementOwner.current === owner;
    return (
      <Box key={workspace.id}>
        {workspace.profiles.renderManagement({
          action: owner?.action ?? null,
          isInteractive: owns,
          onClose: () => {
            if (!owns()) return;
            if (selected.current?.providerId === workspace.id && owner?.action?.kind !== "inspect")
              inspect(workspace.id, selected.current.profileId);
            else {
              if (owner?.action?.kind === "inspect") publishSelection(null);
              publishManagement(null);
            }
          },
          onProfileReady: (profileId) => {
            if (!owns()) return;
            publishSelection({ providerId: workspace.id, profileId });
            setFilter("");
            void workspace.profiles.refresh();
          },
          onConnect: (profile) =>
            owns()
              ? runConnection({ kind: "connect", providerId: workspace.id, profile })
              : Promise.resolve(inactive),
        })}
      </Box>
    );
  });
  const profilesPage = (
    <ConnectionProfilesCatalog
      workspaces={workspaces}
      snapshots={snapshots}
      filter={filter}
      busy={busy}
      selectedId={
        selection === null ? null : catalogProfileId(selection.providerId, selection.profileId)
      }
      management={managementViews}
      onFilter={(value) => {
        if (activation.isInteractive()) setFilter(value);
      }}
      onSelect={(profile) => {
        if (activation.isInteractive()) inspect(profile.providerId, profile.profileId);
      }}
      onAction={(profile, action) => {
        if (!activation.isInteractive()) return;
        publishSelection({ providerId: profile.providerId, profileId: profile.profileId });
        publishManagement({
          providerId: profile.providerId,
          action: { kind: action, profileId: profile.profileId },
        });
      }}
      onCreate={(providerId, actionId) => {
        if (activation.isInteractive())
          publishManagement({ providerId, action: { kind: "create", actionId } });
      }}
      onProviderAction={(profile, actionId) => {
        if (
          !activation.isInteractive() ||
          !profile.actions?.some((action) => action.id === actionId && action.available)
        )
          return;
        publishSelection({ providerId: profile.providerId, profileId: profile.profileId });
        publishManagement({
          providerId: profile.providerId,
          action: { kind: "provider", profileId: profile.profileId, actionId },
        });
      }}
      onConnect={(profile: CatalogProfile) => {
        if (activation.isInteractive())
          void runConnection({
            kind: "connect",
            providerId: profile.providerId,
            profile: {
              id: profile.profileId,
              ...(profile.revision === undefined ? {} : { revision: profile.revision }),
            },
          });
      }}
      onDisconnect={() => {
        if (activation.isInteractive()) void runConnection({ kind: "disconnect" });
      }}
      onRefresh={refresh}
    />
  );
  const ownsFailure = (): boolean =>
    activation.isInteractive() && failure !== undefined && currentFailure.current === failure;
  return (
    <>
      <Box
        key={activation.key}
        data-testid="provider-workspace"
        inert={busy}
        aria-busy={busy}
        sx={{ minWidth: 0, minHeight: 0 }}
      >
        {activation.workspace.render({ profilesPage, isInteractive: activation.isInteractive })}
      </Box>
      <Dialog
        open={failure !== undefined}
        onClose={() => {
          if (ownsFailure()) publishFailure(undefined);
        }}
        aria-labelledby="connection-handoff-failure-title"
      >
        <DialogTitle id="connection-handoff-failure-title">
          Unable to complete connection change
        </DialogTitle>
        <DialogContent>
          <Alert severity="error">
            {failure?.summary} {failure?.recovery}
          </Alert>
        </DialogContent>
        <DialogActions>
          <Button
            onClick={() => {
              if (ownsFailure()) publishFailure(undefined);
            }}
          >
            Keep working
          </Button>
          <Button
            onClick={() => {
              if (ownsFailure() && failure !== undefined) void runConnection(failure.request);
            }}
          >
            Retry connection
          </Button>
        </DialogActions>
      </Dialog>
    </>
  );
}
