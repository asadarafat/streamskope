import { useLayoutEffect, useRef, useState } from "react";
import { Box } from "@mui/material";

import {
  StudioAlert as Alert,
  StudioButton as Button,
  StudioDialog as Dialog,
  StudioDialogActions as DialogActions,
  StudioDialogContent as DialogContent,
  StudioDialogTitle as DialogTitle,
  StudioMenuItem as MenuItem,
  StudioSelect as Select,
} from "./controls";
import type {
  ProviderDeactivationResult,
  ProviderWorkspaceRegistration,
} from "./provider-workspaces";

interface Activation {
  readonly key: number;
  readonly workspace: ProviderWorkspaceRegistration;
  readonly isInteractive: () => boolean;
  interactive: boolean;
  retired: boolean;
}

interface FailedSelection {
  readonly targetId: string;
  readonly summary: string;
  readonly recovery: string;
}

export interface ProviderApplicationProperties {
  readonly workspaces: readonly ProviderWorkspaceRegistration[];
  readonly initialProviderId?: string;
}

/** Provider selection waits for confirmed cleanup while retaining the current workspace. */
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
  const mounted = useRef(true);
  const current = useRef<Activation | undefined>(undefined);
  const nextKey = useRef(0);
  const switching = useRef(false);
  const pending = useRef<Promise<void> | undefined>(undefined);
  const createActivation = (workspace: ProviderWorkspaceRegistration): Activation => {
    const activation: Activation = {
      key: ++nextKey.current,
      workspace,
      interactive: true,
      retired: false,
      isInteractive: (): boolean =>
        mounted.current &&
        current.current === activation &&
        activation.interactive &&
        !activation.retired,
    };
    return activation;
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
  const [failure, setFailure] = useState<FailedSelection>();
  const currentFailure = useRef<FailedSelection | undefined>(undefined);
  const publishFailure = (next: FailedSelection | undefined): void => {
    currentFailure.current = next;
    setFailure(next);
  };

  useLayoutEffect(() => {
    mounted.current = true;
    // React development effect replay is a new ownership lifetime, not renewed old authority.
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
    };
  }, []);

  const selectProvider = (targetId: string): Promise<void> => {
    const source = current.current;
    const target = registry.get(targetId);
    if (
      !mounted.current ||
      source === undefined ||
      source.retired ||
      target === undefined ||
      source.workspace.id === targetId
    ) {
      return Promise.resolve();
    }
    if (switching.current) return pending.current ?? Promise.resolve();
    // Close admission before React commits inert or a portalled callback can run again.
    switching.current = true;
    source.interactive = false;
    setBusy(true);
    publishFailure(undefined);
    const finish = (result: ProviderDeactivationResult): void => {
      if (!mounted.current || current.current !== source || source.retired) return;
      if (result.state === "blocked") {
        source.interactive = true;
        publishFailure({ targetId, summary: result.summary, recovery: result.recovery });
        return;
      }
      source.interactive = false;
      source.retired = true;
      const replacement = createActivation(target);
      current.current = replacement;
      setActivation(replacement);
    };
    const operation = Promise.resolve()
      .then(() => source.workspace.deactivate())
      .then(finish, () =>
        finish({
          state: "blocked",
          summary: "The messaging provider could not finish connection cleanup.",
          recovery: "Keep this workspace open, resolve its connection cleanup, then retry.",
        }),
      )
      .finally(() => {
        if (pending.current !== operation) return;
        pending.current = undefined;
        switching.current = false;
        if (mounted.current) setBusy(false);
      });
    pending.current = operation;
    return operation;
  };
  const providerControl = (
    <Select
      size="small"
      fullWidth={false}
      value={activation.workspace.id}
      disabled={busy}
      inputProps={{ "aria-label": "Messaging provider" }}
      onChange={(event): void => {
        if (!activation.isInteractive()) return;
        void selectProvider(event.target.value);
      }}
      sx={{ minWidth: 112 }}
    >
      {[...registry.values()].map((workspace) => (
        <MenuItem key={workspace.id} value={workspace.id}>
          {workspace.label}
        </MenuItem>
      ))}
    </Select>
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
        {activation.workspace.render({ providerControl, isInteractive: activation.isInteractive })}
      </Box>
      <Dialog
        open={failure !== undefined}
        onClose={(): void => {
          if (ownsFailure()) publishFailure(undefined);
        }}
        aria-labelledby="provider-switch-failure-title"
      >
        <DialogTitle id="provider-switch-failure-title">
          Unable to switch messaging provider
        </DialogTitle>
        <DialogContent>
          <Alert severity="error">
            {failure?.summary} {failure?.recovery}
          </Alert>
        </DialogContent>
        <DialogActions>
          <Button
            onClick={(): void => {
              if (ownsFailure()) publishFailure(undefined);
            }}
          >
            Keep working
          </Button>
          <Button
            onClick={(): void => {
              if (ownsFailure() && failure !== undefined) void selectProvider(failure.targetId);
            }}
          >
            Retry switch
          </Button>
        </DialogActions>
      </Dialog>
    </>
  );
}
