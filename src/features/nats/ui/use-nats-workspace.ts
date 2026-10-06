import { useEffect, useMemo, useRef, useState } from "react";

import type { NatsHost } from "../contracts";

import {
  createNatsWorkspaceOwner,
  type NatsWorkspaceActions,
  type NatsWorkspaceOwner,
} from "./workspace-controller";
import { initialNatsWorkspaceSnapshot, type NatsWorkspaceSnapshot } from "./workspace-state";

export type NatsWorkspaceController = NatsWorkspaceSnapshot & NatsWorkspaceActions;
const alwaysInteractive = (): boolean => true;
export function useNatsWorkspace({
  host,
  isInteractive = alwaysInteractive,
  unavailableRecovery,
}: {
  readonly host: NatsHost | null;
  readonly isInteractive?: (() => boolean) | undefined;
  readonly unavailableRecovery?: string | undefined;
}): NatsWorkspaceController {
  const [snapshot, setSnapshot] = useState(() => initialNatsWorkspaceSnapshot(unavailableRecovery));
  const current = useRef<NatsWorkspaceOwner | null>(null);
  useEffect(() => {
    if (host === null) {
      setSnapshot(
        initialNatsWorkspaceSnapshot(
          unavailableRecovery ?? "Use a desktop build that includes the NATS provider.",
        ),
      );
      return;
    }
    const owner = createNatsWorkspaceOwner(host, isInteractive);
    current.current = owner;
    const unsubscribe = owner.subscribe(setSnapshot);
    setSnapshot(owner.snapshot());
    owner.start();
    return (): void => {
      current.current = null;
      unsubscribe();
      owner.dispose();
    };
  }, [host, isInteractive, unavailableRecovery]);
  const actions = useMemo<NatsWorkspaceActions>(
    () => ({
      createProfile: (input): Promise<boolean> =>
        current.current?.createProfile(input) ?? Promise.resolve(false),
      updateProfile: (profile, input): Promise<boolean> =>
        current.current?.updateProfile(profile, input) ?? Promise.resolve(false),
      deleteProfile: (profile): Promise<boolean> =>
        current.current?.deleteProfile(profile) ?? Promise.resolve(false),
      connectProfile: (profile): Promise<boolean> =>
        current.current?.connectProfile(profile) ?? Promise.resolve(false),
      disconnect: (): Promise<boolean> => current.current?.disconnect() ?? Promise.resolve(false),
      startSubscription: (subject): Promise<boolean> =>
        current.current?.startSubscription(subject) ?? Promise.resolve(false),
      stopSubscription: (): Promise<boolean> =>
        current.current?.stopSubscription() ?? Promise.resolve(false),
      refresh: (): Promise<boolean> => current.current?.refresh() ?? Promise.resolve(false),
      selectRecord: (id): void => current.current?.selectRecord(id),
      clearFailure: (): void => current.current?.clearFailure(),
    }),
    [],
  );
  return { ...snapshot, ...actions };
}
