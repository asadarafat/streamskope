import { useCallback, useEffect, useState } from "react";

import { HOST_PROTOCOL_VERSION, type ProfileSummary, type StreamSkopeHost } from "../contracts";

import type { ProfileAction, ProfileConnectionOperation } from "./ProfilePanel";

interface WorkbenchProfileAction {
  readonly action: ProfileAction;
  readonly profileId: string;
}

interface WorkbenchProfileController {
  readonly profileConnectionError: string | undefined;
  readonly profileConnectionOperation: ProfileConnectionOperation | null;
  readonly profileAction: WorkbenchProfileAction | null;
  readonly profileFilter: string;
  readonly selectedProfileId: string | null;
  readonly setProfileAction: (value: WorkbenchProfileAction | null) => void;
  readonly setProfileFilter: (value: string) => void;
  readonly setSelectedProfileId: (value: string | null) => void;
  readonly toggleProfileConnection: (profile: ProfileSummary) => Promise<void>;
}

export function useWorkbenchProfiles(
  host: StreamSkopeHost,
  connected: boolean,
  profiles: readonly ProfileSummary[],
): WorkbenchProfileController {
  const [profileConnectionError, setProfileConnectionError] = useState<string>();
  const [profileConnectionOperation, setProfileConnectionOperation] =
    useState<ProfileConnectionOperation | null>(null);
  const [profileAction, setProfileAction] = useState<WorkbenchProfileAction | null>(null);
  const [profileFilter, setProfileFilter] = useState("");
  const [selectedProfileId, setSelectedProfileId] = useState<string | null>(null);
  useEffect(() => {
    setSelectedProfileId((current) => {
      if (current !== null && profiles.some((profile) => profile.id === current)) {
        return current;
      }
      return profiles.find((profile) => profile.active)?.id ?? profiles[0]?.id ?? null;
    });
  }, [profiles]);

  const toggleProfileConnection = useCallback(
    async (profile: ProfileSummary): Promise<void> => {
      const profileConnected = connected && profile.active;
      const action = profileConnected ? "disconnect" : "connect";
      setProfileConnectionError(undefined);
      setProfileConnectionOperation({ action, profileId: profile.id });
      try {
        const response = await host.execute(
          profileConnected
            ? {
                command: "connection.disconnect",
                id: globalThis.crypto.randomUUID(),
                payload: {},
                version: HOST_PROTOCOL_VERSION,
              }
            : {
                command: "profiles.connect",
                id: globalThis.crypto.randomUUID(),
                payload: { profileId: profile.id },
                version: HOST_PROTOCOL_VERSION,
              },
        );
        if (!response.ok) {
          setProfileConnectionError(
            `${action === "connect" ? "Connect" : "Disconnect"} ${profile.name}: ${response.error.summary} ${response.error.recovery}`,
          );
        }
      } catch {
        setProfileConnectionError(
          `${action === "connect" ? "Connect" : "Disconnect"} ${profile.name}: The application host did not accept the request. Open Activity for diagnostics.`,
        );
      } finally {
        setProfileConnectionOperation(null);
      }
    },
    [connected, host],
  );

  return {
    profileConnectionError,
    profileConnectionOperation,
    profileAction,
    profileFilter,
    selectedProfileId,
    setProfileAction,
    setProfileFilter,
    setSelectedProfileId,
    toggleProfileConnection,
  };
}
