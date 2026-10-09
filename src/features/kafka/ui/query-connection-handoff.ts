import type { ProviderConnectionDestination } from "../../../platform/ui/provider-workspaces";

import { parseViewSettings, type KafkaViewSettings } from "./investigation-view-settings";

/** One reviewed settings intent; no host, messages, credentials or activation authority. */
export function createKafkaViewConnectionHandoff(): {
  readonly prepare: (query: KafkaViewSettings, profileId: string | undefined) => void;
  readonly disconnected: (destination: ProviderConnectionDestination | undefined) => void;
  readonly connected: (profileId: string) => void;
  readonly restored: () => KafkaViewSettings | undefined;
} {
  let pending:
    { readonly query: KafkaViewSettings; readonly profileId: string | undefined } | undefined;
  let restored: KafkaViewSettings | undefined;
  return {
    prepare: (query, profileId): void => {
      pending = { query: parseViewSettings(query), profileId };
      restored = undefined;
    },
    disconnected: (destination): void => {
      restored = undefined;
      if (
        pending !== undefined &&
        destination?.providerId === "kafka" &&
        destination.profile !== undefined &&
        (pending.profileId === undefined || pending.profileId === destination.profile.id)
      ) {
        pending = { ...pending, profileId: destination.profile.id };
      } else {
        pending = undefined;
      }
    },
    connected: (profileId): void => {
      restored = pending?.profileId === profileId ? pending.query : undefined;
      pending = undefined;
    },
    // A render cannot consume the snapshot: StrictMode may replay that activation.
    restored: (): KafkaViewSettings | undefined => restored,
  };
}
