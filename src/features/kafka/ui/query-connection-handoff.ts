import type { ProviderConnectionDestination } from "../../../platform/ui/provider-workspaces";
import { parseKafkaInvestigationQuery, type KafkaInvestigationQuery } from "../contracts";

/** One reviewed settings intent; no host, messages, credentials or activation authority. */
export function createKafkaQueryConnectionHandoff(): {
  readonly prepare: (query: KafkaInvestigationQuery, profileId: string | undefined) => void;
  readonly disconnected: (destination: ProviderConnectionDestination | undefined) => void;
  readonly connected: (profileId: string) => void;
  readonly restored: () => KafkaInvestigationQuery | undefined;
} {
  let pending:
    { readonly query: KafkaInvestigationQuery; readonly profileId: string | undefined } | undefined;
  let restored: KafkaInvestigationQuery | undefined;
  return {
    prepare: (query, profileId): void => {
      pending = { query: parseKafkaInvestigationQuery(query), profileId };
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
    restored: (): KafkaInvestigationQuery | undefined => restored,
  };
}
