import type { ReactNode } from "react";

export type ProviderDeactivationResult =
  | { readonly state: "ready" }
  | { readonly state: "blocked"; readonly summary: string; readonly recovery: string };

export interface ProviderWorkspaceControls {
  readonly profilesPage: ReactNode;
  /** Authority belongs to this mount; a retired mount never becomes interactive again. */
  readonly isInteractive: () => boolean;
}

export interface ProviderProfileReference {
  readonly id: string;
  readonly revision?: number;
}

/** Safe display projection only; credentials and protocol configuration stay with the provider. */
export interface ProviderProfileSummary extends ProviderProfileReference {
  readonly name: string;
  readonly endpoints: readonly string[];
  readonly authentication: string;
  readonly transport: string;
  readonly source: string;
  readonly active: boolean;
  readonly actions?: readonly {
    readonly id: string;
    readonly label: string;
    readonly available: boolean;
  }[];
}

export type ProviderConnectionOutcome =
  | { readonly ok: true }
  | { readonly ok: false; readonly summary: string; readonly recovery: string };

export interface ProviderProfileCreationAction {
  readonly id: string;
  readonly label: string;
  readonly description: string;
  readonly kind: "direct" | "connector" | "manage";
  readonly available: boolean;
}

export interface ProviderProfilesSnapshot {
  readonly profiles: readonly ProviderProfileSummary[];
  readonly loading: boolean;
  readonly available: boolean;
  readonly storageReady: boolean;
  readonly storageLabel: string;
  readonly failure: { readonly summary: string; readonly recovery: string } | null;
  readonly creationActions: readonly ProviderProfileCreationAction[];
}

export type ProviderProfileManagementAction =
  | { readonly kind: "create"; readonly actionId: string }
  | { readonly kind: "inspect" | "edit" | "delete"; readonly profileId: string }
  | { readonly kind: "provider"; readonly profileId: string; readonly actionId: string }
  | null;

export interface ProviderProfileManagementControls {
  readonly action: ProviderProfileManagementAction;
  /** A closed view cannot submit new work; actual receipts of admitted work remain valid. */
  readonly isInteractive: () => boolean;
  readonly onClose: () => void;
  readonly onProfileReady: (profileId: string) => void;
  readonly onConnect: (profile: ProviderProfileReference) => Promise<ProviderConnectionOutcome>;
}

/** Profile management outlives workspace activation, and never receives stream authority. */
export interface ProviderProfilesFacet {
  /** Stable until relevant control/profile state changes. */
  readonly getSnapshot: () => ProviderProfilesSnapshot;
  readonly subscribe: (listener: () => void) => () => void;
  readonly refresh: () => Promise<void>;
  readonly connect: (profile: ProviderProfileReference) => Promise<ProviderConnectionOutcome>;
  readonly renderManagement: (controls: ProviderProfileManagementControls) => ReactNode;
}

/** Trusted composition registers real workflows; providers retain their own typed hosts. */
export interface ProviderWorkspaceRegistration {
  readonly id: string;
  readonly label: string;
  readonly profiles: ProviderProfilesFacet;
  readonly deactivate: () => Promise<ProviderDeactivationResult>;
  readonly render: (controls: ProviderWorkspaceControls) => ReactNode;
}
