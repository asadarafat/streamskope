import type { ReactNode } from "react";

export type ProviderDeactivationResult =
  | { readonly state: "ready" }
  | { readonly state: "blocked"; readonly summary: string; readonly recovery: string };

export interface ProviderWorkspaceControls {
  readonly providerControl: ReactNode;
  /** Authority belongs to this mount; a retired mount never becomes interactive again. */
  readonly isInteractive: () => boolean;
}

/** Trusted composition registers real workflows; providers retain their own typed hosts. */
export interface ProviderWorkspaceRegistration {
  readonly id: string;
  readonly label: string;
  readonly deactivate: () => Promise<ProviderDeactivationResult>;
  readonly render: (controls: ProviderWorkspaceControls) => ReactNode;
}
