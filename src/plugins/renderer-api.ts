import type { ProfileSummary, StreamSkopeHost } from "../features/kafka/contracts";

import type { PluginApiVersion, PluginProfileSource } from "./contracts";

interface PluginViewContextBase {
  readonly host: StreamSkopeHost;
  readonly profiles: readonly ProfileSummary[];
  readonly themeMode?: "light" | "dark";
  readonly onClose: () => void;
  readonly onProfileReady: (profileId: string) => void;
  readonly onExistingDestination: (value: {
    readonly name: string;
    readonly brokers: readonly string[];
  }) => void;
}

export type PluginViewContext = PluginViewContextBase &
  (
    | { readonly view: "connection"; readonly actionId: string }
    | { readonly view: "profile"; readonly profile: ProfileSummary }
  );

export interface PluginViewMount {
  update(context: PluginViewContext): void;
  dispose(): void;
}

export interface PluginRenderer {
  readonly apiVersion: PluginApiVersion;
  readonly id: string;
  readonly connectionActions: readonly { readonly id: string; readonly label: string }[];
  profileLabel?(source: PluginProfileSource): string | undefined;
  profileSummary?(source: PluginProfileSource): string | undefined;
  mount(element: HTMLElement, context: PluginViewContext): PluginViewMount;
}

export type PluginRendererImporter = (url: string) => Promise<{ readonly default: PluginRenderer }>;
