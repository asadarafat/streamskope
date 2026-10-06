export const PLUGIN_API_VERSION = 4 as const;
export type PluginApiVersion = 2 | 3 | typeof PLUGIN_API_VERSION;

export interface PluginCompatibility {
  readonly streamskope: {
    readonly minimum: string;
    /** Required for API 4; legacy API 3 declares only its minimum desktop build. */
    readonly maximumExclusive?: string;
  };
  readonly target: {
    readonly system: string;
    readonly minimum: string;
    readonly maximum: string;
  };
}

export interface PluginResource {
  readonly path: string;
  readonly sha256: string;
}

export type JsonValue = null | boolean | number | string | JsonObject | readonly JsonValue[];
export interface JsonObject {
  readonly [key: string]: JsonValue;
}

export interface PluginProfileSource {
  readonly kind: "plugin";
  readonly pluginId: string;
  readonly version: 1;
  readonly data: JsonObject;
}

export interface PluginManifest {
  readonly id: string;
  readonly name: string;
  readonly description?: string;
  readonly version: string;
  readonly targetEdaVersion?: string;
  readonly apiVersion: PluginApiVersion;
  /** Required for API 3/4; absent from legacy API 2 packages. */
  readonly compatibility?: PluginCompatibility;
  /** Legacy API 3 identity only. API 4 uses independent Semantic Versions. */
  readonly revision?: number;
  readonly resources?: readonly PluginResource[];
  readonly backend: "backend.cjs";
  readonly renderer: "renderer.js";
  readonly styles?: "renderer.css";
}

export interface PluginInstallation {
  readonly id: string;
  readonly activationId?: string;
  readonly installed?: PluginManifest;
  readonly active?: PluginManifest;
  readonly previous?: PluginManifest;
  readonly pending: "install" | "remove" | null;
  readonly restartRequired: boolean;
  readonly error?: string;
  readonly rendererUrl?: string;
  readonly stylesUrl?: string;
}

export interface PluginSnapshot {
  readonly revision: number;
  readonly plugins: readonly PluginInstallation[];
  readonly error?: string;
}

export type PluginChangeOperation = "install" | "remove" | "retry";

export interface PluginChangeWarning {
  readonly message: string;
  readonly detail: string;
  readonly stateKey?: string;
}

export interface PluginChangePrompt extends PluginChangeWarning {
  readonly pluginId: string;
  readonly token: string;
  readonly title: string;
  readonly confirmLabel: string;
}

export interface PluginCatalogSnapshot {
  readonly plugins: readonly PluginManifest[];
  /** Exact host-known packages; a renderer cannot select a download URL or different bytes. */
  readonly packages?: readonly PluginPackageReference[];
  readonly source?: "live" | "cache" | "unavailable";
  /** Last successful remote check; cached entries never imply current availability. */
  readonly checkedAt?: string;
  readonly error?: string;
}

export interface PluginPackageReference {
  readonly pluginId: string;
  readonly version: string;
  readonly sha256: string;
}

export type PluginPackageInspectInput =
  { readonly source: "file" } | (PluginPackageReference & { readonly source: "catalog" | "cache" });

export type PluginPackageTrust = "publisher" | "official" | "development";

export interface PluginPackagePublisher {
  readonly keyId: string;
  readonly name: string;
}

export interface PluginPackageInspection {
  readonly candidateId: string;
  readonly manifest: PluginManifest;
  readonly sha256: string;
  readonly source: "file" | "catalog" | "cache";
  readonly publisher?: PluginPackagePublisher;
  readonly trust: PluginPackageTrust;
  readonly expiresAt: string;
  readonly installedVersion?: string;
  readonly status: "install" | "update" | "already-installed" | "blocked";
  readonly reason?: string;
}

export interface PluginCachedPackage {
  readonly manifest: PluginManifest;
  readonly sha256: string;
  readonly cachedAt: string;
  readonly publisher?: PluginPackagePublisher;
  readonly trust: PluginPackageTrust;
}

export interface PluginDeliverySnapshot {
  readonly fileInstallationAvailable: boolean;
  readonly cachedPackages: readonly PluginCachedPackage[];
}

export interface PluginNetworkConfiguration {
  readonly mode: "system" | "custom";
  readonly offline: boolean;
  /** Remembered HTTP/HTTPS proxy origin, without credentials, path, query or fragment. */
  readonly proxyUrl: string | null;
}

export type PluginProxyCredentialsChange =
  | { readonly action: "unchanged" }
  | { readonly action: "clear" }
  | { readonly action: "replace"; readonly username: string; readonly password: string };

export interface PluginNetworkUpdateInput {
  readonly configuration: PluginNetworkConfiguration;
  /** Credentials are write-only and are scoped to the remembered proxy origin. */
  readonly credentials: PluginProxyCredentialsChange;
}

export interface PluginNetworkSnapshot {
  readonly revision: number;
  /** Null means saved settings cannot be read safely; remote acquisition remains blocked. */
  readonly configuration: PluginNetworkConfiguration | null;
  readonly credentialsConfigured: boolean;
  readonly credentialStorage: "encrypted" | "session" | "unavailable";
  readonly nativeAvailable: boolean;
  readonly supportedProxyProtocols: readonly ("http" | "https")[];
  readonly error?: string;
}

export interface PluginNetworkTestResult {
  readonly settingsRevision: number;
  readonly checkedAt: string;
  readonly scope: "catalog-only" | "catalog-and-assets";
  /** Required for catalog-only results so an untested asset route is never implied. */
  readonly detail?: string;
}

export interface PluginAcquisitionProgress {
  readonly requestId: string;
  readonly operation: "catalog" | "inspect" | "test";
  readonly phase: "catalog" | "download" | "verify";
  readonly state: "running" | "succeeded" | "cancelled" | "failed";
  /** Bounded counts for the current transfer; unknown totals are omitted. */
  readonly receivedBytes?: number;
  readonly totalBytes?: number;
}

export interface PluginExitPrompt {
  readonly pluginId: string;
  readonly title: string;
  readonly message: string;
  readonly detail: string;
  readonly actions: readonly { readonly id: string; readonly label: string }[];
  readonly cancelAction: string;
}

export interface PluginEvent {
  readonly pluginId: string;
  readonly name: string;
  readonly data: JsonValue;
}

export interface PluginRequest {
  readonly pluginId: string;
  readonly activationId: string;
  readonly method: string;
  readonly input: JsonValue;
  readonly requestId: string;
  readonly correlationId: string;
}
