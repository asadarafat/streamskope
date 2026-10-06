import type {
  PluginDeliverySnapshot,
  PluginManifest,
  PluginPackageInspection,
  PluginPackageInspectInput,
  PluginPackageTrust,
  PluginChangePrompt,
  PluginSnapshot,
} from "../../../plugins/contracts";
import { comparePluginManifests, isPluginCompatibleWithHost } from "../../../plugins/validation";

import type { PluginCatalogRequest, PluginCatalogSource } from "./catalog";
import type { PluginCatalogDiscovery } from "./catalog-discovery";
import { PluginPackageCandidates } from "./package-candidates";
import type { VerifiedPluginPackage } from "./package";
import type { PluginCandidateAuthority } from "./installer";
import { isPluginProblem, pluginProblem } from "./problem";
import { pluginNetworkProblem } from "./network-errors";
import type { PluginStore } from "./store";

export interface InstalledPluginReview {
  readonly manifest?: PluginManifest;
  readonly contentSha256?: string;
  readonly healthy: boolean;
}
interface Options {
  readonly store: PluginStore;
  readonly catalog: PluginCatalogSource;
  readonly discovery: PluginCatalogDiscovery;
  readonly hostRelease: string;
  readonly development: boolean;
  readonly chooseFile?: (signal: AbortSignal) => Promise<Uint8Array | null>;
  readonly assertOpen: () => void;
  readonly installed: (pluginId: string) => Promise<InstalledPluginReview>;
  readonly prepare: (
    pluginId: string,
    candidate: PluginCandidateAuthority,
  ) => Promise<PluginChangePrompt | null>;
  readonly install: (
    pluginId: string,
    bytes: Uint8Array,
    sha256: string,
    token: string | undefined,
    candidate: PluginCandidateAuthority,
  ) => Promise<PluginSnapshot>;
}

function failure(
  error: unknown,
  action: string,
  recovery: string,
  validationDetail = false,
): Error {
  if (isPluginProblem(error)) return error;
  // Filesystem errors contain private host paths. Only deliberately path-free
  // validation errors may contribute detail to a renderer-visible summary.
  const detail =
    validationDetail &&
    error instanceof Error &&
    !("code" in error) &&
    !(error instanceof SyntaxError) &&
    !(error instanceof TypeError)
      ? error.message.slice(0, 800)
      : "The package or private plugin storage is unavailable.";
  return pluginProblem(`${action}: ${detail}`, recovery);
}

/** Acquisition/review remains outside the lifecycle queue; only reviewed immutable bytes reach install. */
export class PluginDeliveryController {
  private readonly candidates = new PluginPackageCandidates();
  private readonly lifetime = new AbortController();
  private inspecting = 0;
  constructor(private readonly options: Options) {}

  async delivery(): Promise<PluginDeliverySnapshot> {
    this.options.assertOpen();
    return {
      fileInstallationAvailable: this.options.chooseFile !== undefined,
      cachedPackages: await this.options.store.packageCache.list(this.options.development),
    };
  }

  private async status(
    verified: VerifiedPluginPackage,
  ): Promise<Pick<PluginPackageInspection, "installedVersion" | "status" | "reason">> {
    const current = await this.options.installed(verified.manifest.id);
    const installedVersion = current.manifest?.version;
    if (!isPluginCompatibleWithHost(verified.manifest, this.options.hostRelease)) {
      const range = verified.manifest.compatibility?.streamskope;
      return {
        ...(installedVersion === undefined ? {} : { installedVersion }),
        status: "blocked",
        reason:
          range === undefined
            ? `This plugin's API or development build is incompatible with StreamSkope ${this.options.hostRelease}. Choose a package built for this desktop.`
            : `Requires StreamSkope ${range.minimum}${range.maximumExclusive === undefined ? " or later" : ` up to, but excluding, ${range.maximumExclusive}`}; this desktop is ${this.options.hostRelease}.`,
      };
    }
    if (current.manifest !== undefined) {
      const comparison = comparePluginManifests(verified.manifest, current.manifest);
      if (comparison < 0)
        return {
          installedVersion: current.manifest.version,
          status: "blocked",
          reason:
            "This package is older than the installed plugin. Choose a compatible newer version.",
        };
      if (
        comparison === 0 &&
        verified.manifest.apiVersion >= 3 &&
        current.contentSha256 !== undefined &&
        current.contentSha256 !== verified.contentSha256
      )
        return {
          installedVersion: current.manifest.version,
          status: "blocked",
          reason:
            "This version has different content from the installed package. The publisher must release a new version.",
        };
      if (current.healthy && current.contentSha256 === verified.contentSha256)
        return { installedVersion: current.manifest.version, status: "already-installed" };
      return { installedVersion: current.manifest.version, status: "update" };
    }
    return { status: "install" };
  }

  async inspectPackage(
    input: PluginPackageInspectInput,
    callerSignal?: AbortSignal,
    onProgress?: PluginCatalogRequest["onProgress"],
  ): Promise<PluginPackageInspection | null> {
    this.options.assertOpen();
    if (this.inspecting >= 2)
      throw pluginProblem(
        "Another plugin package selection is in progress.",
        "Complete or cancel the open package selection and retry.",
      );
    this.inspecting += 1;
    const signal =
      callerSignal === undefined
        ? this.lifetime.signal
        : AbortSignal.any([this.lifetime.signal, callerSignal]);
    let release: (() => void) | undefined;
    try {
      signal.throwIfAborted();
      let bytes: Uint8Array;
      let verified: VerifiedPluginPackage;
      let trust: PluginPackageTrust;
      if (input.source === "file") {
        if (this.options.chooseFile === undefined)
          throw pluginProblem(
            "Install from file is available in the StreamSkope desktop app.",
            "Open the desktop app and select Preferences > Plugins > Install from file.",
          );
        let chosen: Uint8Array | null;
        try {
          chosen = await this.options.chooseFile(signal);
        } catch (error) {
          if (isPluginProblem(error)) throw error;
          throw pluginProblem(
            "The selected plugin file could not be read.",
            "Choose a readable signed portable package and retry.",
          );
        }
        signal.throwIfAborted();
        if (chosen === null) return null;
        bytes = Uint8Array.from(chosen);
        verified = this.options.store.verifyPortablePackage(bytes);
        trust = "publisher";
      } else if (input.source === "cache") {
        const cached = await this.options.store.packageCache.read(input, this.options.development);
        bytes = cached.bytes;
        verified = cached.verified;
        trust = cached.record.trust;
      } else {
        const entry = await this.options.discovery.resolve(input);
        if (entry.downloadUrl === "development" && !this.options.development)
          throw new Error("Development packages are unavailable on this desktop.");
        let downloaded: { readonly bytes: Uint8Array; readonly sha256: string };
        try {
          downloaded =
            this.options.catalog.downloadPinned === undefined
              ? await this.options.catalog.download(input.pluginId, {
                  signal,
                  ...(onProgress === undefined ? {} : { onProgress }),
                })
              : await this.options.catalog.downloadPinned(entry, signal, onProgress);
        } catch (error) {
          throw pluginNetworkProblem(error, signal);
        }
        signal.throwIfAborted();
        bytes = Uint8Array.from(downloaded.bytes);
        if (downloaded.sha256 !== input.sha256)
          throw new Error(
            "The published package changed. Refresh the catalog and review the selected version again.",
          );
        verified = this.options.store.verifyPackage(bytes, input.sha256);
        if (JSON.stringify(verified.manifest) !== JSON.stringify(entry.manifest))
          throw new Error("The package does not match the selected catalog manifest.");
        trust =
          verified.publisher !== undefined
            ? "publisher"
            : this.options.development
              ? "development"
              : "official";
      }
      signal.throwIfAborted();
      onProgress?.("verify", bytes.byteLength, bytes.byteLength);
      release = this.options.store.packageCache.pin(verified.sha256);
      await this.options.store.packageCache.put(bytes, verified.sha256, trust);
      const status = await this.status(verified);
      signal.throwIfAborted();
      this.options.assertOpen();
      const inspection = this.candidates.create(
        {
          manifest: verified.manifest,
          sha256: verified.sha256,
          source: input.source,
          trust,
          ...(verified.publisher === undefined ? {} : { publisher: verified.publisher }),
          ...status,
        },
        release,
      );
      release = undefined;
      return inspection;
    } catch (error) {
      if (signal.aborted)
        throw pluginProblem(
          "Plugin package selection or download was cancelled.",
          "Select and review the package again when ready.",
        );
      throw failure(
        error,
        "Plugin package review failed",
        "Choose an official signed portable package, a verified cached version, or refresh the catalog and retry.",
        true,
      );
    } finally {
      release?.();
      this.inspecting -= 1;
    }
  }

  private authority(value: PluginPackageInspection): PluginCandidateAuthority {
    return {
      candidateId: value.candidateId,
      sha256: value.sha256,
      assertCurrent: (): void => {
        this.options.assertOpen();
        this.candidates.get(value.candidateId);
      },
    };
  }
  async preparePackageChange(candidateId: string): Promise<PluginChangePrompt | null> {
    this.options.assertOpen();
    try {
      const candidate = this.candidates.get(candidateId);
      if (candidate.status === "blocked")
        throw pluginProblem(candidate.reason ?? "This plugin package cannot be installed.");
      const cached = await this.options.store.packageCache.read(
        {
          pluginId: candidate.manifest.id,
          version: candidate.manifest.version,
          sha256: candidate.sha256,
        },
        this.options.development,
      );
      const status = await this.status(cached.verified);
      if (status.status === "blocked")
        throw pluginProblem(status.reason ?? "This plugin package cannot be installed.");
      if (status.status === "already-installed") return null;
      return await this.options.prepare(candidate.manifest.id, this.authority(candidate));
    } catch (error) {
      throw failure(
        error,
        "The reviewed plugin package could not be prepared",
        "Select and review the package again. The current installation is retained.",
      );
    }
  }
  async installPackage(candidateId: string, confirmationToken?: string): Promise<PluginSnapshot> {
    this.options.assertOpen();
    const candidate = this.candidates.begin(candidateId);
    try {
      if (candidate.status === "blocked")
        throw pluginProblem(candidate.reason ?? "This plugin package cannot be installed.");
      const cached = await this.options.store.packageCache.read(
        {
          pluginId: candidate.manifest.id,
          version: candidate.manifest.version,
          sha256: candidate.sha256,
        },
        this.options.development,
      );
      const snapshot = await this.options.install(
        candidate.manifest.id,
        cached.bytes,
        candidate.sha256,
        confirmationToken,
        this.authority(candidate),
      );
      this.candidates.succeeded(candidateId);
      return snapshot;
    } catch (error) {
      this.candidates.failed(candidateId);
      throw failure(
        error,
        "The reviewed plugin package could not be installed",
        "Select and review the package again. The current installation is retained.",
      );
    }
  }
  discardPackage(candidateId: string): void {
    this.candidates.discard(candidateId);
  }
  close(): void {
    this.lifetime.abort();
    this.candidates.close();
  }
}
