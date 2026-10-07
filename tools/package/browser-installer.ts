import { browserRegistryIdentity } from "./browser-registry-metadata";

export const BROWSER_INSTALLER_NAME = "install-browser-workbench.sh";

export interface BrowserInstallerIdentity {
  readonly version: string;
  readonly sourceRevision: string;
  readonly topologySha256: string;
  readonly manifestSha256: string;
}

/** Bind the installer to the same immutable assets qualified for its release. */
export function renderBrowserWorkbenchInstaller(
  identity: BrowserInstallerIdentity,
  template: string,
): string {
  browserRegistryIdentity(identity.version, identity.sourceRevision);
  const replacements = {
    STREAMSKOPE_INSTALL_VERSION: identity.version,
    STREAMSKOPE_INSTALL_SOURCE: identity.sourceRevision,
    STREAMSKOPE_TOPOLOGY_SHA256: identity.topologySha256,
    STREAMSKOPE_MANIFEST_SHA256: identity.manifestSha256,
  };
  for (const digest of [identity.topologySha256, identity.manifestSha256]) {
    if (!/^[a-f0-9]{64}$/u.test(digest))
      throw new Error("Browser installer requires exact topology and manifest SHA256 values.");
  }
  let rendered = template;
  for (const [key, value] of Object.entries(replacements)) {
    const placeholder = `@${key}@`;
    if (rendered.split(placeholder).length !== 2)
      throw new Error(`Browser installer template must contain exactly one ${placeholder}.`);
    rendered = rendered.replace(placeholder, value);
  }
  if (/@STREAMSKOPE_[A-Z_]+@/u.test(rendered))
    throw new Error("Browser installer template contains an unresolved publication field.");
  return rendered;
}
