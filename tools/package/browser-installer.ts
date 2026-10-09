import { isAbsolute } from "node:path";

import {
  BROWSER_DATA_COMPATIBILITY,
  REVIEWED_BROWSER_PREDECESSORS,
} from "../../src/platform/node/browser-data-compatibility";

import { browserRegistryIdentity } from "./browser-registry-metadata";

export const BROWSER_INSTALLER_NAME = "install-browser-workbench.sh";

export interface BrowserInstallerIdentity {
  readonly version: string;
  readonly sourceRevision: string;
  readonly topologySha256: string;
  readonly manifestSha256: string;
}

export interface LocalBrowserDelivery {
  readonly version: string;
  readonly sourceRevision: string;
  readonly platform: "linux/amd64" | "linux/arm64";
  readonly imageId: string;
  readonly manifest: { readonly path: string; readonly sha256: string };
  readonly topology: { readonly path: string; readonly sha256: string };
}
interface LocalBrowserTransitionDelivery {
  readonly schemaVersion: 1;
  readonly releases: readonly [LocalBrowserDelivery, LocalBrowserDelivery];
}

function replace(source: string, key: string, value: string): string {
  const placeholder = `@${key}@`;
  if (source.split(placeholder).length !== 2)
    throw new Error(`Browser installer template must contain exactly one ${placeholder}.`);
  return source.replace(placeholder, () => value);
}
function digest(value: string): void {
  if (!/^[a-f0-9]{64}$/u.test(value))
    throw new Error("Browser installer requires exact topology and manifest SHA256 values.");
}
function render(
  identity: BrowserInstallerIdentity,
  template: string,
  helper: string,
  local: LocalBrowserDelivery | LocalBrowserTransitionDelivery | null,
): string {
  browserRegistryIdentity(identity.version, identity.sourceRevision);
  digest(identity.topologySha256);
  digest(identity.manifestSha256);
  if (
    helper.length === 0 ||
    Buffer.byteLength(helper) > 96 * 1024 ||
    helper.includes("\0") ||
    /^STREAMSKOPE_MAINTENANCE_PY\r?$/mu.test(helper)
  )
    throw new Error("Browser maintenance helper cannot be safely embedded.");
  const encoded = (value: unknown): string => Buffer.from(JSON.stringify(value)).toString("base64");
  let embedded = replace(
    helper,
    "STREAMSKOPE_MAINTENANCE_POLICY_B64",
    encoded({
      dataCompatibility: BROWSER_DATA_COMPATIBILITY,
      predecessors: REVIEWED_BROWSER_PREDECESSORS,
    }),
  );
  embedded = replace(embedded, "STREAMSKOPE_MAINTENANCE_LOCAL_B64", encoded(local));
  let rendered = replace(template, "STREAMSKOPE_MAINTENANCE_HELPER", embedded);
  for (const [key, value] of Object.entries({
    STREAMSKOPE_INSTALL_VERSION: identity.version,
    STREAMSKOPE_INSTALL_SOURCE: identity.sourceRevision,
    STREAMSKOPE_TOPOLOGY_SHA256: identity.topologySha256,
    STREAMSKOPE_MANIFEST_SHA256: identity.manifestSha256,
  }))
    rendered = replace(rendered, key, value);
  if (/@STREAMSKOPE_[A-Z_]+@/u.test(rendered) || Buffer.byteLength(rendered) > 128 * 1024)
    throw new Error("Browser installer has unresolved publication fields or exceeds its bound.");
  return rendered;
}

/** Public delivery is unconditional; local rehearsal cannot alter this constructor. */
export function renderBrowserWorkbenchInstaller(
  identity: BrowserInstallerIdentity,
  template: string,
  transactionHelperSource: string,
): string {
  return render(identity, template, transactionHelperSource, null);
}

/** Internal native rehearsal only. This artifact can never pass public byte qualification. */
export function renderLocalBrowserWorkbenchInstaller(
  local: LocalBrowserDelivery,
  template: string,
  transactionHelperSource: string,
): string {
  validateLocalDelivery(local);
  return renderLocalDelivery(local, template, transactionHelperSource, local);
}

function exactFields(value: object, expected: readonly string[]): void {
  if (Object.keys(value).sort().join(",") !== [...expected].sort().join(","))
    throw new Error("Local browser rehearsal requires closed artifact descriptors.");
}

function validateLocalDelivery(local: LocalBrowserDelivery): void {
  exactFields(local, ["version", "sourceRevision", "platform", "imageId", "manifest", "topology"]);
  browserRegistryIdentity(local.version, local.sourceRevision);
  if (
    (local.platform !== "linux/amd64" && local.platform !== "linux/arm64") ||
    !/^sha256:[a-f0-9]{64}$/u.test(local.imageId)
  )
    throw new Error("Local browser rehearsal requires an exact native image identity.");
  for (const artifact of [local.manifest, local.topology]) {
    exactFields(artifact, ["path", "sha256"]);
    digest(artifact.sha256);
    if (
      !isAbsolute(artifact.path) ||
      artifact.path.length > 4096 ||
      [...artifact.path].some(
        (character) => character.charCodeAt(0) < 32 || character.charCodeAt(0) === 127,
      )
    )
      throw new Error("Local browser rehearsal requires bounded absolute artifact paths.");
  }
}

function renderLocalDelivery(
  target: LocalBrowserDelivery,
  template: string,
  transactionHelperSource: string,
  delivery: LocalBrowserDelivery | LocalBrowserTransitionDelivery,
): string {
  return render(
    {
      version: target.version,
      sourceRevision: target.sourceRevision,
      topologySha256: target.topology.sha256,
      manifestSha256: target.manifest.sha256,
    },
    template,
    transactionHelperSource,
    delivery,
  );
}

/** Closed two-image rehearsal only; public delivery never accepts this authority. */
export function renderLocalBrowserTransitionInstaller(
  target: LocalBrowserDelivery,
  predecessor: LocalBrowserDelivery,
  template: string,
  transactionHelperSource: string,
): string {
  validateLocalDelivery(target);
  validateLocalDelivery(predecessor);
  if (
    target.platform !== predecessor.platform ||
    target.version === predecessor.version ||
    target.imageId === predecessor.imageId
  )
    throw new Error("Local transition requires distinct releases on the same native platform.");
  return renderLocalDelivery(target, template, transactionHelperSource, {
    schemaVersion: 1,
    releases: [target, predecessor],
  });
}

/** Native lab runtimes require a named image; this closed tag remains bound to the sealed ID. */
export function renderLocalBrowserTopology(
  version: string,
  sourceRevision: string,
  template: string,
): { readonly reference: string; readonly topology: string } {
  browserRegistryIdentity(version, sourceRevision);
  const original = "image: ${STREAMSKOPE_IMAGE:=streamskope:0.0.0-dev}";
  if (
    template.split(original).length !== 2 ||
    template.split("image-pull-policy: Never").length !== 2
  )
    throw new Error("Local native qualification requires the reviewed unassigned topology.");
  const reference = `streamskope:${version}`;
  return {
    reference,
    topology: template.replace(original, `image: \${STREAMSKOPE_IMAGE:=${reference}}`),
  };
}
