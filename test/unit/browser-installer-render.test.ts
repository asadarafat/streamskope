import { readFile } from "node:fs/promises";

import { expect, it } from "vitest";

import {
  renderBrowserWorkbenchInstaller,
  renderLocalBrowserWorkbenchInstaller,
  renderLocalBrowserTopology,
} from "../../tools/package/browser-installer";

const identity = {
  version: "1.2.3",
  sourceRevision: "a".repeat(40),
  topologySha256: "b".repeat(64),
  manifestSha256: "c".repeat(64),
};
const template = [
  "@STREAMSKOPE_INSTALL_VERSION@",
  "@STREAMSKOPE_INSTALL_SOURCE@",
  "@STREAMSKOPE_TOPOLOGY_SHA256@",
  "@STREAMSKOPE_MANIFEST_SHA256@",
  "@STREAMSKOPE_MAINTENANCE_HELPER@",
].join("\n");
const helper =
  'POLICY = "@STREAMSKOPE_MAINTENANCE_POLICY_B64@"\nLOCAL = "@STREAMSKOPE_MAINTENANCE_LOCAL_B64@"\n';

it("seals public delivery independently of extra caller fields and binds exact helper bytes", () => {
  const extra = { ...identity, local: "untrusted" };
  const rendered = renderBrowserWorkbenchInstaller(extra, template, helper);
  expect(rendered).toContain('LOCAL = "bnVsbA=="');
  expect(rendered).not.toContain("untrusted");
  expect(
    renderBrowserWorkbenchInstaller(identity, template, `${helper}# reviewed change\n`),
  ).not.toEqual(rendered);
  const policy = /POLICY = "([^"]+)"/u.exec(rendered)![1]!;
  const decoded = JSON.parse(Buffer.from(policy, "base64").toString("utf8")) as {
    predecessors: { version: string }[];
  };
  expect(decoded.predecessors[0]!.version).toBe("0.10.3");
});
it.each([
  "",
  `${helper}\nSTREAMSKOPE_MAINTENANCE_PY\n`,
  `${helper}\0`,
  `${helper}@STREAMSKOPE_UNKNOWN@`,
  `${helper}${"a".repeat(128 * 1024)}`,
  helper + helper,
])("refuses unsafe or ambiguous helper %j", (source) => {
  expect(() => renderBrowserWorkbenchInstaller(identity, template, source)).toThrow();
});
it("uses a separate sealed local constructor with no fabricated registry identity", () => {
  const local = {
    version: identity.version,
    sourceRevision: identity.sourceRevision,
    platform: "linux/arm64" as const,
    imageId: `sha256:${"d".repeat(64)}`,
    manifest: { path: "/private/manifest.json", sha256: identity.manifestSha256 },
    topology: { path: "/private/topology.yml", sha256: identity.topologySha256 },
  };
  const rendered = renderLocalBrowserWorkbenchInstaller(local, template, helper);
  const encoded = /LOCAL = "([^"]+)"/u.exec(rendered)![1]!;
  expect(JSON.parse(Buffer.from(encoded, "base64").toString("utf8"))).toEqual(local);
  expect(rendered).not.toEqual(renderBrowserWorkbenchInstaller(identity, template, helper));
  expect(() =>
    renderLocalBrowserWorkbenchInstaller({ ...local, imageId: "latest" }, template, helper),
  ).toThrow();
  expect(() =>
    renderLocalBrowserWorkbenchInstaller(
      { ...local, manifest: { ...local.manifest, path: "relative" } },
      template,
      helper,
    ),
  ).toThrow();
});
it("embeds the maintained production helper without another runtime download", async () => {
  const rendered = renderBrowserWorkbenchInstaller(
    identity,
    await readFile("tools/package/install-browser-workbench.sh", "utf8"),
    await readFile("tools/package/browser-maintenance.py", "utf8"),
  );
  expect(rendered).not.toMatch(/@STREAMSKOPE_[A-Z_]+@/u);
  expect(rendered).toContain("class Refused");
  expect(Buffer.byteLength(rendered)).toBeLessThan(128 * 1024);
});

it("renders a closed local tag with pull Never without substituting image-ID authority", async () => {
  const source = await readFile("streamskope.clab.yml", "utf8");
  const local = renderLocalBrowserTopology("0.10.4-qa.12345678", identity.sourceRevision, source);
  expect(local.reference).toBe("streamskope:0.10.4-qa.12345678");
  expect(local.topology).toContain("image: ${STREAMSKOPE_IMAGE:=streamskope:0.10.4-qa.12345678}");
  expect(local.topology).toContain("image-pull-policy: Never");
  expect(() =>
    renderLocalBrowserTopology(identity.version, identity.sourceRevision, local.topology),
  ).toThrow();
  expect(() =>
    renderLocalBrowserTopology(
      identity.version,
      identity.sourceRevision,
      source.replace("image-pull-policy: Never", "image-pull-policy: Always"),
    ),
  ).toThrow();
});
