import assert from "node:assert/strict";
import { lstat, readFile } from "node:fs/promises";
import { resolve } from "node:path";
import process from "node:process";
import { fileURLToPath, URL } from "node:url";

const repositoryRoot = fileURLToPath(new URL("../..", import.meta.url));
const projectRoot = resolve(repositoryRoot, "vendors/streamskope/apps");
const applicationRoot = resolve(projectRoot, "capture");
const integrationLink = resolve(
  repositoryRoot,
  "integrations/nokia/eda/vendors/streamskope/apps/capture",
);
const expectedImageRepository = "ghcr.io/asadarafat/streamskope-eda-app";

function requireText(source, value, owner) {
  assert.ok(source.includes(value), `${owner} must contain ${JSON.stringify(value)}.`);
}

function applicationVersion(manifest) {
  const match = manifest.match(
    /^[ ]{2}image: ghcr\.io\/asadarafat\/streamskope-eda-app:(v[^\s]+)$/mu,
  );
  assert.ok(match?.[1], "The EDA manifest must use a versioned StreamSkope Capture image.");
  return match[1];
}

async function verify() {
  const [project, manifest, deployment, crd, openAPI, dockerfile, sessionSource] =
    await Promise.all([
      readFile(resolve(projectRoot, "PROJECT"), "utf8"),
      readFile(resolve(applicationRoot, "manifest.yaml"), "utf8"),
      readFile(resolve(applicationRoot, "agent/config/deployment.yaml"), "utf8"),
      readFile(
        resolve(applicationRoot, "crds/capture.streamskope.io_capturesessions.yaml"),
        "utf8",
      ),
      readFile(
        resolve(applicationRoot, "openapiv3/eda_oas_capture.streamskope.io_capturesessions.json"),
        "utf8",
      ),
      readFile(resolve(applicationRoot, "agent/Dockerfile"), "utf8"),
      readFile(resolve(applicationRoot, "agent/session.go"), "utf8"),
    ]);
  const canonicalMetadata = await lstat(applicationRoot);
  await assert.rejects(
    lstat(resolve(repositoryRoot, "apps/capture.streamskope.io")),
    { code: "ENOENT" },
    "Generated signed catalog projections belong on publication branches, not development source.",
  );
  await assert.rejects(
    lstat(integrationLink),
    { code: "ENOENT" },
    "The EDA catalog tree must not expose a navigation symlink outside its root.",
  );

  assert.equal(
    canonicalMetadata.isDirectory(),
    true,
    "The canonical EDA application must be a directory.",
  );
  for (const line of [
    "components:",
    "capture/crds/capture.streamskope.io_capturesessions.yaml",
    "capture/openapiv3/eda_oas_capture.streamskope.io_capturesessions.json",
    "capture/agent/config/deployment.yaml",
    "name: streamskope-capture-agent",
  ]) {
    requireText(manifest, line, "EDA manifest");
  }
  for (const line of [
    "kind: Deployment",
    "maxSurge: 0",
    "maxUnavailable: 1",
    "kind: Service",
    "type: ClusterIP",
    "kind: HttpProxy",
    "authType: inApiServer",
    "rootUrl: http://streamskope-capture-agent.eda-system.svc:8080/",
    "kind: NetworkPolicy",
    "allowPrivilegeEscalation: false",
    "readOnlyRootFilesystem: true",
  ]) {
    requireText(deployment, line, "EDA deployment");
  }
  assert.equal(
    deployment.includes("NodePort"),
    false,
    "The capture app must not create a NodePort.",
  );
  assert.equal(
    deployment.includes("LoadBalancer"),
    false,
    "The capture app must not create a LoadBalancer.",
  );
  requireText(crd, "kind: CustomResourceDefinition", "CaptureSession CRD");
  requireText(crd, "scope: Namespaced", "CaptureSession CRD");
  JSON.parse(openAPI);
  requireText(dockerfile, "USER 65532:65532", "Capture agent container");
  requireText(dockerfile, "@sha256:", "Capture agent container");
  requireText(sessionSource, "redpanda:v24.3.5@sha256:", "Capture resource plan");
  assert.equal(
    canonicalMetadata.isSymbolicLink(),
    false,
    "The canonical EDA application must not be a symlink.",
  );

  for (const line of [
    "builderVersion: v26.8.2",
    "catalog: https://github.com/asadarafat/streamskope.git",
    "domain: streamskope.io",
    "registry: ghcr.io/asadarafat",
    "vendor: streamskope",
  ]) {
    requireText(project, line, "EDA PROJECT");
  }
  for (const line of [
    "name: capture",
    "group: capture.streamskope.io",
    "version: v1alpha1",
    "- v6.0.0",
    "ociSpecVersion: v1.0.0",
    "expose: readWrite",
    "exportPolicy: all",
    "importPolicy: spec",
  ]) {
    requireText(manifest, line, "EDA manifest");
  }
  assert.equal(manifest.includes(":latest"), false, "The EDA image must not use latest.");
  assert.doesNotMatch(
    manifest,
    /(?:expose: readOnly|exportPolicy: none|importPolicy: none)/u,
    "The EDA manifest must retain read/write access and capture-session exchange.",
  );
  assert.equal(
    manifest.includes("skipTLSVerify"),
    false,
    "The EDA manifest must not weaken registry TLS.",
  );
  assert.equal(
    manifest.includes("skipDigestVerification"),
    false,
    "The EDA manifest must not weaken digest checks.",
  );
  assert.equal(
    manifest.includes("skipSignatureVerification"),
    false,
    "The EDA manifest must not weaken signature checks.",
  );

  for (const path of [
    "README.md",
    "LICENSE",
    "docs/index.md",
    "docs/README.md",
    "docs/CHANGELOG.md",
    "docs/LICENSE.md",
    "docs/SUPPORT.md",
    "docs/vars.yaml",
  ]) {
    const metadata = await lstat(resolve(applicationRoot, path));
    assert.equal(metadata.isFile(), true, `EDA application file ${path} is required.`);
  }
  for (const path of ["docs/media", "docs/resources", "docs/snippets"]) {
    const metadata = await lstat(resolve(applicationRoot, path));
    assert.equal(metadata.isDirectory(), true, `EDA application directory ${path} is required.`);
  }
  const version = applicationVersion(manifest);
  const desktopContract = await readFile(
    resolve(repositoryRoot, "plugins/eda/contracts/eda-capture-types.ts"),
    "utf8",
  );
  const targetVersion = desktopContract.match(/EDA_TARGET_VERSION = "(v\d+\.\d+\.\d+)"/u)?.[1];
  assert.ok(targetVersion, "The desktop contract must declare the full target EDA version.");
  assert.equal(
    version,
    targetVersion,
    "The EDA application version must exactly match the target EDA version.",
  );
  const agentApi = await readFile(resolve(applicationRoot, "agent/api.go"), "utf8");
  requireText(agentApi, `const captureAgentVersion = "${version}"`, "EDA agent version");
  requireText(
    manifest,
    `image: localhost/streamskope-eda-app-agent:${version}`,
    "Bundled agent version",
  );
  requireText(desktopContract, "version: EDA_TARGET_VERSION", "Desktop EDA version");

  const publishIndex = process.argv.indexOf("--publish");
  if (publishIndex !== -1) {
    const requestedVersion = process.argv[publishIndex + 1];
    assert.equal(
      requestedVersion ?? "",
      targetVersion,
      "The release must use the exact target EDA version, without an app-specific suffix.",
    );
    assert.equal(
      applicationVersion(manifest),
      requestedVersion,
      "The requested release must match the manifest image tag.",
    );
    assert.ok(
      manifest.includes("components:"),
      "Publication is blocked until the capture-agent components are generated.",
    );
    const publicKey = await lstat(resolve(applicationRoot, "signing/streamskope-eda.pub"));
    assert.equal(
      publicKey.isFile(),
      true,
      "Publication requires the trusted StreamSkope EDA public key.",
    );
  }

  process.stdout.write(
    `EDA application policy verified for ${expectedImageRepository}:${applicationVersion(manifest)}.\n`,
  );
}

await verify();
