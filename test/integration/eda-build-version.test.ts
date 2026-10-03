import { spawn } from "node:child_process";
import {
  access,
  copyFile,
  mkdir,
  mkdtemp,
  readFile,
  rm,
  symlink,
  writeFile,
} from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

import { afterEach, expect, it } from "vitest";

import { createHttpsTrustFixture } from "../support/https-trust-fixture";

const root = fileURLToPath(new URL("../../", import.meta.url));
const script = join(root, "tools/check/eda-version.ts");
const loader = join(root, "node_modules/tsx/dist/loader.mjs");
const closeables: Array<() => Promise<unknown>> = [];

afterEach(async () => {
  await Promise.allSettled(closeables.splice(0).map(async (close) => close()));
});

async function run(
  version?: unknown,
  options: { github?: boolean; package?: boolean; trusted?: boolean; status?: number } = {},
): Promise<{
  status: number | null;
  output: string;
  evidence: Record<string, unknown>;
  requests: string[];
  builderStarted: boolean;
}> {
  const cwd = await mkdtemp(join(tmpdir(), "streamskope-eda-build-version-"));
  closeables.push(() => rm(cwd, { recursive: true, force: true }));
  const requests: string[] = [];
  const fixture = await createHttpsTrustFixture((request, response) => {
    const path = request.url ?? "";
    requests.push(path);
    response.setHeader("content-type", "application/json");
    if (path.endsWith("/realms/eda/protocol/openid-connect/token")) {
      response.end('{"access_token":"private-fixture-token"}');
    } else if (path === "/core/about/version") {
      expect(request.headers.authorization).toBe("Bearer private-fixture-token");
      response.writeHead(options.status ?? 200);
      response.end(JSON.stringify(version));
    } else response.writeHead(404).end();
  });
  closeables.push(() => fixture.close());
  const ca = join(cwd, "ca.pem");
  await writeFile(ca, fixture.caPem);
  const marker = join(cwd, "builder-started");
  const builder = join(cwd, "builder");
  await writeFile(builder, '#!/bin/sh\ntouch "$(dirname "$0")/builder-started"\nexit 1\n', {
    mode: 0o755,
  });
  if (options.package) {
    await mkdir(join(cwd, "tools/check"), { recursive: true });
    await copyFile(script, join(cwd, "tools/check/eda-version.ts"));
    await copyFile(
      join(root, "tools/check/eda-fixture.ts"),
      join(cwd, "tools/check/eda-fixture.ts"),
    );
    await symlink(
      join(root, "tools/check/eda-source.mjs"),
      join(cwd, "tools/check/eda-source.mjs"),
    );
    await symlink(join(root, "src"), join(cwd, "src"), "dir");
    await symlink(join(root, "plugins"), join(cwd, "plugins"), "dir");
    await symlink(join(root, "node_modules"), join(cwd, "node_modules"), "dir");
  }
  const env = Object.fromEntries(
    Object.entries(process.env).filter(
      ([name]) => !name.startsWith("STREAMSKOPE_EDA_") && name !== "GITHUB_ACTIONS",
    ),
  );
  const child = spawn(
    options.package ? "bash" : process.execPath,
    options.package ? [join(root, "tools/package/eda.sh")] : ["--import", loader, script],
    {
      cwd,
      env: {
        ...env,
        EDABUILDER: builder,
        ...(options.github ? { GITHUB_ACTIONS: "true" } : {}),
        ...(version === undefined
          ? {}
          : {
              STREAMSKOPE_EDA_API_URL: fixture.origin,
              STREAMSKOPE_EDA_API_USERNAME: "fixture-user",
              STREAMSKOPE_EDA_API_PASSWORD: "private-fixture-password",
              STREAMSKOPE_EDA_API_CLIENT_SECRET: "private-fixture-client-secret",
              ...(options.trusted === false ? {} : { STREAMSKOPE_EDA_API_CA: ca }),
            }),
      },
      stdio: ["ignore", "pipe", "pipe"],
    },
  );
  let output = "";
  child.stdout.on("data", (chunk: Buffer) => {
    output += chunk.toString();
  });
  child.stderr.on("data", (chunk: Buffer) => {
    output += chunk.toString();
  });
  const status = await new Promise<number | null>((resolve, reject) => {
    child.once("error", reject);
    child.once("close", resolve);
  });
  const evidence = JSON.parse(
    await readFile(join(cwd, "dist/ci/eda-version.json"), "utf8").catch(() => {
      throw new Error(`Version check produced no evidence: ${output}`);
    }),
  ) as Record<string, unknown>;
  for (const secret of [
    "private-fixture-token",
    "private-fixture-password",
    "private-fixture-client-secret",
  ])
    expect(output + JSON.stringify(evidence)).not.toContain(secret);
  const builderStarted = await access(marker).then(
    () => true,
    () => false,
  );
  return { status, output, evidence, requests, builderStarted };
}

it.each(["v26.8.2", "26.8.2", "v26.8.2-2609021827-g95df2c47"])(
  "verifies the EDA product release through its authenticated API: %s",
  async (version) => {
    const result = await run({ eda: { version }, unrelated: { version: "v99.0.0" } });
    expect(result.status).toBe(0);
    expect(result.evidence).toMatchObject({
      outcome: "passed",
      targetVersion: "v26.8.2",
      releaseVersion: "v26.8.2",
      buildVersion: version,
    });
    expect(result.requests).toEqual([
      "/core/httpproxy/v1/keycloak/realms/eda/protocol/openid-connect/token",
      "/core/about/version",
    ]);
  },
);

it("stops the EDA package before invoking the builder when the running release differs", async () => {
  const result = await run({ eda: { version: "v26.8.1-2609021827-g95df2c47" } }, { package: true });
  expect(result.status).toBe(1);
  expect(result.evidence).toMatchObject({
    outcome: "failed",
    targetVersion: "v26.8.2",
    releaseVersion: "v26.8.1",
  });
  expect(result.builderStarted).toBe(false);
});

it.each([
  { core: { version: "v6.0.0" } },
  { eda: { version: "v26.8.2-rc.1" } },
  { eda: { version: "private-fixture-password" } },
])("rejects missing or invalid product releases: %j", async (version) => {
  const result = await run(version);
  expect(result.status).toBe(1);
  expect(result.evidence.outcome).toBe("failed");
  expect(result.evidence).not.toHaveProperty("releaseVersion");
});

it("requires local connection settings", async () => {
  const result = await run();
  expect(result.status).toBe(1);
  expect(result.evidence.outcome).toBe("failed");
  expect(result.requests).toEqual([]);
});

it("fails closed on an untrusted API certificate", async () => {
  const result = await run({ eda: { version: "v26.8.2" } }, { trusted: false });
  expect(result.status).toBe(1);
  expect(result.evidence.outcome).toBe("failed");
  expect(result.requests).toEqual([]);
});

it("fails closed on an API error", async () => {
  const result = await run({ eda: { version: "v26.8.2" } }, { status: 503 });
  expect(result.status).toBe(1);
  expect(result.evidence.outcome).toBe("failed");
});

it("records GitHub's declared target without claiming cluster verification", async () => {
  const result = await run(undefined, { github: true });
  expect(result.status).toBe(0);
  expect(result.evidence).toMatchObject({ outcome: "not-checked", targetVersion: "v26.8.2" });
  expect(result.evidence).not.toHaveProperty("releaseVersion");
  expect(result.requests).toEqual([]);
});
