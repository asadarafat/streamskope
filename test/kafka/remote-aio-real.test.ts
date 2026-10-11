import { execFile } from "node:child_process";
import { copyFile, mkdir, mkdtemp, rm } from "node:fs/promises";
import { createRequire } from "node:module";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { pathToFileURL } from "node:url";
import { promisify } from "node:util";

import { expect, it } from "vitest";

import { startKafkaFixture } from "../../aio-kafka/fixtures/broker";
import { disposeFixtureResources } from "../../aio-kafka/fixtures/resources";

const execute = promisify(execFile);
const require = createRequire(resolve("package.json"));

it("attaches independent Node-only clients to one AIO broker without local JVM or Docker tools", async () => {
  const fixture = await startKafkaFixture();
  let directory: string | undefined;
  try {
    directory = await mkdtemp(join(tmpdir(), "aio-remote-client-"));
    await mkdir(join(directory, "aio-kafka"));
    await copyFile(
      resolve("aio-kafka/fixture.config.json"),
      join(directory, "aio-kafka/fixture.config.json"),
    );
    const ca = join(directory, "ca.pem");
    await copyFile(fixture.environment.STREAMSKOPE_TEST_CA_PATH!, ca);
    const remote = pathToFileURL(resolve("aio-kafka/fixtures/remote.ts")).href;
    const client = `
      const { preflightRemoteAioFixture } = await import(${JSON.stringify(remote)});
      await preflightRemoteAioFixture();
      process.stdout.write('remote AIO TLS/OAuth preflight passed');
    `;
    const environment = {
      // The client can execute Node, but has no java/keytool/docker/containerlab on PATH.
      PATH: dirname(process.execPath),
      ...fixture.environment,
      STREAMSKOPE_TEST_CA_PATH: ca,
      ...(process.env.SystemRoot === undefined ? {} : { SystemRoot: process.env.SystemRoot }),
    };
    const clients = await Promise.allSettled([
      execute(
        process.execPath,
        ["--import", require.resolve("tsx"), "--input-type=module", "-e", client],
        {
          cwd: directory,
          env: environment,
          timeout: 30_000,
        },
      ),
      execute(
        process.execPath,
        ["--import", require.resolve("tsx"), "--input-type=module", "-e", client],
        {
          cwd: directory,
          env: environment,
          timeout: 30_000,
        },
      ),
    ]);
    for (const result of clients) {
      if (result.status === "rejected") throw result.reason;
      expect(result.value.stdout).toBe("remote AIO TLS/OAuth preflight passed");
    }
    // A fresh client can still attach after both previous clients have exited.
    const final = await execute(
      process.execPath,
      ["--import", require.resolve("tsx"), "--input-type=module", "-e", client],
      {
        cwd: directory,
        env: environment,
        timeout: 30_000,
      },
    );
    expect(final.stdout).toBe("remote AIO TLS/OAuth preflight passed");
  } finally {
    await disposeFixtureResources([
      (): Promise<void> => fixture.dispose(),
      (): Promise<void> =>
        directory === undefined
          ? Promise.resolve()
          : rm(directory, { force: true, recursive: true }),
    ]);
  }
}, 120_000);
