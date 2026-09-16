import { execFile } from "node:child_process";
import { mkdtemp, writeFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { promisify } from "node:util";

import { expect, it } from "vitest";

import { createHttpsTrustFixture } from "../support/https-trust-fixture";

it("uses the Node process trust store without changing system trust or accepting a Kafka CA implicitly", async () => {
  const fixture = await createHttpsTrustFixture((_request, response) => response.end("verified"));
  const directory = await mkdtemp(join(tmpdir(), "streamskope-process-ca-"));
  try {
    const caPath = join(directory, "fixture.pem");
    await writeFile(caPath, fixture.caPem, { mode: 0o600 });
    const script = `import { NodeHttpsTrustTransport } from './src/platform/electron/main/https-trust-transport.ts';
      const result = await new NodeHttpsTrustTransport().get({ url: process.argv[1], authentication: { mode: 'bearer', token: 'fixture-token' }, tls: { mode: 'system' }, headers: [], maximumBytes: 64, signal: new AbortController().signal });
      process.stdout.write(new TextDecoder().decode(result));`;
    const { stdout } = await promisify(execFile)(
      process.execPath,
      ["--import", "tsx", "--input-type=module", "-e", script, fixture.origin],
      { env: { ...process.env, NODE_EXTRA_CA_CERTS: caPath }, timeout: 10000 },
    );
    expect(stdout).toBe("verified");
    expect(fixture.requests).toHaveLength(1);
    expect(fixture.requests[0]?.authorization).toBe("Bearer fixture-token");
    await expect(
      promisify(execFile)(
        process.execPath,
        ["--import", "tsx", "--input-type=module", "-e", script, fixture.origin],
        { env: { ...process.env, NODE_EXTRA_CA_CERTS: "" }, timeout: 10000 },
      ),
    ).rejects.toThrow();
    expect(fixture.requests).toHaveLength(1);
  } finally {
    await fixture.close();
    await rm(directory, { recursive: true, force: true });
  }
});
