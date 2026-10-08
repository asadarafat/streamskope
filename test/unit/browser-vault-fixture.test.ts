import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { createServer } from "node:http";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { expect, it } from "vitest";

import { createBrowserVaultFixture } from "../../tools/package/browser-vault-fixture";

it("refreshes the replacement host protocol and requires the exact saved profile identity", async () => {
  const data = await mkdtemp(join(tmpdir(), "streamskope-vault-fixture-"));
  for (const name of ["setup-code", "vault.json", "nats-profiles.json"])
    await writeFile(
      join(data, name),
      name === "setup-code" ? "disposable-code" : "opaque-encrypted-bytes",
    );
  let locked = true;
  let version = 1;
  let generation = 1;
  let passphrase = "";
  let receivedSecret = "";
  const profile = {
    id: "real-created-profile",
    revision: 1,
    name: "Disposable container qualification",
    servers: ["nats://remote.example.test:4222"],
    authentication: { mode: "token", tokenPresent: true },
    tls: { mode: "plaintext" },
    createdAt: "2026-10-08T00:00:00.000Z",
    updatedAt: "2026-10-08T00:00:00.000Z",
  };
  const commandVersions: number[] = [];
  const server = createServer((request, response) => {
    const chunks: Buffer[] = [];
    request.on("data", (chunk: Buffer) => chunks.push(chunk));
    request.on("end", () => {
      const raw = Buffer.concat(chunks).toString();
      const body = (raw ? JSON.parse(raw) : {}) as Record<string, unknown>;
      const reply = (status: number, value: unknown): void => {
        response.statusCode = status;
        response.end(JSON.stringify(value));
      };
      const authenticate = (): void => {
        locked = false;
        response.setHeader("set-cookie", `session=${generation}; HttpOnly; SameSite=Strict`);
        reply(200, {});
      };
      if (request.url === "/health") return reply(200, { status: locked ? "locked" : "ready" });
      if (request.headers.origin === "http://invalid.example.test") return reply(403, {});
      if (request.url === "/__streamskope_session/create") {
        passphrase = String(body.passphrase);
        return authenticate();
      }
      if (request.url === "/__streamskope_session/unlock")
        return body.passphrase === passphrase ? authenticate() : reply(401, {});
      if (request.url === "/") return reply(locked ? 303 : 200, {});
      if (locked || request.headers.cookie !== `session=${generation}`) return reply(401, {});
      if (request.url === "/__streamskope_session/lock") {
        locked = true;
        return reply(200, {});
      }
      if (request.url === "/__streamskope_host/providers/nats/health")
        return reply(200, { protocolVersion: version });
      if (request.url === "/__streamskope_session/browser-runtime.js")
        return reply(200, "lockVault");
      if (request.url === "/__streamskope_host/providers/nats/commands") {
        commandVersions.push(Number(body.version));
        if (body.version !== version) return reply(409, {});
        if (body.command === "profiles.create") {
          const payload = body.payload as {
            profile: { authentication: { token: { value: string } } };
          };
          receivedSecret = payload.profile.authentication.token.value;
        }
        return reply(200, { ok: true, result: { profiles: { profiles: [profile] } } });
      }
      reply(404, {});
    });
  });
  try {
    await new Promise<void>((accept) => server.listen(0, "127.0.0.1", accept));
    const address = server.address();
    if (!address || typeof address === "string") throw new Error("Fixture server did not bind.");
    const fixture = await createBrowserVaultFixture({ data, port: address.port });
    expect(commandVersions).toEqual([1, 1]);
    expect(() => fixture.assertNoSecrets(receivedSecret)).toThrow();
    expect(() => fixture.assertNoSecrets(passphrase)).toThrow();
    version = 9;
    generation++;
    locked = true;
    await fixture.unlockAfterReplacement();
    expect(commandVersions.at(-1)).toBe(9);
    await fixture.verifyUnlocked();
    await fixture.lock();
    profile.id = "different-profile-with-same-name";
    await expect(fixture.unlockAfterReplacement()).rejects.toThrow(/exact encrypted profile/u);
  } finally {
    await new Promise<void>((accept, reject) =>
      server.close((error) => (error ? reject(error) : accept())),
    );
    await rm(data, { recursive: true, force: true });
  }
});
