import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { expect, it } from "vitest";

import {
  HOST_PROTOCOL_VERSION,
  parseHostEvent,
  type HostEvent,
} from "../../src/features/kafka/contracts";
import { NATS_PROTOCOL_VERSION } from "../../src/features/nats/contracts";
import { openBrowserRuntime } from "../../src/platform/node/browser-runtime";
import { inspectPassphraseVault } from "../../src/platform/node/vault/passphrase-vault";
import { protectedProfileFixtureCa } from "../support/protected-profile-fixture";

it.skipIf(process.platform !== "linux")(
  "reopens production Kafka/NATS providers with encrypted profiles and no implicit fixture profiles",
  async () => {
    const dataRoot = await mkdtemp(join(tmpdir(), "streamskope-browser-runtime-"));
    const passphrase = "independent fixture vault passphrase";
    let runtime = await openBrowserRuntime(dataRoot, passphrase, "create");
    const kafkaEvents: HostEvent[] = [];
    runtime.providers.get("kafka")!.subscribe((wire) => kafkaEvents.push(parseHostEvent(wire)));
    const kafkaCommand = (command: string, payload: unknown): unknown => ({
      id: `kafka-${command}`,
      version: HOST_PROTOCOL_VERSION,
      command,
      payload,
    });
    const natsCommand = (command: string, payload: unknown): unknown => ({
      id: `nats-${command}`,
      version: NATS_PROTOCOL_VERSION,
      command,
      payload,
    });
    try {
      expect(await inspectPassphraseVault(dataRoot)).toBe("present");
      await expect(runtime.providers.get("kafka")!.dispatch({ version: -1 })).rejects.toMatchObject(
        { name: "ProviderWireValidationError", stage: "command" },
      );
      await expect(
        runtime.providers.get("kafka")!.dispatch(kafkaCommand("profiles.list", {})),
      ).resolves.toMatchObject({ ok: true });
      expect(
        kafkaEvents.filter((event) => event.event === "profiles.changed").at(-1),
      ).toMatchObject({ payload: { profiles: [] } });
      await expect(
        runtime.providers.get("nats")!.dispatch(natsCommand("profiles.list", {})),
      ).resolves.toMatchObject({ ok: true, result: { profiles: { profiles: [] } } });
      const caPem = await protectedProfileFixtureCa();
      await expect(
        runtime.providers.get("kafka")!.dispatch(
          kafkaCommand("profiles.create", {
            profile: {
              name: "Remote Kafka",
              brokers: ["remote-kafka.example.test:9093"],
              trust: {
                kind: "pem",
                label: "fixture CA",
                material: { mode: "replace", value: caPem },
                password: { mode: "clear" },
              },
              oauth: {
                clientId: "fixture-client",
                clientSecret: { mode: "replace", value: "private-kafka-secret" },
                tokenEndpoint: "https://auth.example.test/token",
                scope: "kafka",
              },
            },
          }),
        ),
      ).resolves.toMatchObject({ ok: true });
      await expect(
        runtime.providers.get("nats")!.dispatch(
          natsCommand("profiles.create", {
            profile: {
              name: "Remote NATS",
              servers: ["tls://remote-nats.example.test:4222"],
              authentication: {
                mode: "token",
                token: { mode: "replace", value: "private-nats-token" },
              },
              tls: { mode: "tls", caPem: { mode: "replace", value: caPem } },
            },
          }),
        ),
      ).resolves.toMatchObject({ ok: true });
      for (const filename of ["kafka-profiles.json", "nats-profiles.json", "vault.json"]) {
        const bytes = await readFile(join(dataRoot, filename), "utf8");
        expect(bytes).not.toContain(passphrase);
        expect(bytes).not.toContain("private-kafka-secret");
        expect(bytes).not.toContain("private-nats-token");
        expect(bytes).not.toContain(caPem);
      }
      await runtime.lock();
      await expect(
        openBrowserRuntime(dataRoot, "wrong fixture passphrase", "unlock"),
      ).rejects.toMatchObject({ code: "unlock-failed" });
      runtime = await openBrowserRuntime(dataRoot, passphrase, "unlock");
      kafkaEvents.length = 0;
      runtime.providers.get("kafka")!.subscribe((wire) => kafkaEvents.push(parseHostEvent(wire)));
      await expect(
        runtime.providers.get("kafka")!.dispatch(kafkaCommand("profiles.list", {})),
      ).resolves.toMatchObject({ ok: true });
      expect(
        kafkaEvents.filter((event) => event.event === "profiles.changed").at(-1),
      ).toMatchObject({
        payload: {
          profiles: [{ name: "Remote Kafka", oauth: { clientSecretPresent: true } }],
          store: { protection: "passphrase-protected", state: "ready" },
        },
      });
      await expect(
        runtime.providers.get("nats")!.dispatch(natsCommand("profiles.list", {})),
      ).resolves.toMatchObject({
        ok: true,
        result: {
          profiles: {
            profiles: [{ name: "Remote NATS", authentication: { tokenPresent: true } }],
            capability: { protection: "passphrase-protected", state: "ready" },
          },
        },
      });
    } finally {
      await runtime.lock();
      await rm(dataRoot, { recursive: true, force: true });
    }
  },
);
