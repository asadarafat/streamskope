import { expect, it, vi } from "vitest";

const owner = vi.hoisted(() => ({
  shutdown: vi.fn(),
  pluginClose: vi.fn(),
  lock: vi.fn(),
  nats: vi.fn(),
}));
vi.mock("../../src/platform/node/kafka-backend", () => ({
  createKafkaBackend: (): unknown => ({ shutdown: owner.shutdown }),
}));
vi.mock("../../src/platform/node/nats-backend", () => ({ createNatsBackend: owner.nats }));
vi.mock("../../src/platform/node/plugins/runtime", () => ({
  PluginRuntime: class {
    close = owner.pluginClose;
  },
}));
vi.mock("../../src/platform/node/vault/passphrase-vault", () => ({
  openPassphraseVault: (): Promise<unknown> =>
    Promise.resolve({
      lock: owner.lock,
      paths: { kafkaProfiles: "/private/kafka", natsProfiles: "/private/nats" },
      protector: {},
      capability: {},
    }),
}));

import { operationalDiagnostic } from "../../src/platform/diagnostics";
import { openBrowserRuntime } from "../../src/platform/node/browser-runtime";
import { WebGatewayCleanupUnconfirmedError } from "../../src/platform/node/web-gateway-errors";

it("retains a partial runtime's vault lease and waits for sibling cleanup while identifying its failed owner", async () => {
  let finishPlugin!: () => void;
  owner.nats.mockImplementation(() => {
    throw new Error("private NATS constructor failure");
  });
  owner.shutdown.mockRejectedValue(new Error("private Kafka cleanup token"));
  owner.pluginClose.mockImplementation(
    () =>
      new Promise<void>((resolve) => {
        finishPlugin = resolve;
      }),
  );
  let settled = false;
  const opening = openBrowserRuntime("/private/data", "private passphrase", "unlock");
  void opening.then(
    () => {
      settled = true;
    },
    () => {
      settled = true;
    },
  );
  await vi.waitFor(() => expect(owner.pluginClose).toHaveBeenCalledOnce());
  expect(owner.shutdown).toHaveBeenCalledOnce();
  expect(settled).toBe(false);
  expect(owner.lock).not.toHaveBeenCalled();
  finishPlugin();
  const error: unknown = await opening.catch((error: unknown) => error);
  expect(error).toBeInstanceOf(WebGatewayCleanupUnconfirmedError);
  const diagnostic = operationalDiagnostic(error, "RUNTIME_START_FAILED");
  expect(diagnostic).toMatchObject({
    code: "KAFKA_CLEANUP_UNCONFIRMED",
    owner: "kafka",
    stage: "cleanup",
  });
  expect(JSON.stringify(diagnostic)).not.toContain("private");
  expect(owner.lock).not.toHaveBeenCalled();
});
