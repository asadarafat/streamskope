import { generateKeyPairSync } from "node:crypto";

import type { TrustedPluginPublisher } from "../../src/platform/node/plugins/publishers";

/** Ephemeral test-only trust is passed by the caller, never installed through environment settings. */
export function pluginPublisherFixture(
  pluginIds: readonly string[] = ["streamskope.eda", "streamskope.nsp"],
): {
  readonly encodedKey: string;
  readonly publishers: readonly TrustedPluginPublisher[];
} {
  const { privateKey, publicKey } = generateKeyPairSync("ed25519");
  return {
    encodedKey: Buffer.from(privateKey.export({ format: "pem", type: "pkcs8" })).toString("base64"),
    publishers: [
      {
        keyId: "ephemeral-release-fixture",
        name: "Test release publisher",
        publicKey: publicKey.export({ format: "pem", type: "spki" }).toString(),
        pluginIds,
      },
    ],
  };
}
