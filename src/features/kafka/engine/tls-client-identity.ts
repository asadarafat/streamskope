import type { ConnectionOptions } from "node:tls";

import type { ConnectionClientIdentity } from "../contracts";

/** Identity is explicit per endpoint; trust inheritance must never forward a private key. */
export function tlsClientIdentityOptions(
  identity: ConnectionClientIdentity | undefined,
): Pick<ConnectionOptions, "cert" | "key" | "passphrase"> {
  return identity === undefined
    ? {}
    : {
        cert: identity.certificatePem,
        key: identity.privateKeyPem,
        ...(identity.passphrase === undefined ? {} : { passphrase: identity.passphrase }),
      };
}
