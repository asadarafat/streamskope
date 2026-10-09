import { createPrivateKey } from "node:crypto";
import type { ConnectionOptions } from "node:tls";

import type { ConnectionClientIdentity } from "../contracts";

class TlsClientIdentityError extends Error {
  readonly code = "TLS_CLIENT_IDENTITY_INVALID";

  constructor(cause: unknown) {
    super("The configured TLS client private key could not be loaded.", { cause });
    this.name = "TlsClientIdentityError";
  }
}

/** Identity is explicit per endpoint; trust inheritance must never forward a private key. */
export function tlsClientIdentityOptions(
  identity: ConnectionClientIdentity | undefined,
): Pick<ConnectionOptions, "cert" | "key" | "passphrase"> {
  if (identity === undefined) return {};
  try {
    // A wrong passphrase can pass cipher padding and fail in the ASN.1 decoder.
    // Classify at this identity boundary; generic OpenSSL errors are ambiguous.
    createPrivateKey({ key: identity.privateKeyPem, passphrase: identity.passphrase });
  } catch (error) {
    throw new TlsClientIdentityError(error);
  }
  return {
    cert: identity.certificatePem,
    key: identity.privateKeyPem,
    ...(identity.passphrase === undefined ? {} : { passphrase: identity.passphrase }),
  };
}
