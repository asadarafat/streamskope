import { execFile } from "node:child_process";
import { randomBytes } from "node:crypto";
import { readFile, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { promisify } from "node:util";

const run = promisify(execFile);

export interface KafkaAuthenticationFixture {
  readonly tlsBroker: string;
  readonly mutualTlsBroker: string;
  readonly mutualSaslBroker: string;
  readonly saslPlaintextBroker: string;
  readonly username: string;
  readonly password: string;
  readonly caPem: string;
  readonly certificatePem: string;
  readonly privateKeyPem: string;
  readonly passphrase: string;
}

/** Disposable loopback credentials; owned by and removed with the native broker fixture. */
export async function createKafkaAuthenticationMaterials(options: {
  readonly directory: string;
  readonly tlsPort: number;
  readonly mutualTlsPort: number;
  readonly mutualSaslPort: number;
  readonly saslPlaintextPort: number;
  readonly brokerCaPath: string;
}): Promise<{
  readonly fixture: KafkaAuthenticationFixture;
  readonly listeners: string;
  readonly properties: Readonly<Record<string, string>>;
  readonly storageArguments: readonly string[];
}> {
  const username = "connectionmatrix";
  const password = randomBytes(24).toString("hex");
  const passphrase = randomBytes(24).toString("hex");
  const certificatePath = join(options.directory, "client.pem");
  const keyPath = join(options.directory, "client.key");
  const passwordPath = join(options.directory, "client.password");
  const trustPath = join(options.directory, "clients.p12");
  await writeFile(passwordPath, passphrase, { mode: 0o600 });
  await run(
    "openssl",
    [
      "req",
      "-x509",
      "-newkey",
      "rsa:2048",
      "-sha256",
      "-days",
      "2",
      "-keyout",
      keyPath,
      "-out",
      certificatePath,
      "-passout",
      `file:${passwordPath}`,
      "-subj",
      "/CN=streamskope-fixture-client",
      "-addext",
      "extendedKeyUsage=clientAuth",
    ],
    { timeout: 30_000 },
  );
  await run(
    "keytool",
    [
      "-importcert",
      "-noprompt",
      "-alias",
      "client",
      "-file",
      certificatePath,
      "-keystore",
      trustPath,
      "-storetype",
      "PKCS12",
      "-storepass:file",
      passwordPath,
    ],
    { timeout: 30_000 },
  );
  const properties: Record<string, string> = {
    "sasl.enabled.mechanisms": "OAUTHBEARER,PLAIN,SCRAM-SHA-256,SCRAM-SHA-512",
    "listener.name.mtls.ssl.client.auth": "required",
    "listener.name.mutual.ssl.client.auth": "required",
    "ssl.truststore.type": "PKCS12",
    "ssl.truststore.location": trustPath.replaceAll("\\", "/"),
    "ssl.truststore.password": passphrase,
  };
  for (const listener of ["client", "mutual", "clear"]) {
    properties[`listener.name.${listener}.plain.sasl.jaas.config`] =
      `org.apache.kafka.common.security.plain.PlainLoginModule required user_${username}="${password}";`;
    for (const mechanism of ["scram-sha-256", "scram-sha-512"])
      properties[`listener.name.${listener}.${mechanism}.sasl.jaas.config`] =
        "org.apache.kafka.common.security.scram.ScramLoginModule required;";
  }
  // The TLS client-certificate listener also accepts OAuth, independently of certificate identity.
  properties["listener.name.mutual.sasl.enabled.mechanisms"] =
    "OAUTHBEARER,PLAIN,SCRAM-SHA-256,SCRAM-SHA-512";
  properties["listener.name.mutual.oauthbearer.sasl.jaas.config"] =
    "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;";
  properties["listener.name.mutual.oauthbearer.sasl.server.callback.handler.class"] =
    "org.apache.kafka.common.security.oauthbearer.OAuthBearerValidatorCallbackHandler";
  properties["listener.name.clear.sasl.enabled.mechanisms"] = "PLAIN,SCRAM-SHA-256,SCRAM-SHA-512";
  return {
    fixture: {
      tlsBroker: `127.0.0.1:${options.tlsPort}`,
      mutualTlsBroker: `127.0.0.1:${options.mutualTlsPort}`,
      mutualSaslBroker: `127.0.0.1:${options.mutualSaslPort}`,
      saslPlaintextBroker: `127.0.0.1:${options.saslPlaintextPort}`,
      username,
      password,
      passphrase,
      caPem: await readFile(options.brokerCaPath, "utf8"),
      certificatePem: await readFile(certificatePath, "utf8"),
      privateKeyPem: await readFile(keyPath, "utf8"),
    },
    listeners: `MTLS://127.0.0.1:${options.mutualTlsPort},MUTUAL://127.0.0.1:${options.mutualSaslPort},CLEAR://127.0.0.1:${options.saslPlaintextPort}`,
    properties,
    storageArguments: ["SCRAM-SHA-256", "SCRAM-SHA-512"].flatMap((mechanism) => [
      "--add-scram",
      `${mechanism}=[name=${username},password=${password}]`,
    ]),
  };
}
