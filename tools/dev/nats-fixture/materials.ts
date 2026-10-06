import { execFile } from "node:child_process";
import { randomBytes } from "node:crypto";
import { chmod, readFile, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { promisify } from "node:util";

const execute = promisify(execFile);

export interface NatsMaterial {
  readonly directory: string;
  readonly token: string;
  readonly caPem: string;
  readonly untrustedCaPem: string;
}

/** Generated credentials stay in one private owner directory and never enter CLI arguments. */
export async function prepareNatsMaterial(options: {
  readonly directory: string;
  readonly certificateDays: number;
  readonly certificate?: "dns-and-ip" | "dns-only" | undefined;
  readonly signal?: AbortSignal | undefined;
}): Promise<NatsMaterial> {
  const { directory, certificateDays } = options;
  if (!Number.isSafeInteger(certificateDays) || certificateDays < 1 || certificateDays > 365)
    throw new Error("NATS fixture certificate lifetime is invalid.");
  options.signal?.throwIfAborted();
  const run = async (
    command: string,
    arguments_: readonly string[],
    settings: { readonly timeout: number },
  ): Promise<void> => {
    await execute(command, [...arguments_], {
      ...settings,
      ...(options.signal === undefined ? {} : { signal: options.signal }),
    });
  };
  await chmod(directory, 0o700);
  await run(
    "openssl",
    [
      "req",
      "-x509",
      "-newkey",
      "rsa:2048",
      "-nodes",
      "-keyout",
      join(directory, "ca-key.pem"),
      "-out",
      join(directory, "ca.pem"),
      "-days",
      String(certificateDays),
      "-subj",
      "/CN=StreamSkope isolated NATS CA",
      "-addext",
      "basicConstraints=critical,CA:TRUE",
    ],
    { timeout: 30_000 },
  );
  await run(
    "openssl",
    [
      "req",
      "-new",
      "-newkey",
      "rsa:2048",
      "-nodes",
      "-keyout",
      join(directory, "server-key.pem"),
      "-out",
      join(directory, "server.csr"),
      "-subj",
      "/CN=localhost",
    ],
    { timeout: 30_000 },
  );
  await run(
    "openssl",
    [
      "req",
      "-x509",
      "-newkey",
      "rsa:2048",
      "-nodes",
      "-keyout",
      join(directory, "untrusted-ca-key.pem"),
      "-out",
      join(directory, "untrusted-ca.pem"),
      "-days",
      String(certificateDays),
      "-subj",
      "/CN=StreamSkope unrelated NATS CA",
      "-addext",
      "basicConstraints=critical,CA:TRUE",
    ],
    { timeout: 30_000 },
  );
  await writeFile(
    join(directory, "server.ext"),
    [
      "basicConstraints=critical,CA:FALSE",
      "keyUsage=critical,digitalSignature,keyEncipherment",
      "extendedKeyUsage=serverAuth",
      options.certificate === "dns-only"
        ? "subjectAltName=DNS:localhost"
        : "subjectAltName=DNS:localhost,IP:127.0.0.1",
      "",
    ].join("\n"),
    { mode: 0o600 },
  );
  await run(
    "openssl",
    [
      "x509",
      "-req",
      "-in",
      join(directory, "server.csr"),
      "-CA",
      join(directory, "ca.pem"),
      "-CAkey",
      join(directory, "ca-key.pem"),
      "-CAcreateserial",
      "-out",
      join(directory, "server.pem"),
      "-days",
      String(certificateDays),
      "-sha256",
      "-extfile",
      join(directory, "server.ext"),
    ],
    { timeout: 30_000 },
  );
  await Promise.all(
    [
      "ca-key.pem",
      "ca.pem",
      "untrusted-ca-key.pem",
      "untrusted-ca.pem",
      "server-key.pem",
      "server.csr",
      "server.pem",
      "ca.srl",
    ].map((file) => chmod(join(directory, file), 0o600)),
  );
  const token = randomBytes(32).toString("hex");
  await writeFile(join(directory, "token"), token, { mode: 0o600 });
  return {
    directory,
    token,
    caPem: await readFile(join(directory, "ca.pem"), "utf8"),
    untrustedCaPem: await readFile(join(directory, "untrusted-ca.pem"), "utf8"),
  };
}

export async function writeNatsMaterialConfig(
  material: NatsMaterial,
  options: {
    readonly name: string;
    readonly host: "127.0.0.1" | "0.0.0.0";
    readonly port: number;
    readonly authentication?: "token" | "anonymous-restricted" | undefined;
  },
): Promise<void> {
  if (
    !/^[a-z][a-z0-9-]{1,100}$/u.test(options.name) ||
    !Number.isSafeInteger(options.port) ||
    options.port < 1 ||
    options.port > 65_535
  )
    throw new Error("NATS fixture configuration is invalid.");
  await writeFile(
    join(material.directory, "nats.conf"),
    [
      `server_name: "${options.name}"`,
      `host: "${options.host}"`,
      `port: ${options.port}`,
      "max_payload: 1048576",
      'write_deadline: "2s"',
      "debug: false",
      "trace: false",
      ...(options.authentication === "anonymous-restricted"
        ? [
            'no_auth_user: "fixture-anonymous"',
            'authorization { users: [{user: "fixture-anonymous", permissions: {publish: ">", subscribe: "qualification.allowed"}}], timeout: 2 }',
          ]
        : [`authorization { token: "${material.token}", timeout: 2 }`]),
      'tls { cert_file: "/fixture/server.pem", key_file: "/fixture/server-key.pem", timeout: 2 }',
      "",
    ].join("\n"),
    { mode: 0o600 },
  );
}
