import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { execFile } from "node:child_process";
import { tmpdir } from "node:os";
import { promisify } from "node:util";
import { join } from "node:path";

import { beforeAll, describe, expect, it } from "vitest";

import { PROFILE_LIMITS } from "../../src/kafka/contracts";
import {
  KafkaTrustMaterialError,
  KafkaTruststorePasswordError,
  parseTrustMaterial,
  StreamSkopeTrustMaterialDecoder,
} from "../../src/kafka/engine";

const actualJksPath = join(process.cwd(), "node_modules/jks-js/examples/assets/truststore.jks");
const actualJksKeystorePath = join(
  process.cwd(),
  "node_modules/jks-js/examples/assets/keystore.jks",
);

let certificatePem: string;
let actualJks: string;
let actualJksKeystore: string;

function decode(
  input: Parameters<typeof parseTrustMaterial>[0],
): Promise<ReturnType<typeof parseTrustMaterial>> {
  return Promise.resolve().then(() => parseTrustMaterial(input));
}

beforeAll(async () => {
  const [jks, keystore] = await Promise.all([
    readFile(actualJksPath),
    readFile(actualJksKeystorePath),
  ]);
  actualJks = jks.toString("base64");
  actualJksKeystore = keystore.toString("base64");
  certificatePem = parseTrustMaterial({
    kind: "jks",
    material: actualJks,
    password: "password",
  }).caPem;
});

describe("StreamSkope trust-material decoder", () => {
  it("retains validity bounds for certificates omitted from the display limit", async () => {
    const directory = await mkdtemp(join(tmpdir(), "streamskope-certificate-evidence-"));
    const execute = promisify(execFile);
    try {
      const certificates: string[] = [];
      for (let serial = 1; serial <= 16; serial += 1) {
        const subject = `/CN=evidence-${serial}${serial === 1 ? `/OU=${"a".repeat(60)}`.repeat(10) : ""}`;
        const result = await execute(
          "openssl",
          [
            "req",
            "-x509",
            "-newkey",
            "ed25519",
            "-nodes",
            "-keyout",
            join(directory, "fixture.key"),
            "-days",
            "3650",
            "-subj",
            subject,
            "-set_serial",
            String(serial),
          ],
          { timeout: 5_000 },
        );
        certificates.push(result.stdout);
      }
      const result = await decode({
        kind: "pem",
        material: [...certificates, certificatePem].join("\n"),
      });
      expect(result.evidence).toMatchObject({
        count: 17,
        truncated: true,
        validity: { earliestExpiry: "2020-05-30T15:10:39.000Z" },
      });
      expect(result.evidence?.certificates).toHaveLength(16);
      expect(result.evidence?.certificates[0]?.subject).toHaveLength(512);
      expect(result.evidence?.certificates[0]?.issuer).toHaveLength(512);
      expect(result.evidence?.certificates[0]?.truncated).toBe(true);
      expect(
        result.evidence?.certificates.every(
          (certificate) => !certificate.subject.includes("jks-js"),
        ),
      ).toBe(true);
      const bundlePath = join(directory, "bundle.pem");
      const bundleOutput = join(directory, "bundle.p12");
      await writeFile(bundlePath, [...certificates, certificatePem].join("\n"));
      await execute(
        "openssl",
        [
          "pkcs12",
          "-export",
          "-nokeys",
          "-in",
          bundlePath,
          "-out",
          bundleOutput,
          "-passout",
          "pass:password",
        ],
        { timeout: 5_000 },
      );
      const pkcs12 = await decode({
        kind: "pkcs12",
        material: (await readFile(bundleOutput)).toString("base64"),
        password: "password",
      });
      expect(pkcs12.evidence).toMatchObject({
        count: 17,
        truncated: true,
        validity: { earliestExpiry: "2020-05-30T15:10:39.000Z" },
      });
      const leafPath = join(directory, "leaf.pem");
      await writeFile(leafPath, certificates[15] ?? "");
      await execute(
        "openssl",
        [
          "pkcs12",
          "-export",
          "-inkey",
          join(directory, "fixture.key"),
          "-in",
          leafPath,
          "-out",
          bundleOutput,
          "-passout",
          "pass:password",
        ],
        { timeout: 5_000 },
      );
      await expect(
        decode({
          kind: "pkcs12",
          material: (await readFile(bundleOutput)).toString("base64"),
          password: "password",
        }),
      ).rejects.toBeInstanceOf(KafkaTrustMaterialError);
    } finally {
      await rm(directory, { recursive: true, force: true });
    }
  });
  it("returns real bounded certificate evidence independently confirmed with OpenSSL", async () => {
    const result = await decode({ kind: "jks", material: actualJks, password: "password" });
    expect(result.evidence).toEqual({
      count: 1,
      truncated: false,
      validity: {
        earliestExpiry: "2020-05-30T15:10:39.000Z",
        latestStart: "2020-03-01T15:10:39.000Z",
      },
      certificates: [
        {
          subject: "C=jks-js\nST=jks-js\nL=jks-js\nO=lenchv\nOU=jks-js\nCN=jks-js",
          issuer: "C=jks-js\nST=jks-js\nL=jks-js\nO=lenchv\nOU=jks-js\nCN=jks-js",
          validFrom: "2020-03-01T15:10:39.000Z",
          validTo: "2020-05-30T15:10:39.000Z",
          fingerprint:
            "38:1A:16:4F:D1:C8:B6:C5:82:A1:54:BC:2A:05:6E:C9:69:78:BA:5F:46:71:91:0C:70:0A:CB:C7:85:3C:CD:62",
          truncated: false,
        },
      ],
    });
  });
  it("validates certificate-only PEM and deduplicates repeated certificates", async () => {
    const result = await decode({
      kind: "pem",
      material: `${certificatePem}\n${certificatePem}`,
    });

    expect(result.kind).toBe("pem");
    expect(result.caPem.match(/-----BEGIN CERTIFICATE-----/g)).toHaveLength(1);
    expect(result.caPem).not.toMatch(/PRIVATE KEY/);
  });

  it("detects and extracts an actual JKS-magic truststore by content", async () => {
    const result = await decode({
      kind: "jks",
      material: actualJks,
      password: "password",
    });

    expect(result.kind).toBe("jks");
    expect(result.caPem).toContain("-----BEGIN CERTIFICATE-----");
    expect(result.caPem).not.toMatch(/PRIVATE KEY/);
  });

  it("rejects a wrong truststore password without echoing it", async () => {
    const result = decode({
      kind: "jks",
      material: actualJks,
      password: "sentinel-wrong-password",
    });

    await expect(result).rejects.toBeInstanceOf(KafkaTruststorePasswordError);
    await expect(result).rejects.not.toThrow(/sentinel-wrong-password/);
  });

  it("rejects a keystore containing private-key material", async () => {
    await expect(
      decode({
        kind: "jks",
        material: actualJksKeystore,
        password: "password",
      }),
    ).rejects.toBeInstanceOf(KafkaTrustMaterialError);
  });

  it.each([
    ["malformed base64", "not base64"],
    [
      "binary input beyond 8 MiB",
      Buffer.alloc(PROFILE_LIMITS.trustBinaryBytes + 1).toString("base64"),
    ],
  ])("rejects %s before trust parsing", async (_label, material) => {
    await expect(
      decode({
        kind: "jks",
        material,
        password: "password",
      }),
    ).rejects.toBeInstanceOf(KafkaTrustMaterialError);
  });

  it("honors cancellation before synchronous parsing begins", async () => {
    const controller = new AbortController();
    controller.abort();

    await expect(
      new StreamSkopeTrustMaterialDecoder({ script: "must-not-start.cjs" }).decode(
        {
          kind: "jks",
          material: actualJks,
          password: "password",
        },
        controller.signal,
      ),
    ).rejects.toMatchObject({ name: "AbortError" });
  });
});
