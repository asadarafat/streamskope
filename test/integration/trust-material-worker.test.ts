import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { beforeAll, describe, expect, it } from "vitest";

import { PROFILE_LIMITS } from "../../src/kafka/contracts";
import { KafkaTrustMaterialError, StreamSkopeTrustMaterialDecoder } from "../../src/kafka/engine";
import { createHostTrustMaterialDecoder } from "../../src/main";

let actualJks: string;

beforeAll(async () => {
  actualJks = (
    await readFile(join(process.cwd(), "node_modules/jks-js/examples/assets/truststore.jks"))
  ).toString("base64");
});

function after(milliseconds: number): Promise<"timer"> {
  return new Promise((resolve) => {
    setTimeout(() => {
      resolve("timer");
    }, milliseconds);
  });
}

describe("host trust-material worker", () => {
  it("keeps the host event loop responsive while bounded binary parsing runs", async () => {
    const decoder = createHostTrustMaterialDecoder();
    const operation = decoder
      .decode({
        kind: "jks",
        material: Buffer.alloc(PROFILE_LIMITS.trustBinaryBytes).toString("base64"),
        password: "password",
      })
      .then(
        () => "decode" as const,
        () => "decode" as const,
      );

    await expect(Promise.race([operation, after(10)])).resolves.toBe("timer");
    await expect(operation).resolves.toBe("decode");
  });

  it("returns valid worker results and reconstructs bounded parser failures", async () => {
    const decoder = createHostTrustMaterialDecoder();

    const result = await decoder.decode({
      kind: "jks",
      material: actualJks,
      password: "password",
    });
    expect(result.caPem).toContain("-----BEGIN CERTIFICATE-----");
    expect(result.kind).toBe("jks");
    expect(result.evidence).toMatchObject({
      count: 1,
      validity: {
        earliestExpiry: "2020-05-30T15:10:39.000Z",
        latestStart: "2020-03-01T15:10:39.000Z",
      },
      certificates: [
        {
          subject: "C=jks-js\nST=jks-js\nL=jks-js\nO=lenchv\nOU=jks-js\nCN=jks-js",
          truncated: false,
        },
      ],
    });
    await expect(
      decoder.decode({
        kind: "jks",
        material: "not base64",
        password: "sentinel-never-echo",
      }),
    ).rejects.toBeInstanceOf(KafkaTrustMaterialError);
  });

  it("terminates obsolete parsing when cancellation arrives after dispatch", async () => {
    const decoder = createHostTrustMaterialDecoder();
    const controller = new AbortController();
    const operation = decoder.decode(
      {
        kind: "jks",
        material: Buffer.alloc(PROFILE_LIMITS.trustBinaryBytes).toString("base64"),
        password: "password",
      },
      controller.signal,
    );
    controller.abort();

    await expect(operation).rejects.toMatchObject({ name: "AbortError" });
  });

  it("rejects instead of hanging when a worker exits without a reply", async () => {
    const temporaryDirectory = await mkdtemp(join(tmpdir(), "streamskope-worker-test-"));
    const workerPath = join(temporaryDirectory, "empty-worker.cjs");
    await writeFile(workerPath, "process.exit(0);\n", "utf8");
    const decoder = new StreamSkopeTrustMaterialDecoder({
      execArgv: [],
      script: workerPath,
    });

    try {
      const operation = decoder.decode({
        kind: "jks",
        material: actualJks,
        password: "password",
      });

      await expect(operation).rejects.toBeInstanceOf(KafkaTrustMaterialError);
    } finally {
      await rm(temporaryDirectory, { force: true, recursive: true });
    }
  });
});
