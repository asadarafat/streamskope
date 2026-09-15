import { mkdtemp, mkdir, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { deflateRawSync } from "node:zlib";

import { afterEach, describe, expect, it } from "vitest";

import {
  findSensitiveArtifactPaths,
  PRIVATE_RUNBOOK_ARTIFACT_SENTINEL,
  repositoryArtifactScanPolicy,
  RULE_EXPRESSION_ARTIFACT_SENTINEL,
} from "../../tools/sensitive-artifact-policy";

const temporaryDirectories: string[] = [];

async function temporaryDirectory(): Promise<string> {
  const directory = await mkdtemp(join(tmpdir(), "streamskope-artifact-scan-"));
  temporaryDirectories.push(directory);
  return directory;
}

function compressedZip(contents: string): Buffer {
  const fileName = Buffer.from("trace.trace");
  const plain = Buffer.from(contents);
  const compressed = deflateRawSync(plain);
  const localHeader = Buffer.alloc(30);
  localHeader.writeUInt32LE(0x04034b50, 0);
  localHeader.writeUInt16LE(20, 4);
  localHeader.writeUInt16LE(8, 8);
  localHeader.writeUInt32LE(compressed.byteLength, 18);
  localHeader.writeUInt32LE(plain.byteLength, 22);
  localHeader.writeUInt16LE(fileName.byteLength, 26);

  const centralHeader = Buffer.alloc(46);
  centralHeader.writeUInt32LE(0x02014b50, 0);
  centralHeader.writeUInt16LE(20, 4);
  centralHeader.writeUInt16LE(20, 6);
  centralHeader.writeUInt16LE(8, 10);
  centralHeader.writeUInt32LE(compressed.byteLength, 20);
  centralHeader.writeUInt32LE(plain.byteLength, 24);
  centralHeader.writeUInt16LE(fileName.byteLength, 28);

  const localRecord = Buffer.concat([localHeader, fileName, compressed]);
  const centralRecord = Buffer.concat([centralHeader, fileName]);
  const end = Buffer.alloc(22);
  end.writeUInt32LE(0x06054b50, 0);
  end.writeUInt16LE(1, 8);
  end.writeUInt16LE(1, 10);
  end.writeUInt32LE(centralRecord.byteLength, 12);
  end.writeUInt32LE(localRecord.byteLength, 16);
  return Buffer.concat([localRecord, centralRecord, end]);
}

afterEach(async () => {
  await Promise.all(
    temporaryDirectories
      .splice(0)
      .map((directory) => rm(directory, { force: true, recursive: true })),
  );
});

describe("sensitive artifact policy", () => {
  it("runs the artifact gate after every Playwright host project", async () => {
    const runner = await readFile(
      new URL("../../tools/run-playwright-e2e.mjs", import.meta.url),
      "utf8",
    );

    expect(runner).not.toContain('if (project !== "web")');
    expect(runner).toMatch(
      /child\.once\("exit",[\s\S]*?spawn\(process\.execPath, \[tsxCli, artifactScanner\]/u,
    );
  });

  it("owns every private sentinel and scans evidence plus packaged application payloads", async () => {
    const root = await temporaryDirectory();
    const packageRoot = join(root, "dist", "package");
    await mkdir(join(root, "aio-kafka"), { recursive: true });
    await mkdir(packageRoot, { recursive: true });
    await writeFile(
      join(root, "aio-kafka", "fixture.config.json"),
      JSON.stringify({
        oauthClientSecret: "fixture-secret-sentinel",
        seedPayload: '{"source":"private-payload-sentinel"}',
      }),
    );

    await expect(repositoryArtifactScanPolicy(root)).resolves.toEqual({
      roots: [
        join(root, "test-results"),
        join(root, "playwright-report"),
        join(root, "artifacts", "host-logs"),
        join(root, "dist", "renderer"),
        join(root, "dist", "electron"),
        join(root, "dist", "performance"),
        packageRoot,
        join(root, "dist", "release"),
      ],
      sensitiveValues: [
        "fixture-secret-sentinel",
        "fixture-secret-sentinel-invalid",
        '{"source":"private-payload-sentinel"}',
        RULE_EXPRESSION_ARTIFACT_SENTINEL,
        PRIVATE_RUNBOOK_ARTIFACT_SENTINEL,
      ],
    });
  });

  it("reports text and binary artifacts containing an exact submitted secret", async () => {
    const root = await temporaryDirectory();
    const nested = join(root, "nested");
    await mkdir(nested);
    const textArtifact = join(root, "report.json");
    const binaryArtifact = join(nested, "capture.bin");
    const traceArtifact = join(nested, "trace.zip");
    await writeFile(textArtifact, '{"detail":"submitted-secret"}');
    await writeFile(
      binaryArtifact,
      Buffer.concat([Buffer.from([0, 1, 2]), Buffer.from("submitted-secret"), Buffer.from([3])]),
    );
    const trace = compressedZip('{"detail":"submitted-secret"}');
    expect(trace.includes(Buffer.from("submitted-secret"))).toBe(false);
    await writeFile(traceArtifact, trace);

    await expect(findSensitiveArtifactPaths([root], ["submitted-secret"])).resolves.toEqual(
      [binaryArtifact, textArtifact, traceArtifact].sort(),
    );
  });

  it("accepts clean artifacts and roots that do not exist", async () => {
    const root = await temporaryDirectory();
    const cleanArtifact = join(root, "screenshot.png");
    await writeFile(cleanArtifact, Buffer.from([0, 1, 2, 3]));

    await expect(
      findSensitiveArtifactPaths([root, join(root, "missing")], ["submitted-secret"]),
    ).resolves.toEqual([]);
  });
});
