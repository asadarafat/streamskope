import { createHash } from "node:crypto";
import { lstat, mkdtemp, readFile, readdir, rename, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { afterEach, describe, expect, it, vi } from "vitest";

import { NodeRecordExportArtifacts } from "../../src/platform/node/record-export-artifacts";
import { NodeRecordExportSaver } from "../../src/platform/node/record-export-save";
import { artifactReceipt, readArtifact, sealedArtifact } from "../support/record-export-artifact";

const cleanup: Array<() => Promise<void>> = [];
afterEach(async () => {
  for (const close of cleanup.splice(0).reverse()) await close();
});
async function fixture(
  options: ConstructorParameters<typeof NodeRecordExportArtifacts>[0] = {},
): Promise<{ root: string; store: NodeRecordExportArtifacts }> {
  const root = await mkdtemp(join(tmpdir(), "streamskope-artifact-test-"));
  cleanup.push(() => rm(root, { recursive: true, force: true }));
  const store = new NodeRecordExportArtifacts({ temporaryRoot: root, ...options });
  cleanup.push(async () => {
    store.revoke();
    await store.drain();
  });
  return { root, store };
}

describe("encrypted transient record exports", () => {
  it("streams exact authenticated bytes and an independently hashable receipt from private encrypted files", async () => {
    const { root, store } = await fixture();
    expect(await readdir(root)).toEqual([]);
    const text = '{"secret":"private-export-sentinel","value":"' + "é".repeat(90_000) + '"}\n';
    const { artifact } = await sealedArtifact(store, text);
    const directory = join(root, (await readdir(root))[0]!);
    expect((await lstat(directory)).mode & 0o777).toBe(0o700);
    for (const file of await readdir(directory)) {
      const path = join(directory, file);
      expect((await lstat(path)).mode & 0o777).toBe(0o600);
      expect((await readFile(path)).includes(Buffer.from("private-export-sentinel"))).toBe(false);
    }
    const bytes = await readArtifact(store.delivery, {
      artifactId: artifact.artifactId,
      part: "data",
    });
    expect(bytes.toString()).toBe(text);
    expect(createHash("sha256").update(bytes).digest("hex")).toBe(artifact.output.sha256);
    const receipt = await readArtifact(store.delivery, {
      artifactId: artifact.artifactId,
      part: "receipt",
    });
    expect(receipt.byteLength).toBe(artifact.receiptBytes);
    expect(createHash("sha256").update(receipt).digest("hex")).toBe(artifact.receiptSha256);
    expect(JSON.parse(receipt.toString())).toMatchObject({
      schema: "streamskope.record-export/v1",
      output: artifact.output,
    });
    expect(
      await readArtifact(store.delivery, { artifactId: artifact.artifactId, part: "data" }),
    ).toEqual(bytes);
  });

  it.each(["tamper", "truncate", "append"])(
    "refuses %s before releasing the final plaintext chunk",
    async (mode) => {
      const { root, store } = await fixture();
      const { artifact } = await sealedArtifact(store);
      const path = join(root, (await readdir(root))[0]!, "data.encrypted");
      const bytes = await readFile(path);
      if (mode === "tamper") bytes[20] = bytes[20]! ^ 1;
      await writeFile(
        path,
        mode === "truncate"
          ? bytes.subarray(0, bytes.length - 1)
          : mode === "append"
            ? Buffer.concat([bytes, Buffer.from([0])])
            : bytes,
      );
      const delivered: Uint8Array[] = [];
      await expect(
        store.delivery.withDownload(
          { artifactId: artifact.artifactId, part: "data" },
          {
            signal: new AbortController().signal,
            assertCurrent: () => undefined,
          },
          async (chunks) => {
            for await (const chunk of chunks) delivered.push(chunk);
          },
        ),
      ).rejects.toThrow();
      expect(delivered).toEqual([]);
    },
  );

  it("handles short writes without acknowledging partial bytes and poisons a failed row", async () => {
    let fail = false;
    const { store } = await fixture({
      write: async (file, bytes, offset) => {
        if (fail) return 0;
        return (await file.write(bytes, offset, Math.min(7, bytes.byteLength - offset), null))
          .bytesWritten;
      },
    });
    const successful = await sealedArtifact(store);
    expect(
      (
        await readArtifact(store.delivery, {
          artifactId: successful.artifact.artifactId,
          part: "data",
        })
      ).toString(),
    ).toBe(successful.text);
    await successful.sink.discard();
    const sink = await store.create({
      jobId: "failed-row",
      format: "csv",
      maximumBytes: 1000,
      lifetimeMs: 1000,
      signal: new AbortController().signal,
      assertCurrent: () => undefined,
    });
    fail = true;
    await expect(sink.write(Buffer.from("value\n"))).rejects.toThrow("could not be written");
    await expect(sink.seal(artifactReceipt(6))).rejects.toThrow();
    await sink.discard();
  });

  it("revokes reads immediately, drains active consumers and permits a fresh generation", async () => {
    const { store, root } = await fixture();
    const { artifact } = await sealedArtifact(store);
    let revoked = false;
    const reading = store.delivery.withDownload(
      { artifactId: artifact.artifactId, part: "data" },
      {
        signal: new AbortController().signal,
        assertCurrent: () => undefined,
      },
      async (_chunks, signal) =>
        new Promise<void>((resolve) =>
          signal.addEventListener(
            "abort",
            () => {
              revoked = true;
              resolve();
            },
            { once: true },
          ),
        ),
    );
    const failed = expect(reading).rejects.toThrow();
    await Promise.resolve();
    store.revoke();
    expect(revoked).toBe(true);
    expect(() =>
      store.delivery.describe({ artifactId: artifact.artifactId, part: "data" }),
    ).toThrow();
    await failed;
    await store.drain();
    expect(await readdir(root)).toEqual([]);
    const next = await sealedArtifact(store, "next\n");
    expect(next.artifact.artifactId).not.toBe(artifact.artifactId);
  });

  it("bounds concurrent downloads and expires all artifact parts", async () => {
    const { store } = await fixture();
    const { artifact } = await sealedArtifact(store, "value\n", 100);
    const reads = [1, 2].map(() =>
      store.delivery.withDownload(
        { artifactId: artifact.artifactId, part: "data" },
        {
          signal: new AbortController().signal,
          assertCurrent: () => undefined,
        },
        async (_chunks, signal) =>
          new Promise<void>((resolve) =>
            signal.addEventListener("abort", () => resolve(), { once: true }),
          ),
      ),
    );
    const results = Promise.allSettled(reads);
    await expect(
      readArtifact(store.delivery, { artifactId: artifact.artifactId, part: "receipt" }),
    ).rejects.toThrow("Two export");
    await vi.waitFor(() =>
      expect(() =>
        store.delivery.describe({ artifactId: artifact.artifactId, part: "receipt" }),
      ).toThrow(),
    );
    expect((await results).every((result) => result.status === "rejected")).toBe(true);
    await store.drain();
  });

  it("retains failed cleanup debt and allows an explicit retry", async () => {
    let blocked = true;
    const { store, root } = await fixture({
      remove: async (directory) => {
        if (blocked) throw new Error("fixture removal denied");
        await rm(directory, { recursive: true, force: true });
      },
    });
    const { sink, artifact } = await sealedArtifact(store);
    await expect(sink.discard()).rejects.toThrow();
    await expect(store.drain()).rejects.toThrow("cleanup");
    expect(() =>
      store.delivery.describe({ artifactId: artifact.artifactId, part: "data" }),
    ).toThrow();
    expect(await readdir(root)).toHaveLength(1);
    blocked = false;
    await sink.discard();
    await store.drain();
    expect(await readdir(root)).toEqual([]);
    await sealedArtifact(store, "new generation\n");
  });

  it("bounds a stalled download independently of the artifact lifetime", async () => {
    const { store } = await fixture({ downloadTimeoutMs: 20 });
    const { artifact } = await sealedArtifact(store, "retained\n", 60000);
    await expect(
      store.delivery.withDownload(
        { artifactId: artifact.artifactId, part: "data" },
        {
          signal: new AbortController().signal,
          assertCurrent: () => undefined,
        },
        async (_chunks, signal) =>
          new Promise<void>((resolve) =>
            signal.addEventListener("abort", () => resolve(), { once: true }),
          ),
      ),
    ).rejects.toThrow();
    expect(store.delivery.describe({ artifactId: artifact.artifactId, part: "data" }).bytes).toBe(
      9,
    );
    expect(
      (
        await readArtifact(store.delivery, { artifactId: artifact.artifactId, part: "data" })
      ).toString(),
    ).toBe("retained\n");
  });

  it("does not adopt another process's stale encrypted files", async () => {
    const { store, root } = await fixture();
    const { artifact } = await sealedArtifact(store);
    const restarted = new NodeRecordExportArtifacts({ temporaryRoot: root });
    expect(() =>
      restarted.delivery.describe({ artifactId: artifact.artifactId, part: "data" }),
    ).toThrow();
    await restarted.drain();
    expect((await readdir(root)).length).toBe(1);
    expect(
      (await readArtifact(store.delivery, { artifactId: artifact.artifactId, part: "data" }))
        .length,
    ).toBeGreaterThan(0);
  });

  it("retains a failed download descriptor close as cleanup debt instead of admitting more reads", async () => {
    let blocked = false;
    const { store } = await fixture({
      close: async (file): Promise<void> => {
        if (blocked) throw new Error("fixture close denied");
        await file.close();
      },
    });
    const { sink, artifact } = await sealedArtifact(store);
    blocked = true;
    await expect(
      readArtifact(store.delivery, { artifactId: artifact.artifactId, part: "data" }),
    ).rejects.toThrow("cleanup");
    expect(() =>
      store.delivery.describe({ artifactId: artifact.artifactId, part: "data" }),
    ).toThrow();
    await expect(store.drain()).rejects.toThrow("cleanup");
    blocked = false;
    await sink.discard();
    await store.drain();
  });

  it("keeps native partial-file ownership through a close failure and explicit cleanup retry", async () => {
    const { store, root } = await fixture();
    const { artifact } = await sealedArtifact(store);
    let blocked = true;
    const saver = new NodeRecordExportSaver(store.delivery, {
      close: async (file): Promise<void> => {
        if (blocked) throw new Error("fixture native close denied");
        await file.close();
      },
    });
    cleanup.push(() => saver.drain());
    const destination = join(root, "preserved.jsonl");
    await writeFile(destination, "existing file");
    const reference = { artifactId: artifact.artifactId, part: "data" } as const;
    const authority = {
      signal: new AbortController().signal,
      assertCurrent: (): void => undefined,
    };
    await expect(saver.save(reference, destination, authority)).rejects.toThrow("cleanup");
    expect(await readFile(destination, "utf8")).toBe("existing file");
    expect((await readdir(root)).some((name) => name.endsWith(".partial"))).toBe(true);
    await expect(saver.drain()).rejects.toThrow("cleanup");
    blocked = false;
    await saver.drain();
    expect((await readdir(root)).some((name) => name.endsWith(".partial"))).toBe(false);
  });

  it("saves through a private sibling temporary file and preserves existing output on corruption", async () => {
    const { store, root } = await fixture();
    const { artifact, text } = await sealedArtifact(store);
    const reference = { artifactId: artifact.artifactId, part: "data" } as const;
    const destination = join(root, "chosen.jsonl");
    const authority = {
      signal: new AbortController().signal,
      assertCurrent: (): void => undefined,
    };
    const saver = new NodeRecordExportSaver(store.delivery);
    cleanup.push(() => saver.drain());
    await saver.save(reference, destination, authority);
    expect(await readFile(destination, "utf8")).toBe(text);
    expect((await lstat(destination)).mode & 0o777).toBe(0o600);
    const directory = (await readdir(root)).find((name) => name.startsWith("streamskope-export-"))!;
    const encrypted = join(root, directory, "data.encrypted");
    const bytes = await readFile(encrypted);
    bytes[20] = bytes[20]! ^ 1;
    await writeFile(encrypted, bytes);
    await expect(saver.save(reference, destination, authority)).rejects.toThrow();
    expect(await readFile(destination, "utf8")).toBe(text);
    expect((await readdir(root)).some((name) => name.endsWith(".partial"))).toBe(false);
  });

  it("preserves the confirmed native Save result when revocation races an admitted rename", async () => {
    const { store, root } = await fixture();
    const { artifact, text } = await sealedArtifact(store);
    const revoked = new AbortController();
    const saver = new NodeRecordExportSaver(store.delivery, {
      rename: async (temporary, destination): Promise<void> => {
        await rename(temporary, destination);
        revoked.abort();
      },
    });
    cleanup.push(() => saver.drain());
    const destination = join(root, "committed.jsonl");
    await writeFile(destination, "previous output");
    await expect(
      saver.save({ artifactId: artifact.artifactId, part: "data" }, destination, {
        signal: revoked.signal,
        assertCurrent: () => revoked.signal.throwIfAborted(),
      }),
    ).resolves.toBeUndefined();
    expect(revoked.signal.aborted).toBe(true);
    expect(await readFile(destination, "utf8")).toBe(text);
    expect((await readdir(root)).some((name) => name.endsWith(".partial"))).toBe(false);
  });
});
