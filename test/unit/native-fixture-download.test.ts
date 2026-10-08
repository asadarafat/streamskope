import { createServer } from "node:http";

import { expect, it } from "vitest";

import { fetchNativeKafkaArchive } from "../support/native-kafka-fixture";

async function withDownloads(
  responses: readonly { readonly status: number; readonly body: string }[],
  check: (fetchArchive: typeof fetch, requests: () => number) => Promise<void>,
): Promise<void> {
  let requests = 0;
  const server = createServer((_request, response) => {
    const supplied = responses[requests++];
    response.writeHead(supplied?.status ?? 500).end(supplied?.body ?? "unexpected request");
  });
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", resolve);
  });
  const address = server.address();
  if (address === null || typeof address === "string") throw new Error("Missing test address.");
  const fetchArchive: typeof fetch = (_input, options) =>
    fetch(`http://127.0.0.1:${address.port}/archive`, options);
  try {
    await check(fetchArchive, () => requests);
  } finally {
    server.closeAllConnections();
    await new Promise<void>((resolve, reject) =>
      server.close((error) => (error ? reject(error) : resolve())),
    );
  }
}

it("uses the first available archive without contacting another endpoint", async () => {
  await withDownloads(
    [{ status: 200, body: "verified separately by the downloader" }],
    async (fetchArchive, requests) => {
      const response = await fetchNativeKafkaArchive(fetchArchive);
      expect(await response.text()).toBe("verified separately by the downloader");
      expect(requests()).toBe(1);
    },
  );
});

it("can retrieve an archived release when the current download is unavailable", async () => {
  await withDownloads(
    [
      { status: 404, body: "release moved" },
      { status: 200, body: "archived bytes" },
    ],
    async (fetchArchive, requests) => {
      const response = await fetchNativeKafkaArchive(fetchArchive);
      expect(await response.text()).toBe("archived bytes");
      expect(requests()).toBe(2);
    },
  );
});

it("fails when neither endpoint can provide an archive", async () => {
  await withDownloads(
    [
      { status: 503, body: "private upstream error" },
      { status: 404, body: "private archive error" },
    ],
    async (fetchArchive, requests) => {
      await expect(fetchNativeKafkaArchive(fetchArchive)).rejects.toThrow(
        "Unable to download the pinned Kafka fixture.",
      );
      expect(requests()).toBe(2);
    },
  );
});
