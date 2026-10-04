import { mkdtemp, readFile, rm, stat, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { expect, it } from "vitest";

import { AtomicObservationFileStore } from "../../src/platform/node/kafka-observation-file-store";
import type { ObservationHistory } from "../../src/features/kafka/contracts/observations";
import { parseObservationHistory } from "../../src/features/kafka/contracts/observation-validation";

it("retains only validated bounded history across restart with private permissions and preserves corrupt bytes", async () => {
  const dir = await mkdtemp(join(tmpdir(), "streamskope-observations-")),
    path = join(dir, "history", "observations.json");
  try {
    const store = new AtomicObservationFileStore(path);
    expect(await store.load()).toEqual({ schemaVersion: 1, series: [] });
    const history: ObservationHistory = {
      schemaVersion: 1,
      series: [
        {
          clusterId: "c",
          topicId: "t",
          topic: "events",
          groupId: "group",
          samples: [
            {
              id: "sample",
              segmentId: "session",
              startedAt: 1000,
              observedAt: 1020,
              source: "kafka-api",
              requestMs: 20,
              providerCalls: 2,
              state: "ready",
              groupState: "stable",
              members: 1,
              brokerCount: 1,
              controllerKnown: true,
              groupCoverage: "complete",
              partitions: [
                {
                  partition: 0,
                  leader: 1,
                  replicas: 1,
                  inSyncReplicas: 1,
                  endOffset: "50",
                  committedOffset: "40",
                  lag: "10",
                },
              ],
              alerts: [],
              records: null,
            },
          ],
        },
      ],
    };
    await store.commit(history);
    expect(await new AtomicObservationFileStore(path).load()).toEqual(
      parseObservationHistory(history),
    );
    if (process.platform !== "win32") expect((await stat(path)).mode & 0o777).toBe(0o600);
    await writeFile(path, '{"schemaVersion":999,"private":"do not overwrite"}');
    await expect(store.load()).rejects.toThrow("not been replaced");
    expect(await readFile(path, "utf8")).toContain("do not overwrite");
    await expect(
      store.commit({ ...history, series: [...history.series, ...history.series] }),
    ).rejects.toThrow();
    expect(await readFile(path, "utf8")).toContain("do not overwrite");
    await store.commit({ schemaVersion: 1, series: [] });
    expect((await store.load()).series).toEqual([]);
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});
