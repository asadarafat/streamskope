import type { NatsProfileStore } from "../../../src/features/nats/application/profile-types";

import { loadNatsFixtureRecord, natsFixtureConnection } from "./ownership";

export async function prepareLocalNatsDevelopmentProfile(
  store: NatsProfileStore,
  repositoryRoot: string,
): Promise<"seeded" | "unchanged" | "unavailable"> {
  if ((await store.load()).length > 0) return "unchanged";
  const record = await loadNatsFixtureRecord(repositoryRoot);
  if (record === undefined) return "unavailable";
  const connection = await natsFixtureConnection(record);
  const timestamp = new Date().toISOString();
  await store.save([
    {
      ...connection,
      id: "local-aio-nats",
      revision: 1,
      name: "Local AIO NATS",
      createdAt: timestamp,
      updatedAt: timestamp,
    },
  ]);
  return "seeded";
}
