/** Attempt every owned cleanup, preserving failure so callers cannot claim success. */
export async function disposeFixtureResources(
  actions: readonly (() => Promise<unknown>)[],
): Promise<void> {
  const failures: unknown[] = [];
  for (const action of actions) {
    try {
      await action();
    } catch (error) {
      failures.push(error);
    }
  }
  if (failures.length > 0) throw new AggregateError(failures, "AIO Kafka fixture cleanup failed.");
}
