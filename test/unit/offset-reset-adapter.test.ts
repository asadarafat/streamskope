import { Consumer } from "@platformatic/kafka";
import { afterEach, expect, it, vi } from "vitest";

import { PlatformaticOffsetReset } from "../../src/features/kafka/engine/platformatic-offset-reset";

afterEach(() => vi.restoreAllMocks());

const input = {
  groupId: "recovery",
  targets: [{ topic: "events", partition: 0, offset: "2" }],
};

function setup(): PlatformaticOffsetReset {
  vi.spyOn(Consumer.prototype, "consume").mockRejectedValue(new Error("Read permission denied"));
  return new PlatformaticOffsetReset(
    { brokers: ["127.0.0.1:1"], tlsEnabled: false, operationTimeoutMs: 10_000 },
    new AbortController().signal,
  );
}

it("waits for preview consumer cleanup before reporting unavailable examples", async () => {
  const reset = setup();
  let completeCleanup!: () => void;
  const cleanup = new Promise<void>((resolve) => {
    completeCleanup = resolve;
  });
  const promiseConsumer: { close(force?: boolean): Promise<void> } = Consumer.prototype;
  const closing = vi.spyOn(promiseConsumer, "close").mockReturnValue(cleanup);
  let settled = false;
  const pending = reset.examples(input).finally(() => {
    settled = true;
  });
  await vi.waitFor(() => expect(closing).toHaveBeenCalledWith(true));
  expect(settled).toBe(false);
  completeCleanup();
  expect(await pending).toEqual({ examples: [], exampleStatus: "unavailable" });
});

it("rejects the preview when consumer cleanup cannot be confirmed", async () => {
  const reset = setup();
  vi.spyOn(Consumer.prototype, "close").mockRejectedValue(new Error("Consumer close failed"));
  await expect(reset.examples(input)).rejects.toThrow("Consumer close failed");
});
