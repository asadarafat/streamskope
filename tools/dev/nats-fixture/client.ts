import { NatsFixtureError } from "./ownership";

/** Bounds SDK PONG/message promises; the race also observes every late rejection. */
export async function boundedNatsOperation<T>(work: Promise<T>, timeout = 2_000): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([
      work,
      new Promise<never>((_resolve, reject) => {
        timer = setTimeout(
          () => reject(new NatsFixtureError("Local AIO NATS operation timed out.")),
          Math.max(1, timeout),
        );
      }),
    ]);
  } finally {
    clearTimeout(timer);
  }
}
