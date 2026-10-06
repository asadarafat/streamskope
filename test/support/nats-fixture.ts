import {
  startNatsServer,
  type NatsServer,
  type NatsServerOptions,
} from "../../tools/dev/nats-fixture/server";

export type NatsFixture = Omit<NatsServer, "ownership">;

/** Isolated owned server using the same pinned token/TLS definition as Local AIO NATS. */
export function startNatsFixture(
  options: Pick<NatsServerOptions, "certificate" | "authentication" | "network"> = {},
): Promise<NatsFixture> {
  return startNatsServer(options);
}
