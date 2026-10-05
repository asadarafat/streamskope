import { createServer } from "node:http";
import { resolve } from "node:path";

import {
  parseCorrelatedNatsResponse,
  parseNatsCommand,
  parseNatsEvent,
  type NatsCommandName,
  type NatsCommandResponse,
  type NatsEvent,
  type NatsRecord,
} from "../../src/features/nats/contracts";
import { launchWebDevelopment, type RunningWebDevelopment } from "../../src/platform/dev-host";
import { createKafkaBackend } from "../../src/platform/node/kafka-backend";
import { createKafkaProviderEndpoint } from "../../src/platform/node/kafka-provider";
import { createNatsBackend } from "../../src/platform/node/nats-backend";
import { createNatsProviderEndpoint } from "../../src/platform/node/nats-provider";
import {
  ProviderHostRegistry,
  type ProviderWireEndpoint,
} from "../../src/platform/node/provider-host";

import { startNatsFixture, type NatsFixture } from "./nats-fixture";

export interface NatsBrowserReceipt {
  readonly command: NatsCommandName;
  readonly admitted: number;
  readonly completed: number;
  readonly response: NatsCommandResponse;
}

export interface NatsBrowserFixture {
  readonly server: NatsFixture;
  readonly launch: RunningWebDevelopment;
  readonly events: readonly NatsEvent[];
  readonly receipts: readonly NatsBrowserReceipt[];
  readonly sensitiveValues: readonly string[];
  containsSensitive(value: unknown): boolean;
  publicBoundarySafe(): boolean;
  records(): readonly NatsRecord[];
  dispose(): Promise<void>;
}

async function reservePort(): Promise<number> {
  const server = createServer();
  try {
    await new Promise<void>((accept, reject) => {
      server.once("error", reject);
      server.listen(0, "127.0.0.1", () => {
        server.off("error", reject);
        accept();
      });
    });
    const address = server.address();
    if (address === null || typeof address === "string")
      throw new Error("The browser fixture did not reserve a loopback port.");
    return address.port;
  } finally {
    if (server.listening)
      await new Promise<void>((accept, reject) => {
        server.close((error) => (error === undefined ? accept() : reject(error)));
      });
  }
}

/** Real product entry, real default backends, owned server, and public receipts only. */
export async function startNatsBrowserFixture(
  authentication: "token" | "anonymous-restricted" = "token",
): Promise<NatsBrowserFixture> {
  let server: NatsFixture | undefined;
  let providers: ProviderHostRegistry | undefined;
  const ownedEndpoints: ProviderWireEndpoint[] = [];
  let launch: RunningWebDevelopment | undefined;
  let unsubscribe: (() => void) | undefined;
  let disposeWork: Promise<void> | undefined;
  const dispose = (): Promise<void> => {
    disposeWork ??= Promise.resolve().then(async (): Promise<void> => {
      // Attempt both independently: a host cleanup failure cannot strand the owned server.
      const cleanup = await Promise.allSettled([
        Promise.resolve().then(async (): Promise<void> => {
          if (launch !== undefined) await launch.close();
          else if (providers !== undefined) await providers.shutdown();
          else {
            const cleanup = await Promise.allSettled(
              ownedEndpoints.map((endpoint) => endpoint.shutdown()),
            );
            if (cleanup.some((result) => result.status === "rejected"))
              throw new Error("The owned browser provider endpoints did not stop.");
          }
        }),
        Promise.resolve().then(() => server?.dispose()),
      ]);
      unsubscribe?.();
      if (cleanup.some((result) => result.status === "rejected"))
        throw new Error("Owned NATS browser fixture cleanup could not be confirmed.");
    });
    return disposeWork;
  };

  try {
    server = await startNatsFixture({ authentication });
    const activeServer = server;
    // Certificate lines also detect PEM serialized with escaped newlines in JSON artifacts.
    const sensitiveValues = [
      activeServer.token,
      `${activeServer.token}-invalid`,
      activeServer.caPem,
      activeServer.untrustedCaPem,
      ...[activeServer.caPem, activeServer.untrustedCaPem].flatMap((pem) =>
        pem.split("\n").filter((line) => line.length >= 32 && !line.startsWith("-----")),
      ),
    ];
    const containsSensitive = (value: unknown): boolean => {
      const text = typeof value === "string" ? value : JSON.stringify(value);
      return text !== undefined && sensitiveValues.some((secret) => text.includes(secret));
    };
    let leaked = false;
    let order = 0;
    const events: NatsEvent[] = [];
    const receipts: NatsBrowserReceipt[] = [];
    const actualEndpoint = createNatsProviderEndpoint(createNatsBackend());
    ownedEndpoints.push(actualEndpoint);
    unsubscribe = actualEndpoint.subscribe((wire): void => {
      leaked ||= containsSensitive(wire);
      if (!containsSensitive(wire)) events.push(parseNatsEvent(wire));
    });
    const observedEndpoint: ProviderWireEndpoint = {
      ...actualEndpoint,
      dispatch: async (wire): Promise<unknown> => {
        // The parsed submitted command lives only in this call, never in recorded evidence.
        const submitted = parseNatsCommand(wire);
        const admitted = ++order;
        const response = await actualEndpoint.dispatch(wire);
        leaked ||= containsSensitive(response);
        if (containsSensitive(response))
          throw new Error("The NATS public command boundary exposed private profile material.");
        receipts.push({
          command: submitted.command,
          admitted,
          completed: ++order,
          response: parseCorrelatedNatsResponse(response, submitted),
        });
        return response;
      },
    };
    const kafkaEndpoint = createKafkaProviderEndpoint(createKafkaBackend());
    ownedEndpoints.push(kafkaEndpoint);
    providers = new ProviderHostRegistry([kafkaEndpoint, observedEndpoint]);
    launch = await launchWebDevelopment({
      providers,
      hostPort: await reservePort(),
      rendererPort: await reservePort(),
      rendererRoot: resolve(process.cwd()),
    });
    return {
      server: activeServer,
      launch,
      events,
      receipts,
      sensitiveValues,
      containsSensitive,
      publicBoundarySafe: () => !leaked,
      records: () =>
        events.flatMap((event) => (event.event === "records.batch" ? event.payload.records : [])),
      dispose,
    };
  } catch {
    await dispose();
    throw new Error("The real NATS browser fixture could not start; existing labs were unchanged.");
  }
}
