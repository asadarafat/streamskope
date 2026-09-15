import { isIP } from "node:net";
import { connect as connectTcp, type Socket } from "node:net";
import { performance } from "node:perf_hooks";
import { connect as connectTls, type TLSSocket } from "node:tls";

import type { KafkaLatencyProbeIssue } from "../contracts";

import type { KafkaClientInput } from "./types";

export interface KafkaLatencyNetworkResult {
  readonly endpoint: string;
  readonly issues: readonly KafkaLatencyProbeIssue[];
  readonly tcpConnectMs: number | null;
  readonly tlsHandshakeMs: number | null;
}

function endpoint(input: string): { readonly host: string; readonly port: number } {
  let url: URL;
  try {
    url = new URL(`tcp://${input}`);
  } catch (error) {
    throw new Error("The primary Kafka broker endpoint is invalid.", { cause: error });
  }
  const port = Number(url.port);
  if (!Number.isSafeInteger(port) || port < 1 || port > 65_535 || url.hostname.length === 0) {
    throw new Error("The primary Kafka broker endpoint must include a valid host and port.");
  }
  return { host: url.hostname.replaceAll(/^\[|\]$/gu, ""), port };
}

function socketDuration(
  create: () => Socket | TLSSocket,
  successEvent: "connect" | "secureConnect",
  signal: AbortSignal,
): Promise<number> {
  if (signal.aborted) {
    return Promise.reject(new DOMException("Network probe cancelled.", "AbortError"));
  }
  return new Promise<number>((resolve, reject) => {
    const started = performance.now();
    const socket = create();
    let settled = false;
    const finish = (error?: unknown): void => {
      if (settled) {
        return;
      }
      settled = true;
      signal.removeEventListener("abort", onAbort);
      socket.removeAllListeners();
      socket.destroy();
      if (error === undefined) {
        resolve(performance.now() - started);
      } else {
        reject(
          error instanceof Error
            ? error
            : new Error("The Kafka network probe rejected with a non-error value.", {
                cause: error,
              }),
        );
      }
    };
    const onAbort = (): void => {
      finish(new DOMException("Network probe cancelled.", "AbortError"));
    };
    signal.addEventListener("abort", onAbort, { once: true });
    socket.once(successEvent, () => {
      finish();
    });
    socket.once("error", finish);
  });
}

export async function probeKafkaNetwork(
  input: KafkaClientInput,
  signal: AbortSignal,
): Promise<KafkaLatencyNetworkResult> {
  const primary = input.brokers[0];
  if (primary === undefined) {
    throw new Error("A latency probe requires a primary Kafka broker.");
  }
  const parsed = endpoint(primary);
  const issues: KafkaLatencyProbeIssue[] = [];
  let tcpConnectMs: number | null = null;
  let tlsHandshakeMs: number | null = null;
  try {
    tcpConnectMs = await socketDuration(
      () => connectTcp({ host: parsed.host, port: parsed.port }),
      "connect",
      signal,
    );
  } catch {
    issues.push({
      recovery: "Verify the primary broker endpoint and TCP network path.",
      stage: "tcp",
      summary: "TCP connection latency is unavailable.",
    });
  }
  try {
    tlsHandshakeMs = await socketDuration(
      () =>
        connectTls({
          ca: [input.caPem],
          host: parsed.host,
          port: parsed.port,
          rejectUnauthorized: true,
          ...(isIP(parsed.host) === 0 ? { servername: parsed.host } : {}),
        }),
      "secureConnect",
      signal,
    );
  } catch {
    issues.push({
      recovery: "Verify the broker certificate, hostname, CA trust, and TLS network path.",
      stage: "tls",
      summary: "TLS handshake latency is unavailable.",
    });
  }
  return {
    endpoint: primary,
    issues,
    tcpConnectMs,
    tlsHandshakeMs,
  };
}
