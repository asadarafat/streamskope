import type { KafkaClusterServiceContext } from "../application/types";
import type {
  ConnectPort,
  ConnectState,
  ConnectMutationReceipt,
} from "../application/connect-service";
import type {
  ConnectRawOffset,
  ConnectOffsetRead,
  ConnectOffsetMutation,
} from "../application/connect-offset-types";
import { connectOffsetPosition } from "../contracts/connect-offsets";
import { connectName } from "../contracts/connect";
import { record, exactKeys, text } from "../contracts/validation-primitives";

import type { BoundedJsonHttpPort } from "./bounded-json-http";
import { readConnectHttp, mutateConnectHttp } from "./connect-http-request";

function unavailable(status: number): Exclude<ConnectOffsetRead["status"], "available"> {
  if (status === 401 || status === 403) return "denied";
  if ([404, 405, 501].includes(status)) return "unsupported";
  return "unavailable";
}
function offsets(
  value: unknown,
  state: ConnectState,
): { mapping: "kafka-sink" | "file-source"; offsets: readonly ConnectRawOffset[] } {
  const source =
    state.config["connector.class"] === "org.apache.kafka.connect.file.FileStreamSourceConnector";
  const sink =
    state.config["connector.class"] === "org.apache.kafka.connect.file.FileStreamSinkConnector";
  if (
    (!source && !sink) ||
    Object.keys(state.config).some((key) => /(?:^|\.)bootstrap\.servers$/u.test(key))
  )
    throw new Error("Connector offset mapping or broker routing is unsupported.");
  const body = record(value, "offsets");
  exactKeys(body, ["offsets"], "offsets");
  if (!Array.isArray(body.offsets) || body.offsets.length > 128)
    throw new Error("Offset limit exceeded.");
  const seen = new Set<string>();
  const result: ConnectRawOffset[] = [];
  for (const value of body.offsets as unknown[]) {
    const item = record(value, "offset");
    exactKeys(item, ["partition", "offset"], "offset");
    const partition = record(item.partition, "partition");
    let parsed: Record<string, string | number>, label: string;
    if (source) {
      exactKeys(partition, ["filename"], "partition");
      const filename = text(partition.filename, "source filename", 8192);
      if (!state.config.file || filename !== state.config.file)
        throw new Error("Source partition mapping is unsupported.");
      parsed = { filename };
      label = "Source partition";
    } else {
      exactKeys(partition, ["kafka_topic", "kafka_partition"], "partition");
      const topic = text(partition.kafka_topic, "topic", 249),
        number = connectOffsetPosition(partition.kafka_partition);
      if (
        !/^[a-zA-Z0-9._-]+$/u.test(topic) ||
        topic === "." ||
        topic === ".." ||
        number > 2_147_483_647
      )
        throw new Error("Sink partition mapping is unsupported.");
      parsed = { kafka_topic: topic, kafka_partition: number };
      label = `${topic} · partition ${number}`;
    }
    const key = JSON.stringify(parsed);
    if (seen.has(key)) throw new Error("Duplicate offset partition.");
    seen.add(key);
    // A null offset is an absent position, not position zero.
    if (item.offset === null) continue;
    const offset = record(item.offset, "position"),
      field = source ? "position" : "kafka_offset";
    exactKeys(offset, [field], "position");
    const position = connectOffsetPosition(offset[field]);
    result.push({ partition: parsed, offset: { [field]: position }, position, label });
  }
  result.sort((a, b) =>
    JSON.stringify(a.partition).localeCompare(JSON.stringify(b.partition), "en-US"),
  );
  return { mapping: source ? "file-source" : "kafka-sink", offsets: result };
}
export async function inspectConnectOffsets(
  http: BoundedJsonHttpPort,
  port: Pick<ConnectPort, "load">,
  context: KafkaClusterServiceContext,
  name: string,
  signal: AbortSignal,
): Promise<ConnectOffsetRead> {
  name = connectName(name);
  try {
    const root = await readConnectHttp(http, context, signal, "GET", "/");
    if (root.status < 200 || root.status >= 300) return { status: unavailable(root.status) };
    const info = record(root.body, "worker"),
      clusterId = text(info.kafka_cluster_id, "cluster identity", 128),
      workerVersion = text(info.version, "worker version", 80);
    const connector = await port.load(context, name, signal);
    if (!connector) return { status: "missing" };
    const response = await readConnectHttp(
      http,
      context,
      signal,
      "GET",
      `/connectors/${encodeURIComponent(name)}/offsets`,
    );
    if (response.status < 200 || response.status >= 300)
      return { status: unavailable(response.status) };
    try {
      return {
        status: "available",
        clusterId,
        workerVersion,
        connector,
        ...offsets(response.body, connector),
      };
    } catch {
      return { status: "unsupported" };
    }
  } catch (error) {
    signal.throwIfAborted();
    const status =
      error !== null && typeof error === "object" && "status" in error ? error.status : undefined;
    return { status: typeof status === "number" ? unavailable(status) : "unavailable" };
  }
}
export function applyConnectOffsets(
  http: BoundedJsonHttpPort,
  context: KafkaClusterServiceContext,
  input: ConnectOffsetMutation,
  signal: AbortSignal,
): Promise<ConnectMutationReceipt> {
  const path = `/connectors/${encodeURIComponent(connectName(input.name))}/offsets`;
  return input.action === "reset"
    ? mutateConnectHttp(http, context, signal, "DELETE", path)
    : mutateConnectHttp(http, context, signal, "PATCH", path, {
        offsets: [{ partition: input.partition, offset: input.offset }],
      });
}
