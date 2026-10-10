import type { ConnectOffsetMapping } from "../contracts/connect-offsets";

import type { ConnectMutationReceipt, ConnectState } from "./connect-service";
import type { KafkaClusterServiceContext } from "./types";

/** Private source partitions never cross the renderer contract. */
export interface ConnectRawOffset {
  readonly partition: Readonly<Record<string, string | number>>;
  readonly offset: Readonly<Record<string, number>>;
  readonly label: string;
  readonly position: number;
}
export interface ConnectOffsetState {
  readonly status: "available";
  readonly clusterId: string;
  readonly workerVersion: string;
  readonly connector: ConnectState;
  readonly mapping: ConnectOffsetMapping;
  readonly offsets: readonly ConnectRawOffset[];
}
export type ConnectOffsetRead =
  | ConnectOffsetState
  | {
      readonly status: "unsupported" | "denied" | "missing" | "unavailable";
    };
export interface ConnectOffsetMutation {
  readonly name: string;
  readonly action: "set" | "remove" | "reset";
  readonly partition: Readonly<Record<string, string | number>> | null;
  readonly offset: Readonly<Record<string, number>> | null;
}
export interface ConnectOffsetsPort {
  inspectOffsets(
    context: KafkaClusterServiceContext,
    name: string,
    signal: AbortSignal,
  ): Promise<ConnectOffsetRead>;
  applyOffsets(
    context: KafkaClusterServiceContext,
    input: ConnectOffsetMutation,
    signal: AbortSignal,
  ): Promise<ConnectMutationReceipt>;
}
