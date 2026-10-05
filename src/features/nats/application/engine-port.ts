import type { NatsRecord, NatsSafeFailure } from "../contracts";

import type { NatsConnectionInput } from "./profile-types";

export type NatsCopiedMessage = Omit<NatsRecord, "id" | "generation">;
export type NatsMessageReceipt =
  | { readonly kind: "record"; readonly record: NatsCopiedMessage }
  | {
      readonly kind: "omitted";
      readonly reason: "payload-limit" | "metadata-limit";
      readonly payloadBytes: number;
    };
/** The engine owns actual SDK work; disconnect is reusable, shutdown retires admission. */
export interface NatsEngine {
  connect(
    input: NatsConnectionInput,
    options: {
      readonly signal?: AbortSignal;
      readonly onConnectionLoss: (failure: NatsSafeFailure) => void;
    },
  ): Promise<void>;
  startSubscription(
    subject: string,
    options: {
      readonly signal?: AbortSignal;
      readonly onMessage: (receipt: NatsMessageReceipt) => void;
      readonly onFailure: (failure: NatsSafeFailure) => void;
    },
  ): Promise<void>;
  stopSubscription(): Promise<void>;
  disconnect(): Promise<void>;
  shutdown(): Promise<void>;
}
