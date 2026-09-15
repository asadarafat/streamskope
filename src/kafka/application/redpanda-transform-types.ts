import type { RedpandaTransformSummary } from "../contracts";

import type { KafkaClusterServiceContext } from "./types";

export interface RedpandaTransformPort {
  delete(context: KafkaClusterServiceContext, name: string, signal: AbortSignal): Promise<void>;
  list(
    context: KafkaClusterServiceContext,
    signal: AbortSignal,
  ): Promise<readonly RedpandaTransformSummary[]>;
}
