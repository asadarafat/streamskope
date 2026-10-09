import type {
  ConnectionState,
  ConsumptionState,
  HostError,
  KafkaConsumerGroupInventoryState,
  KafkaFetchRequest,
  TopicListState,
} from "../contracts";

export function isKafkaConsumptionActive(
  state: ConsumptionState,
  request: KafkaFetchRequest | null,
): boolean {
  return (
    state === "loading" ||
    state === "fetching" ||
    state === "streaming" ||
    (state === "empty" && request?.mode === "tail")
  );
}

export type KafkaConsumptionStopLabel = "Stop tail" | "Pause search" | "Pause read" | "Retry stop";

export function kafkaConsumptionStopLabel(
  state: ConsumptionState,
  request: KafkaFetchRequest | null,
  error: HostError | null,
): KafkaConsumptionStopLabel | null {
  if (request === null) return null;
  if (
    state === "failed" &&
    error?.code === "TIMEOUT" &&
    error.retryable &&
    error.target === "kafka-consumption-cleanup"
  )
    return "Retry stop";
  return isKafkaConsumptionActive(state, request)
    ? request.mode === "tail"
      ? "Stop tail"
      : request.search === undefined
        ? "Pause read"
        : "Pause search"
    : null;
}

export function connectionColor(
  state: ConnectionState,
): "default" | "error" | "success" | "warning" {
  if (state === "connected") {
    return "success";
  }
  if (state === "failed") {
    return "error";
  }
  if (state === "connecting" || state === "disconnecting") {
    return "warning";
  }
  return "default";
}

export function topicStatusLabel(
  state: "unavailable" | TopicListState,
  topicCount: number,
): string {
  switch (state) {
    case "loading":
      return "Refreshing topics";
    case "ready":
      return `${topicCount} topics`;
    case "denied":
      return "Topic access denied";
    case "failed":
      return "Topic refresh failed";
    case "unavailable":
      return "Topics not loaded";
  }
}

export function consumerGroupStatusLabel(
  state: KafkaConsumerGroupInventoryState,
  groupCount: number,
): string {
  switch (state) {
    case "loading":
      return "Refreshing consumer groups";
    case "ready":
      return `${groupCount} consumer groups`;
    case "empty":
      return "No consumer groups";
    case "denied":
      return "Consumer-group access denied";
    case "failed":
      return "Consumer-group refresh failed";
    case "stale":
      return `${groupCount} consumer groups · stale`;
    case "unavailable":
      return "Consumer groups not loaded";
  }
}

export function consumptionStateLabel(state: ConsumptionState): string {
  switch (state) {
    case "complete":
      return "Read finished";
    case "fetching":
      return "Fetching snapshot";
    case "loading":
      return "Starting";
    case "streaming":
      return "Streaming";
    case "empty":
      return "No messages";
    case "stopped":
      return "Stopped";
    case "failed":
      return "Consumption failed";
    case "unavailable":
      return "Idle";
  }
}
