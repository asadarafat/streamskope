import type { EdaCaptureProducerKind, EdaCaptureSource } from "../contracts";

function record(value: unknown): Record<string, unknown> | undefined {
  return value !== null && typeof value === "object" && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : undefined;
}

export function exportedTopics(resource: Record<string, unknown>): readonly string[] {
  const exports = record(resource.spec)?.exports;
  if (!Array.isArray(exports)) return [];
  return [
    ...new Set(
      exports.flatMap((entry) => {
        const topic = record(entry)?.topic;
        return typeof topic === "string" && topic.length > 0 ? [topic] : [];
      }),
    ),
  ].sort((left, right) => left.localeCompare(right, "en-US"));
}

export function sourceFromResource(
  resource: Record<string, unknown>,
  apiVersion: EdaCaptureSource["apiVersion"],
  kind: EdaCaptureProducerKind,
  fallbackNamespace: string,
): EdaCaptureSource | undefined {
  const metadata = record(resource.metadata);
  const name = metadata?.name;
  const namespace = metadata?.namespace;
  const labels = record(metadata?.labels);
  const topics = exportedTopics(resource);
  if (
    typeof name !== "string" ||
    name === "streamskope-capture" ||
    labels?.["app.kubernetes.io/managed-by"] === "streamskope-capture-agent" ||
    labels?.["app.kubernetes.io/managed-by"] === "streamskope" ||
    topics.length === 0
  ) {
    return undefined;
  }
  const brokersValue = record(resource.spec)?.brokers;
  const brokers =
    typeof brokersValue === "string"
      ? brokersValue
          .split(",")
          .map((value) => value.trim())
          .filter(
            (value) =>
              value.length <= 512 &&
              /^(?:[a-zA-Z0-9._-]+|\[[a-fA-F0-9:]+\]):[0-9]{1,5}$/u.test(value),
          )
          .slice(0, 32)
      : [];
  return {
    apiVersion,
    kind,
    name,
    namespace: typeof namespace === "string" ? namespace : fallbackNamespace,
    topics,
    brokers,
  };
}
