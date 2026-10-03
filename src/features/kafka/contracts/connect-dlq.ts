import type { KafkaMessage } from "./types";
export interface ConnectDlqContext {
  readonly topic: string;
  readonly partition: string;
  readonly offset: string;
  readonly connector: string;
  readonly task: string;
  readonly stage: string;
}
/** Interpret retained, already protected headers; never bypass masking via original bytes. */
export function connectDlqContext(
  message: Pick<KafkaMessage, "headers" | "truncated">,
): ConnectDlqContext | null {
  if (message.truncated) return null;
  const h = message.headers;
  const topic = h["__connect.errors.topic"],
    partition = h["__connect.errors.partition"],
    offset = h["__connect.errors.offset"];
  if (
    !topic ||
    !/^[a-zA-Z0-9._-]{1,249}$/u.test(topic) ||
    !partition ||
    !/^\d{1,10}$/u.test(partition) ||
    !offset ||
    !/^\d{1,20}$/u.test(offset)
  )
    return null;
  const safe = (key: string): string => h[key]?.slice(0, 200) ?? "Unknown";
  return {
    topic,
    partition,
    offset,
    connector: safe("__connect.errors.connector.name"),
    task: safe("__connect.errors.task.id"),
    stage: safe("__connect.errors.stage"),
  };
}
