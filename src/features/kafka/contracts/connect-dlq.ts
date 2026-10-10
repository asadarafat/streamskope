import type { KafkaMessage } from "./types";
import { connectName } from "./connect";
import { offsetPosition } from "./offset-reset";
export interface ConnectDlqContext {
  readonly topic: string;
  readonly partition: string;
  readonly offset: string;
  readonly connector: string;
  readonly task: string;
  readonly stage: string;
}
export type ConnectDlqEvidence =
  | { readonly state: "absent" }
  | {
      readonly state: "unavailable";
      readonly reason: "incomplete" | "ambiguous" | "protected" | "invalid";
    }
  | { readonly state: "reported"; readonly context: ConnectDlqContext };

const PREFIX = "__connect.errors.";
const fields = ["topic", "partition", "offset", "connector.name", "task.id", "stage"] as const;
// Declared Apache Connect stages; reported metadata does not establish task causation.
const stages = [
  "TASK_POLL",
  "TASK_PUT",
  "TRANSFORMATION",
  "KEY_CONVERTER",
  "VALUE_CONVERTER",
  "HEADER_CONVERTER",
  "KAFKA_PRODUCE",
  "KAFKA_CONSUME",
];
function int32(value: string): string {
  if (!/^(0|[1-9][0-9]{0,9})$/u.test(value) || BigInt(value) > 2_147_483_647n)
    throw new Error("Invalid reported partition or task.");
  return value;
}
/** Inspect protected ordered headers; aliases only detect presence, never establish a locator. */
export function inspectConnectDlqEvidence(
  message: Pick<KafkaMessage, "headers" | "truncated" | "structured">,
): ConnectDlqEvidence {
  const headers = message.structured?.headers ?? [];
  if (
    !headers.some((h) => h.key.startsWith(PREFIX)) &&
    !Object.keys(message.headers).some((key) => key.startsWith(PREFIX))
  )
    return { state: "absent" };
  if (message.truncated || message.structured?.headersState !== "complete")
    return { state: "unavailable", reason: "incomplete" };
  // A malformed name might conceal a reserved key. Do not infer uniqueness.
  if (headers.some((h) => h.error !== null)) return { state: "unavailable", reason: "invalid" };
  const values: string[] = [];
  for (const field of fields) {
    const selected = headers.filter((h) => h.key === PREFIX + field);
    if (selected.length > 1) return { state: "unavailable", reason: "ambiguous" };
    if (!selected.length || selected[0]!.value === null)
      return { state: "unavailable", reason: "incomplete" };
    const value = selected[0]!.value;
    if (value === "[MASKED]") return { state: "unavailable", reason: "protected" };
    values.push(value);
  }
  try {
    const [topic, partition, offset, connector, task, stage] = values as [
      string,
      string,
      string,
      string,
      string,
      string,
    ];
    if (
      !/^[a-zA-Z0-9._-]{1,249}$/u.test(topic) ||
      topic === "." ||
      topic === ".." ||
      !stages.includes(stage)
    )
      throw new Error("Unsupported reported context.");
    return {
      state: "reported",
      context: {
        topic,
        partition: int32(partition),
        offset: offsetPosition(offset, "reported offset"),
        connector: connectName(connector),
        task: int32(task),
        stage,
      },
    };
  } catch {
    return { state: "unavailable", reason: "invalid" };
  }
}
