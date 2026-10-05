import { parseHostEvent, type HostEvent } from "../../src/features/kafka/contracts";

/** Reads one bounded test observation; the caller owns the HTTP abort deadline. */
export async function readKafkaEventsUntil(
  reader: ReadableStreamDefaultReader<Uint8Array>,
  predicate: (events: readonly HostEvent[]) => boolean,
): Promise<readonly HostEvent[]> {
  const events: HostEvent[] = [];
  const decoder = new TextDecoder();
  let buffer = "";
  for (;;) {
    const result = await reader.read();
    if (result.done) {
      throw new Error("Development-host event stream ended before expected events arrived.");
    }
    buffer += decoder.decode(result.value, { stream: true });
    let boundary = buffer.indexOf("\n\n");
    while (boundary >= 0) {
      const block = buffer.slice(0, boundary);
      buffer = buffer.slice(boundary + 2);
      const data = block
        .split("\n")
        .filter((line) => line.startsWith("data: "))
        .map((line) => line.slice(6))
        .join("\n");
      if (data.length > 0) {
        events.push(parseHostEvent(JSON.parse(data) as unknown));
        if (predicate(events)) return events;
      }
      boundary = buffer.indexOf("\n\n");
    }
  }
}
