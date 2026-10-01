import { expect, it } from "vitest";

import { HOST_PROTOCOL_VERSION, type HostEvent } from "../../src/features/kafka/contracts";
import { createKafkaBackend } from "../../src/platform/node/kafka-backend";

it("starts the production Kafka composition and manages ordinary profiles without plugins", async () => {
  const backend = createKafkaBackend();
  const events: HostEvent[] = [];
  backend.subscribe((event) => events.push(event));
  try {
    const created = await backend.execute({
      command: "profiles.create",
      id: "ordinary",
      version: HOST_PROTOCOL_VERSION,
      payload: {
        profile: { name: "Ordinary Kafka", brokers: ["localhost:9092"], transport: "plaintext" },
      },
    });
    expect(created.ok).toBe(true);
    const listed = await backend.execute({
      command: "profiles.list",
      id: "list",
      version: HOST_PROTOCOL_VERSION,
      payload: {},
    });
    expect(listed.ok).toBe(true);
    const profileEvent = events.filter((event) => event.event === "profiles.changed").at(-1);
    expect(profileEvent?.payload.profiles.map((profile) => profile.name)).toEqual([
      "Ordinary Kafka",
    ]);
    const plugins = await backend.execute({
      command: "plugins.list",
      id: "plugins",
      version: HOST_PROTOCOL_VERSION,
      payload: {},
    });
    expect(plugins).toMatchObject({ ok: true, result: { pluginSnapshot: { plugins: [] } } });
    const absent = await backend.execute({
      command: "plugin.execute",
      id: "absent",
      version: HOST_PROTOCOL_VERSION,
      payload: { pluginId: "example.capture", method: "preflight", input: {} },
    });
    expect(absent).toMatchObject({ ok: false, error: { code: "BACKEND_UNAVAILABLE" } });
  } finally {
    await backend.shutdown();
  }
});
