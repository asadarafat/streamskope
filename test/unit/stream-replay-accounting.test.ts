import { expect, it } from "vitest";

import {
  command,
  ControlledMessageStream,
  createFacade,
  message,
  RecordingActiveConnection,
  RecordingConnectionPort,
  settleAsyncIteration,
} from "../support/kafka-backend-facade-fixture";
import { StreamReplayAccounting } from "../support/stream-replay-accounting";

it("accounts for terminal omissions even when Stop publishes no message batch", async () => {
  const stream = new ControlledMessageStream();
  const connection = new RecordingActiveConnection();
  connection.messageStreamOperations.push(() => Promise.resolve(stream));
  const port = new RecordingConnectionPort();
  port.openOperations.push(() => Promise.resolve(connection));
  const facade = createFacade(port, () => undefined);
  const accounting = new StreamReplayAccounting();
  facade.subscribe((event) => accounting.observe(event));
  try {
    await facade.execute(command("connection.connect", "connect"));
    await facade.execute(command("messages.start", "start"));
    for (let index = 0; index < 20; index += 1) {
      stream.push(message(String(index)));
      await settleAsyncIteration();
    }
    facade.setMessagePresentationPaused(true);
    expect(await facade.execute(command("messages.stop", "stop"))).toMatchObject({ ok: true });
    expect(accounting.snapshot(20)).toEqual({
      accepted: 20,
      published: 0,
      hostDisplayDrops: 20,
      queued: 0,
      unacceptedGenerated: 0,
      accountingPassed: true,
    });
    expect(accounting.snapshot(21)).toMatchObject({
      unacceptedGenerated: 1,
      accountingPassed: true,
    });
    expect(accounting.snapshot(22).accountingPassed).toBe(false);
    expect(accounting.snapshot(19).accountingPassed).toBe(false);
  } finally {
    await facade.shutdown();
  }
});
