import { readFile } from "node:fs/promises";
import { join } from "node:path";

import AxeBuilder from "@axe-core/playwright";
import { expect, type Page, type TestInfo } from "@playwright/test";

import type { SecureConnectionInput, KafkaMessage } from "../../src/features/kafka/contracts";
import { StreamSkopeKafkaEngine } from "../../src/features/kafka/engine";
import type { KafkaCompleteRecord } from "../../src/features/kafka/contracts/record-bytes";

import {
  fetchTopicMessages,
  expectNoHorizontalOverflow,
  openWorkbenchResource,
} from "./workbench-browser";

/** Browser acceptance uses a separately owned connector and independent broker readback. */
export async function qualifyConnectDlqBrowser(input: {
  readonly page: Page;
  readonly info: TestInfo;
  readonly connection: SecureConnectionInput;
  readonly workerUrl: string;
  readonly gatewayOrigin: string;
  readonly dataRoot: string;
  readonly passphrase: string;
  readonly commands: readonly string[];
}): Promise<void> {
  const { page } = input;
  const topic = "connect-browser-json",
    dlq = "connect-browser-dlq",
    target = "connect-browser-repaired",
    name = "browser-dlq-sink",
    profile = "Owned Connect fixture";
  const b64 = (s: string): string => Buffer.from(s).toString("base64");
  const original: KafkaCompleteRecord = {
    state: "complete",
    encoding: "base64",
    key: b64("dlq-key"),
    value: b64('{"amount":1}'),
    headers: [
      { key: b64("trace"), value: b64("first") },
      { key: b64("trace"), value: null },
      { key: b64("trace"), value: b64("last") },
    ],
  };
  const reader = await new StreamSkopeKafkaEngine().openConnection(
    input.connection,
    AbortSignal.timeout(15_000),
  );
  const read = async (t: string): Promise<KafkaMessage[]> => {
    const stream = await reader.openMessageStream(
      { topic: t, mode: "earliest", maxMessages: 10 },
      AbortSignal.timeout(10_000),
    );
    const records: KafkaMessage[] = [];
    try {
      for await (const r of stream) records.push(r);
    } finally {
      await stream.close();
    }
    return records;
  };
  const sourceOffset = async (): Promise<unknown> => {
    const response = await fetch(`${input.workerUrl}/connectors/${name}/offsets`);
    expect(response.ok).toBe(true);
    return response.json();
  };
  try {
    for (const t of [topic, dlq, target])
      expect(
        await reader.applyWrite!({
          kind: "topic",
          topic: t,
          partitions: 1,
          replicationFactor: 1,
          configs: [],
        }),
      ).toMatchObject({ state: "acknowledged" });
    const created = await fetch(`${input.workerUrl}/connectors`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        name,
        config: {
          "connector.class": "org.apache.kafka.connect.file.FileStreamSinkConnector",
          "tasks.max": "1",
          topics: topic,
          file: "/tmp/browser-dlq-owned.txt",
          "value.converter": "org.apache.kafka.connect.json.JsonConverter",
          "value.converter.schemas.enable": "true",
          "errors.tolerance": "all",
          "errors.deadletterqueue.topic.name": dlq,
          "errors.deadletterqueue.topic.replication.factor": "1",
          "errors.deadletterqueue.context.headers.enable": "true",
        },
      }),
    });
    expect(created.ok).toBe(true);
    await expect
      .poll(
        async () => {
          const response = await fetch(`${input.workerUrl}/connectors/${name}/status`);
          if (response.status === 404) return null;
          expect(response.ok).toBe(true);
          return ((await response.json()) as { tasks: { state: string }[] }).tasks[0]?.state;
        },
        { timeout: 30_000 },
      )
      .toBe("RUNNING");
    expect(
      await reader.applyWrite!({ kind: "record", topic, partition: 0, record: original }),
    ).toMatchObject({ state: "acknowledged" });
    await expect.poll(async () => (await read(dlq)).length, { timeout: 30_000 }).toBe(1);
    await expect
      .poll(async () => JSON.stringify(await sourceOffset()), { timeout: 15_000 })
      .toContain('"kafka_offset":1');
    const beforeOffsets = await sourceOffset(),
      actualDlq = (await read(dlq))[0]!.original;
    await page.setViewportSize({ width: 1440, height: 1000 });
    const main = page.getByRole("main", { name: "Kafka Connect page" });
    await main
      .getByRole("button", { name: "Dismiss receipt and start another review", exact: true })
      .click();
    await main.getByRole("button", { name: "Refresh connectors", exact: true }).click();
    await main.getByRole("combobox", { name: "Existing connector", exact: true }).click();
    await page.getByRole("option", { name, exact: true }).click();
    await main.getByRole("button", { name: `Browse DLQ: ${dlq}`, exact: true }).focus();
    await page.keyboard.press("Enter");
    await expect(
      page
        .getByRole("main", { name: "Topic detail page" })
        .getByRole("heading", { name: dlq, exact: true }),
    ).toBeVisible();
    await page.getByRole("combobox", { name: "Read mode", exact: true }).click();
    await page.getByRole("option", { name: "First N", exact: true }).click();
    await fetchTopicMessages(page, dlq);
    await page
      .getByRole("grid", { name: "Kafka messages" })
      .getByText('{"amount":1}', { exact: true })
      .first()
      .click();
    await page.getByRole("tab", { name: "Metadata", exact: true }).click();
    const evidence = page.getByRole("region", { name: "Reported Connect context", exact: true });
    await expect(evidence).toContainText(`${topic}`);
    await expect(evidence).toContainText("0 / 0");
    await expect(evidence).toContainText(`${name} / 0`);
    await expect(evidence).toContainText("VALUE_CONVERTER");
    await expect(evidence).toContainText("not a verified broker/topic identity");
    expect(
      (await new AxeBuilder({ page }).include('[aria-label="Message evidence content"]').analyze())
        .violations,
    ).toEqual([]);
    await page.screenshot({
      path: input.info.outputPath("connect-dlq-context.png"),
      animations: "disabled",
    });
    await page.setViewportSize({ width: 390, height: 844 });
    await page.getByRole("tab", { name: "Metadata", exact: true }).click();
    await expect(evidence).toBeVisible();
    await expectNoHorizontalOverflow(page);
    await evidence.scrollIntoViewIfNeeded();
    expect(
      (await new AxeBuilder({ page }).include('[aria-label="Message evidence content"]').analyze())
        .violations,
    ).toEqual([]);
    await page.screenshot({
      path: input.info.outputPath("connect-dlq-context-mobile.png"),
      animations: "disabled",
    });
    await page.setViewportSize({ width: 1440, height: 1000 });
    await page.getByRole("button", { name: "Replay…", exact: true }).click();
    const replay = page.getByRole("dialog", {
      name: "Copy or replay selected records",
      exact: true,
    });
    await replay.getByRole("textbox", { name: "Destination topic", exact: true }).fill(target);
    await replay.getByRole("checkbox", { name: "Transform structured value", exact: true }).check();
    await replay
      .getByRole("textbox", { name: "Value JSON Pointer edits", exact: true })
      .fill(JSON.stringify([{ op: "set", path: "/repaired", json: "true" }]));
    await replay.getByRole("button", { name: "Preview replay", exact: true }).focus();
    await page.keyboard.press("Enter");
    const confirmation = `${profile} / ${target} / 0`;
    await expect(
      replay.getByRole("textbox", { name: `Type ${confirmation} to confirm`, exact: true }),
    ).toBeVisible();
    await expect(replay.getByText("Verified writer mappings", { exact: true })).toBeVisible();
    await expect(replay).toContainText(b64('{"amount":1,"repaired":true}'));
    expect(await read(target)).toEqual([]);
    const apply = replay.getByRole("button", { name: "Apply reviewed replay", exact: true });
    await expect(apply).toBeDisabled();
    const beforeApply = input.commands.filter((c) => c === "records.replay.apply").length;
    expect(
      (await new AxeBuilder({ page }).include('[role="dialog"]').analyze()).violations,
    ).toEqual([]);
    await page.screenshot({
      path: input.info.outputPath("connect-dlq-structured-review.png"),
      animations: "disabled",
    });
    await replay
      .getByRole("textbox", { name: `Type ${confirmation} to confirm`, exact: true })
      .fill(confirmation);
    await apply.focus();
    await page.keyboard.press("Enter");
    await expect(
      replay.getByText(/1 acknowledged; 0 unknown; 0 rejected; 0 unsent/u),
    ).toBeVisible();
    await expect(replay).toContainText("Journal: confirmed. Storage: durable.");
    await expect(apply).toBeDisabled();
    expect(input.commands.filter((c) => c === "records.replay.apply")).toHaveLength(
      beforeApply + 1,
    );
    expect((await read(target)).map((r) => r.original)).toEqual([
      { ...actualDlq, value: b64('{"amount":1,"repaired":true}') },
    ]);
    expect((await read(dlq)).map((r) => r.original)).toEqual([actualDlq]);
    expect(await sourceOffset()).toEqual(beforeOffsets);
    await replay.getByRole("button", { name: "Close", exact: true }).click();
    const encrypted = await readFile(
      join(input.dataRoot, "history", "kafka-repair-jobs.json"),
      "utf8",
    );
    expect(JSON.parse(encrypted)).toMatchObject({ schemaVersion: 3 });
    expect(encrypted).not.toContain("dlq-key");
    expect(encrypted).not.toContain(b64('{"amount":1,"repaired":true}'));
    const locked = await page.request.post(`${input.gatewayOrigin}/__streamskope_session/lock`, {
      headers: { origin: input.gatewayOrigin },
      data: {},
    });
    expect(locked.status()).toBe(200);
    await page.goto(input.gatewayOrigin);
    await page.getByLabel("Vault passphrase", { exact: true }).fill(input.passphrase);
    await page.getByRole("button", { name: "Unlock", exact: true }).click();
    await expect(page.getByTestId("connection-profiles-grid")).toBeVisible();
    await expect(page.getByLabel("Connection status")).not.toContainText("Connected");
    expect(input.commands.filter((c) => c === "records.replay.apply")).toHaveLength(
      beforeApply + 1,
    );
    await page
      .getByRole("button", { name: `Connect insecure plaintext profile ${profile}` })
      .click();
    await expect(page.getByLabel("Connection status")).toContainText("Connected");
    await openWorkbenchResource(page, "Connection Profiles");
    await page.getByRole("button", { name: "Repair history", exact: true }).click();
    const history = page.getByRole("dialog", { name: "Repair jobs and receipts", exact: true });
    await expect(history.getByText(/Protected, durable host storage/u)).toBeVisible();
    await expect(history.getByRole("cell", { name: `${target}/0@0`, exact: true })).toBeVisible();
    await expect(history.getByRole("cell", { name: "acknowledged", exact: true })).toBeVisible();
    await expect(history.locator("..")).toHaveCSS("opacity", "1");
    expect(
      (await new AxeBuilder({ page }).include('[role="dialog"]').analyze()).violations,
    ).toEqual([]);
    await page.screenshot({
      path: input.info.outputPath("connect-dlq-reopened-history.png"),
      animations: "disabled",
    });
    await history.getByRole("button", { name: "Close", exact: true }).click();
    expect(await read(target)).toHaveLength(1);
    expect(input.commands.filter((c) => c === "records.replay.apply")).toHaveLength(
      beforeApply + 1,
    );
  } finally {
    await reader.close();
  }
}
