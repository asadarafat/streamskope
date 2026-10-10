// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, expect, it, vi } from "vitest";

import { type HostCommand, type StreamSkopeHost } from "../../src/features/kafka/contracts";
import { emptyObservationWatch } from "../../src/features/kafka/contracts/observation-watch";
import { ObservedHealthPage } from "../../src/features/kafka/ui/ObservedHealthPage";
import { StreamSkopeThemeProvider } from "../../src/platform/ui/StreamSkopeThemeProvider";
import { testHostExecute } from "../support/host-response";
import { observation, observationSeries } from "../support/observation-fixture";

afterEach(cleanup);

it("restores saved selection and thresholds on restart without connection or collection authority, including summary-only history", async () => {
  const { retainObservations } =
    await import("../../src/features/kafka/application/observation-store");
  const history = retainObservations(
    { schemaVersion: 1, series: [observationSeries([observation(0)])] },
    observation(0).observedAt,
  );
  const saved = {
    ...history,
    series: [],
    settings: {
      input: {
        topic: "events",
        groupId: "workers",
        sampleRecords: true,
        thresholds: { lag: 19, requestMs: 250 },
      },
      connectionName: "Previous host",
      clusterId: "fixture-cluster",
      topicId: "fixture-topic",
      savedAt: observation(0).observedAt,
    },
  };
  const commands: HostCommand[] = [];
  const host: StreamSkopeHost = {
    openExternalUrl: () => Promise.reject(new Error("Unexpected URL")),
    subscribe: () => () => undefined,
    execute: testHostExecute(async (command) => {
      await Promise.resolve();
      commands.push(command);
      const base = {
        version: command.version,
        id: command.id,
        command: command.command,
        ok: true as const,
      };
      if (command.command === "observations.history")
        return {
          ...base,
          result: {
            correlationId: "fixture",
            snapshot: { ...saved, durability: "durable" as const },
          },
        };
      if (command.command === "observations.watch.status")
        return { ...base, result: { correlationId: "fixture", watch: emptyObservationWatch() } };
      throw new Error("Saved settings must not grant collection or connection authority.");
    }),
  };
  render(
    <StreamSkopeThemeProvider>
      <ObservedHealthPage
        host={host}
        topics={["events"]}
        connectionName="Test Kafka"
        onOpenTopic={vi.fn()}
        onOpenGroup={vi.fn()}
      />
    </StreamSkopeThemeProvider>,
  );
  await waitFor(() => expect(screen.getByLabelText("Observed topic")).toHaveValue("events"));
  expect(screen.getByLabelText("Observed consumer group (optional)")).toHaveValue("workers");
  fireEvent.click(screen.getByRole("button", { name: "History and collection settings" }));
  expect(screen.getByLabelText("Lag alert threshold (optional)")).toHaveValue("19");
  expect(screen.getByLabelText("Request time alert threshold, ms (optional)")).toHaveValue("250");
  expect(
    screen.getByRole("checkbox", { name: "Sample records for size and key distribution" }),
  ).toBeChecked();
  fireEvent.click(screen.getByRole("button", { name: "Retained five-minute summaries (1)" }));
  expect(screen.getByRole("table", { name: "Five-minute observation summaries" })).toBeVisible();
  expect(screen.getByText("100", { exact: true })).toBeVisible();
  expect(screen.getByText(/No raw observations/)).toBeVisible();
  expect(
    commands.every((c) =>
      ["observations.history", "observations.watch.status"].includes(c.command),
    ),
  ).toBe(true);
});
