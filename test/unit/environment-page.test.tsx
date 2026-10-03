// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, expect, it, vi } from "vitest";

import type { HostCommand, StreamSkopeHost } from "../../src/features/kafka/contracts";
import {
  ENVIRONMENT_CONFIG_KEYS,
  type EnvironmentSnapshot,
} from "../../src/features/kafka/contracts/environment-snapshot";
import { EnvironmentPage } from "../../src/features/kafka/ui/EnvironmentPage";
import { StreamSkopeThemeProvider } from "../../src/platform/ui/StreamSkopeThemeProvider";
import { testHostExecute } from "../support/host-response";

afterEach(cleanup);
it("imports without contacting Kafka and requires selection and exact confirmation before a single promotion", async () => {
  const source: EnvironmentSnapshot = {
    format: "streamskope.topic-config/v1",
    clusterId: "source",
    observedAt: "2026-10-03T12:00:00.000Z",
    topics: [
      {
        name: "events",
        topicId: "source-topic",
        configs: ENVIRONMENT_CONFIG_KEYS.map((key) => ({
          key,
          value: key === "retention.ms" ? "1000" : null,
          mutable: key === "retention.ms",
        })),
      },
    ],
  };
  const target: EnvironmentSnapshot = {
    ...source,
    clusterId: "target",
    topics: source.topics.map((topic) => ({
      ...topic,
      topicId: "target-topic",
      configs: topic.configs.map((c) => (c.key === "retention.ms" ? { ...c, value: "2000" } : c)),
    })),
  };
  const commands: HostCommand[] = [];
  const host: StreamSkopeHost = {
    subscribe: () => () => undefined,
    openExternalUrl: () => Promise.reject(new Error("Unexpected URL")),
    execute: testHostExecute((command) => {
      commands.push(command);
      const result =
        command.command === "environments.capture"
          ? { snapshot: target }
          : command.command === "environments.review"
            ? {
                review: {
                  planId: "plan",
                  expiresAt: "2026-10-03T12:02:00.000Z",
                  confirmation: "promote 1 settings to target",
                  source,
                  target,
                  changes: [
                    {
                      topic: "events",
                      key: "retention.ms",
                      source: "1000",
                      target: "2000",
                      supported: true,
                      reason: "Existing mutable topic setting",
                    },
                  ],
                },
              }
            : {
                outcome: {
                  results: [{ topic: "events", state: "acknowledged", verified: true }],
                  detail: "Selected setting applied.",
                },
              };
      return Promise.resolve({
        command: command.command,
        id: command.id,
        version: command.version,
        ok: true,
        result: { correlationId: "c", ...result },
      });
    }),
  };
  render(
    <StreamSkopeThemeProvider>
      <EnvironmentPage
        host={host}
        profiles={[]}
        canWrite
        transfer={{ download: vi.fn().mockResolvedValue("saved"), copy: vi.fn() }}
      />
    </StreamSkopeThemeProvider>,
  );
  fireEvent.change(screen.getByLabelText("Import source snapshot JSON"), {
    target: { value: JSON.stringify(source) },
  });
  fireEvent.click(screen.getByRole("button", { name: "Use imported source" }));
  await screen.findByRole("button", { name: "Export source snapshot" });
  expect(commands).toEqual([]);
  fireEvent.click(screen.getByRole("button", { name: "Capture destination and compare" }));
  const select = await screen.findByRole("checkbox", { name: "Promote events retention.ms" });
  expect(screen.getByRole("button", { name: "Review selected promotion" })).toBeDisabled();
  fireEvent.click(select);
  fireEvent.click(screen.getByRole("button", { name: "Review selected promotion" }));
  const confirmation = await screen.findByLabelText("Type promote 1 settings to target to confirm");
  expect(screen.getByRole("button", { name: "Apply reviewed promotion" })).toBeDisabled();
  expect(commands.some((c) => c.command === "environments.apply")).toBe(false);
  fireEvent.change(confirmation, { target: { value: "promote 1 settings to target" } });
  fireEvent.click(screen.getByRole("button", { name: "Apply reviewed promotion" }));
  await screen.findByText("Selected setting applied.");
  expect(commands.filter((c) => c.command === "environments.apply")).toHaveLength(1);
  expect(screen.getByRole("button", { name: "Apply reviewed promotion" })).toBeDisabled();
});
