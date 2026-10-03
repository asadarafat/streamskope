// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { act, cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, expect, it, vi } from "vitest";

import type { HostCommand, StreamSkopeHost } from "../../src/features/kafka/contracts";
import { RelationshipsPage } from "../../src/features/kafka/ui/RelationshipsPage";
import { StreamSkopeThemeProvider } from "../../src/platform/ui/StreamSkopeThemeProvider";
import { testHostExecute } from "../support/host-response";
import { relationshipFixture } from "../support/relationship-fixture";
import { OBSERVED_AT } from "../support/observation-fixture";

afterEach(() => {
  cleanup();
  vi.useRealTimers();
});
it("requires explicit discovery, preserves provenance, filters graph evidence and withholds stale impact", async () => {
  vi.useFakeTimers();
  vi.setSystemTime(OBSERVED_AT);
  const fixture = relationshipFixture(),
    commands: HostCommand[] = [];
  const host: StreamSkopeHost = {
    subscribe: () => () => undefined,
    openExternalUrl: () => Promise.reject(new Error("Unexpected navigation")),
    execute: testHostExecute(async (command) => {
      commands.push(command);
      const graph =
        command.command === "relationships.capture"
          ? await fixture.service.capture(command.payload)
          : undefined;
      return {
        command: command.command,
        id: command.id,
        version: command.version,
        ok: true,
        result: { correlationId: "c", ...(graph ? { graph } : {}) },
      };
    }),
  };
  const view = render(
    <StreamSkopeThemeProvider>
      <RelationshipsPage host={host} />
    </StreamSkopeThemeProvider>,
  );
  expect(commands).toHaveLength(0);
  fireEvent.change(screen.getByLabelText("Topics (up to three, comma-separated)"), {
    target: { value: "events" },
  });
  fireEvent.change(screen.getByLabelText("Impact subject (optional)"), {
    target: { value: "base" },
  });
  expect(screen.getByRole("button", { name: "Discover relationships" })).toBeDisabled();
  fireEvent.change(screen.getByLabelText("Exact subject version"), { target: { value: "1" } });
  await act(async () => {
    fireEvent.click(screen.getByRole("button", { name: "Discover relationships" }));
    await vi.advanceTimersByTimeAsync(0);
  });
  expect(commands[0]).toMatchObject({
    command: "relationships.capture",
    payload: { sampleRecords: false },
  });
  expect(screen.getByRole("table", { name: "Potential schema impact" })).toHaveTextContent(
    "workers",
  );
  expect(screen.getByRole("table", { name: "Relationship evidence" })).toHaveTextContent(
    "inferred",
  );
  fireEvent.click(screen.getByRole("button", { name: "Show relationships for workers" }));
  expect(screen.getByRole("table", { name: "Relationship evidence" })).not.toHaveTextContent(
    "registered-id",
  );
  fireEvent.click(screen.getByRole("button", { name: "Show all relationships" }));
  await act(async () => {
    await vi.advanceTimersByTimeAsync(60_001);
  });
  expect(
    screen.getByText("Impact assessment is withheld for this stale snapshot. Discover again."),
  ).toBeVisible();
  expect(commands).toHaveLength(1);
  view.unmount();
  expect(commands.at(-1)?.command).toBe("relationships.cancel");
});
