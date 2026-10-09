// @vitest-environment jsdom

import "@testing-library/jest-dom/vitest";
import { cleanup, render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, expect, it, vi } from "vitest";

import {
  createKafkaQueryLink,
  serializeKafkaQuery,
  KAFKA_QUERY_TRANSFER_LIMITS,
} from "../../src/features/kafka/contracts";
import { InvestigationTransferControls } from "../../src/features/kafka/ui/InvestigationTransferControls";
import { queryViewSettings } from "../../src/features/kafka/ui/investigation-view-settings";
import { KAFKA_VIEW_TRANSFER_LIMITS } from "../../src/features/kafka/contracts/view-transfer";
import { takeInitialQueryImport } from "../../src/platform/electron/renderer/query-entry";
import { pasteText } from "../support/paste-text";

const query = {
  schemaVersion: 1,
  request: { mode: "earliest", topic: "orders", maxMessages: 25 },
} as const;
afterEach(() => {
  cleanup();
  window.history.replaceState({}, "", "/");
});

it("reviews JSON/link and file imports without opening, and requires a separate action to restore controls", async () => {
  const user = userEvent.setup();
  const onRestore = vi.fn();
  const transfer = { copy: vi.fn(), download: vi.fn().mockResolvedValue("saved") };
  render(
    <InvestigationTransferControls
      profiles={[]}
      transfer={transfer}
      captureExport={() => ({ settings: queryViewSettings(query), suggestedName: null })}
      viewExportAvailable
      queryExportAvailable
      readActive={false}
      onRestore={onRestore}
    />,
  );
  await pasteText(
    user,
    screen.getByRole("textbox", { name: "View or query JSON/link" }),
    createKafkaQueryLink(query),
  );
  await user.click(screen.getByRole("button", { name: "Review import" }));
  expect(screen.getByLabelText("Imported query preview")).toHaveTextContent('"orders"');
  expect(onRestore).not.toHaveBeenCalled();
  await user.click(screen.getByRole("button", { name: "Open imported query" }));
  expect(onRestore).toHaveBeenCalledExactlyOnceWith(queryViewSettings(query), undefined);
  await user.upload(
    screen.getByLabelText("View or query file"),
    new File(
      [serializeKafkaQuery({ ...query, request: { ...query.request, topic: "payments" } })],
      "query.json",
      { type: "application/json" },
    ),
  );
  expect(await screen.findByLabelText("Imported query preview")).toHaveTextContent("payments");
  expect(onRestore).toHaveBeenCalledTimes(1);
  await user.click(screen.getByText("Query settings only"));
  await user.click(screen.getByRole("button", { name: "Export query JSON" }));
  expect(await screen.findByText("Query file saved.")).toBeVisible();
  expect(transfer.download).toHaveBeenCalledExactlyOnceWith({
    fileName: "streamskope-query.json",
    mediaType: "application/json",
    content: serializeKafkaQuery(query),
    byteSize: new TextEncoder().encode(serializeKafkaQuery(query)).length,
  });
});

it("discards an invalid import preview and reports failed sharing instead of claiming success", async () => {
  const user = userEvent.setup();
  const onRestore = vi.fn();
  render(
    <InvestigationTransferControls
      profiles={[]}
      transfer={{ copy: vi.fn().mockRejectedValue(new Error("denied")), download: vi.fn() }}
      captureExport={() => ({ settings: queryViewSettings(query), suggestedName: null })}
      viewExportAvailable
      queryExportAvailable
      readActive={false}
      onRestore={onRestore}
      initialImport={serializeKafkaQuery(query)}
    />,
  );
  await user.click(screen.getByRole("button", { name: "Review import" }));
  const input = screen.getByRole("textbox", { name: "View or query JSON/link" });
  await user.clear(input);
  await pasteText(user, input, '{"schemaVersion":2}');
  await user.click(screen.getByRole("button", { name: "Review import" }));
  expect(screen.getByRole("alert")).toHaveTextContent("invalid or unsupported import");
  expect(screen.queryByRole("button", { name: "Open imported query" })).not.toBeInTheDocument();
  await user.upload(
    screen.getByLabelText("View or query file"),
    new File([" ".repeat(KAFKA_VIEW_TRANSFER_LIMITS.documentBytes + 1)], "query.json", {
      type: "application/json",
    }),
  );
  expect(
    await screen.findByText(
      "View files are limited to 128 KiB; query files remain limited to 32 KiB.",
    ),
  ).toBeVisible();
  await user.click(screen.getByText("Query settings only"));
  await user.click(screen.getByRole("button", { name: "Copy query link" }));
  expect(await screen.findByText(/Sharing failed/u)).toBeVisible();
  expect(onRestore).not.toHaveBeenCalled();
});

it("takes an initial browser query fragment into the review flow and removes it from the address", () => {
  window.history.replaceState({}, "", createKafkaQueryLink(query, window.location.origin + "/"));
  const pending = takeInitialQueryImport(window);
  expect(pending).toMatch(/^#query=/u);
  expect(window.location.hash).toBe("");
  expect(takeInitialQueryImport(window)).toBeUndefined();
  window.history.replaceState({}, "", "/#query=" + "x".repeat(60_000));
  expect(takeInitialQueryImport(window)).toHaveLength(
    KAFKA_QUERY_TRANSFER_LIMITS.linkCharacters + 1,
  );
});
