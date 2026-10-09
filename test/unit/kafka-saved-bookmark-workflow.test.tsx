// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { cleanup, render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, expect, it, vi } from "vitest";

import { KafkaQueryLibrary } from "../../src/features/kafka/application/query-library";
import {
  HOST_PROTOCOL_VERSION,
  createDefaultKafkaInvestigationView,
  type HostCommand,
  type KafkaSavedView,
  type StreamSkopeHost,
} from "../../src/features/kafka/contracts";
import {
  createEmptyKafkaSavedRecordContext,
  type KafkaRecordLocator,
} from "../../src/features/kafka/contracts/record-locator";
import { SavedViewsDialog } from "../../src/features/kafka/ui/SavedViewsDialog";
import { SavedViewBookmarks } from "../../src/features/kafka/ui/SavedViewBookmarks";
import { testHostExecute } from "../support/host-response";

const locator: KafkaRecordLocator = {
  schemaVersion: 1,
  clusterId: "cluster-a",
  topicId: "4b914431-8917-44aa-ac51-982639d70b7e",
  topic: "orders",
  partition: 0,
  offset: "7",
  leaderEpoch: 2,
};
const view: KafkaSavedView = {
  id: "other-view",
  name: "Payments incident",
  profileId: "local-profile",
  configuration: {
    schemaVersion: 1,
    request: { topic: "payments", mode: "newest", maxMessages: 25 },
  },
  view: createDefaultKafkaInvestigationView({ kind: "topic", workspace: "monitor" }),
  records: { ...createEmptyKafkaSavedRecordContext(), comparison: { ...locator, offset: "2" } },
};
afterEach(cleanup);
async function fixture(initial = view): Promise<{
  library: KafkaQueryLibrary;
  host: StreamSkopeHost;
  commands: HostCommand[];
}> {
  const library = new KafkaQueryLibrary();
  await library.put(initial);
  const commands: HostCommand[] = [];
  const host: StreamSkopeHost = {
    execute: testHostExecute(async (command) => {
      commands.push(command);
      const snapshot =
        command.command === "queries.list"
          ? await library.list()
          : command.command === "queries.put"
            ? await library.put(command.payload.query, command.payload.expected)
            : ((): never => {
                throw new Error("No other commands expected");
              })();
      return {
        command: command.command,
        id: command.id,
        ok: true,
        version: HOST_PROTOCOL_VERSION,
        result: { correlationId: command.id, snapshot },
      };
    }),
    subscribe: () => () => undefined,
    openExternalUrl: () => Promise.reject(new Error("Unused")),
  };
  return { library, host, commands };
}

it("saves into the chosen view without replacing its settings, then opens a bookmark passively", async () => {
  const { host, library, commands } = await fixture();
  const user = userEvent.setup(),
    onRestore = vi.fn(),
    captureCurrent = vi.fn(() => ({
      configuration: {
        schemaVersion: 1 as const,
        request: { topic: "unrelated", mode: "earliest" as const, maxMessages: 100 },
      },
      view: createDefaultKafkaInvestigationView(),
      records: createEmptyKafkaSavedRecordContext(),
    }));
  render(
    <SavedViewsDialog
      host={host}
      profiles={[]}
      currentResource="unrelated"
      currentQueryAvailable
      readActive={false}
      captureCurrent={captureCurrent}
      onRestore={onRestore}
      onClose={vi.fn()}
      bookmarkCandidate={locator}
    />,
  );
  await user.click(await screen.findByRole("combobox", { name: "Saved view" }));
  await user.click(screen.getByRole("option", { name: "Payments incident" }));
  await user.clear(screen.getByRole("textbox", { name: "New bookmark name" }));
  await user.type(screen.getByRole("textbox", { name: "New bookmark name" }), "First failure");
  await user.click(screen.getByRole("button", { name: "Save bookmark" }));
  expect(
    await screen.findByText("Bookmark saved. Open the view to use its positions."),
  ).toBeVisible();
  const stored = (await library.list()).queries[0]!;
  expect(stored).toMatchObject({
    ...view,
    records: { ...view.records, bookmarks: [{ name: "First failure", locator }] },
  });
  expect(captureCurrent).not.toHaveBeenCalled();
  const write = commands.find((command) => command.command === "queries.put");
  expect(write?.payload).toMatchObject({ expected: view });
  expect(JSON.stringify(stored.records)).not.toMatch(
    /payload|original|structured|credential|continuation/,
  );
  // Clear unavailable profile reference for this explicit Open only; saving did not change it.
  await user.click(screen.getByRole("combobox", { name: "Local connection profile" }));
  await user.click(screen.getByRole("option", { name: "Choose a connection when opening" }));
  await user.click(screen.getByRole("combobox", { name: "Saved bookmark" }));
  await user.click(screen.getByRole("option", { name: "First failure" }));
  await user.click(screen.getByRole("button", { name: "Open bookmarked topic" }));
  expect(onRestore).toHaveBeenCalledOnce();
  const restored = onRestore.mock
    .calls[0]?.[0] as import("../../src/features/kafka/ui/investigation-view-settings").KafkaViewSettings;
  expect(restored.configuration?.request).toEqual({
    topic: "orders",
    mode: "earliest",
    maxMessages: 1,
  });
  expect(restored.records.selected).toEqual(locator);
  expect(onRestore.mock.calls[0]?.[1]).toBeUndefined();
  expect(
    commands.every((command) => ["queries.list", "queries.put"].includes(command.command)),
  ).toBe(true);
});

it("does not overwrite a concurrent edit when saving a bookmark", async () => {
  const { host, library } = await fixture();
  const user = userEvent.setup();
  render(
    <SavedViewsDialog
      host={host}
      profiles={[]}
      currentResource="orders"
      currentQueryAvailable
      readActive={false}
      captureCurrent={() => view}
      onRestore={vi.fn()}
      onClose={vi.fn()}
      bookmarkCandidate={locator}
    />,
  );
  await user.click(await screen.findByRole("combobox", { name: "Saved view" }));
  await user.click(screen.getByRole("option", { name: "Payments incident" }));
  await library.put({ ...view, name: "Edited in another window" });
  await user.click(screen.getByRole("button", { name: "Save bookmark" }));
  expect(await screen.findByText(/did not confirm/)).toBeVisible();
  expect((await library.list()).queries[0]?.name).toBe("Edited in another window");
  expect((await library.list()).queries[0]?.records.bookmarks).toEqual([]);
  expect(screen.queryByText(/Bookmark saved/)).not.toBeInTheDocument();
});

it("reuses duplicate positions at capacity and keeps rename/removal available", async () => {
  const bookmarks = Array.from({ length: 32 }, (_, index) => ({
    id: `b${String(index)}`,
    name: `Record ${String(index)}`,
    locator: { ...locator, offset: String(index) },
  }));
  const selected = { ...view, records: { ...view.records, bookmarks } };
  const write = vi
      .fn<
        (next: KafkaSavedView, expected: KafkaSavedView | null, status: string) => Promise<boolean>
      >()
      .mockResolvedValue(true),
    user = userEvent.setup();
  render(
    <SavedViewBookmarks
      selected={selected}
      candidate={locator}
      busy={false}
      libraryCount={256}
      newViewAllowed={false}
      newViewName=""
      profileId={undefined}
      captureCurrent={() => view}
      write={write}
      onRestore={vi.fn()}
      readActive={false}
    />,
  );
  expect(screen.getByRole("button", { name: "Update bookmark" })).toBeEnabled();
  await user.clear(screen.getByRole("textbox", { name: "New bookmark name" }));
  await user.type(
    screen.getByRole("textbox", { name: "New bookmark name" }),
    "Renamed at capacity",
  );
  await user.click(screen.getByRole("button", { name: "Update bookmark" }));
  expect(write).toHaveBeenCalledOnce();
  expect(write.mock.calls[0]?.[0].records.bookmarks).toContainEqual({
    id: "b7",
    name: "Renamed at capacity",
    locator,
  });
  expect(write.mock.calls[0]?.[1]).toEqual(selected);
  expect(write.mock.calls[0]?.[2]).toBe("Bookmark updated.");
  expect(write.mock.calls[0]?.[0].records.bookmarks).toHaveLength(32);
  await user.click(screen.getByRole("combobox", { name: "Saved bookmark" }));
  await user.click(screen.getByRole("option", { name: "Record 7" }));
  expect(screen.getByRole("button", { name: "Remove bookmark" })).toBeEnabled();
});

it("explains capacity and cluster mismatches without silently switching profiles", () => {
  const props = {
    selected: view,
    candidate: locator,
    busy: false,
    libraryCount: 256,
    newViewAllowed: false,
    newViewName: "",
    profileId: undefined,
    captureCurrent: (): KafkaSavedView => view,
    write: vi.fn(),
    onRestore: vi.fn(),
    readActive: false,
  };
  const { rerender } = render(<SavedViewBookmarks {...props} />);
  expect(screen.getByRole("button", { name: "Save bookmark" })).toBeDisabled();
  expect(screen.getByText(/Bookmark capacity reached/)).toBeVisible();
  rerender(
    <SavedViewBookmarks
      {...props}
      libraryCount={0}
      candidate={{ ...locator, clusterId: "cluster-b" }}
    />,
  );
  expect(screen.getByText(/another cluster/)).toBeVisible();
  expect(screen.getByRole("button", { name: "Save bookmark" })).toBeDisabled();
  expect(props.write).not.toHaveBeenCalled();
  expect(props.onRestore).not.toHaveBeenCalled();
});
