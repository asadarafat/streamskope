// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { act, cleanup, render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, expect, it, vi } from "vitest";

import type { ProfileSummary } from "../../src/features/kafka/contracts";
import {
  parseKafkaInvestigationTransfer,
  serializeKafkaPortableView,
  type KafkaPortableView,
} from "../../src/features/kafka/contracts/view-transfer";
import { InvestigationTransferControls } from "../../src/features/kafka/ui/InvestigationTransferControls";
import { portableViewSettings } from "../../src/features/kafka/ui/investigation-view-settings";
import { pasteText } from "../support/paste-text";

const locator = {
  schemaVersion: 1,
  clusterId: "original-cluster",
  topicId: "4b914431-8917-44aa-ac51-982639d70b7e",
  leaderEpoch: 7,
  topic: "orders",
  partition: 0,
  offset: "9007199254740993",
} as const;
const view: KafkaPortableView = {
  kind: "streamskope.kafka-view",
  schemaVersion: 1,
  suggestedName: "Worker incident",
  configuration: null,
  view: {
    schemaVersion: 1,
    destination: { kind: "consumer-group", groupId: "workers" },
    messages: {
      visibleColumns: ["preview", "offset"],
      columnWidths: [],
      inspectorWidth: 400,
      filtersOpen: true,
    },
  },
  records: {
    selected: locator,
    comparison: { ...locator, offset: "1" },
    bookmarks: [{ name: "First failure", locator }],
  },
};
const profile: ProfileSummary = {
  id: "receiver-profile",
  name: "Local cluster",
  transport: "plaintext",
  active: false,
  brokers: ["localhost:9092"],
  createdAt: "2026-10-10T00:00:00.000Z",
  updatedAt: "2026-10-10T00:00:00.000Z",
};
const settings = portableViewSettings(view, () => "source-bookmark-id");
function props(): Parameters<typeof InvestigationTransferControls>[0] {
  return {
    transfer: { copy: vi.fn(), download: vi.fn().mockResolvedValue("saved") },
    captureExport: () => ({ settings, suggestedName: view.suggestedName }),
    viewExportAvailable: true,
    queryExportAvailable: false,
    profiles: [profile],
    readActive: false,
    onRestore: vi.fn(),
  };
}
afterEach(() => {
  cleanup();
  vi.unstubAllGlobals();
});

it("reviews all view fields without allocating identities, then explicitly opens with new local keys", async () => {
  const input = props();
  const user = userEvent.setup();
  const id = vi.spyOn(globalThis.crypto, "randomUUID");
  render(<InvestigationTransferControls {...input} initialImport={JSON.stringify(view)} />);
  await user.click(screen.getByRole("button", { name: "Review import" }));
  expect(screen.getByLabelText("Imported view preview")).toHaveTextContent("original-cluster");
  expect(screen.getByLabelText("Imported view preview")).toHaveTextContent("9007199254740993");
  expect(screen.getByLabelText("Imported view preview")).toHaveTextContent("First failure");
  expect(input.onRestore).not.toHaveBeenCalled();
  expect(id).not.toHaveBeenCalled();
  await user.click(screen.getByRole("combobox", { name: "Connection for imported investigation" }));
  await user.click(screen.getByRole("option", { name: "Local cluster" }));
  await user.click(screen.getByRole("button", { name: "Open imported view" }));
  expect(id).toHaveBeenCalledOnce();
  expect(input.onRestore).toHaveBeenCalledWith(
    {
      configuration: null,
      view: view.view,
      records: {
        ...view.records,
        bookmarks: [{ ...view.records.bookmarks[0], id: expect.any(String) as unknown }],
      },
    },
    "receiver-profile",
  );
  const restored = vi.mocked(input.onRestore).mock.calls[0]![0];
  expect(restored.records.bookmarks[0]?.id).not.toBe("source-bookmark-id");
  expect(restored.records.selected).toEqual(locator);
  await user.click(screen.getByRole("button", { name: "Open imported view" }));
  const reopened = vi.mocked(input.onRestore).mock.calls[1]![0];
  expect(reopened.records.bookmarks[0]?.id).not.toBe(restored.records.bookmarks[0]?.id);
});

it("retains stop and local-profile guards and clears reviewed authority when input changes", async () => {
  const input = props();
  const user = userEvent.setup();
  const { rerender } = render(
    <InvestigationTransferControls {...input} initialImport={JSON.stringify(view)} readActive />,
  );
  await user.click(screen.getByRole("button", { name: "Review import" }));
  expect(screen.getByRole("button", { name: "Open imported view" })).toBeDisabled();
  await user.click(screen.getByRole("combobox", { name: "Connection for imported investigation" }));
  await user.click(screen.getByRole("option", { name: "Local cluster" }));
  rerender(<InvestigationTransferControls {...input} profiles={[]} />);
  expect(screen.getByRole("button", { name: "Open imported view" })).toBeDisabled();
  const textbox = screen.getByRole("textbox", { name: "View or query JSON/link" });
  await user.clear(textbox);
  expect(screen.queryByRole("button", { name: "Open imported view" })).not.toBeInTheDocument();
  await pasteText(user, textbox, JSON.stringify({ ...view, password: "fixture-secret" }));
  await user.click(screen.getByRole("button", { name: "Review import" }));
  expect(screen.getByRole("alert")).toHaveTextContent("invalid or unsupported import");
  expect(screen.getByRole("alert")).not.toHaveTextContent("fixture-secret");
  expect(input.onRestore).not.toHaveBeenCalled();
});

it.each(["saved", "started", "cancelled"] as const)(
  "exports group-only settings without IDs and reports the actual %s transfer outcome",
  async (outcome) => {
    const input = props();
    const download = vi.fn().mockResolvedValue(outcome);
    const user = userEvent.setup();
    render(<InvestigationTransferControls {...input} transfer={{ copy: vi.fn(), download }} />);
    await user.click(screen.getByText("Query settings only"));
    expect(screen.getByRole("button", { name: "Export query JSON" })).toBeDisabled();
    expect(screen.getByRole("button", { name: "Copy query link" })).toBeDisabled();
    await user.click(screen.getByRole("button", { name: "Export view JSON" }));
    expect(await screen.findByRole("status")).toHaveTextContent(
      outcome === "saved"
        ? "View file saved."
        : outcome === "started"
          ? "View download started."
          : "View export cancelled.",
    );
    const document = download.mock.calls[0]![0] as {
      fileName: string;
      content: string;
      byteSize: number;
    };
    expect(document.fileName).toBe("streamskope-view.json");
    expect(document.byteSize).toBe(new TextEncoder().encode(document.content).length);
    expect(document.content).not.toMatch(/source-bookmark-id|receiver-profile|localhost/);
    expect(parseKafkaInvestigationTransfer(document.content)).toEqual({ kind: "view", view });
    expect(input.onRestore).not.toHaveBeenCalled();
  },
);

it("ignores replaced FileReader callbacks, aborts on typing and unmount, and never restores a stale preview", async () => {
  const readers: ControlledReader[] = [];
  class ControlledReader extends FileReader {
    aborted = false;
    override get readyState(): 0 | 1 | 2 {
      return this.aborted ? 2 : 1;
    }
    override readAsText(): void {
      readers.push(this);
    }
    override abort(): void {
      this.aborted = true;
    }
    finish(content: string): void {
      Object.defineProperty(this, "result", { configurable: true, value: content });
      this.onload?.call(this, new ProgressEvent("load") as ProgressEvent<FileReader>);
    }
  }
  vi.stubGlobal("FileReader", ControlledReader);
  const user = userEvent.setup();
  const input = props();
  const { unmount } = render(<InvestigationTransferControls {...input} />);
  const file = screen.getByLabelText("View or query file");
  await user.upload(file, new File(["first"], "first.json", { type: "application/json" }));
  await user.upload(file, new File(["second"], "second.json", { type: "application/json" }));
  expect(readers[0]?.aborted).toBe(true);
  act(() =>
    readers[0]!.finish(serializeKafkaPortableView({ ...view, suggestedName: "Stale first" })),
  );
  expect(screen.queryByLabelText("Imported view preview")).not.toBeInTheDocument();
  act(() =>
    readers[1]!.finish(serializeKafkaPortableView({ ...view, suggestedName: "Current second" })),
  );
  expect(screen.getByLabelText("Imported view preview")).toHaveTextContent("Current second");
  await user.upload(file, new File(["third"], "third.json", { type: "application/json" }));
  await pasteText(user, screen.getByRole("textbox", { name: "View or query JSON/link" }), "manual");
  expect(readers[2]?.aborted).toBe(true);
  act(() => readers[2]!.finish(serializeKafkaPortableView(view)));
  expect(screen.getByRole("textbox", { name: "View or query JSON/link" })).toHaveValue("manual");
  expect(screen.queryByLabelText("Imported view preview")).not.toBeInTheDocument();
  await user.upload(file, new File(["fourth"], "fourth.json", { type: "application/json" }));
  unmount();
  expect(readers[3]?.aborted).toBe(true);
  act(() => readers[3]!.finish(serializeKafkaPortableView(view)));
  expect(input.onRestore).not.toHaveBeenCalled();
});

it("never updates a replacement transfer owner with a previous download result", async () => {
  let finish!: (outcome: "saved") => void;
  const download = new Promise<"saved">((resolve) => {
    finish = resolve;
  });
  const input = props();
  const user = userEvent.setup();
  const first = render(
    <InvestigationTransferControls
      {...input}
      transfer={{ copy: vi.fn(), download: () => download }}
    />,
  );
  await user.click(screen.getByRole("button", { name: "Export view JSON" }));
  first.unmount();
  render(<InvestigationTransferControls {...input} />);
  await act(async () => {
    finish("saved");
    await download;
  });
  expect(screen.queryByRole("status")).not.toBeInTheDocument();
});
