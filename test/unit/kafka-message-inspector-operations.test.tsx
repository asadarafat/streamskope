// @vitest-environment jsdom

import "@testing-library/jest-dom/vitest";

import { cleanup, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";

import type { KafkaExploredMessage, KafkaLiveRuleEvaluation } from "../../src/features/kafka/contracts";
import { MessageInspector } from "../../src/features/kafka/ui/MessageInspector";
import type { TextDocumentTransferPort } from "../../src/features/kafka/ui";

const evaluated: KafkaLiveRuleEvaluation = {
  activeMatchCount: 0,
  activeMatches: [],
  durationMicros: 0,
  errorCount: 0,
  errors: [],
  evaluatedRules: 0,
  omittedEvidence: 0,
  omittedRules: 0,
  state: "evaluated",
  suppressedMatchCount: 0,
  suppressedMatches: [],
};

function message(
  payload: string | null,
  overrides: Partial<KafkaExploredMessage> = {},
): KafkaExploredMessage {
  return {
    headers: {},
    id: "message-1",
    key: "key",
    offset: "42",
    originalByteSize: payload?.length ?? 0,
    partition: 2,
    payload,
    preview: payload ?? "",
    ruleEvaluation: evaluated,
    timestamp: "2026-07-25T12:00:00.000Z",
    topic: "orders",
    truncated: false,
    ...overrides,
  };
}

function setup(
  selected: KafkaExploredMessage,
  copyImplementation: (content: string) => Promise<void> = () => Promise.resolve(),
): {
  readonly copy: ReturnType<typeof vi.fn<TextDocumentTransferPort["copy"]>>;
  readonly user: ReturnType<typeof userEvent.setup>;
} {
  const copy = vi.fn<TextDocumentTransferPort["copy"]>(copyImplementation);
  const transfer: TextDocumentTransferPort = {
    copy,
    download: vi.fn<TextDocumentTransferPort["download"]>(() => Promise.resolve()),
  };
  render(<MessageInspector message={selected} onClose={() => undefined} transfer={transfer} />);
  return { copy, user: userEvent.setup() };
}

async function openValue(user: ReturnType<typeof userEvent.setup>): Promise<void> {
  await user.click(screen.getByRole("tab", { name: "Value" }));
}

afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
});

describe("Kafka message inspector operations", () => {
  it("copies the exact immutable value including surrounding whitespace after confirmation", async () => {
    const exact = "  first line\nsecond line  \n";
    const { copy, user } = setup(message(exact));

    await openValue(user);
    await user.click(screen.getByRole("button", { name: "Copy value" }));

    await waitFor(() => {
      expect(copy).toHaveBeenCalledWith(exact);
    });
    expect(screen.getByRole("status")).toHaveTextContent("Value copied to clipboard.");
  });

  it("reports clipboard rejection without content or false completion", async () => {
    const { user } = setup(message("sensitive-message-value"), () =>
      Promise.reject(new Error("permission denied: sensitive-message-value")),
    );

    await openValue(user);
    await user.click(screen.getByRole("button", { name: "Copy value" }));

    expect(await screen.findByRole("alert")).toHaveTextContent(
      "The selected value could not be copied.",
    );
    expect(screen.getByRole("alert")).not.toHaveTextContent("sensitive-message-value");
    expect(screen.queryByText(/copied to clipboard/u)).not.toBeInTheDocument();
  });

  it("distinguishes a retained preview from a Kafka null value", async () => {
    const truncated = message(null, {
      originalByteSize: 2_000_000,
      preview: "retained prefix",
      truncated: true,
    });
    const { copy, user } = setup(truncated);

    await openValue(user);
    expect(screen.getByRole("button", { name: "Copy retained preview" })).toBeEnabled();
    expect(screen.getByRole("button", { name: "Open preview in editor" })).toBeEnabled();
    await user.click(screen.getByRole("button", { name: "Copy retained preview" }));
    expect(copy).toHaveBeenCalledWith("retained prefix");
    expect(screen.getByRole("status")).toHaveTextContent("Retained preview copied to clipboard.");

    cleanup();
    const empty = setup(message(null));
    await openValue(empty.user);
    expect(screen.getByRole("button", { name: "Copy value" })).toBeDisabled();
    expect(screen.getByRole("button", { name: "Open in editor" })).toBeDisabled();
    expect(screen.getByText("Kafka supplied a null value.")).toBeVisible();
  });

  it("opens exact JSON and plain text in an editable local scratch document", async () => {
    const rawJson = '  {"approved":true}\n';
    const { user } = setup(message(rawJson));

    await openValue(user);
    const open = screen.getByRole("button", { name: "Open in editor" });
    await user.click(open);
    let dialog = screen.getByRole("dialog", { name: "Message value editor" });
    expect(within(dialog).getByText("JSON")).toBeVisible();
    const editor = within(dialog).getByRole("textbox", { name: "Scratch message value" });
    expect(editor).toHaveValue(rawJson);
    expect(dialog).toHaveTextContent("Scratch edits do not change or publish the Kafka record.");

    fireEvent.change(editor, { target: { value: "changed locally" } });
    expect(editor).toHaveValue("changed locally");
    await user.click(within(dialog).getByRole("button", { name: "Close editor" }));
    await waitFor(() => {
      expect(
        screen.queryByRole("dialog", { name: "Message value editor" }),
      ).not.toBeInTheDocument();
    });

    await user.click(open);
    dialog = screen.getByRole("dialog", { name: "Message value editor" });
    expect(within(dialog).getByRole("textbox", { name: "Scratch message value" })).toHaveValue(
      rawJson,
    );

    cleanup();
    const plain = setup(message("not-json"));
    await openValue(plain.user);
    await plain.user.click(screen.getByRole("button", { name: "Open in editor" }));
    expect(
      within(screen.getByRole("dialog", { name: "Message value editor" })).getByText("Plain text"),
    ).toBeVisible();
  });

  it("opens an incomplete preview with a warning and treats hostile content as inert text", async () => {
    const hostile =
      '<script>globalThis.__messageEditorExecuted=true</script><a href="javascript:alert(1)">x</a>';
    const { user } = setup(
      message(null, {
        originalByteSize: 2_000_000,
        preview: hostile,
        truncated: true,
      }),
    );

    await openValue(user);
    await user.click(screen.getByRole("button", { name: "Open preview in editor" }));
    const dialog = screen.getByRole("dialog", { name: "Message preview editor" });
    expect(within(dialog).getByRole("alert")).toHaveTextContent(
      "Only the retained preview is available.",
    );
    expect(within(dialog).getByRole("textbox", { name: "Scratch message value" })).toHaveValue(
      hostile,
    );
    expect(dialog.querySelector("script")).toBeNull();
    expect(dialog.querySelector("a")).toBeNull();
    expect((globalThis as { __messageEditorExecuted?: boolean }).__messageEditorExecuted).toBe(
      undefined,
    );
  });

  it("closes with Escape and returns focus to the invoking editor action", async () => {
    const { user } = setup(message("plain value"));
    await openValue(user);
    const open = screen.getByRole("button", { name: "Open in editor" });
    open.focus();

    await user.keyboard("{Enter}");
    expect(screen.getByRole("dialog", { name: "Message value editor" })).toBeVisible();
    await user.keyboard("{Escape}");

    await waitFor(() => {
      expect(
        screen.queryByRole("dialog", { name: "Message value editor" }),
      ).not.toBeInTheDocument();
    });
    expect(open).toHaveFocus();
  });
});
