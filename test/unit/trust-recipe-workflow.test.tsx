// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";

import { act, cleanup, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";

import { createKafkaBackend } from "../../src/main";
import { type HostCommand, type HostEvent, type StreamSkopeHost } from "../../src/kafka/contracts";
import { TrustRecipeManager } from "../../src/kafka/ui/TrustRecipeManager";
import type { TextDocumentTransferPort } from "../../src/kafka/ui/text-document-transfer";
import { InMemoryKafkaTrustRecipeStore } from "../../src/kafka/application";

const backends: ReturnType<typeof createKafkaBackend>[] = [];
afterEach(async () => {
  cleanup();
  vi.useRealTimers();
  vi.restoreAllMocks();
  await Promise.all(backends.splice(0).map((backend) => backend.shutdown()));
});

function setup(
  transfer?: TextDocumentTransferPort,
  recipeStore?: InMemoryKafkaTrustRecipeStore,
  delayCommitEvents = false,
): {
  commands: HostCommand[];
  closeCount: () => number;
  host: StreamSkopeHost;
  deliverCommitEvents: () => void;
} {
  const backend = createKafkaBackend(
    undefined,
    undefined,
    undefined,
    undefined,
    undefined,
    undefined,
    recipeStore,
  );
  backends.push(backend);
  const commands: HostCommand[] = [];
  let closed = 0;
  let holdEvents = false;
  const deliveries: Array<() => void> = [];
  const host: StreamSkopeHost = {
    execute(command) {
      commands.push(command);
      if (
        delayCommitEvents &&
        (command.command === "recipes.create" || command.command === "recipes.update")
      )
        holdEvents = true;
      return backend.execute(command);
    },
    subscribe: (listener) =>
      backend.subscribe((event: HostEvent) => {
        if (holdEvents && event.event === "recipes.changed") deliveries.push(() => listener(event));
        else listener(event);
      }),
    openExternalUrl: () => Promise.reject(new Error("Not expected")),
  };
  render(
    <TrustRecipeManager
      host={host}
      transfer={transfer}
      onClose={() => {
        closed += 1;
      }}
    />,
  );
  return {
    commands,
    closeCount: () => closed,
    host,
    deliverCommitEvents: (): void => {
      deliveries.splice(0).forEach((deliver) => deliver());
    },
  };
}

describe("Trust acquisition template manager", () => {
  it("confirms a normalized no-op without requiring a new revision", async () => {
    setup(undefined, undefined, true);
    const user = userEvent.setup();
    await user.click(await screen.findByRole("button", { name: /SSH certificate file/u }));
    fireEvent.change(screen.getByLabelText("Retrieval profile name"), {
      target: { value: "SSH certificate file " },
    });
    await user.click(screen.getByRole("button", { name: "Save retrieval profile" }));
    await screen.findByText("Retrieval profile saved.");
    expect(screen.getByLabelText("Retrieval profile name")).toHaveValue("SSH certificate file");
  });
  it.each(["timeout", "unmount"] as const)(
    "bounds pending save confirmation on %s",
    async (outcome) => {
      const { commands, deliverCommitEvents } = setup(undefined, undefined, true);
      const user = userEvent.setup();
      await user.click(await screen.findByRole("button", { name: /SSH certificate file/u }));
      fireEvent.change(screen.getByLabelText("Retrieval profile name"), {
        target: { value: "Pending confirmation" },
      });
      vi.useFakeTimers();
      await act(async () => {
        fireEvent.click(screen.getByRole("button", { name: "Save retrieval profile" }));
        await Promise.resolve();
      });
      expect(screen.queryByText("Retrieval profile saved.")).not.toBeInTheDocument();
      if (outcome === "unmount") cleanup();
      await act(async () => {
        await vi.advanceTimersByTimeAsync(5_001);
      });
      if (outcome === "timeout") {
        expect(screen.getByText(/no committed template was received/u)).toBeVisible();
        expect(screen.getByLabelText("Retrieval profile name")).toHaveValue("Pending confirmation");
      }
      await act(async () => {
        deliverCommitEvents();
        await Promise.resolve();
      });
      expect(screen.queryByText("Retrieval profile saved.")).not.toBeInTheDocument();
      expect(commands.filter((command) => command.command === "recipes.update")).toHaveLength(1);
      cleanup();
      expect(vi.getTimerCount()).toBe(0);
    },
  );
  it.each(["create", "update"] as const)(
    "waits for the committed %s snapshot after acknowledgement",
    async (operation) => {
      const { deliverCommitEvents } = setup(undefined, undefined, true);
      const user = userEvent.setup();
      await user.click(await screen.findByRole("button", { name: /SSH certificate file/u }));
      if (operation === "create")
        await user.click(screen.getByRole("button", { name: "Duplicate" }));
      fireEvent.change(screen.getByLabelText("Retrieval profile name"), {
        target: { value: "Confirmed later" },
      });
      await user.click(screen.getByRole("button", { name: "Save retrieval profile" }));
      expect(screen.queryByText(/no committed template was received/u)).not.toBeInTheDocument();
      expect(screen.queryByText("Retrieval profile saved.")).not.toBeInTheDocument();
      expect(screen.getByRole("button", { name: "Working…" })).toBeDisabled();
      await act(async () => {
        deliverCommitEvents();
        await Promise.resolve();
      });
      await screen.findByText("Retrieval profile saved.");
      expect(screen.getByLabelText("Retrieval profile name")).toHaveValue("Confirmed later");
      expect(screen.getByRole("button", { name: "Save retrieval profile" })).toBeDisabled();
    },
  );
  it("reviews and saves the NSP starter in an existing empty library without executing it", async () => {
    const store = new InMemoryKafkaTrustRecipeStore(
      { durability: "session", state: "ready" },
      { version: 1, recipes: [] },
    );
    const { commands } = setup(undefined, store);
    const user = userEvent.setup();
    const builtIns = await screen.findByRole("button", { name: "Built-in profiles" });
    await waitFor(() => expect(builtIns).toBeEnabled());
    const before = commands.length;
    await user.click(builtIns);
    await user.click(screen.getByRole("menuitem", { name: "nsp-26-04" }));
    expect(screen.getByLabelText("Retrieval profile name")).toHaveValue("nsp-26-04");
    expect(screen.getByLabelText("Material command")).toHaveProperty(
      "value",
      expect.stringContaining("kubectl exec"),
    );
    expect(screen.getByLabelText("Password command")).toHaveProperty(
      "value",
      expect.stringContaining("kubectl get secret"),
    );
    expect(screen.getByLabelText("Suggested OAuth endpoint")).toHaveValue(
      "https://{{host}}/rest-gateway/rest/api/v1/auth/token",
    );
    expect(commands).toHaveLength(before);
    expect((await store.load())?.recipes).toEqual([]);
    fireEvent.change(screen.getByLabelText("Material command"), {
      target: { value: "read-reviewed-jks" },
    });
    fireEvent.change(screen.getByLabelText("Retrieval profile name"), {
      target: { value: "NSP-26-04" },
    });
    await user.click(screen.getByRole("button", { name: "Save retrieval profile" }));
    await waitFor(() =>
      expect(commands.slice(before).map((command) => command.command)).toEqual(["recipes.create"]),
    );
    expect((await store.load())?.recipes).toMatchObject([
      { name: "NSP-26-04", ssh: { value: "read-reviewed-jks" }, revision: 1 },
    ]);
    const savedCount = commands.length;
    await user.click(builtIns);
    await user.click(screen.getByRole("menuitem", { name: "nsp-26-04" }));
    expect(screen.getByLabelText("Material command")).toHaveValue("read-reviewed-jks");
    expect(screen.getByRole("button", { name: "Save retrieval profile" })).toBeDisabled();
    expect(commands).toHaveLength(savedCount);
  });

  it("guards unsaved edits before opening a built-in profile", async () => {
    const { commands } = setup();
    const user = userEvent.setup();
    await user.click(await screen.findByRole("button", { name: /SSH certificate file/u }));
    fireEvent.change(screen.getByLabelText("Retrieval profile name"), {
      target: { value: "Keep my draft" },
    });
    const before = commands.length;
    await user.click(screen.getByRole("button", { name: "Built-in profiles" }));
    await user.click(screen.getByRole("menuitem", { name: "nsp-26-04" }));
    await user.click(screen.getByRole("button", { name: "Keep editing" }));
    expect(screen.getByLabelText("Retrieval profile name")).toHaveValue("Keep my draft");
    expect(commands).toHaveLength(before);
  });

  it("disables built-in profiles when library storage is unavailable", async () => {
    const { commands } = setup(
      undefined,
      new InMemoryKafkaTrustRecipeStore({ durability: "durable", state: "unavailable" }),
    );
    await screen.findByText(/Preserve the template file/u);
    expect(screen.getByRole("button", { name: "Built-in profiles" })).toBeDisabled();
    expect(commands.every((command) => command.command === "recipes.list")).toBe(true);
  });
  it("offers generic Secret Retrieval Profiles without a conversion workflow", async () => {
    const { commands } = setup();
    expect(await screen.findByRole("dialog", { name: "Secret Retrieval Profiles" })).toBeVisible();
    expect(
      screen.queryByRole("button", { name: "Convert legacy templates" }),
    ).not.toBeInTheDocument();
    expect(commands.some((command) => command.command === "recipes.legacy.convert")).toBe(false);
  });
  it("keeps the library and draft unchanged after file selection cancellation or a read failure", async () => {
    const { commands } = setup();
    const user = userEvent.setup();
    await user.click(await screen.findByRole("button", { name: /SSH certificate file/u }));
    const name = screen.getByLabelText("Retrieval profile name");
    const before = commands.length;
    const input = screen.getByLabelText("Import template file");
    fireEvent.change(input, { target: { files: [] } });
    expect(commands).toHaveLength(before);
    expect(name).toHaveValue("SSH certificate file");
    vi.spyOn(FileReader.prototype, "readAsText").mockImplementation(function (this: FileReader) {
      this.dispatchEvent(new ProgressEvent("error"));
    });
    fireEvent.change(input, { target: { files: [new File(["not exposed"], "unreadable.json")] } });
    await screen.findByText(
      "The template file could not be read. Select a readable JSON file and retry.",
    );
    expect(commands).toHaveLength(before);
    expect(name).toHaveValue("SSH certificate file");
    expect(screen.queryByText("not exposed")).not.toBeInTheDocument();
  });

  it("does not mutate the library or report export success when the save boundary fails", async () => {
    const { commands } = setup({
      copy: () => Promise.resolve(),
      download: () => Promise.reject(new Error("The selected destination is not writable.")),
    });
    const user = userEvent.setup();
    await user.click(await screen.findByRole("button", { name: /SSH certificate file/u }));
    const before = commands.length;
    await user.click(screen.getByRole("button", { name: "Export" }));
    await screen.findByText("The selected destination is not writable.");
    expect(commands.slice(before).map((command) => command.command)).toEqual(["recipes.export"]);
    expect(screen.getByLabelText("Retrieval profile name")).toHaveValue("SSH certificate file");
  });

  it("duplicates for review, cancels export without claiming a save, and confirms exact deletion", async () => {
    const { commands } = setup({
      copy: () => Promise.resolve(),
      download: () => Promise.resolve("cancelled"),
    });
    const user = userEvent.setup();
    await user.click(await screen.findByRole("button", { name: /SSH certificate file/u }));
    await user.click(screen.getByRole("button", { name: "Duplicate" }));
    expect(screen.getByLabelText("Retrieval profile name")).toHaveValue(
      "SSH certificate file copy",
    );
    expect(commands.some((command) => command.command === "recipes.create")).toBe(false);
    await user.click(screen.getByRole("button", { name: "Save retrieval profile" }));
    await screen.findByText("Retrieval profile saved.");
    await user.click(screen.getByRole("button", { name: "Export" }));
    await screen.findByText("Export cancelled. No template changed.");
    await user.click(screen.getByRole("button", { name: "Delete retrieval profile" }));
    await screen.findByRole("dialog", {
      name: "Delete retrieval profile SSH certificate file copy?",
    });
    expect(screen.getByText("No profiles use this template.")).toBeVisible();
    expect(commands.some((command) => command.command === "recipes.usage")).toBe(true);
    expect(commands.some((command) => command.command === "recipes.delete")).toBe(false);
    await user.click(screen.getByRole("button", { name: "Confirm deletion" }));
    await screen.findByText("Template deleted. Stored Kafka trust was not changed.");
  });
  it("creates one complete recipe without remote operations and filters without discarding its editor", async () => {
    const { commands } = setup();
    const user = userEvent.setup();
    await screen.findByRole("button", { name: "New retrieval profile" });
    expect(screen.queryByText(/Acquisition still uses legacy/u)).not.toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "New retrieval profile" }));
    await user.type(screen.getByLabelText("Retrieval profile name"), "Lab certificate");
    await user.type(screen.getByLabelText("Remote file"), "/etc/kafka/ca.pem");
    await user.click(screen.getByRole("button", { name: "Save retrieval profile" }));
    await screen.findByText("Retrieval profile saved.");
    expect(commands.filter((command) => command.command === "recipes.create")).toHaveLength(1);
    expect(commands.every((command) => command.command.startsWith("recipes."))).toBe(true);
    await user.type(screen.getByLabelText("Search retrieval profiles"), "absent");
    expect(screen.getByText("No matching templates.")).toBeVisible();
    expect(screen.getByLabelText("Retrieval profile name")).toHaveValue("Lab certificate");
    expect(screen.queryByRole("tab")).not.toBeInTheDocument();
  });

  it("validates the source before saving and requires confirmation to abandon an edited draft", async () => {
    const { commands, closeCount } = setup();
    const user = userEvent.setup();
    await user.click(await screen.findByRole("button", { name: "New retrieval profile" }));
    await user.type(screen.getByLabelText("Retrieval profile name"), "Unfinished");
    await user.click(screen.getByRole("button", { name: "Save retrieval profile" }));
    expect(commands.some((command) => command.command === "recipes.create")).toBe(false);
    expect(screen.getByLabelText("Remote file")).toHaveAttribute("aria-invalid", "true");
    await user.click(await screen.findByRole("button", { name: "Close retrieval profiles" }));
    const confirm = screen.getByRole("dialog", { name: "Discard template changes?" });
    await user.click(within(confirm).getByRole("button", { name: "Keep editing" }));
    expect(closeCount()).toBe(0);
    expect(screen.getByLabelText("Retrieval profile name")).toHaveValue("Unfinished");
    await user.click(await screen.findByRole("button", { name: "Close retrieval profiles" }));
    await user.click(screen.getByRole("button", { name: "Discard changes" }));
    expect(closeCount()).toBe(1);
  });

  it("reviews imported files without saving and rejects oversized files before host submission", async () => {
    const { commands } = setup();
    await screen.findByRole("button", { name: "New retrieval profile" });
    const input = screen.getByLabelText("Import template file");
    fireEvent.change(input, { target: { files: [new File(["x".repeat(262145)], "huge.json")] } });
    await screen.findByText(/exceeds the.*byte limit/u);
    expect(commands.some((command) => command.command === "recipes.import.preview")).toBe(false);
    const content = JSON.stringify({
      format: "streamskope-trust-recipe",
      version: 1,
      recipe: {
        name: "Imported CA",
        kind: "pem",
        method: "ssh",
        syntax: "named-v1",
        ssh: { source: "file", value: "/etc/ca.pem", password: { source: "none" } },
        parameters: [],
        timeoutSeconds: 30,
      },
    });
    fireEvent.change(input, { target: { files: [new File([content], "ca.json")] } });
    await waitFor(() =>
      expect(screen.getByLabelText("Retrieval profile name")).toHaveValue("Imported CA"),
    );
    expect(commands.some((command) => command.command === "recipes.create")).toBe(false);
    expect(screen.getByText(/Review imported commands/u)).toBeVisible();
  });
});
