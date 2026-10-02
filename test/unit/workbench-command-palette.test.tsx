// @vitest-environment jsdom

import "@testing-library/jest-dom/vitest";
import { cleanup, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { useState } from "react";
import { afterEach, expect, it, vi } from "vitest";

import { WorkbenchCommandPalette } from "../../src/features/kafka/ui/WorkbenchCommandPalette";

afterEach(cleanup);

it("navigates enabled results with arrows and Enter, and leaves disabled commands inert", async () => {
  const user = userEvent.setup();
  const unavailable = vi.fn();
  const queries = vi.fn();
  render(
    <WorkbenchCommandPalette
      open
      connected={false}
      profiles={[]}
      topics={[]}
      onClose={vi.fn()}
      onOpenResource={vi.fn()}
      onOpenTopic={vi.fn()}
      onSelectProfile={vi.fn()}
      actions={[
        { id: "read", label: "Read unavailable", disabled: true, run: unavailable },
        { id: "queries", label: "Saved queries", disabled: false, run: queries },
      ]}
    />,
  );
  const search = screen.getByRole("searchbox");
  await waitFor(() => expect(search).toHaveFocus());
  await user.keyboard("{ArrowDown}");
  expect(screen.getByRole("button", { name: "Saved queries" })).toHaveFocus();
  await user.keyboard("{Enter}");
  expect(queries).toHaveBeenCalledTimes(1);
  await user.click(search);
  await user.type(search, "Read unavailable");
  await user.keyboard("{ArrowDown}{Enter}");
  expect(unavailable).not.toHaveBeenCalled();
  expect(search).toHaveFocus();
});

it("returns focus to the opener on Escape and supports first/last result navigation", async () => {
  const user = userEvent.setup();
  function Fixture(): React.JSX.Element {
    const [open, setOpen] = useState(false);
    return (
      <>
        <button onClick={() => setOpen(true)}>Commands</button>
        <WorkbenchCommandPalette
          open={open}
          connected
          profiles={[]}
          topics={[]}
          onClose={() => setOpen(false)}
          onOpenResource={vi.fn()}
          onOpenTopic={vi.fn()}
          onSelectProfile={vi.fn()}
        />
      </>
    );
  }
  render(<Fixture />);
  await user.tab();
  await user.keyboard("{Enter}");
  await waitFor(() => expect(screen.getByRole("searchbox")).toHaveFocus());
  await user.keyboard("{ArrowDown}");
  const first = document.activeElement;
  await user.keyboard("{End}");
  expect(document.activeElement).not.toBe(first);
  await user.keyboard("{Home}");
  expect(document.activeElement).toBe(first);
  await user.keyboard("{Escape}");
  await waitFor(() => expect(screen.getByRole("button", { name: "Commands" })).toHaveFocus());
});
