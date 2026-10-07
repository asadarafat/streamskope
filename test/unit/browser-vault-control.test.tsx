// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";

import { cleanup, render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, expect, it, vi } from "vitest";

import { BrowserVaultControl } from "../../src/platform/ui/BrowserVaultControl";
import { StreamSkopeThemeProvider } from "../../src/platform/ui/StreamSkopeThemeProvider";

afterEach(() => {
  cleanup();
  delete window.streamSkopeBrowserRuntime;
});

it("keeps desktop and development headers free of unavailable vault actions", () => {
  render(
    <StreamSkopeThemeProvider>
      <BrowserVaultControl />
    </StreamSkopeThemeProvider>,
  );
  expect(screen.queryByRole("button", { name: /Lock vault/u })).not.toBeInTheDocument();
});

it("uses the browser host's cleanup barrier once and shows an actionable failure", async () => {
  const lockVault = vi.fn().mockRejectedValue(new Error("private error must not be shown"));
  window.streamSkopeBrowserRuntime = { pluginFileUpload: true, lockVault };
  render(
    <StreamSkopeThemeProvider>
      <BrowserVaultControl />
    </StreamSkopeThemeProvider>,
  );
  const user = userEvent.setup();
  await user.click(screen.getByRole("button", { name: "Lock vault and disconnect" }));
  expect(lockVault).toHaveBeenCalledTimes(1);
  expect(
    await screen.findByRole("dialog", { name: "Vault lock could not be confirmed" }),
  ).toHaveTextContent("check any active remote capture resources");
  expect(screen.queryByText("private error must not be shown")).not.toBeInTheDocument();
  await user.click(screen.getByRole("button", { name: "Close" }));
});
