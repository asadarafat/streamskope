// @vitest-environment jsdom

import "@testing-library/jest-dom/vitest";

import { cleanup, render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, expect, it } from "vitest";

import { WorkbenchApplicationBar } from "../../src/kafka/ui/WorkbenchApplicationBar";
import { StreamSkopeThemeProvider } from "../../src/ui/StreamSkopeThemeProvider";

afterEach(cleanup);

it("keeps the Material crescent icon while switching theme preferences", async () => {
  const user = userEvent.setup();
  render(
    <StreamSkopeThemeProvider>
      <WorkbenchApplicationBar
        navigatorOpen
        navigatorTemporary={false}
        onOpenCommandPalette={() => undefined}
        onOpenPreferences={() => undefined}
        onToggleNavigator={() => undefined}
      />
    </StreamSkopeThemeProvider>,
  );
  const button = screen.getByRole("button", { name: "Theme" });
  expect(within(button).getByTestId("NightlightRoundIcon")).toBeInTheDocument();
  for (const label of ["Light", "Dark", "System"]) {
    await user.click(button);
    await user.click(screen.getByRole("menuitem", { name: label }));
    expect(within(button).getByTestId("NightlightRoundIcon")).toBeInTheDocument();
    expect(button).toHaveAccessibleDescription(`Current theme: ${label.toLowerCase()}`);
  }
});
