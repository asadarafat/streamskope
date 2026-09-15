// @vitest-environment jsdom
// @vitest-environment-options {"url":"http://clab.orb.local:5173/"}

import "@testing-library/jest-dom/vitest";

import { screen } from "@testing-library/react";
import { expect, it, vi } from "vitest";

it("starts the workbench from a clean browser-development address", async () => {
  const fetchMock = vi.fn<typeof fetch>().mockResolvedValue(new Response(null, { status: 503 }));
  vi.stubGlobal("fetch", fetchMock);
  document.body.innerHTML = '<div id="root"></div>';

  await import("../../src/renderer/main");

  expect(await screen.findByRole("navigation", { name: "StreamSkope resources" })).toBeVisible();
  expect(screen.getByRole("banner", { name: "StreamSkope application bar" })).toHaveTextContent(
    "StreamSkope",
  );
  expect(screen.queryByText("Development launch link required")).not.toBeInTheDocument();
  expect(window.location.hash).toBe("");
  expect(window.sessionStorage).toHaveLength(0);
  expect(fetchMock).toHaveBeenCalledWith(
    "http://clab.orb.local:5173/__streamskope_host/events",
    expect.objectContaining({ credentials: "same-origin", mode: "same-origin" }),
  );
});
