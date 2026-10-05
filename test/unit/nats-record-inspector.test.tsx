// @vitest-environment jsdom

import "@testing-library/jest-dom/vitest";

import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import { RecordInspector } from "../../src/features/nats/ui/RecordInspector";
import { uiNatsRecord } from "../support/nats-ui-host-fixture";

afterEach((): void => {
  cleanup();
  vi.restoreAllMocks();
});

describe("NATS record inspector optional presentation", () => {
  it("keeps exact original text and prepares JSON only when explicitly requested", (): void => {
    const source = '  {"value":"雪 🙂","items":[1,2]}\n';
    const parse = vi.spyOn(JSON, "parse");
    render(
      <RecordInspector record={uiNatsRecord("first", "generation", source)} onClose={vi.fn()} />,
    );
    expect(screen.getByLabelText("Original payload").textContent).toBe(source);
    expect(screen.getByRole("button", { name: "Close record inspector" })).toHaveFocus();
    expect(parse).not.toHaveBeenCalledWith(source);
    fireEvent.click(screen.getByRole("button", { name: "Pretty JSON" }));
    expect(parse).toHaveBeenCalledWith(source);
    expect(screen.getByLabelText("Pretty JSON payload").textContent).toBe(
      '{\n  "value": "雪 🙂",\n  "items": [\n    1,\n    2\n  ]\n}',
    );
    fireEvent.click(screen.getByRole("button", { name: "Original" }));
    expect(screen.getByLabelText("Original payload").textContent).toBe(source);
    fireEvent.click(screen.getByRole("button", { name: "Pretty JSON" }));
    expect(parse.mock.calls.filter(([input]): boolean => input === source)).toHaveLength(1);
  });

  it("preserves deeply nested original JSON and gives a safe notice instead of preparing it", (): void => {
    const source = `${"[".repeat(120_000)}0${"]".repeat(120_000)}`;
    const parse = vi.spyOn(JSON, "parse");
    render(
      <RecordInspector record={uiNatsRecord("deep", "generation", source)} onClose={vi.fn()} />,
    );
    expect(screen.getByLabelText("Original payload").textContent).toBe(source);
    fireEvent.click(screen.getByRole("button", { name: "Pretty JSON" }));
    expect(parse).not.toHaveBeenCalledWith(source);
    expect(screen.queryByLabelText("Pretty JSON payload")).not.toBeInTheDocument();
    expect(screen.getByRole("alert")).toHaveTextContent("32-level or 1 MiB presentation limit");
    expect(screen.getByLabelText("Original payload").textContent).toBe(source);
  });

  it("keeps invalid JSON unchanged and resets presentation when selecting another record", (): void => {
    const source = '{"sensitive-value": invalid}';
    const props = { record: uiNatsRecord("invalid", "generation", source), onClose: vi.fn() };
    const view = render(<RecordInspector {...props} />);
    fireEvent.click(screen.getByRole("button", { name: "Pretty JSON" }));
    expect(screen.getByRole("alert")).toHaveTextContent("payload is not valid JSON");
    expect(screen.getByRole("alert")).not.toHaveTextContent("sensitive-value");
    expect(screen.getByLabelText("Original payload").textContent).toBe(source);
    view.rerender(
      <RecordInspector {...props} record={uiNatsRecord("next", "generation", '{"next":true}')} />,
    );
    expect(screen.queryByRole("alert")).not.toBeInTheDocument();
    expect(screen.getByLabelText("Original payload").textContent).toBe('{"next":true}');
    fireEvent.click(screen.getByRole("button", { name: "Pretty JSON" }));
    expect(screen.getByLabelText("Pretty JSON payload").textContent).toBe('{\n  "next": true\n}');
  });

  it("preserves binary and empty payload behavior and handles Close and Escape", (): void => {
    const onClose = vi.fn();
    const binary = {
      ...uiNatsRecord("binary"),
      payload: { encoding: "base64" as const, data: "AP+AAQ==" },
      payloadBytes: 4,
    };
    const view = render(<RecordInspector record={binary} onClose={onClose} />);
    expect(screen.getByLabelText("Original payload").textContent).toBe("AP+AAQ==");
    expect(screen.queryByRole("button", { name: "Pretty JSON" })).not.toBeInTheDocument();
    fireEvent.keyDown(screen.getByRole("button", { name: "Close record inspector" }), {
      key: "Escape",
    });
    expect(onClose).toHaveBeenCalledOnce();
    view.rerender(
      <RecordInspector record={uiNatsRecord("empty", "generation", "")} onClose={onClose} />,
    );
    expect(screen.getByLabelText("Original payload").textContent).toBe("");
    expect(screen.getByText("Empty payload (0 bytes); the payload is present.")).toBeVisible();
    expect(screen.queryByRole("button", { name: "Pretty JSON" })).not.toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: "Close record inspector" }));
    expect(onClose).toHaveBeenCalledTimes(2);
  });
});
