import { afterEach, describe, expect, it, vi } from "vitest";

import { prepareNatsJsonPresentation } from "../../src/features/nats/ui/bounded-json-presentation";

afterEach((): void => {
  vi.restoreAllMocks();
});

describe("bounded NATS JSON presentation", () => {
  it.each([
    '{"text":"snow ☃, emoji 🙂, \\n and \\t","nested":[true,null,{},[]]}',
    '{"\\ud800":"\\udfff","control":"\\u0000\\u001f","number":1e400}',
    '{"brackets":"[\\"{ and }\\"]","number":-0,"tiny":1e-7,"large":1e20}',
    '{"literal lone surrogate":"\ud800","separator":"\u2028"}',
  ])("matches standard two-space JSON without altering the source: %s", (source): void => {
    const expected = JSON.stringify(JSON.parse(source) as unknown, null, 2);
    expect(prepareNatsJsonPresentation(source)).toEqual({ state: "ready", text: expected });
  });

  it("accepts 32 container levels and rejects deeper input before parsing", (): void => {
    const accepted = `${"[".repeat(32)}0${"]".repeat(32)}`;
    expect(prepareNatsJsonPresentation(accepted).state).toBe("ready");
    const deep = `${"[".repeat(120_000)}0${"]".repeat(120_000)}`;
    expect(new TextEncoder().encode(deep).length).toBeLessThan(256 * 1024);
    const parse = vi.spyOn(JSON, "parse");
    expect(prepareNatsJsonPresentation(deep)).toEqual({ state: "limited", reason: "depth" });
    expect(parse).not.toHaveBeenCalled();
  });

  it("rejects a legal compact wide value before allocating its expanded JSON", (): void => {
    const wide = `${"[".repeat(32)}${Array.from({ length: 20_000 }, (): string => "0").join(",")}${"]".repeat(32)}`;
    expect(new TextEncoder().encode(wide).length).toBeLessThan(256 * 1024);
    const stringify = vi.spyOn(JSON, "stringify");
    expect(prepareNatsJsonPresentation(wide)).toEqual({ state: "limited", reason: "output" });
    expect(stringify).not.toHaveBeenCalled();
  });

  it("allows exactly 1 MiB of formatted UTF-8 and refuses one byte more before formatting", (): void => {
    function atBoundary(padding: number): string {
      const entries = [
        `"${"x".repeat(padding)}"`,
        ...Array.from({ length: 15_617 }, (): string => "0"),
      ];
      return `${"[".repeat(32)}${entries.join(",")}${"]".repeat(32)}`;
    }
    const accepted = prepareNatsJsonPresentation(atBoundary(59));
    expect(accepted.state).toBe("ready");
    if (accepted.state !== "ready")
      throw new Error("Expected the exact presentation boundary to fit.");
    expect(new TextEncoder().encode(accepted.text).length).toBe(1024 * 1024);
    const stringify = vi.spyOn(JSON, "stringify");
    expect(prepareNatsJsonPresentation(atBoundary(60))).toEqual({
      state: "limited",
      reason: "output",
    });
    expect(stringify).not.toHaveBeenCalled();
  });

  it("accounts for UTF-8 payload and presentation bytes rather than UTF-16 character counts", (): void => {
    const compact = `${"[".repeat(32)}${Array.from({ length: 15_000 }, (): string => '"🙂"').join(",")}${"]".repeat(32)}`;
    expect(new TextEncoder().encode(compact).length).toBeLessThan(256 * 1024);
    expect(prepareNatsJsonPresentation(compact)).toEqual({ state: "limited", reason: "output" });
    expect(prepareNatsJsonPresentation(`"${"🙂".repeat(70_000)}"`)).toEqual({
      state: "limited",
      reason: "input",
    });
  });

  it("returns an invalid result without exposing parse errors or payload fragments", (): void => {
    expect(prepareNatsJsonPresentation('{"private-fragment": invalid}')).toEqual({
      state: "invalid",
    });
  });
});
