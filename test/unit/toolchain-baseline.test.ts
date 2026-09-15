import { describe, expect, it } from "vitest";

describe("supported development runtime", () => {
  it("runs the verification suite on Node 24", () => {
    const majorVersion = Number.parseInt(process.versions.node.split(".")[0] ?? "", 10);

    expect(majorVersion).toBe(24);
  });
});
