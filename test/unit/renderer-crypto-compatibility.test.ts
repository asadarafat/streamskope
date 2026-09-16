import { describe, expect, it, vi } from "vitest";

import { installRendererRandomUuid } from "../../src/platform/electron/renderer/crypto-compatibility";

describe("renderer crypto compatibility", () => {
  it("installs an RFC 4122 version 4 UUID backed by random bytes", () => {
    const crypto = {
      getRandomValues(array: Uint8Array): Uint8Array {
        array.set(Array.from({ length: 16 }, (_value, index) => index));
        return array;
      },
    };

    installRendererRandomUuid(crypto);

    const randomUUID = Reflect.get(crypto, "randomUUID") as (() => string) | undefined;
    expect(randomUUID).toBeTypeOf("function");
    if (typeof randomUUID !== "function") {
      throw new Error("Renderer UUID compatibility was not installed.");
    }
    expect(randomUUID()).toBe("00010203-0405-4607-8809-0a0b0c0d0e0f");
  });

  it("retains the platform implementation when randomUUID is available", () => {
    const randomUUID = vi.fn(() => "platform-random-uuid");
    const crypto = {
      getRandomValues(array: Uint8Array): Uint8Array {
        throw new Error(`Unexpected random byte request for ${array.byteLength} bytes.`);
      },
      randomUUID,
    };

    installRendererRandomUuid(crypto);

    expect(crypto.randomUUID).toBe(randomUUID);
    expect(crypto.randomUUID()).toBe("platform-random-uuid");
  });
});
