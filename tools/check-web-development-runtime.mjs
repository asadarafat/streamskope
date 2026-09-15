import { Buffer } from "node:buffer";

import { transformSync } from "esbuild";

transformSync("const value: number = 1", { loader: "ts" });
await import("vite");
const { crc32 } = await import("@node-rs/crc32");
crc32(Buffer.from("StreamSkope"));
