import { Buffer } from "node:buffer";
import { createHash } from "node:crypto";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import process from "node:process";
import { URL } from "node:url";

import { chromium } from "@playwright/test";

// Only regeneration needs Playwright; normal packaging uses the checked-in assets.
const root = new URL("../", import.meta.url);
const source = "src/platform/ui/assets/streamskope.svg";
const svg = await readFile(new URL(source, root));
const browser = await chromium.launch();
const pngs = new Map();
try {
  const page = await browser.newPage({ deviceScaleFactor: 1 });
  for (const size of [16, 32, 48, 64, 128, 256, 512, 1024]) {
    await page.setViewportSize({ width: size, height: size });
    await page.setContent(
      `<style>html,body{margin:0;width:100%;height:100%}svg{display:block;width:100%;height:100%}</style>${svg.toString("utf8")}`,
    );
    pngs.set(size, await page.screenshot({ omitBackground: true }));
  }
} finally {
  await browser.close();
}

const icnsFrames = [
  ["ic07", 128],
  ["ic08", 256],
  ["ic09", 512],
  ["ic10", 1024],
  ["ic11", 32],
  ["ic12", 64],
].map(([type, size]) => {
  const png = pngs.get(size);
  const header = Buffer.alloc(8);
  header.write(type, 0, "ascii");
  header.writeUInt32BE(png.length + 8, 4);
  return Buffer.concat([header, png]);
});
const icnsHeader = Buffer.alloc(8);
icnsHeader.write("icns", 0, "ascii");
icnsHeader.writeUInt32BE(8 + icnsFrames.reduce((length, frame) => length + frame.length, 0), 4);

const icoSizes = [16, 32, 48, 64, 128, 256];
const icoDirectory = Buffer.alloc(6 + icoSizes.length * 16);
icoDirectory.writeUInt16LE(1, 2);
icoDirectory.writeUInt16LE(icoSizes.length, 4);
let offset = icoDirectory.length;
for (const [index, size] of icoSizes.entries()) {
  const entry = 6 + index * 16;
  const png = pngs.get(size);
  icoDirectory[entry] = size === 256 ? 0 : size;
  icoDirectory[entry + 1] = icoDirectory[entry];
  icoDirectory.writeUInt16LE(1, entry + 4);
  icoDirectory.writeUInt16LE(32, entry + 6);
  icoDirectory.writeUInt32LE(png.length, entry + 8);
  icoDirectory.writeUInt32LE(offset, entry + 12);
  offset += png.length;
}

const outputs = {
  "assets/icons/streamskope.png": pngs.get(1024),
  "assets/icons/streamskope.icns": Buffer.concat([icnsHeader, ...icnsFrames]),
  "assets/icons/streamskope.ico": Buffer.concat([
    icoDirectory,
    ...icoSizes.map((size) => pngs.get(size)),
  ]),
};
await mkdir(new URL("assets/icons/", root), { recursive: true });
for (const [path, content] of Object.entries(outputs)) {
  await writeFile(new URL(path, root), content);
}
const sha256 = Object.fromEntries(
  Object.entries({ [source]: svg, ...outputs }).map(([path, content]) => [
    path,
    createHash("sha256").update(content).digest("hex"),
  ]),
);
await writeFile(
  new URL("assets/icons/generation.json", root),
  `${JSON.stringify({ source, sha256 }, null, 2)}\n`,
);
process.stdout.write("Generated StreamSkope PNG, ICNS and ICO icons from the shared SVG.\n");
