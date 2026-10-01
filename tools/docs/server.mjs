/* global URL */
import assert from "node:assert/strict";
import { createServer } from "node:http";
import { readFile } from "node:fs/promises";
import { resolve, extname, sep } from "node:path";
const mime = {
  ".html": "text/html",
  ".js": "text/javascript",
  ".css": "text/css",
  ".json": "application/json",
  ".svg": "image/svg+xml",
  ".png": "image/png",
  ".mp3": "audio/mpeg",
  ".mp4": "video/mp4",
};
export async function serveSite(root, prefix) {
  const server = createServer(async (request, response) => {
    try {
      const path = decodeURIComponent(new URL(request.url, "http://localhost").pathname);
      assert(path.startsWith(prefix));
      const file = resolve(
        root,
        path.slice(prefix.length) + (path.endsWith("/") ? "index.html" : ""),
      );
      assert(file.startsWith(root + sep));
      response.setHeader("Content-Type", mime[extname(file)] ?? "application/octet-stream");
      const bytes = await readFile(file);
      response.setHeader("Accept-Ranges", "bytes");
      const range = request.headers.range?.match(/^bytes=(\d+)-(\d*)$/);
      if (range) {
        const start = Number(range[1]);
        const end = Math.min(range[2] ? Number(range[2]) : bytes.length - 1, bytes.length - 1);
        if (start > end || start >= bytes.length) {
          response.writeHead(416, { "Content-Range": `bytes */${bytes.length}` }).end();
          return;
        }
        response.writeHead(206, {
          "Content-Range": `bytes ${start}-${end}/${bytes.length}`,
          "Content-Length": end - start + 1,
        });
        response.end(bytes.subarray(start, end + 1));
      } else {
        response.setHeader("Content-Length", bytes.length);
        response.end(bytes);
      }
    } catch {
      response.writeHead(404).end("Not found");
    }
  });
  await new Promise((done) => server.listen(0, "127.0.0.1", done));
  return server;
}
