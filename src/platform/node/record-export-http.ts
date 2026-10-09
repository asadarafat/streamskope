import type { IncomingMessage, ServerResponse } from "node:http";

import { parseArtifactReference } from "../desktop";

import {
  RecordExportFileError,
  type RecordExportDelivery,
  type RecordExportDownloadAuthority,
} from "./record-export-artifacts";

export const RECORD_EXPORT_HTTP_PATH = "/__streamskope_host/exports/";

function waitForResponse(
  response: ServerResponse,
  event: "drain" | "finish",
  signal: AbortSignal,
): Promise<void> {
  return new Promise((resolve, reject) => {
    const clear = (): void => {
      response.removeListener(event, done);
      response.removeListener("error", failed);
      response.removeListener("close", closed);
      signal.removeEventListener("abort", aborted);
    };
    const done = (): void => {
      clear();
      resolve();
    };
    const failed = (): void => {
      clear();
      reject(new RecordExportFileError("unavailable"));
    };
    const closed = (): void => {
      if (event === "finish" && response.writableFinished) done();
      else failed();
    };
    const aborted = (): void => {
      response.destroy();
      failed();
    };
    response.once(event, done);
    response.once("error", failed);
    response.once("close", closed);
    signal.addEventListener("abort", aborted, { once: true });
    if (signal.aborted) aborted();
    else if (response.destroyed) closed();
  });
}

/** Called by the host ONLY after its cookie/token, host and origin gate. */
export async function handleRecordExportRequest(
  delivery: RecordExportDelivery,
  request: IncomingMessage,
  response: ServerResponse,
  authority: RecordExportDownloadAuthority,
): Promise<boolean> {
  if (!request.url?.startsWith(RECORD_EXPORT_HTTP_PATH)) return false;
  try {
    authority.signal.throwIfAborted();
    authority.assertCurrent();
    if (!["GET", "HEAD"].includes(request.method ?? "") || request.headers.range !== undefined)
      throw new RecordExportFileError("unavailable");
    const parts = request.url.slice(RECORD_EXPORT_HTTP_PATH.length).split("/");
    if (parts.length !== 2) throw new RecordExportFileError("unavailable");
    const reference = parseArtifactReference({ artifactId: parts[0], part: parts[1] });
    const metadata = delivery.describe(reference);
    const headers = (): void => {
      authority.signal.throwIfAborted();
      authority.assertCurrent();
      response.setHeader("Cache-Control", "no-store");
      response.setHeader("X-Content-Type-Options", "nosniff");
      response.setHeader("Content-Type", metadata.mediaType);
      response.setHeader("Content-Length", metadata.bytes);
      response.setHeader("Content-Disposition", `attachment; filename="${metadata.fileName}"`);
    };
    if (request.method === "HEAD") {
      headers();
      response.end();
      return true;
    }
    const disconnected = new AbortController();
    const onClose = (): void => {
      if (!response.writableFinished) disconnected.abort();
    };
    response.on("close", onClose);
    try {
      await delivery.withDownload(
        reference,
        { ...authority, signal: AbortSignal.any([authority.signal, disconnected.signal]) },
        async (chunks, signal) => {
          signal.throwIfAborted();
          headers();
          const aborted = (): void => {
            response.destroy();
          };
          signal.addEventListener("abort", aborted, { once: true });
          try {
            for await (const chunk of chunks) {
              signal.throwIfAborted();
              authority.assertCurrent();
              if (!response.write(chunk)) await waitForResponse(response, "drain", signal);
            }
            signal.throwIfAborted();
            authority.assertCurrent();
            const finished = waitForResponse(response, "finish", signal);
            response.end();
            await finished;
          } finally {
            signal.removeEventListener("abort", aborted);
          }
        },
      );
    } finally {
      response.removeListener("close", onClose);
    }
  } catch (error) {
    if (response.headersSent || response.destroyed) response.destroy();
    else {
      response.removeHeader("Content-Length");
      response.removeHeader("Content-Disposition");
      response.statusCode =
        error instanceof RecordExportFileError && error.code === "busy" ? 409 : 404;
      response.setHeader("Cache-Control", "no-store");
      response.setHeader("Content-Type", "application/json");
      response.end(
        JSON.stringify({
          error:
            "The export is unavailable, expired or already being downloaded. Create a new export or retry when the current download finishes.",
        }),
      );
    }
  }
  return true;
}
