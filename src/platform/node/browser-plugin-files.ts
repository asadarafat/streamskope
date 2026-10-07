import { AsyncLocalStorage } from "node:async_hooks";
import type { IncomingMessage, ServerResponse } from "node:http";

import { MAX_PLUGIN_ARCHIVE_BYTES } from "./plugins/package";

const FILE_ROUTE = "/__streamskope_session/plugin-file/";
const COMMAND_ID = /^[A-Za-z0-9_-]{1,128}$/u;
const FILE_LIFETIME_MS = 60_000;
const MAX_STAGED_FILES = 2;
const MAX_UPLOAD_DURATION_MS = 30_000;

interface StagedFile {
  readonly bytes: Buffer;
  readonly expiresAt: number;
  readonly expiry: ReturnType<typeof setTimeout>;
}

interface FileSelectionContext {
  readonly id: string;
  selected?: Buffer;
}

class BrowserPluginFileError extends Error {
  constructor(
    readonly status: number,
    readonly code: string,
    message: string,
  ) {
    super(message);
    this.name = "BrowserPluginFileError";
  }
}

function fileError(status: number, code: string, summary: string): BrowserPluginFileError {
  return new BrowserPluginFileError(status, code, summary);
}

function errorResponse(response: ServerResponse, error: BrowserPluginFileError): void {
  if (response.destroyed || response.writableEnded) return;
  response.writeHead(error.status, {
    "cache-control": "no-store",
    "content-type": "application/json; charset=utf-8",
  });
  response.end(JSON.stringify({ error: { code: error.code, summary: error.message } }));
}

/** One unlocked browser session owns this bounded, transient file-selection bridge. */
export class BrowserPluginFiles {
  private readonly staged = new Map<string, StagedFile>();
  private readonly context = new AsyncLocalStorage<FileSelectionContext>();
  private readonly active = new Map<string, FileSelectionContext>();
  private pendingUpload: AbortController | undefined;
  private closed = false;

  stage(commandId: string, bytes: Uint8Array): void {
    this.assertOpen();
    this.assertId(commandId);
    this.pruneExpired();
    if (this.staged.has(commandId) || this.active.has(commandId)) {
      throw fileError(409, "PLUGIN_FILE_BUSY", "This plugin file selection is already in use.");
    }
    if (bytes.byteLength === 0 || bytes.byteLength > MAX_PLUGIN_ARCHIVE_BYTES) {
      throw fileError(413, "PLUGIN_FILE_SIZE", "A plugin file must be between 1 byte and 48 MiB.");
    }
    const stagedBytes = [...this.staged.values()].reduce((sum, file) => sum + file.bytes.length, 0);
    const selected = [...this.active.values()].filter(
      (selection) => selection.selected !== undefined,
    );
    const selectedBytes = selected.reduce((sum, selection) => sum + selection.selected!.length, 0);
    if (
      this.staged.size + selected.length >= MAX_STAGED_FILES ||
      stagedBytes + selectedBytes + bytes.byteLength > MAX_PLUGIN_ARCHIVE_BYTES
    ) {
      throw fileError(409, "PLUGIN_FILE_BUSY", "Complete the current plugin file selection first.");
    }
    const expiry = setTimeout(() => this.discard(commandId), FILE_LIFETIME_MS);
    expiry.unref();
    this.staged.set(commandId, {
      bytes: Buffer.from(bytes),
      expiresAt: Date.now() + FILE_LIFETIME_MS,
      expiry,
    });
  }

  discard(commandId: string): void {
    const file = this.staged.get(commandId);
    if (file === undefined) return;
    clearTimeout(file.expiry);
    file.bytes.fill(0);
    this.staged.delete(commandId);
  }

  async run<Result>(commandId: string, action: () => Promise<Result>): Promise<Result> {
    this.assertOpen();
    // Normal commands do not need a staged file; only chooseFile consumes one.
    if (this.active.has(commandId)) {
      throw fileError(409, "PLUGIN_FILE_BUSY", "This command is already in progress.");
    }
    const selection: FileSelectionContext = { id: commandId };
    this.active.set(commandId, selection);
    try {
      return await this.context.run(selection, action);
    } finally {
      selection.selected?.fill(0);
      this.discard(commandId);
      this.active.delete(commandId);
    }
  }

  chooseFile(signal: AbortSignal): Promise<Uint8Array | null> {
    try {
      this.assertOpen();
      const selection = this.context.getStore();
      if (selection === undefined || selection.selected !== undefined) {
        throw fileError(409, "PLUGIN_FILE_MISSING", "Select a plugin file for this request first.");
      }
      if (signal.aborted) {
        this.discard(selection.id);
        signal.throwIfAborted();
      }
      this.pruneExpired();
      const file = this.staged.get(selection.id);
      if (file === undefined) {
        throw fileError(409, "PLUGIN_FILE_MISSING", "Select a plugin file for this request first.");
      }
      clearTimeout(file.expiry);
      this.staged.delete(selection.id);
      selection.selected = file.bytes;
      return Promise.resolve(file.bytes);
    } catch (error) {
      return Promise.reject(
        error instanceof Error
          ? error
          : new Error("Plugin file selection failed.", { cause: error }),
      );
    }
  }

  /** Caller must already have enforced the exact origin and unlocked-session cookie. */
  async handleRequest(request: IncomingMessage, response: ServerResponse): Promise<boolean> {
    const path = request.url ?? "";
    if (!path.startsWith(FILE_ROUTE)) return false;
    const commandId = path.slice(FILE_ROUTE.length);
    try {
      this.assertOpen();
      this.assertId(commandId);
      if (request.method === "DELETE") {
        this.discard(commandId);
      } else if (request.method === "POST") {
        await this.upload(request, commandId);
      } else {
        throw fileError(405, "METHOD_NOT_ALLOWED", "Use POST to select a plugin file.");
      }
      if (!response.destroyed && !response.writableEnded) {
        response.writeHead(204, { "cache-control": "no-store" });
        response.end();
      }
    } catch (error) {
      if (!request.complete) {
        // Finish the bounded error response, then close an unread or oversized body.
        response.once("finish", (): void => {
          request.destroy();
        });
      }
      errorResponse(
        response,
        error instanceof BrowserPluginFileError
          ? error
          : fileError(400, "PLUGIN_FILE_UPLOAD", "The plugin file upload could not be completed."),
      );
    }
    return true;
  }

  close(): void {
    this.closed = true;
    this.pendingUpload?.abort();
    for (const id of this.staged.keys()) this.discard(id);
    for (const selection of this.active.values()) selection.selected?.fill(0);
  }

  private assertOpen(): void {
    if (this.closed) {
      throw fileError(
        503,
        "VAULT_LOCKED",
        "Unlock the StreamSkope vault and select the file again.",
      );
    }
  }

  private assertId(id: string): void {
    if (!COMMAND_ID.test(id)) {
      throw fileError(400, "PLUGIN_FILE_REQUEST", "The plugin file request identifier is invalid.");
    }
  }

  private pruneExpired(): void {
    for (const [id, file] of this.staged) {
      if (file.expiresAt <= Date.now()) this.discard(id);
    }
  }

  private async upload(request: IncomingMessage, commandId: string): Promise<void> {
    if (this.pendingUpload !== undefined) {
      throw fileError(409, "PLUGIN_FILE_BUSY", "Another plugin file is being uploaded.");
    }
    if (request.headers["content-type"] !== "application/octet-stream") {
      throw fileError(415, "PLUGIN_FILE_TYPE", "Upload the signed plugin file as binary data.");
    }
    const declaredLength = request.headers["content-length"];
    if (
      declaredLength !== undefined &&
      (!/^\d+$/u.test(declaredLength) ||
        Number(declaredLength) === 0 ||
        Number(declaredLength) > MAX_PLUGIN_ARCHIVE_BYTES)
    ) {
      throw fileError(413, "PLUGIN_FILE_SIZE", "A plugin file must be between 1 byte and 48 MiB.");
    }
    const controller = new AbortController();
    this.pendingUpload = controller;
    const timeout = setTimeout(() => controller.abort(), MAX_UPLOAD_DURATION_MS);
    timeout.unref();
    const abort = (): void => {
      request.destroy();
    };
    controller.signal.addEventListener("abort", abort, { once: true });
    const chunks: Buffer[] = [];
    let bytes = 0;
    let joined: Buffer | undefined;
    try {
      for await (const chunk of request.iterator({ destroyOnReturn: false })) {
        const value = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk as Uint8Array);
        chunks.push(value);
        bytes += value.byteLength;
        if (bytes > MAX_PLUGIN_ARCHIVE_BYTES) {
          throw fileError(413, "PLUGIN_FILE_SIZE", "The plugin file exceeds 48 MiB.");
        }
        controller.signal.throwIfAborted();
      }
      this.assertOpen();
      controller.signal.throwIfAborted();
      joined = Buffer.concat(chunks, bytes);
      this.stage(commandId, joined);
    } finally {
      clearTimeout(timeout);
      controller.signal.removeEventListener("abort", abort);
      for (const chunk of chunks) chunk.fill(0);
      joined?.fill(0);
      this.pendingUpload = undefined;
    }
  }
}
