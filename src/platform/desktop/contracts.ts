export const DESKTOP_PLATFORM_VERSION = 1 as const;

export const DESKTOP_TEXT_DOCUMENT_LIMITS = Object.freeze({
  bytes: 16 * 1_048_576,
  fileNameCharacters: 255,
});

export const DESKTOP_ACTIONS = ["activity.open", "preferences.open"] as const;
export type DesktopActionName = (typeof DESKTOP_ACTIONS)[number];

export interface DesktopTextDocument {
  readonly byteSize: number;
  readonly content: string;
  readonly fileName: string;
  readonly mediaType: "application/json";
}

export interface DesktopAction {
  readonly action: DesktopActionName;
  readonly version: typeof DESKTOP_PLATFORM_VERSION;
}

export interface DesktopSaveResult {
  readonly state: "cancelled" | "saved";
  readonly version: typeof DESKTOP_PLATFORM_VERSION;
}

export type DesktopActionListener = (action: DesktopAction) => void;

export interface StreamSkopeDesktop {
  saveTextDocument(document: DesktopTextDocument): Promise<DesktopSaveResult>;
  subscribeActions(listener: DesktopActionListener): () => void;
}

export class DesktopPlatformContractError extends Error {
  constructor(
    readonly path: string,
    message: string,
  ) {
    super(`${path} ${message}`);
    this.name = "DesktopPlatformContractError";
  }
}

function record(value: unknown, path: string): Readonly<Record<string, unknown>> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new DesktopPlatformContractError(path, "must be an object.");
  }
  return value as Readonly<Record<string, unknown>>;
}

function exactKeys(
  value: Readonly<Record<string, unknown>>,
  expected: readonly string[],
  path: string,
): void {
  const actual = Object.keys(value).sort();
  const keys = [...expected].sort();
  if (actual.length !== keys.length || actual.some((key, index) => key !== keys[index])) {
    throw new DesktopPlatformContractError(path, "contains undeclared fields.");
  }
}

export function parseDesktopTextDocument(
  value: unknown,
  path = "desktop document",
): DesktopTextDocument {
  const document = record(value, path);
  exactKeys(document, ["byteSize", "content", "fileName", "mediaType"], path);
  if (typeof document.content !== "string") {
    throw new DesktopPlatformContractError(`${path}.content`, "must be text.");
  }
  const byteSize = new TextEncoder().encode(document.content).byteLength;
  if (
    !Number.isSafeInteger(document.byteSize) ||
    document.byteSize !== byteSize ||
    byteSize > DESKTOP_TEXT_DOCUMENT_LIMITS.bytes
  ) {
    throw new DesktopPlatformContractError(
      `${path}.byteSize`,
      `must equal content size within ${String(DESKTOP_TEXT_DOCUMENT_LIMITS.bytes)} bytes.`,
    );
  }
  if (
    typeof document.fileName !== "string" ||
    document.fileName.length === 0 ||
    document.fileName.length > DESKTOP_TEXT_DOCUMENT_LIMITS.fileNameCharacters ||
    !document.fileName.endsWith(".json") ||
    document.fileName.includes("/") ||
    document.fileName.includes("\\") ||
    document.fileName === ".json"
  ) {
    throw new DesktopPlatformContractError(
      `${path}.fileName`,
      "must be a bounded safe JSON file name.",
    );
  }
  if (document.mediaType !== "application/json") {
    throw new DesktopPlatformContractError(`${path}.mediaType`, "must equal application/json.");
  }
  try {
    JSON.parse(document.content);
  } catch {
    throw new DesktopPlatformContractError(`${path}.content`, "must contain valid JSON.");
  }
  return {
    byteSize,
    content: document.content,
    fileName: document.fileName,
    mediaType: document.mediaType,
  };
}

export function parseDesktopAction(value: unknown): DesktopAction {
  const action = record(value, "desktop action");
  exactKeys(action, ["action", "version"], "desktop action");
  if (
    action.version !== DESKTOP_PLATFORM_VERSION ||
    typeof action.action !== "string" ||
    !DESKTOP_ACTIONS.includes(action.action as DesktopActionName)
  ) {
    throw new DesktopPlatformContractError("desktop action", "is not declared.");
  }
  return {
    action: action.action as DesktopActionName,
    version: DESKTOP_PLATFORM_VERSION,
  };
}

export function parseDesktopSaveResult(value: unknown): DesktopSaveResult {
  const result = record(value, "desktop save result");
  exactKeys(result, ["state", "version"], "desktop save result");
  if (
    result.version !== DESKTOP_PLATFORM_VERSION ||
    (result.state !== "cancelled" && result.state !== "saved")
  ) {
    throw new DesktopPlatformContractError("desktop save result", "is invalid.");
  }
  return {
    state: result.state,
    version: DESKTOP_PLATFORM_VERSION,
  };
}
