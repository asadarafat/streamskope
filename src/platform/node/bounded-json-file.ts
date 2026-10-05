import { readBoundedFile } from "./bounded-file";

function isMissingFile(error: unknown): boolean {
  return error !== null && typeof error === "object" && "code" in error && error.code === "ENOENT";
}

/** Only an absent file is optional; JSON and document failures belong to the caller's policy. */
export async function readOptionalBoundedJsonFile<T>(
  path: string,
  maximumBytes: number,
  parse: (value: unknown) => T,
  signal?: AbortSignal,
): Promise<T | undefined> {
  let contents: Buffer;
  try {
    contents = await readBoundedFile(path, maximumBytes, { signal });
  } catch (error) {
    if (isMissingFile(error)) return undefined;
    throw error;
  }
  return parse(JSON.parse(contents.toString("utf8")) as unknown);
}
