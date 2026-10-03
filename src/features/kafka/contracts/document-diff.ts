// Shared bounded comparison for record projections and exact schema versions.
export interface DocumentDifference {
  readonly path: string;
  readonly kind: "added" | "removed" | "changed";
  readonly before: string;
  readonly after: string;
}
export interface DocumentComparison {
  readonly rows: readonly DocumentDifference[];
  readonly limited: boolean;
  readonly error?: string;
}
export const DOCUMENT_DIFF_LIMITS = {
  characters: 524_288,
  nodes: 20_000,
  depth: 32,
  rows: 500,
  preview: 512,
} as const;
const missing = Symbol("missing");
class ExactNumber {
  constructor(readonly value: string) {}
}
function preview(value: unknown): string {
  if (value === missing) return "(missing)";
  if (value instanceof ExactNumber) return `${value.value} (exact number)`;
  let text: string;
  try {
    text = JSON.stringify(value) ?? "(unavailable)";
  } catch {
    return "(value exceeds preview depth)";
  }
  return text.length > DOCUMENT_DIFF_LIMITS.preview
    ? `${text.slice(0, DOCUMENT_DIFF_LIMITS.preview)}… (preview)`
    : text;
}
function parseExactJson(text: string): unknown {
  return JSON.parse(text, (_key: string, value: unknown, context?: { source?: string }) =>
    typeof value === "number" &&
    (!Number.isFinite(value) || (Number.isInteger(value) && !Number.isSafeInteger(value))) &&
    context?.source
      ? new ExactNumber(context.source)
      : value,
  ) as unknown;
}
export function compareDocuments(
  before: string,
  after: string,
  mode: "json" | "text",
): DocumentComparison {
  if (
    before.length > DOCUMENT_DIFF_LIMITS.characters ||
    after.length > DOCUMENT_DIFF_LIMITS.characters
  )
    return {
      rows: [],
      limited: true,
      error: "Comparison inputs exceed 512 KiB of text each. Use a smaller record or schema.",
    };
  const rows: DocumentDifference[] = [];
  let limited = false;
  let nodes = 0;
  const add = (path: string, left: unknown, right: unknown): void => {
    if (rows.length >= DOCUMENT_DIFF_LIMITS.rows) {
      limited = true;
      return;
    }
    rows.push({
      path: path || "/",
      kind: left === missing ? "added" : right === missing ? "removed" : "changed",
      before: preview(left),
      after: preview(right),
    });
  };
  const walk = (left: unknown, right: unknown, path: string, depth: number): void => {
    if (
      ++nodes > DOCUMENT_DIFF_LIMITS.nodes ||
      depth > DOCUMENT_DIFF_LIMITS.depth ||
      rows.length >= DOCUMENT_DIFF_LIMITS.rows
    ) {
      limited = true;
      return;
    }
    if (Object.is(left, right)) return;
    if (left instanceof ExactNumber || right instanceof ExactNumber) {
      if (!(
        left instanceof ExactNumber &&
        right instanceof ExactNumber &&
        left.value === right.value
      ))
        add(path, left, right);
      return;
    }
    if (
      left !== null &&
      right !== null &&
      typeof left === "object" &&
      typeof right === "object" &&
      Array.isArray(left) === Array.isArray(right)
    ) {
      const a = left as Record<string, unknown>;
      const b = right as Record<string, unknown>;
      const keys = Array.isArray(left)
        ? Array.from({ length: Math.max(left.length, (right as unknown[]).length) }, (_, i) =>
            String(i),
          )
        : [...new Set([...Object.keys(a), ...Object.keys(b)])].sort();
      for (const key of keys) {
        if (limited) break;
        walk(
          Object.hasOwn(a, key) ? a[key] : missing,
          Object.hasOwn(b, key) ? b[key] : missing,
          `${path}/${key.replaceAll("~", "~0").replaceAll("/", "~1")}`,
          depth + 1,
        );
      }
    } else add(path, left, right);
  };
  try {
    if (mode === "json") walk(parseExactJson(before), parseExactJson(after), "", 0);
    else {
      const a = before.split("\n");
      const b = after.split("\n");
      for (let i = 0; i < Math.max(a.length, b.length); i++) {
        if (++nodes > DOCUMENT_DIFF_LIMITS.nodes || rows.length >= DOCUMENT_DIFF_LIMITS.rows) {
          limited = true;
          break;
        }
        if (a[i] !== b[i]) add(`Line ${String(i + 1)}`, a[i] ?? missing, b[i] ?? missing);
      }
    }
    return { rows, limited };
  } catch {
    return {
      rows: [],
      limited: false,
      error:
        "Both inputs must be valid JSON. Choose text or original bytes for an uninterpreted comparison.",
    };
  }
}
