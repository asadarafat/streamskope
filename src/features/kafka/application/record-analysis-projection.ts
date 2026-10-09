import type { KafkaMessage } from "../contracts/types";
import type { RecordReadSettings } from "../contracts/finite-record-read";
import type { RecordAnalysisCell, RecordAnalysisColumn } from "../contracts/record-analysis";
import { utf8ByteLength } from "../contracts/message-limits";
import {
  compileKafkaProjectionPath,
  type KafkaProjectionPath,
} from "../contracts/rule-expression-parser";
import {
  readKafkaProjectionPath,
  type KafkaRuleWorkBudget,
} from "../contracts/rule-expression-evaluator";
import { KafkaRuleSampleError, parseKafkaRuleSample } from "../contracts/rule-sample";
import type { RecordField } from "../contracts/structured-record";

type Sample =
  | { readonly state: "parsed"; readonly value: unknown }
  | { readonly state: "text"; readonly value: string }
  | RecordAnalysisCell;
interface ColumnProjection {
  readonly source: "key" | "value";
  readonly path: KafkaProjectionPath;
  readonly masks: readonly (readonly string[])[];
}

/** Compile once; every projection reads the same protected canonical field, never original bytes. */
export class RecordAnalysisProjection {
  private readonly columns: readonly ColumnProjection[];
  private readonly maskKey: boolean;
  private readonly maskValueText: boolean;

  constructor(columns: readonly RecordAnalysisColumn[], settings: RecordReadSettings) {
    this.maskKey = settings.protection.maskKey;
    this.maskValueText = settings.protection.valuePaths.length > 0;
    const masks = settings.protection.valuePaths.map((pointer) =>
      pointer === ""
        ? []
        : pointer
            .slice(1)
            .split("/")
            .map((part) => part.replace(/~1/gu, "/").replace(/~0/gu, "~")),
    );
    this.columns = columns.map((column) => {
      const path = compileKafkaProjectionPath(column.path);
      const names = path.segments.map((segment) =>
        segment.kind === "property" ? segment.name : String(segment.index),
      );
      return {
        source: column.source,
        path,
        masks:
          column.source === "key"
            ? []
            : masks.filter(
                (mask) =>
                  mask.length <= names.length && mask.every((name, index) => name === names[index]),
              ),
      };
    });
  }

  project(message: KafkaMessage, budget: KafkaRuleWorkBudget): readonly RecordAnalysisCell[] {
    const samples = new Map<"key" | "value", Sample>();
    return this.columns.map((column): RecordAnalysisCell => {
      budget.spend();
      let sample = samples.get(column.source);
      if (!sample) {
        sample = this.sample(message.structured?.[column.source], column.source, budget);
        samples.set(column.source, sample);
      }
      if (sample.state === "text")
        return column.path.segments.length === 0
          ? { state: "scalar", value: sample.value }
          : { state: "unavailable", reason: "not-json" };
      if (sample.state !== "parsed") return sample;
      if (column.masks.some((mask) => this.maskExists(mask, sample.value, budget)))
        return { state: "masked" };
      const selected = readKafkaProjectionPath(column.path, sample.value, budget);
      if (!selected.found) return { state: "missing" };
      const value = selected.value;
      if (
        value === null ||
        typeof value === "string" ||
        typeof value === "boolean" ||
        (typeof value === "number" && Number.isFinite(value))
      )
        return { state: "scalar", value };
      return { state: "unavailable", reason: Array.isArray(value) ? "array" : "object" };
    });
  }

  private sample(
    field: RecordField | undefined,
    source: "key" | "value",
    budget: KafkaRuleWorkBudget,
  ): Sample {
    if (!field) return { state: "unavailable", reason: "not-captured" };
    if (field.state === "null") return { state: source === "value" ? "tombstone" : "null-key" };
    if (field.state === "masked" || (source === "key" && this.maskKey)) return { state: "masked" };
    if (field.state === "error")
      return {
        state: "unavailable",
        reason: field.code === "not-captured" ? "not-captured" : "decoding-error",
      };
    if (field.codec === "bytes") return { state: "unavailable", reason: "bytes" };
    if (field.json === null) {
      if (source === "value" && this.maskValueText) return { state: "masked" };
      if (field.codec !== "utf8") return { state: "unavailable", reason: "decoding-error" };
      budget.spend(utf8ByteLength(field.text));
      return { state: "text", value: field.text };
    }
    // JSON syntax plus structural traversal are both bounded by its encoded size.
    // Charge before parsing, sharing this work across all columns of the field.
    budget.spend(2 * utf8ByteLength(field.json));
    try {
      return { state: "parsed", value: parseKafkaRuleSample(field.json) };
    } catch (error) {
      return {
        state: "unavailable",
        reason:
          error instanceof KafkaRuleSampleError && error.reason === "limit-exceeded"
            ? "sample-limit"
            : "decoding-error",
      };
    }
  }

  private maskExists(
    mask: readonly string[],
    sample: unknown,
    budget: KafkaRuleWorkBudget,
  ): boolean {
    let value = sample;
    for (const name of mask) {
      // Protection JSON Pointers deliberately do not target array metadata.
      if (Array.isArray(value) && !/^(?:0|[1-9]\d*)$/u.test(name)) return false;
      const next = readKafkaProjectionPath(
        { origin: "root", segments: [{ kind: "property", name }] },
        value,
        budget,
      );
      if (!next.found) return false;
      value = next.value;
    }
    return true;
  }
}
