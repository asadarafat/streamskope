import type { KafkaMessage } from "../contracts/types";
import type { RecordReadSettings } from "../contracts/finite-record-read";
import {
  RECORD_ANALYSIS_LIMITS,
  recordAnalysisGroupIdentity,
  type RecordAnalysisCell,
  type RecordAnalysisColumnCounts,
  type RecordAnalysisGroupKey,
  type RecordAnalysisInput,
  type RecordAnalysisLimitReason,
  type RecordAnalysisLimits,
  type RecordAnalysisResult,
  type RecordAnalysisRow,
} from "../contracts/record-analysis";
import { utf8ByteLength } from "../contracts/message-limits";
import { KafkaRuleWorkBudget } from "../contracts/rule-expression-evaluator";

import { RecordAnalysisProjection } from "./record-analysis-projection";

const jsonBytes = (value: unknown): number => utf8ByteLength(JSON.stringify(value));
interface Group {
  readonly key: RecordAnalysisGroupKey;
  readonly count: number;
}

/** A refused record changes neither its aggregates nor its retained preview. */
export class RecordAnalysisAccumulator {
  private readonly projection: RecordAnalysisProjection;
  private readonly groupColumn: number | null;
  private columns: readonly RecordAnalysisColumnCounts[];
  private readonly groups = new Map<string, Group>();
  private readonly preview: RecordAnalysisRow[] = [];
  private previewFull = false;
  private previewBytes = 2;
  private groupBytes = 2;
  private groupedRecords = 0;
  private excluded = { masked: 0, unavailable: 0 };
  private workUnits = 0;
  private counted = 0;

  constructor(
    input: RecordAnalysisInput,
    settings: RecordReadSettings,
    private readonly limits: RecordAnalysisLimits = RECORD_ANALYSIS_LIMITS,
  ) {
    this.projection = new RecordAnalysisProjection(input.columns, settings);
    this.columns = input.columns.map((column) => ({
      columnId: column.id,
      scalar: 0,
      missing: 0,
      nullKey: 0,
      tombstone: 0,
      masked: 0,
      unavailable: 0,
    }));
    this.groupColumn =
      input.groupBy === null
        ? null
        : input.columns.findIndex((column) => column.id === input.groupBy);
    if (this.groupColumn === -1)
      throw new Error("The grouping column must be selected for projection.");
    if (this.resultSize(this.columns, 0, this.excluded, 0, 2, 2, 0) > limits.resultBytes)
      throw new Error("The analysis result budget cannot hold its column metadata.");
  }

  get countedRecords(): number {
    return this.counted;
  }

  accept(message: KafkaMessage): "committed" | RecordAnalysisLimitReason {
    const budget = new KafkaRuleWorkBudget(
      Math.min(this.limits.recordWork, this.limits.work - this.workUnits),
    );
    try {
      budget.spend();
      const cells = this.projection.project(message, budget);
      const columns = this.columns.map((column, index): RecordAnalysisColumnCounts => {
        const cell = cells[index]!;
        const counter = cell.state === "null-key" ? "nullKey" : cell.state;
        return { ...column, [counter]: column[counter] + 1 };
      });
      let group: Group | undefined;
      let identity: string | undefined;
      let groupBytes = this.groupBytes;
      let groupedRecords = this.groupedRecords;
      const excluded = { ...this.excluded };
      if (this.groupColumn !== null) {
        const key = cells[this.groupColumn]!;
        if (key.state === "masked" || key.state === "unavailable") excluded[key.state]++;
        else {
          // The tagged scalar remains exact; never replace an oversized key with a prefix or bucket.
          identity = recordAnalysisGroupIdentity(key);
          const keyBytes = utf8ByteLength(identity);
          budget.spend(keyBytes);
          if (keyBytes > this.limits.groupKeyBytes) return "group-key-limit";
          const previous = this.groups.get(identity);
          if (!previous && this.groups.size >= this.limits.groups) return "group-limit";
          group = { key, count: (previous?.count ?? 0) + 1 };
          groupBytes += previous
            ? jsonBytes(group) - jsonBytes(previous)
            : jsonBytes(group) + (this.groups.size === 0 ? 0 : 1);
          groupedRecords++;
        }
      }
      let row: RecordAnalysisRow | undefined;
      let previewBytes = this.previewBytes;
      let previewFull = this.previewFull;
      if (!previewFull && this.preview.length < this.limits.previewRows) {
        const candidate: RecordAnalysisRow = {
          partition: message.partition,
          offset: message.offset,
          timestamp: message.timestamp,
          cells: cells.map((cell): RecordAnalysisCell =>
            cell.state === "scalar" && jsonBytes(cell.value) > this.limits.cellBytes
              ? { state: "unavailable", reason: "value-limit" }
              : cell,
          ),
        };
        const bytes = jsonBytes(candidate);
        budget.spend(bytes);
        const nextBytes = previewBytes + bytes + (this.preview.length === 0 ? 0 : 1);
        if (nextBytes <= this.limits.previewBytes) {
          row = candidate;
          previewBytes = nextBytes;
        } else previewFull = true;
      } else previewFull = true;
      const workUnits = this.workUnits + budget.spent;
      const omitted = this.counted + 1 - (this.preview.length + (row ? 1 : 0));
      if (
        this.resultSize(
          columns,
          groupedRecords,
          excluded,
          omitted,
          previewBytes,
          groupBytes,
          workUnits,
        ) > this.limits.resultBytes
      )
        return "result-byte-limit";
      this.columns = columns;
      if (group && identity !== undefined) this.groups.set(identity, group);
      this.groupBytes = groupBytes;
      this.groupedRecords = groupedRecords;
      this.excluded = excluded;
      if (row) this.preview.push(row);
      this.previewBytes = previewBytes;
      this.previewFull = previewFull;
      this.workUnits = workUnits;
      this.counted++;
      return "committed";
    } catch (error) {
      if (budget.exhausted) return "work-limit";
      throw error;
    }
  }

  snapshot(): RecordAnalysisResult {
    const result: RecordAnalysisResult = {
      columns: this.columns.map((column) => ({ ...column })),
      grouping:
        this.groupColumn === null
          ? null
          : {
              groups: [...this.groups.values()].map((group) => ({
                key: { ...group.key },
                count: group.count,
              })),
              groupedRecords: this.groupedRecords,
              excluded: { ...this.excluded },
            },
      preview: this.preview.map((row) => ({
        ...row,
        cells: row.cells.map((cell) => ({ ...cell })),
      })),
      previewOmittedRecords: this.counted - this.preview.length,
      previewBytes: this.previewBytes,
      workUnits: this.workUnits,
    };
    // Full serialization occurs only when a bounded result is requested, never for each input row.
    const bytes = jsonBytes(result);
    if (
      bytes > this.limits.resultBytes ||
      bytes !==
        this.resultSize(
          this.columns,
          this.groupedRecords,
          this.excluded,
          result.previewOmittedRecords,
          this.previewBytes,
          this.groupBytes,
          this.workUnits,
        )
    )
      throw new Error("Analysis result exceeded its accounted byte budget.");
    return result;
  }

  private resultSize(
    columns: readonly RecordAnalysisColumnCounts[],
    groupedRecords: number,
    excluded: { readonly masked: number; readonly unavailable: number },
    omitted: number,
    previewBytes: number,
    groupBytes: number,
    workUnits: number,
  ): number {
    // Only a fixed-size skeleton and at most twelve counters are encoded per row.
    return (
      jsonBytes({
        columns,
        grouping: this.groupColumn === null ? null : { groups: [], groupedRecords, excluded },
        preview: [],
        previewOmittedRecords: omitted,
        previewBytes,
        workUnits,
      }) +
      previewBytes -
      2 +
      (this.groupColumn === null ? 0 : groupBytes - 2)
    );
  }
}
