import { HostContractValidationError } from "../contracts";
import { contiguousObservations } from "../contracts/observation-analysis";
import { createDefaultKafkaInvestigationView } from "../contracts/investigation-view";
import { createEmptyKafkaSavedRecordContext } from "../contracts/record-locator";
import {
  OBSERVATION_LIMITS,
  type ObservationSeries,
  type KafkaObservation,
} from "../contracts/observations";

import { parseViewSettings, type PreparedKafkaInvestigation } from "./investigation-view-settings";

export interface FindingEvidence {
  readonly complete: boolean;
  readonly samples: readonly KafkaObservation[];
  readonly firstAt: number | null;
  readonly lastAt: number | null;
  readonly fresh: boolean;
}

/** Resolve actual support without substituting another sample for missing evidence. */
export function findingEvidence(
  series: ObservationSeries,
  ids: readonly string[],
  now: number,
): FindingEvidence {
  const unique = new Set(ids);
  const samples = series.samples.filter((sample) => unique.has(sample.id));
  const first = samples.at(0),
    last = samples.at(-1);
  const eligible = new Set(
    contiguousObservations(series.samples)
      .slice(-OBSERVATION_LIMITS.analysisSamples)
      .map((sample) => sample.id),
  );
  const complete =
    ids.length > 0 &&
    unique.size === ids.length &&
    samples.length === ids.length &&
    samples.every((sample) => eligible.has(sample.id));
  return {
    complete,
    samples,
    firstAt: first?.observedAt ?? null,
    lastAt: last?.observedAt ?? null,
    fresh:
      complete &&
      last !== undefined &&
      now >= last.observedAt &&
      now - last.observedAt <= OBSERVATION_LIMITS.staleMs,
  };
}

/** Controls only: no diagnosis, captured payload, positive identity or read authority. */
export function prepareObservationInvestigation(
  series: ObservationSeries,
  destination: "topic" | "consumer-group",
): PreparedKafkaInvestigation {
  const recent = contiguousObservations(series.samples).slice(-OBSERVATION_LIMITS.analysisSamples);
  const first = recent.at(0),
    latest = recent.at(-1);
  if (!first || !latest)
    throw new HostContractValidationError("Investigation", "capture an observation first");
  if (destination === "consumer-group" && series.groupId === null)
    throw new HostContractValidationError("Investigation", "choose a consumer group first");
  const startTimeMs = latest.records?.startTimeMs ?? first.startedAt;
  const endTimeMs = latest.records?.endTimeMs ?? Math.max(startTimeMs + 1, latest.observedAt);
  return {
    resource:
      destination === "topic" ? `${series.topic} · messages` : `Consumer group ${series.groupId!}`,
    name: `${destination === "topic" ? series.topic : series.groupId!}: observed investigation`.slice(
      0,
      128,
    ),
    settings: parseViewSettings({
      configuration:
        destination === "consumer-group"
          ? null
          : {
              schemaVersion: 1,
              request: {
                topic: series.topic,
                mode: "time-window",
                startTimeMs,
                endTimeMs,
                maxMessages: 100,
              },
            },
      view: createDefaultKafkaInvestigationView(
        destination === "consumer-group"
          ? { kind: "consumer-group", groupId: series.groupId! }
          : { kind: "topic", workspace: "messages" },
      ),
      records: createEmptyKafkaSavedRecordContext(),
    }),
  };
}
