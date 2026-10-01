import { PROFILE_LIMITS } from "../../../src/features/kafka/contracts/profile-types";
import { HostContractValidationError } from "../../../src/features/kafka/contracts/validation-error";
import {
  declaredValue,
  exactKeys,
  record,
  text,
} from "../../../src/features/kafka/contracts/validation-primitives";

import type { ProfileEdaCaptureSource } from "./profile-types";
import { EDA_CAPTURE_LIMITS, type EdaCaptureSourceIdentity } from "./eda-capture-types";

function parseEdaCaptureSourceIdentity(value: unknown, path: string): EdaCaptureSourceIdentity {
  const source = record(value, path);
  exactKeys(source, ["apiVersion", "kind", "name", "namespace"], path);
  return {
    apiVersion: declaredValue(
      source.apiVersion,
      ["kafka.eda.nokia.com/v1", "kafka.eda.nokia.com/v1alpha1"],
      `${path}.apiVersion`,
    ),
    kind: declaredValue(source.kind, ["ClusterProducer", "Producer"], `${path}.kind`),
    name: text(source.name, `${path}.name`, EDA_CAPTURE_LIMITS.nameCharacters),
    namespace: text(source.namespace, `${path}.namespace`, EDA_CAPTURE_LIMITS.nameCharacters),
  };
}

export function parseProfileSource(value: unknown, path: string): ProfileEdaCaptureSource {
  const source = record(value, path);
  const kind = declaredValue(source.kind, ["eda-capture"], `${path}.kind`);
  exactKeys(
    source,
    [
      "broker",
      "clusterBroker",
      "exporterName",
      "kind",
      "source",
      "state",
      "topics",
      "workloadName",
      "edaApiUrl",
      "context",
      "sessionId",
    ],
    path,
  );
  const topics = source.topics;
  if (!Array.isArray(topics) || topics.length > EDA_CAPTURE_LIMITS.topics) {
    throw new HostContractValidationError(
      `${path}.topics`,
      `must contain at most ${EDA_CAPTURE_LIMITS.topics} topics`,
    );
  }
  return {
    ...(source.edaApiUrl === undefined
      ? {}
      : {
          edaApiUrl: text(source.edaApiUrl, `${path}.edaApiUrl`, EDA_CAPTURE_LIMITS.urlCharacters),
        }),
    ...(source.context === undefined
      ? {}
      : { context: text(source.context, `${path}.context`, EDA_CAPTURE_LIMITS.nameCharacters) }),
    ...(source.sessionId === undefined
      ? {}
      : { sessionId: text(source.sessionId, `${path}.sessionId`, 128) }),
    broker: text(source.broker, `${path}.broker`, PROFILE_LIMITS.brokerCharacters),
    clusterBroker: text(
      source.clusterBroker,
      `${path}.clusterBroker`,
      PROFILE_LIMITS.brokerCharacters,
    ),
    exporterName: text(
      source.exporterName,
      `${path}.exporterName`,
      EDA_CAPTURE_LIMITS.nameCharacters,
    ),
    kind,
    source: parseEdaCaptureSourceIdentity(source.source, `${path}.source`),
    state: declaredValue(source.state, ["ready"], `${path}.state`),
    topics: topics.map((topic, index) =>
      text(topic, `${path}.topics[${String(index)}]`, EDA_CAPTURE_LIMITS.nameCharacters),
    ),
    workloadName: text(
      source.workloadName,
      `${path}.workloadName`,
      EDA_CAPTURE_LIMITS.nameCharacters,
    ),
  };
}
