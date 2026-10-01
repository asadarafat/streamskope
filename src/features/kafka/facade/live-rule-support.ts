import {
  type KafkaExploredMessage,
  type KafkaLiveRuleCapability,
  type KafkaMessage,
} from "../contracts";
import type { KafkaLiveRuleRuntime } from "../application";

import type { ActiveFacadeConsumption, ActivityInput } from "./facade-support";

export function exploredKafkaMessage(
  runtime: KafkaLiveRuleRuntime,
  message: KafkaMessage,
): KafkaExploredMessage {
  return Object.freeze({
    ...message,
    ruleEvaluation: runtime.evaluate(message),
  });
}

export function internalLiveRuleFailureActivity(
  consumption: ActiveFacadeConsumption,
): ActivityInput {
  return {
    correlationId: consumption.correlationId,
    detail:
      "Live rule evaluation failed internally. Kafka consumption continues; review the rule catalog and restart consumption.",
    object: consumption.request.topic,
    operation: "Evaluate live rules",
    outcome: "failed",
    severity: "error",
  };
}

export function unavailableLiveRuleActivity(
  consumption: ActiveFacadeConsumption,
  capability: KafkaLiveRuleCapability,
): ActivityInput {
  return {
    correlationId: consumption.correlationId,
    detail: `The live rule catalog is unavailable. Kafka consumption will continue without rule evaluation. ${capability.recovery}`,
    object: consumption.request.topic,
    operation: "Prepare live rules",
    outcome: "failed",
    severity: "warning",
  };
}
