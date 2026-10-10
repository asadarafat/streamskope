import { HOST_PROTOCOL_VERSION, type HostCommand, type HostCommandResponse } from "../contracts";
import {
  parseTopicAdministrationReview,
  parseTopicAdministrationOutcome,
} from "../contracts/topic-administration";
import { TopicAdministrationService } from "../application/topic-administration-service";
import type { KafkaApplicationSession } from "../application";

import { failureResponse, type ActivityInput } from "./facade-support";

export class TopicAdministrationFacade {
  private readonly service: TopicAdministrationService;
  constructor(
    session: KafkaApplicationSession,
    private readonly activity: (input: ActivityInput) => void,
  ) {
    this.service = new TopicAdministrationService(() => session.topicAdministrationScope());
  }
  async execute(
    command: Extract<HostCommand, { command: "topics.change.review" | "topics.change.apply" }>,
    correlationId: string,
  ): Promise<HostCommandResponse> {
    try {
      if (command.command === "topics.change.review")
        return {
          command: command.command,
          id: command.id,
          version: HOST_PROTOCOL_VERSION,
          ok: true,
          result: {
            correlationId,
            review: parseTopicAdministrationReview(await this.service.review(command.payload)),
          },
        };
      const outcome = parseTopicAdministrationOutcome(
        await this.service.apply(command.payload.planId, command.payload.confirmation),
      );
      this.activity({
        correlationId,
        operation: "Apply reviewed topic change",
        object: outcome.input.topic,
        detail: outcome.detail,
        outcome: outcome.state === "acknowledged" ? "succeeded" : "failed",
        severity:
          outcome.state === "acknowledged" &&
          outcome.verification === "verified" &&
          outcome.cleanup === "confirmed"
            ? "info"
            : "warning",
      });
      return {
        command: command.command,
        id: command.id,
        version: HOST_PROTOCOL_VERSION,
        ok: true,
        result: { correlationId, outcome },
      };
    } catch {
      return failureResponse(command, {
        code: "VALIDATION",
        stage: "kafka",
        correlationId,
        retryable: false,
        activeStateChanged: false,
        summary: "The topic change review could not be accepted.",
        recovery:
          "Check topic DESCRIBE and DELETE/ALTER permissions, supported identity APIs, partition counts and exact confirmation, then review again. Internal topics, stale reviews and changed connections are refused. Inspect any uncertain outcome before another attempt.",
      });
    }
  }
}
