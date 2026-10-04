import type {
  HostCommand,
  HostCommandResponse,
  HostEvent,
  HostEventListener,
  KafkaExploredMessage,
  KafkaFetchRequest,
  KafkaLiveRuleCapability,
  KafkaLiveRuleEvaluation,
  StreamSkopeHost,
} from "../../src/features/kafka/contracts";

import { testHostAccepted } from "./host-response";

export class FakeHost implements StreamSkopeHost {
  readonly commands: HostCommand[] = [];
  subscribeCalls = 0;
  private readonly listeners = new Set<HostEventListener>();

  emit(event: HostEvent): void {
    for (const listener of this.listeners) {
      listener(event);
    }
  }

  execute<Command extends HostCommand>(
    command: Command,
  ): Promise<HostCommandResponse<Command["command"]>>;
  execute(command: HostCommand): Promise<HostCommandResponse> {
    this.commands.push(command);
    return Promise.resolve(testHostAccepted(command, `correlation-${command.id}`));
  }

  openExternalUrl(): Promise<never> {
    return Promise.reject(new Error("External URL action was not expected."));
  }

  subscribe(listener: HostEventListener): () => void {
    this.subscribeCalls += 1;
    this.listeners.add(listener);
    return (): void => {
      this.listeners.delete(listener);
    };
  }
}

export const readyRuleCapability: KafkaLiveRuleCapability = {
  applicableRules: 0,
  omittedRules: 0,
  state: "ready",
};

export function tailRequest(topic = "test"): KafkaFetchRequest {
  return {
    maxMessages: 1_000,
    mode: "tail",
    topic,
  };
}

export const evaluatedRuleResult: KafkaLiveRuleEvaluation = {
  activeMatchCount: 0,
  activeMatches: [],
  durationMicros: 0,
  errorCount: 0,
  errors: [],
  evaluatedRules: 0,
  omittedEvidence: 0,
  omittedRules: 0,
  state: "evaluated",
  suppressedMatchCount: 0,
  suppressedMatches: [],
};

export function message(
  id: string,
  payload: string | null = '{"status":"ready"}',
  overrides: Partial<KafkaExploredMessage> = {},
): KafkaExploredMessage {
  return {
    headers: { "content-type": "application/json" },
    id,
    key: "order-1",
    offset: id,
    originalByteSize: payload?.length ?? 1_048_577,
    partition: 0,
    payload,
    preview: payload ?? "oversized preview",
    ruleEvaluation:
      payload === null
        ? {
            ...evaluatedRuleResult,
            reason: "payload-truncated",
            state: "unavailable",
          }
        : evaluatedRuleResult,
    timestamp: "2026-07-25T15:00:00.000Z",
    topic: "test",
    truncated: payload === null,
    ...overrides,
  };
}
