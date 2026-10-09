import {
  HOST_PROTOCOL_VERSION,
  KAFKA_OPERATIONAL_PREFERENCE_DEFAULTS,
  parseCorrelatedHostResponse,
  type HostCommand,
  type HostCommandResponse,
  type StreamSkopeBackend,
} from "../../src/features/kafka/contracts";

function record(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

/** Validate fixture responses just like a bridge, with neutral data for common editor/startup queries. */
export function testHostResponse<Command extends HostCommand>(
  command: Command,
  envelope: unknown,
): HostCommandResponse<Command["command"]> {
  if (
    record(envelope) &&
    envelope.ok === true &&
    record(envelope.result) &&
    Object.keys(envelope.result).every((key) => key === "correlationId")
  ) {
    const correlationId = envelope.result.correlationId;
    if (command.command === "preferences.get")
      envelope = {
        ...envelope,
        result: {
          correlationId,
          snapshot: {
            preferences: KAFKA_OPERATIONAL_PREFERENCE_DEFAULTS,
            store: { durability: "session", state: "ready" },
          },
        },
      };
    else if (command.command === "records.export.status")
      envelope = {
        ...envelope,
        result: {
          correlationId,
          snapshot: {
            scopeId: "00000000-0000-4000-8000-000000000000",
            revision: 0,
            available: false,
            operation: null,
          },
        },
      };
    else if (command.command === "profiles.binding.get")
      envelope = {
        ...envelope,
        result: {
          correlationId,
          bindingDetail: {
            profileId: command.payload.profileId,
            revision: 1,
            binding: null,
          },
        },
      };
    else if (command.command === "trustAcquisition.editor.open")
      envelope = {
        ...envelope,
        result: { correlationId, editor: { id: "fixture-editor", generation: 1 } },
      };
    else if (command.command === "trustAcquisition.capabilities")
      envelope = {
        ...envelope,
        result: { correlationId, sshAgent: "unavailable", methods: ["ssh"] },
      };
  }
  return parseCorrelatedHostResponse(envelope, command);
}

export type TestHostDispatch = (command: HostCommand) => Promise<unknown>;

/** Keep test dispatchers simple while exposing the same generic execute contract as production. */
export function testHostExecute(dispatch: TestHostDispatch): StreamSkopeBackend["execute"] {
  return async <Command extends HostCommand>(
    command: Command,
  ): Promise<HostCommandResponse<Command["command"]>> =>
    testHostResponse(command, await dispatch(command));
}

/** Acknowledge a fixture command, supplying neutral results for common startup queries. */
export function testHostAccepted<Command extends HostCommand>(
  command: Command,
  correlationId: string,
): HostCommandResponse<Command["command"]> {
  return testHostResponse(command, {
    command: command.command,
    id: command.id,
    ok: true,
    result: { correlationId },
    version: HOST_PROTOCOL_VERSION,
  });
}
