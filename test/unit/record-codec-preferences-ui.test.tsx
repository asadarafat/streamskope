// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { cleanup, render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, expect, it } from "vitest";

import {
  KAFKA_OPERATIONAL_PREFERENCE_DEFAULTS,
  type HostCommand,
  type HostCommandResponse,
  type StreamSkopeHost,
} from "../../src/features/kafka/contracts";
import { HOST_PROTOCOL_VERSION } from "../../src/features/kafka/contracts";
import { initialKafkaUiState, reduceKafkaHostEvent } from "../../src/features/kafka/ui/state";
import { RecordCodecPreferencesPanel } from "../../src/features/kafka/ui/RecordCodecPreferencesPanel";

const snapshot = {
  preferences: KAFKA_OPERATIONAL_PREFERENCE_DEFAULTS,
  store: { durability: "durable", state: "ready" },
} as const;
class Host implements StreamSkopeHost {
  commands: HostCommand[] = [];
  fail = false;
  execute<C extends HostCommand>(command: C): Promise<HostCommandResponse<C["command"]>>;
  execute(command: HostCommand): Promise<HostCommandResponse> {
    this.commands.push(command);
    if (this.fail) return Promise.reject(new Error("Unavailable"));
    if (command.command !== "preferences.update") throw new Error("Unexpected command");
    return Promise.resolve({
      command: command.command,
      id: command.id,
      version: command.version,
      ok: true,
      result: {
        correlationId: "codecs",
        snapshot: {
          ...snapshot,
          preferences: {
            ...snapshot.preferences,
            codecs: command.payload.patch.codecs ?? snapshot.preferences.codecs,
          },
        },
      },
    });
  }
  subscribe(): () => void {
    return () => undefined;
  }
  openExternalUrl(): Promise<never> {
    return Promise.reject(new Error("Unused"));
  }
}
afterEach(cleanup);

it("saves separate manual overrides through the host and restores saved selection on reopening", async () => {
  const host = new Host();
  const user = userEvent.setup();
  const view = render(<RecordCodecPreferencesPanel host={host} snapshot={snapshot} disconnected />);
  expect(screen.getByRole("button", { name: "Save record encodings" })).toBeDisabled();
  await user.click(screen.getByRole("combobox", { name: "Default key encoding" }));
  await user.click(screen.getByRole("option", { name: "UTF-8 text" }));
  await user.click(screen.getByRole("combobox", { name: "Default value encoding" }));
  await user.click(screen.getByRole("option", { name: "Confluent Protobuf" }));
  await user.click(screen.getByRole("button", { name: "Save record encodings" }));
  expect(host.commands).toMatchObject([
    {
      command: "preferences.update",
      payload: { patch: { codecs: { key: "utf8", value: "protobuf" } } },
    },
  ]);
  expect(await screen.findByRole("status")).toHaveTextContent("Record encodings saved");
  expect(screen.getByRole("button", { name: "Save record encodings" })).toBeDisabled();
  view.unmount();
  render(
    <RecordCodecPreferencesPanel
      host={host}
      snapshot={{
        ...snapshot,
        preferences: { ...snapshot.preferences, codecs: { key: "utf8", value: "protobuf" } },
      }}
      disconnected
    />,
  );
  expect(screen.getByRole("combobox", { name: "Default value encoding" })).toHaveTextContent(
    "Confluent Protobuf",
  );
});

it("keeps failed changes dirty and does not announce them as saved", async () => {
  const host = new Host();
  host.fail = true;
  const user = userEvent.setup();
  render(<RecordCodecPreferencesPanel host={host} snapshot={snapshot} disconnected />);
  await user.click(screen.getByRole("combobox", { name: "Default value encoding" }));
  await user.click(screen.getByRole("option", { name: "UTF-8 JSON" }));
  await user.click(screen.getByRole("button", { name: "Save record encodings" }));
  expect(await screen.findByRole("alert")).toHaveTextContent(
    "last confirmed settings remain active",
  );
  expect(screen.getByRole("button", { name: "Save record encodings" })).toBeEnabled();
  expect(screen.queryByRole("status")).not.toBeInTheDocument();
});

it("requires disconnection and authoritative preference storage", () => {
  const host = new Host();
  const view = render(
    <RecordCodecPreferencesPanel host={host} snapshot={snapshot} disconnected={false} />,
  );
  expect(screen.getByRole("combobox", { name: "Default key encoding" })).toHaveAttribute(
    "aria-disabled",
    "true",
  );
  expect(screen.getByText(/Disconnect Kafka before changing record encodings/u)).toBeVisible();
  view.rerender(<RecordCodecPreferencesPanel host={host} snapshot={null} disconnected />);
  expect(screen.getByText(/Preference storage is not ready/u)).toBeVisible();
  expect(screen.getByRole("button", { name: "Save record encodings" })).toBeDisabled();
  expect(host.commands).toHaveLength(0);
});

it("clears previously retained records when confirmed encoding preferences change", () => {
  const state = {
    ...initialKafkaUiState,
    messages: [
      {
        id: "retained",
        topic: "orders",
        partition: 0,
        offset: "1",
        timestamp: "2026-10-09T00:00:00Z",
        headers: {},
        key: null,
        payload: "{}",
        preview: "{}",
        originalByteSize: 2,
        truncated: false,
        ruleEvaluation: {
          state: "evaluated" as const,
          activeMatchCount: 0,
          activeMatches: [],
          suppressedMatchCount: 0,
          suppressedMatches: [],
          durationMicros: 0,
          errorCount: 0,
          errors: [],
          evaluatedRules: 0,
          omittedEvidence: 0,
          omittedRules: 0,
        },
      },
    ],
    preferenceSnapshot: snapshot,
  };
  const next = reduceKafkaHostEvent(state, {
    event: "preferences.changed",
    version: HOST_PROTOCOL_VERSION,
    sequence: 1,
    payload: {
      ...snapshot,
      preferences: { ...snapshot.preferences, codecs: { key: "utf8", value: "avro" } },
    },
  });
  expect(next.messages).toEqual([]);
  expect(next.preferenceSnapshot?.preferences.codecs.value).toBe("avro");
});
