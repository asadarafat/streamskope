// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { cleanup, render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, expect, it } from "vitest";

import type { StructuredRecord } from "../../src/features/kafka/contracts/structured-record";
import { RecordDecodePanel } from "../../src/features/kafka/ui/RecordDecodePanel";
import { formatRecordJson } from "../../src/features/kafka/ui/record-presentation";

afterEach(cleanup);
const record: StructuredRecord = {
  version: 1,
  headersState: "complete",
  protection: "none",
  headers: [],
  key: { state: "null", codec: "auto", writerSchema: null },
  value: {
    state: "decoded",
    codec: "avro",
    text: '{"id":9007199254740993,"secret":"[MASKED]"}',
    json: '{"id":9007199254740993,"secret":"[MASKED]"}',
    writerSchema: { id: 9, format: "avro", messageType: null, registry: "https://registry.test/" },
  },
};

it("shows the captured protected projection and exact writer identity without an independent decoder", () => {
  render(<RecordDecodePanel structured={record} />);
  expect(screen.getByLabelText("Decoded JSON")).toHaveTextContent("9007199254740993");
  expect(screen.getByLabelText("Decoded JSON")).toHaveTextContent("[MASKED]");
  expect(screen.getByText("Writer schema ID 9 · avro")).toBeVisible();
  expect(screen.getByText("Registry: https://registry.test/")).toBeVisible();
  expect(screen.queryByRole("button", { name: "Decode record" })).not.toBeInTheDocument();
});

it("distinguishes tombstones, masked fields, and malformed records with a known writer", async () => {
  const user = userEvent.setup();
  const view = render(
    <RecordDecodePanel
      structured={{ ...record, value: { state: "null", codec: "auto", writerSchema: null } }}
    />,
  );
  expect(screen.getByText(/Kafka null value \(tombstone\)/u)).toBeVisible();
  view.rerender(
    <RecordDecodePanel
      structured={{
        ...record,
        value: { state: "masked", codec: "avro", writerSchema: record.value.writerSchema },
      }}
    />,
  );
  expect(screen.getByText(/withheld by the record protection policy/u)).toBeVisible();
  expect(screen.queryByLabelText("Decoded JSON")).not.toBeInTheDocument();
  view.rerender(
    <RecordDecodePanel
      structured={{
        ...record,
        value: {
          state: "error",
          codec: "avro",
          writerSchema: record.value.writerSchema,
          code: "malformed",
          detail: "Invalid writer payload.",
        },
      }}
    />,
  );
  expect(
    screen.getByText(/Decoding unavailable \(malformed\): Invalid writer payload/u),
  ).toBeVisible();
  expect(screen.getByText("Writer schema ID 9 · avro")).toBeVisible();
  await user.click(screen.getByRole("combobox", { name: "Record part" }));
  await user.click(screen.getByRole("option", { name: /^Key$/u }));
  expect(screen.getByText(/Kafka null key/u)).toBeVisible();
});

it("labels missing legacy evidence instead of interpreting a preview", () => {
  render(<RecordDecodePanel structured={undefined} />);
  expect(screen.getByText(/Structured evidence was not captured/u)).toBeVisible();
});

it("formats without changing large numbers, exponents, whitespace or escaped string data", () => {
  const value =
    '{"integer":9007199254740993,"exponent":1e999,"string":" a  \\" ","array":[{},[],null]}';
  const formatted = formatRecordJson(value)!;
  expect(formatted).toContain("9007199254740993");
  expect(formatted).toContain("1e999");
  expect(formatted).toContain('" a  \\" "');
  expect(JSON.parse(formatted)).toEqual(JSON.parse(value));
  expect(formatRecordJson("{malformed")).toBeNull();
});

it("shows the selected Protobuf message type within its writer schema", () => {
  render(
    <RecordDecodePanel
      structured={{
        ...record,
        value: {
          state: "decoded",
          codec: "protobuf",
          text: '{"id":"event-1"}',
          json: '{"id":"event-1"}',
          writerSchema: {
            id: 10,
            format: "protobuf",
            registry: "https://registry.test/",
            messageType: ".fixture.Event",
          },
        },
      }}
    />,
  );
  expect(screen.getByText("Writer schema ID 10 · protobuf")).toBeVisible();
  expect(screen.getByText("Message type: .fixture.Event")).toBeVisible();
});
