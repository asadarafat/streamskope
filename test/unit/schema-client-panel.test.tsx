// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, expect, it } from "vitest";

import type {
  HostCommand,
  SchemaVersionDetail,
  StreamSkopeHost,
} from "../../src/features/kafka/contracts";
import { SchemaClientPanel } from "../../src/features/kafka/ui/SchemaClientPanel";
import { StreamSkopeThemeProvider } from "../../src/platform/ui/StreamSkopeThemeProvider";
import { testHostExecute } from "../support/host-response";

afterEach(cleanup);
it("generates only the selected schema without producing and clears output on a version change", async () => {
  const commands: HostCommand[] = [];
  const host: StreamSkopeHost = {
    subscribe: () => () => undefined,
    openExternalUrl: () => Promise.reject(new Error("Unexpected URL")),
    execute: testHostExecute((command) => {
      commands.push(command);
      return Promise.resolve({
        command: command.command,
        id: command.id,
        version: command.version,
        ok: true,
        result: {
          correlationId: "c",
          client: {
            generator: "Ajv 8.20.0 standalone (MIT)",
            language: "JavaScript CommonJS",
            subject: "events",
            version: 1,
            schemaId: 7,
            sha256: "a".repeat(64),
            source: "module.exports = {};",
          },
        },
      });
    }),
  };
  const schema: SchemaVersionDetail = {
    subject: "events",
    version: 1,
    id: 7,
    schemaType: "JSON",
    schema: '{"type":"string"}',
    references: [],
  };
  const page = (value: SchemaVersionDetail): React.JSX.Element => (
    <StreamSkopeThemeProvider>
      <SchemaClientPanel host={host} schema={value} enabled />
    </StreamSkopeThemeProvider>
  );
  const mounted = render(page(schema));
  fireEvent.click(screen.getByRole("button", { name: "Generate JavaScript client" }));
  await screen.findByRole("button", { name: "Copy client source" });
  expect(commands).toHaveLength(1);
  expect(commands[0]).toMatchObject({
    command: "schemas.client",
    payload: { subject: "events", version: 1 },
  });
  mounted.rerender(page({ ...schema, version: 2, schemaType: "AVRO" }));
  await waitFor(() =>
    expect(screen.queryByRole("button", { name: "Copy client source" })).toBeNull(),
  );
  expect(screen.getByRole("button", { name: "Generate JavaScript client" })).toBeDisabled();
});
