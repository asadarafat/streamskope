// @vitest-environment jsdom

import "@testing-library/jest-dom/vitest";

import { cleanup, render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it } from "vitest";

import {
  HOST_PROTOCOL_VERSION,
  kafkaAclIdentity,
  type HostCommand,
  type HostCommandResponse,
  type KafkaAclBinding,
  type StreamSkopeHost,
} from "../../src/kafka/contracts";
import { AclPage } from "../../src/kafka/ui/AclPage";
import { SchemaRegistryPage } from "../../src/kafka/ui/SchemaRegistryPage";
import { TransformsPage } from "../../src/kafka/ui/TransformsPage";
import { StreamSkopeThemeProvider } from "../../src/ui/StreamSkopeThemeProvider";

class PageHost implements StreamSkopeHost {
  readonly commands: HostCommand[] = [];

  execute(command: HostCommand): Promise<HostCommandResponse> {
    this.commands.push(command);
    return Promise.resolve({
      command: command.command,
      id: command.id,
      ok: true,
      result: { correlationId: command.id },
      version: HOST_PROTOCOL_VERSION,
    });
  }

  openExternalUrl(): Promise<never> {
    return Promise.reject(new Error("not expected"));
  }

  subscribe(): () => void {
    return () => undefined;
  }
}

afterEach(cleanup);

const acl: KafkaAclBinding = {
  host: "*",
  operation: "READ",
  patternType: "LITERAL",
  permission: "ALLOW",
  principal: "User:orders-api",
  resourceName: "orders.events",
  resourceType: "TOPIC",
};

describe("cluster service pages", () => {
  it("requires the complete ACL identity before exact deletion", async () => {
    const host = new PageHost();
    const user = userEvent.setup();
    render(
      <StreamSkopeThemeProvider>
        <AclPage
          connected
          host={host}
          snapshot={{
            acls: [acl],
            connectionName: "Local",
            omittedAcls: 0,
            refreshedAt: "2026-08-12T12:00:00.000Z",
            state: "ready",
          }}
        />
      </StreamSkopeThemeProvider>,
    );

    await user.click(screen.getByRole("button", { name: "Delete" }));
    const dialog = screen.getByRole("dialog", { name: "Delete exact ACL binding" });
    const deleteButton = within(dialog).getByRole("button", { name: "Delete binding" });
    expect(deleteButton).toBeDisabled();
    await user.type(
      within(dialog).getByRole("textbox", { name: "Exact ACL identity" }),
      kafkaAclIdentity(acl),
    );
    await user.click(deleteButton);

    expect(host.commands.at(-1)).toMatchObject({
      command: "acls.delete",
      payload: { acl, confirmation: kafkaAclIdentity(acl) },
    });
  });

  it("presents schema references and keeps soft deletion distinct from permanent deletion", async () => {
    const host = new PageHost();
    const user = userEvent.setup();
    const inventory = {
      connectionName: "Local",
      endpoint: "http://schema:8081",
      omittedSubjects: 0,
      refreshedAt: "2026-08-12T12:00:00.000Z",
      state: "ready" as const,
      subjects: ["orders-value"],
    };
    const unavailableDetail = {
      compatibilityLevel: null,
      connectionName: "Local",
      endpoint: "http://schema:8081",
      refreshedAt: null,
      schema: null,
      state: "unavailable" as const,
      subject: null,
      versions: [],
    };
    const view = render(
      <StreamSkopeThemeProvider>
        <SchemaRegistryPage
          compatibility={null}
          connected
          detail={unavailableDetail}
          host={host}
          inventory={inventory}
        />
      </StreamSkopeThemeProvider>,
    );
    await user.click(screen.getByText("orders-value"));
    view.rerender(
      <StreamSkopeThemeProvider>
        <SchemaRegistryPage
          compatibility={null}
          connected
          detail={{
            compatibilityLevel: "BACKWARD",
            connectionName: "Local",
            endpoint: "http://schema:8081",
            refreshedAt: "2026-08-12T12:00:01.000Z",
            schema: {
              id: 42,
              references: [{ name: "Customer", subject: "customer-value", version: 2 }],
              schema: '{"type":"record","name":"Order","fields":[]}',
              schemaType: "AVRO",
              subject: "orders-value",
              version: 3,
            },
            state: "ready",
            subject: "orders-value",
            versions: [1, 2, 3],
          }}
          host={host}
          inventory={inventory}
        />
      </StreamSkopeThemeProvider>,
    );

    expect(screen.getByText("Customer").parentElement).toHaveTextContent(
      "Customer → customer-value@2",
    );
    await user.click(screen.getByRole("button", { name: "Delete version" }));
    const dialog = screen.getByRole("dialog", { name: "Delete schema version" });
    expect(within(dialog).getByRole("combobox", { name: "Deletion mode" })).toHaveTextContent(
      "Soft delete",
    );
    await user.type(
      within(dialog).getByRole("textbox", { name: "Type orders-value@3 to confirm" }),
      "orders-value@3",
    );
    await user.click(within(dialog).getByRole("button", { name: "Soft delete" }));

    expect(host.commands.at(-1)).toMatchObject({
      command: "schemas.delete",
      payload: {
        confirmation: "orders-value@3",
        mode: "soft",
        target: { kind: "version", subject: "orders-value", version: 3 },
      },
    });
  });

  it("shows transform processor evidence, isolated logs, and deletion consequences", async () => {
    const host = new PageHost();
    const user = userEvent.setup();
    const transform = {
      aggregateStatus: "errored" as const,
      compression: "none",
      environment: [{ name: "TOKEN", valuePresent: true }],
      inputTopic: "orders.raw",
      maximumLag: 9,
      name: "mask-orders",
      offset: null,
      outputTopics: ["orders.masked"],
      statuses: [{ lag: 9, nodeId: 2, partition: 1, status: "errored" as const }],
    };
    const inventory = {
      connectionName: "Local",
      endpoint: "http://admin:9644",
      omittedTransforms: 0,
      refreshedAt: "2026-08-12T12:00:00.000Z",
      state: "ready" as const,
      transforms: [transform],
    };
    const view = render(
      <StreamSkopeThemeProvider>
        <TransformsPage
          connected
          detail={{
            connectionName: "Local",
            endpoint: "http://admin:9644",
            refreshedAt: null,
            state: "unavailable",
            transform: null,
            transformName: null,
          }}
          host={host}
          inventory={inventory}
          logs={{
            connectionName: "Local",
            logs: [],
            omittedLogs: 0,
            refreshedAt: null,
            state: "unavailable",
            transformName: null,
          }}
        />
      </StreamSkopeThemeProvider>,
    );
    await user.click(screen.getByText("mask-orders"));
    view.rerender(
      <StreamSkopeThemeProvider>
        <TransformsPage
          connected
          detail={{
            connectionName: "Local",
            endpoint: "http://admin:9644",
            refreshedAt: "2026-08-12T12:00:01.000Z",
            state: "ready",
            transform,
            transformName: "mask-orders",
          }}
          host={host}
          inventory={inventory}
          logs={{
            connectionName: "Local",
            logs: [
              {
                level: "error",
                message: "processor failed",
                offset: "7",
                partition: 1,
                timestamp: "2026-08-12T12:00:01.000Z",
              },
            ],
            omittedLogs: 4,
            refreshedAt: "2026-08-12T12:00:01.000Z",
            state: "ready",
            transformName: "mask-orders",
          }}
        />
      </StreamSkopeThemeProvider>,
    );

    expect(screen.getByText("9", { selector: "p" })).toBeVisible();
    expect(screen.getByText("processor failed")).toBeVisible();
    expect(screen.getByText(/4 older matching records omitted/)).toBeVisible();
    await user.click(screen.getByRole("button", { name: "Delete transform" }));
    const dialog = screen.getByRole("dialog", { name: "Delete transform" });
    expect(dialog).toHaveTextContent("orders.raw");
    expect(dialog).toHaveTextContent("orders.masked");
  });
});
