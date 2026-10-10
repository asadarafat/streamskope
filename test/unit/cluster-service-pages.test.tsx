// @vitest-environment jsdom

import "@testing-library/jest-dom/vitest";

import { cleanup, fireEvent, render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it } from "vitest";

import {
  kafkaAclIdentity,
  type HostCommand,
  type HostCommandResponse,
  type KafkaAclBinding,
  type StreamSkopeHost,
} from "../../src/features/kafka/contracts";
import { AclPage } from "../../src/features/kafka/ui/AclPage";
import { SchemaRegistryPage } from "../../src/features/kafka/ui/SchemaRegistryPage";
import { TransformsPage } from "../../src/features/kafka/ui/TransformsPage";
import { StreamSkopeThemeProvider } from "../../src/platform/ui/StreamSkopeThemeProvider";
import { testHostAccepted } from "../support/host-response";

class PageHost implements StreamSkopeHost {
  readonly commands: HostCommand[] = [];

  execute<Command extends HostCommand>(
    command: Command,
  ): Promise<HostCommandResponse<Command["command"]>>;
  execute(command: HostCommand): Promise<HostCommandResponse> {
    this.commands.push(command);
    if (command.command === "schemas.change.review")
      return Promise.resolve({
        command: command.command,
        id: command.id,
        version: command.version,
        ok: true,
        result: {
          correlationId: "schema",
          review: {
            planId: "schema-review",
            expiresAt: new Date(Date.now() + 120000).toISOString(),
            connectionName: "Local",
            input: command.payload,
            before: null,
            policy: { globalLevel: "BACKWARD", subjectLevel: null, effectiveLevel: "BACKWARD" },
            compatible: true,
          },
        },
      });
    if (command.command === "acls.change.review")
      return Promise.resolve({
        command: command.command,
        id: command.id,
        version: command.version,
        ok: true,
        result: {
          correlationId: "acl",
          review: {
            planId: "acl-review",
            input: command.payload,
            connectionName: "Local",
            expiresAt: new Date(Date.now() + 120_000).toISOString(),
            beforePresent: true,
            afterPresent: false,
            beforeAccess: null,
            afterAccess: null,
          },
        },
      });
    if (command.command === "acls.change.apply")
      return Promise.resolve({
        command: command.command,
        id: command.id,
        version: command.version,
        ok: true,
        result: {
          correlationId: "acl",
          outcome: {
            state: "acknowledged",
            verification: "verified",
            receipt: null,
            detail: "Deleted exact binding",
          },
        },
      });
    return Promise.resolve(testHostAccepted(command, command.id));
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
  it("reviews the selected ACL before an exactly confirmed deletion", async () => {
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
    const dialog = screen.getByRole("dialog", { name: "Review ACL delete" });
    const deleteButton = within(dialog).getByRole("button", { name: "Apply reviewed ACL change" });
    expect(deleteButton).toBeDisabled();
    await user.type(
      within(dialog).getByRole("textbox", { name: "Client IP seen by Kafka" }),
      "127.0.0.1",
    );
    await user.click(within(dialog).getByRole("button", { name: "Preview ACL change" }));
    expect(host.commands.at(-1)).toMatchObject({
      command: "acls.change.review",
      payload: {
        action: "delete",
        acl,
        access: { topic: acl.resourceName, principal: acl.principal, host: "127.0.0.1" },
      },
    });
    expect(deleteButton).toBeDisabled();
    await user.type(
      await within(dialog).findByRole("textbox", { name: "Exact change confirmation" }),
      `delete ${kafkaAclIdentity(acl)}`,
    );
    await user.click(deleteButton);
    expect(host.commands.find((command) => command.command === "acls.change.apply")).toMatchObject({
      command: "acls.change.apply",
      payload: { planId: "acl-review", confirmation: `delete ${kafkaAclIdentity(acl)}` },
    });
    expect(deleteButton).toBeDisabled();
    expect(host.commands.some((command) => command.command === "acls.delete")).toBe(false);
  });

  it("invalidates a compatibility result when the proposed schema or references change", async () => {
    const host = new PageHost();
    const user = userEvent.setup();
    const inventory = {
      connectionName: "Local",
      endpoint: "http://schema:8081",
      omittedSubjects: 0,
      refreshedAt: null,
      state: "ready" as const,
      subjects: [],
    };
    const detail = {
      compatibilityLevel: null,
      connectionName: "Local",
      endpoint: "http://schema:8081",
      refreshedAt: null,
      schema: null,
      state: "unavailable" as const,
      subject: null,
      versions: [],
    };
    render(
      <SchemaRegistryPage
        compatibility={{
          compatible: true,
          messages: ["The subject has no registered version."],
          subject: "new-schema",
          version: "latest",
        }}
        connected
        detail={detail}
        host={host}
        inventory={inventory}
      />,
    );
    await user.click(screen.getByRole("button", { name: "Create subject" }));
    fireEvent.change(screen.getByRole("textbox", { name: "Subject" }), {
      target: { value: "new-schema" },
    });
    fireEvent.change(screen.getByRole("textbox", { name: "Proposed schema" }), {
      target: { value: '{"type":"string"}' },
    });
    expect(screen.getByRole("button", { name: "Register reviewed schema" })).toBeDisabled();
    await user.click(screen.getByRole("button", { name: "Review schema change" }));
    expect(await screen.findByText(/New subject: no existing writer/)).toBeVisible();
    fireEvent.change(
      screen.getByRole("textbox", { name: "Type new-schema to confirm registration" }),
      { target: { value: "new-schema" } },
    );
    expect(screen.getByRole("button", { name: "Register reviewed schema" })).toBeEnabled();
    fireEvent.change(screen.getByRole("textbox", { name: "Proposed schema" }), {
      target: { value: '{"type":"int"}' },
    });
    expect(screen.getByRole("button", { name: "Register reviewed schema" })).toBeDisabled();
    expect(screen.queryByText(/New subject: no existing writer/)).not.toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Review schema change" }));
    fireEvent.change(screen.getByRole("textbox", { name: "Pinned references" }), {
      target: { value: '[{"name":"dep","subject":"dependency","version":1}]' },
    });
    expect(screen.getByRole("button", { name: "Register reviewed schema" })).toBeDisabled();
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
