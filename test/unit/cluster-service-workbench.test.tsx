// @vitest-environment jsdom

import "@testing-library/jest-dom/vitest";

import { act, cleanup, render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it } from "vitest";

import { StreamSkopeApp } from "../../src/app/StreamSkopeApp";
import {
  HOST_PROTOCOL_VERSION,
  type HostCommand,
  type HostCommandResponse,
  type HostEvent,
  type HostEventListener,
  type StreamSkopeHost,
} from "../../src/kafka/contracts";

class FeatureHost implements StreamSkopeHost {
  readonly commands: HostCommand[] = [];
  private readonly listeners = new Set<HostEventListener>();
  emit(event: HostEvent): void {
    for (const listener of this.listeners) listener(event);
  }
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
  subscribe(listener: HostEventListener): () => void {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }
}

afterEach(() => {
  cleanup();
  localStorage.clear();
});

describe("cluster service workbench", () => {
  it("exposes operational Schema Registry, ACL, and transform paths", async () => {
    const host = new FeatureHost();
    const user = userEvent.setup();
    render(<StreamSkopeApp host={host} />);
    act(() => {
      host.emit({
        event: "connection.state",
        payload: { connectionName: "Local", state: "connected" },
        sequence: 1,
        version: HOST_PROTOCOL_VERSION,
      });
      host.emit({
        event: "topics.changed",
        payload: { refreshedAt: "2026-08-12T12:00:00.000Z", state: "ready", topics: [] },
        sequence: 2,
        version: HOST_PROTOCOL_VERSION,
      });
    });
    const navigation = await screen.findByRole("navigation", { name: "StreamSkope resources" });

    await user.click(within(navigation).getByRole("button", { name: "Schema Registry" }));
    expect(screen.getByRole("heading", { name: "Schema Registry" })).toBeVisible();
    await waitFor(() =>
      expect(host.commands.some((command) => command.command === "schemas.list")).toBe(true),
    );
    act(() =>
      host.emit({
        event: "schemas.changed",
        payload: {
          connectionName: "Local",
          endpoint: "http://schema:8081",
          omittedSubjects: 0,
          refreshedAt: "2026-08-12T12:00:01.000Z",
          state: "ready",
          subjects: ["orders-value"],
        },
        sequence: 3,
        version: HOST_PROTOCOL_VERSION,
      }),
    );
    await user.click(screen.getByText("orders-value"));
    expect(host.commands.at(-1)).toMatchObject({
      command: "schemas.load",
      payload: { subject: "orders-value", version: "latest" },
    });

    await user.click(within(navigation).getByRole("button", { name: "Access Control Lists" }));
    expect(screen.getByRole("heading", { name: "Access Control Lists" })).toBeVisible();
    await waitFor(() =>
      expect(host.commands.some((command) => command.command === "acls.list")).toBe(true),
    );
    expect(screen.getByRole("button", { name: "Create ACL" })).toBeEnabled();

    await user.click(within(navigation).getByRole("button", { name: "Transforms" }));
    expect(screen.getByRole("heading", { name: "Data Transforms" })).toBeVisible();
    await waitFor(() =>
      expect(host.commands.some((command) => command.command === "transforms.list")).toBe(true),
    );
    act(() =>
      host.emit({
        event: "transforms.changed",
        payload: {
          connectionName: "Local",
          endpoint: "http://admin:9644",
          omittedTransforms: 0,
          refreshedAt: "2026-08-12T12:00:02.000Z",
          state: "ready",
          transforms: [
            {
              aggregateStatus: "running",
              compression: "none",
              environment: [{ name: "TOKEN", valuePresent: true }],
              inputTopic: "orders.raw",
              maximumLag: 0,
              name: "mask-orders",
              offset: null,
              outputTopics: ["orders.masked"],
              statuses: [{ lag: 0, nodeId: 1, partition: 0, status: "running" }],
            },
          ],
        },
        sequence: 4,
        version: HOST_PROTOCOL_VERSION,
      }),
    );
    await user.click(screen.getByText("mask-orders"));
    await waitFor(() =>
      expect(host.commands.some((command) => command.command === "transforms.logs.load")).toBe(
        true,
      ),
    );
  });
});
