// @vitest-environment jsdom

import "@testing-library/jest-dom/vitest";

import { fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import {
  HOST_PROTOCOL_VERSION,
  type HostCommandResponse,
  type HostTextDocument,
  type KafkaClusterDiagnosticsSnapshot,
  type StreamSkopeHost,
} from "../../src/features/kafka/contracts";
import {
  ClusterDetailsDialog,
  type ClusterDetailsTransferPort,
} from "../../src/features/kafka/ui/ClusterDetailsDialog";

const profile = {
  brokers: ["127.0.0.1:19093"],
  id: "profile-local",
  name: "Local validation",
} as const;

const ready: KafkaClusterDiagnosticsSnapshot = {
  cluster: {
    brokers: [
      { host: "kafka-1", nodeId: 1, port: 9093, rack: null },
      { host: "kafka-2", nodeId: 2, port: 9094, rack: "rack-b" },
    ],
    clusterId: "fixture-cluster",
    configuration: [
      {
        documentation: "Default partition count.",
        isDefault: true,
        isSensitive: false,
        name: "num.partitions",
        readOnly: false,
        source: "default",
        synonyms: [],
        type: "int",
        value: "3",
      },
      {
        documentation: null,
        isDefault: false,
        isSensitive: true,
        name: "ssl.keystore.password",
        readOnly: true,
        source: "static-broker",
        synonyms: [],
        type: "password",
        value: null,
      },
    ],
    configurationSourceBrokerId: 1,
    controllerId: 1,
  },
  endpoint: "127.0.0.1:19093",
  fetchedAt: "2026-07-25T13:00:00.000Z",
  profile,
  state: "ready",
};

const exportDocument: HostTextDocument = {
  byteSize: 20,
  content: '{\n  "cluster": {}\n}\n',
  fileName: "streamskope-cluster-fixture-cluster.json",
  mediaType: "application/json",
};

type ExecuteMock = ReturnType<typeof vi.fn<StreamSkopeHost["execute"]>>;

function accepted(command: HostCommandResponse["command"]): HostCommandResponse {
  return {
    command,
    id: `${command}-request`,
    ok: true,
    result:
      command === "clusterDetails.export"
        ? {
            correlationId: "cluster-correlation",
            document: exportDocument,
          }
        : { correlationId: "cluster-correlation" },
    version: HOST_PROTOCOL_VERSION,
  };
}

function setup(snapshot: KafkaClusterDiagnosticsSnapshot = ready): {
  readonly copy: ReturnType<typeof vi.fn<ClusterDetailsTransferPort["copy"]>>;
  readonly download: ReturnType<typeof vi.fn<ClusterDetailsTransferPort["download"]>>;
  readonly execute: ExecuteMock;
  readonly onClose: ReturnType<typeof vi.fn>;
  readonly transfer: ClusterDetailsTransferPort;
} {
  const execute = vi.fn<
    (command: Parameters<StreamSkopeHost["execute"]>[0]) => Promise<HostCommandResponse>
  >((command) => Promise.resolve(accepted(command.command)));
  const onClose = vi.fn();
  const copy = vi.fn<ClusterDetailsTransferPort["copy"]>(() => Promise.resolve());
  const download = vi.fn<ClusterDetailsTransferPort["download"]>(() => Promise.resolve());
  const transfer = {
    copy,
    download,
  };
  render(
    <ClusterDetailsDialog
      host={{
        execute,
        openExternalUrl: (): Promise<never> =>
          Promise.reject(new Error("External URL action was not expected.")),
        subscribe: (): (() => void) => () => undefined,
      }}
      onClose={onClose}
      snapshot={snapshot}
      transfer={transfer}
    />,
  );
  return { copy, download, execute, onClose, transfer };
}

describe("Kafka cluster-details Material UI workflow", () => {
  it("presents precise summary, brokers, configuration and masked sensitive values", async () => {
    setup();

    const dialog = screen.getByRole("dialog", { name: "Cluster details — Local validation" });
    expect(dialog).toBeVisible();
    expect(within(dialog).getByRole("heading", { level: 3, name: "Brokers" })).toBeVisible();
    expect(
      within(dialog).getByRole("heading", { level: 3, name: "Broker configuration" }),
    ).toBeVisible();
    expect(
      within(within(dialog).getByRole("group", { name: "Cluster detail actions" }))
        .getAllByRole("button")
        .map((button) => button.textContent),
    ).toEqual(["Refresh", "Copy JSON", "Close", "Download JSON"]);
    expect(screen.getByRole("status")).toHaveTextContent("Current cluster data.");
    expect(screen.getByText("fixture-cluster")).toBeVisible();
    expect(screen.getByText("Broker 1")).toBeVisible();
    expect(screen.getByText("2 brokers")).toBeVisible();
    expect(screen.getByText("2026-07-25 · 13:00:00 UTC")).toBeVisible();
    expect(screen.getByRole("cell", { name: "kafka-2" })).toBeVisible();
    expect(screen.getByRole("gridcell", { name: "num.partitions" })).toBeVisible();
    expect(screen.getByRole("gridcell", { name: "••••" })).toBeVisible();
    expect(screen.queryByText("unique-broker-secret")).not.toBeInTheDocument();

    fireEvent.change(screen.getByLabelText("Filter broker configuration"), {
      target: { value: "ssl" },
    });
    await waitFor(() => {
      expect(screen.queryByRole("gridcell", { name: "num.partitions" })).not.toBeInTheDocument();
    });
    expect(screen.getByRole("gridcell", { name: "ssl.keystore.password" })).toBeVisible();
  });

  it("refreshes and transfers only the host-produced export document", async () => {
    const { copy, download, execute } = setup();

    fireEvent.click(screen.getByRole("button", { name: "Refresh cluster details" }));
    await waitFor(() => {
      expect(execute.mock.calls.at(-1)?.[0]).toMatchObject({
        command: "clusterDetails.load",
        payload: {},
        version: HOST_PROTOCOL_VERSION,
      });
    });
    expect(typeof execute.mock.calls.at(-1)?.[0].id).toBe("string");

    fireEvent.click(screen.getByRole("button", { name: "Copy cluster details JSON" }));
    await waitFor(() => {
      expect(copy).toHaveBeenCalledWith(exportDocument.content);
    });
    expect(screen.getByRole("status")).toHaveTextContent("Cluster JSON copied.");

    fireEvent.click(screen.getByRole("button", { name: "Download cluster details JSON" }));
    await waitFor(() => {
      expect(download).toHaveBeenCalledWith(exportDocument);
    });
    expect(screen.getByRole("status")).toHaveTextContent("Cluster JSON download started.");
  });

  it("keeps stale evidence visible, explains failure and disables export", () => {
    const stale: KafkaClusterDiagnosticsSnapshot = {
      ...ready,
      error: {
        activeStateChanged: false,
        code: "TIMEOUT",
        correlationId: "cluster-correlation",
        recovery: "Retry cluster details.",
        retryable: true,
        stage: "broker",
        summary: "Kafka broker metadata access timed out.",
      },
      state: "stale",
    };
    setup(stale);

    expect(screen.getByText(/Displayed cluster data is stale/u)).toBeVisible();
    expect(screen.getByText(/Kafka broker metadata access timed out/u)).toBeVisible();
    expect(screen.getByRole("button", { name: "Copy cluster details JSON" })).toBeDisabled();
    expect(screen.getByRole("button", { name: "Download cluster details JSON" })).toBeDisabled();
    expect(screen.getByRole("button", { name: "Refresh cluster details" })).toBeEnabled();
  });

  it("shows partial configuration honestly and remains keyboard closable", () => {
    const partial: KafkaClusterDiagnosticsSnapshot = {
      ...ready,
      cluster: {
        ...ready.cluster,
        configuration: [],
        configurationIssue: {
          code: "authorization-denied",
          recovery: "Request DESCRIBE_CONFIGS.",
          summary: "Broker configuration is not permitted for this connection.",
        },
      },
      state: "partial",
    };
    const { onClose } = setup(partial);

    expect(screen.getByText(/Broker configuration is not permitted/u)).toBeVisible();
    expect(screen.getByRole("button", { name: "Download cluster details JSON" })).toBeEnabled();

    const dialog = screen.getByRole("dialog");
    fireEvent.keyDown(dialog, { key: "Escape" });
    expect(onClose).toHaveBeenCalledTimes(1);
  });

  it("reports transfer failure without claiming completion", async () => {
    const { download } = setup();
    download.mockRejectedValueOnce(new Error("download blocked"));

    fireEvent.click(screen.getByRole("button", { name: "Download cluster details JSON" }));

    await waitFor(() => {
      expect(screen.getByRole("alert")).toHaveTextContent(
        "The JSON export failed. No file was saved. Retry the export.",
      );
    });
    expect(within(screen.getByRole("dialog")).queryByText(/download started/u)).toBeNull();
  });
});
