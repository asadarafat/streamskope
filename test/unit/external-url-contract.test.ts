import { describe, expect, it } from "vitest";

import {
  HOST_PROTOCOL_VERSION,
  HostContractValidationError,
  parseExternalUrlOpenRequest,
  parseExternalUrlOpenResult,
} from "../../src/features/kafka/contracts";

const RUNBOOK_URL = "https://runbooks.example.test/kafka/latency?cluster=local#recovery";

describe("external URL contract", () => {
  it("preserves one exact credential-free HTTPS request and accepted result", () => {
    const request = {
      url: RUNBOOK_URL,
      version: HOST_PROTOCOL_VERSION,
    } as const;
    const result = {
      state: "accepted",
      version: HOST_PROTOCOL_VERSION,
    } as const;

    expect(parseExternalUrlOpenRequest(request)).toEqual(request);
    expect(parseExternalUrlOpenResult(result)).toEqual(result);
  });

  it.each([
    {
      label: "non-HTTPS URL",
      value: { url: "http://runbooks.example.test/kafka", version: HOST_PROTOCOL_VERSION },
    },
    {
      label: "URL credentials",
      value: {
        url: "https://operator:private@runbooks.example.test/kafka",
        version: HOST_PROTOCOL_VERSION,
      },
    },
    {
      label: "local file",
      value: { url: "file:///tmp/runbook.html", version: HOST_PROTOCOL_VERSION },
    },
    {
      label: "shell command field",
      value: {
        command: "open /tmp/private",
        url: RUNBOOK_URL,
        version: HOST_PROTOCOL_VERSION,
      },
    },
    {
      label: "stale version",
      value: { url: RUNBOOK_URL, version: HOST_PROTOCOL_VERSION - 1 },
    },
  ])("rejects $label before an external action", ({ value }) => {
    expect(() => parseExternalUrlOpenRequest(value)).toThrow(HostContractValidationError);
  });

  it.each([
    { state: "opened", version: HOST_PROTOCOL_VERSION },
    { state: "accepted", unexpected: true, version: HOST_PROTOCOL_VERSION },
    { state: "accepted", version: HOST_PROTOCOL_VERSION - 1 },
  ])("rejects an undeclared or stale platform result", (value) => {
    expect(() => parseExternalUrlOpenResult(value)).toThrow(HostContractValidationError);
  });
});
