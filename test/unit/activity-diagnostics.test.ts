import { describe, expect, it } from "vitest";

import {
  ACTIVITY_DETAIL_CHARACTER_LIMIT,
  ActivityHistory,
  redactSensitiveText,
  translateHostFailure,
} from "../../src/platform/activity";

describe("safe operational diagnostics", () => {
  it("translates an actionable failure while redacting exact and classified secrets", () => {
    const submittedSecret = "NokiaNsp@";
    const accessToken = "eyJhbGciOiJIUzI1NiJ9.fixture.signature";
    const authorization = "Basic YWRtaW46Tm9raWFOc3BA";
    const privateKey = "-----BEGIN PRIVATE KEY-----\nprivate-material\n-----END PRIVATE KEY-----";
    const oversizedBody = "x".repeat(ACTIVITY_DETAIL_CHARACTER_LIMIT * 3);

    const translated = translateHostFailure({
      activeStateChanged: false,
      cause: new Error(
        [
          `credential=${submittedSecret}`,
          `Authorization: ${authorization}`,
          `access_token=${accessToken}`,
          'client_secret="another-secret"',
          privateKey,
          oversizedBody,
        ].join("\n"),
      ),
      code: "OAUTH_REJECTED",
      correlationId: "correlation-oauth-1",
      recovery: `Check the token endpoint and replace ${submittedSecret}.`,
      retryable: false,
      sensitiveValues: [submittedSecret, accessToken, authorization],
      stage: "oauth",
      summary: `OAuth rejected ${submittedSecret}.`,
      target: "http://127.0.0.1:5000/token?client_secret=another-secret&scope=kafka",
    });

    const visibleOutput = JSON.stringify(translated);
    expect(visibleOutput).not.toContain(submittedSecret);
    expect(visibleOutput).not.toContain(accessToken);
    expect(visibleOutput).not.toContain(authorization);
    expect(visibleOutput).not.toContain("another-secret");
    expect(visibleOutput).not.toContain("private-material");
    expect(translated.error).toEqual({
      activeStateChanged: false,
      code: "OAUTH_REJECTED",
      correlationId: "correlation-oauth-1",
      recovery: "Check the token endpoint and replace [REDACTED].",
      retryable: false,
      stage: "oauth",
      summary: "OAuth rejected [REDACTED].",
      target: "http://127.0.0.1:5000/token?client_secret=[REDACTED]&scope=kafka",
    });
    expect(translated.detail).toContain("Stage: OAuth");
    expect(translated.detail).toContain("Category: OAUTH_REJECTED");
    expect(translated.detail).toContain("Active connection changed: No");
    expect(translated.detail).toContain(
      "Next action: Check the token endpoint and replace [REDACTED].",
    );
    expect(translated.detail).toContain("[REDACTED PRIVATE KEY]");
    expect(translated.detail.length).toBeLessThanOrEqual(ACTIVITY_DETAIL_CHARACTER_LIMIT);
    expect(translated.detail).toContain("[truncated]");
  });

  it("redacts an incomplete private-key block before it can be truncated", () => {
    const safe = redactSensitiveText(
      `upstream response\n-----BEGIN RSA PRIVATE KEY-----\nincomplete-private${"x".repeat(
        ACTIVITY_DETAIL_CHARACTER_LIMIT * 2,
      )}`,
    );

    expect(safe).toBe("upstream response\n[REDACTED PRIVATE KEY]");
    expect(safe).not.toContain("incomplete-private");
  });

  it("keeps bounded safe activity with the newest failure after eviction", () => {
    const history = new ActivityHistory(3);
    for (let index = 1; index <= 4; index += 1) {
      history.record(
        {
          correlationId: `correlation-${index}`,
          detail:
            index === 4
              ? "OAuth failed with client_secret=do-not-display."
              : `Operation ${index} completed.`,
          id: `activity-${index}`,
          object: "Local aio",
          operation: index === 4 ? "Connection test" : "Background check",
          outcome: index === 4 ? "failed" : "succeeded",
          severity: index === 4 ? "error" : "info",
          timestamp: `2026-07-25T12:00:0${index}.000Z`,
        },
        ["do-not-display"],
      );
    }

    expect(history.entries().map((entry) => entry.id)).toEqual([
      "activity-2",
      "activity-3",
      "activity-4",
    ]);
    expect(history.entries().at(-1)).toMatchObject({
      id: "activity-4",
      outcome: "failed",
      severity: "error",
    });
    expect(JSON.stringify(history.entries())).not.toContain("do-not-display");
    expect(history.entries().at(-1)?.detail).toBe("OAuth failed with client_secret=[REDACTED].");
  });
});
