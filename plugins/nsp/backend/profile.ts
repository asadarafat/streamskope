import { randomUUID } from "node:crypto";

import {
  HOST_PROTOCOL_VERSION,
  type HostError,
  type ProfileSummary,
  type ProfileTlsCreateInput,
} from "../../../src/features/kafka/contracts";
import type { PluginBackendHost } from "../../../src/plugins/api";
import { fromPluginProfileSource, toPluginProfileSource, type NspConnectInput } from "../contracts";

import { nspProblem } from "./errors";

export class NspProfileError extends Error {
  constructor(readonly failure: HostError) {
    super(failure.summary);
  }
}

export interface NspTrust {
  readonly truststoreBase64: string;
  readonly truststorePassword: string;
}

export function selectProfile(
  profiles: readonly ProfileSummary[],
  input: NspConnectInput,
): ProfileSummary | undefined {
  if (input.profileId !== undefined) {
    const selected = profiles.find((profile) => profile.id === input.profileId);
    if (selected === undefined || fromPluginProfileSource(selected.source)?.apiUrl !== input.apiUrl)
      throw nspProblem("Choose an NSP profile belonging to this API URL.");
    return selected;
  }
  const matches = profiles.filter(
    (profile) => fromPluginProfileSource(profile.source)?.apiUrl === input.apiUrl,
  );
  if (matches.length > 1)
    throw nspProblem(
      "More than one NSP profile exists for this API. Refresh one from its profile panel.",
    );
  return matches[0];
}

export function profileDraft(
  input: NspConnectInput,
  trust: NspTrust,
  workflowName: string,
  existing: ProfileSummary | undefined,
  authentication: "tls" | "oauth",
): ProfileTlsCreateInput {
  const brokers = input.brokers ?? existing?.brokers ?? [`${new URL(input.apiUrl).hostname}:9192`];
  return {
    transport: "tls",
    name: existing?.name ?? `NSP ${new URL(input.apiUrl).host}`,
    brokers,
    trust: {
      kind: "jks",
      label: "NSP Kafka CA truststore",
      material: { mode: "replace", value: trust.truststoreBase64 },
      password: { mode: "replace", value: trust.truststorePassword },
    },
    ...(authentication === "tls"
      ? {}
      : {
          oauth: {
            clientId: input.username,
            clientSecret: { mode: "replace" as const, value: input.password },
            scope: "",
            tokenEndpoint: `${input.apiUrl}/rest-gateway/rest/api/v1/auth/token`,
          },
        }),
    source: toPluginProfileSource({ apiUrl: input.apiUrl, brokers, workflowName, authentication }),
    ...(existing?.services === undefined ? {} : { services: existing.services }),
  };
}

/** Use the core profile test/save path, including trust validation and protected storage. */
export async function qualifyAndSaveProfile(
  host: PluginBackendHost,
  input: NspConnectInput,
  trust: NspTrust,
  workflowName: string,
  existing: ProfileSummary | undefined,
  signal: AbortSignal,
  stage: (step: "test" | "save", message: string) => void,
): Promise<string> {
  const preferred = input.authentication ?? "auto";
  let draft = profileDraft(
    input,
    trust,
    workflowName,
    existing,
    preferred === "oauth" ? "oauth" : "tls",
  );
  const test = async (): Promise<void> => {
    signal.throwIfAborted();
    const result = await host.execute({
      command: "profiles.test",
      id: randomUUID(),
      version: HOST_PROTOCOL_VERSION,
      payload: { mode: "create", profile: draft },
    });
    signal.throwIfAborted();
    if (!result.ok) throw new NspProfileError(result.error);
  };
  stage("test", "Verifying the Kafka connection with the retrieved truststore.");
  try {
    await test();
  } catch (error) {
    if (
      preferred !== "auto" ||
      !(error instanceof NspProfileError) ||
      error.failure.code !== "KAFKA_AUTHENTICATION"
    )
      throw error;
    draft = profileDraft(input, trust, workflowName, existing, "oauth");
    stage("test", "The broker requires authentication; verifying OAuth with the NSP credentials.");
    await test();
  }
  signal.throwIfAborted();
  stage(
    "save",
    existing === undefined
      ? "Saving the verified NSP connection profile."
      : "Refreshing the existing NSP connection profile.",
  );
  const result =
    existing === undefined
      ? await host.execute({
          command: "profiles.create",
          id: randomUUID(),
          version: HOST_PROTOCOL_VERSION,
          payload: { profile: draft },
        })
      : await host.execute({
          command: "profiles.update",
          id: randomUUID(),
          version: HOST_PROTOCOL_VERSION,
          payload: {
            profileId: existing.id,
            profile: {
              ...draft,
              ...(existing.revision === undefined ? {} : { expectedRevision: existing.revision }),
            },
          },
        });
  // Saving is an atomic commit point. A cancellation arriving after commit cannot undo success.
  if (!result.ok) throw new NspProfileError(result.error);
  if (result.result.profileId === undefined)
    throw new Error("NSP profile save returned no identity.");
  return result.result.profileId;
}
