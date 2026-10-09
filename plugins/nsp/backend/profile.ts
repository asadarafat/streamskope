import { randomUUID } from "node:crypto";

import {
  HOST_PROTOCOL_VERSION,
  type HostError,
  type ProfileSummary,
  type ProfileTlsCreateInput,
  type ProfileTlsUpdateInput,
} from "../../../src/features/kafka/contracts";
import {
  retainedClientIdentity,
  retainedServiceEndpoints,
} from "../../../src/features/kafka/contracts/profile-retain-input";
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
  existing: undefined,
  authentication: "tls" | "oauth",
): ProfileTlsCreateInput;
export function profileDraft(
  input: NspConnectInput,
  trust: NspTrust,
  workflowName: string,
  existing: ProfileSummary | undefined,
  authentication: "tls" | "oauth",
): ProfileTlsUpdateInput;
export function profileDraft(
  input: NspConnectInput,
  trust: NspTrust,
  workflowName: string,
  existing: ProfileSummary | undefined,
  authentication: "tls" | "oauth",
): ProfileTlsUpdateInput {
  const preserveSasl =
    (input.authentication === undefined || input.authentication === "auto") &&
    existing?.sasl !== undefined;
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
    ...(authentication === "tls" || preserveSasl
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
    ...(existing?.services === undefined
      ? {}
      : { services: retainedServiceEndpoints(existing.services) }),
    ...(existing?.clientIdentity === undefined
      ? {}
      : { clientIdentity: retainedClientIdentity(existing.clientIdentity) }),
    ...(preserveSasl && existing?.sasl !== undefined
      ? {
          sasl: {
            mechanism: existing.sasl.mechanism,
            username: existing.sasl.username,
            password: { mode: "retain" as const },
          },
        }
      : {}),
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
    preferred === "oauth" || (preferred === "auto" && existing?.oauth !== undefined)
      ? "oauth"
      : "tls",
  );
  const test = async (): Promise<void> => {
    signal.throwIfAborted();
    const result = await host.execute({
      command: "profiles.test",
      id: randomUUID(),
      version: HOST_PROTOCOL_VERSION,
      payload:
        existing === undefined
          ? {
              mode: "create",
              profile: profileDraft(
                input,
                trust,
                workflowName,
                undefined,
                draft.oauth === undefined ? "tls" : "oauth",
              ),
            }
          : {
              mode: "update",
              profileId: existing.id,
              profile: {
                ...draft,
                ...(existing.revision === undefined ? {} : { expectedRevision: existing.revision }),
              },
            },
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
      draft.sasl !== undefined ||
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
          payload: {
            profile: profileDraft(
              input,
              trust,
              workflowName,
              undefined,
              draft.oauth === undefined ? "tls" : "oauth",
            ),
          },
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
