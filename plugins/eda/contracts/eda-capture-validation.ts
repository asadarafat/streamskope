import { HostContractValidationError } from "../../../src/features/kafka/contracts/validation-error";
import {
  declaredValue,
  exactKeys,
  optionalText,
  positiveBoundedInteger,
  record,
  text,
} from "../../../src/features/kafka/contracts/validation-primitives";

import {
  EDA_CAPTURE_LIMITS,
  EDA_CAPTURE_APPLICATION,
  type EdaCaptureApplicationInput,
  type EdaCaptureApplicationStatus,
  EDA_CAPTURE_PROGRESS_PHASES,
  type EdaApiCredentialsInput,
  type EdaCaptureDeployInput,
  type EdaCaptureDeployment,
  type EdaCaptureInspectInput,
  type EdaCaptureInspection,
  type EdaCaptureProducerKind,
  type EdaCaptureProgress,
  type EdaCaptureSource,
  type EdaCaptureSourceIdentity,
} from "./eda-capture-types";

const PRODUCER_API_VERSIONS = ["kafka.eda.nokia.com/v1", "kafka.eda.nokia.com/v1alpha1"] as const;
const PRODUCER_KINDS = ["ClusterProducer", "Producer"] as const;
const DNS_LABEL = /^[a-z0-9](?:[-a-z0-9]*[a-z0-9])?$/u;
const DNS_SUBDOMAIN = /^[a-z0-9](?:[-a-z0-9.]*[a-z0-9])?$/u;
const CAPTURE_SESSION_ID =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/u;

function sessionId(value: unknown, path: string): string {
  const parsed = text(value, path, 36);
  if (!CAPTURE_SESSION_ID.test(parsed))
    throw new HostContractValidationError(path, "must be a lowercase capture session UUID");
  return parsed;
}

function kubernetesName(value: unknown, path: string, namespace = false): string {
  const parsed = text(value, path, namespace ? 63 : EDA_CAPTURE_LIMITS.nameCharacters);
  const pattern = namespace ? DNS_LABEL : DNS_SUBDOMAIN;
  if (!pattern.test(parsed)) {
    throw new HostContractValidationError(path, "must be a lowercase Kubernetes DNS name");
  }
  return parsed;
}

function contextName(value: unknown, path: string): string {
  const parsed = text(value, path, EDA_CAPTURE_LIMITS.nameCharacters);
  if (parsed.trim() !== parsed) {
    throw new HostContractValidationError(path, "must not have surrounding whitespace");
  }
  return parsed;
}

function imageReference(value: unknown, path: string): string {
  const image = text(value, path, EDA_CAPTURE_LIMITS.imageCharacters);
  if (/\s/u.test(image) || image.includes("://")) {
    throw new HostContractValidationError(
      path,
      "must be an OCI image reference without a URL scheme",
    );
  }
  return image;
}

function registryHost(value: unknown, path: string): string {
  const host = text(value, path, EDA_CAPTURE_LIMITS.hostCharacters);
  if (/\s/u.test(host) || host.includes("://") || host.includes("/")) {
    throw new HostContractValidationError(
      path,
      "must be a DNS name or IP address without a scheme, path, or port",
    );
  }
  return host;
}

function edaApiUrl(value: unknown, path: string): string {
  const input = text(value, path, EDA_CAPTURE_LIMITS.urlCharacters);
  let url: URL;
  try {
    url = new URL(input);
  } catch {
    throw new HostContractValidationError(path, "must be a valid HTTPS URL");
  }
  if (
    url.protocol !== "https:" ||
    url.username.length > 0 ||
    url.password.length > 0 ||
    (url.pathname !== "/" && url.pathname !== "") ||
    url.search.length > 0 ||
    url.hash.length > 0
  ) {
    throw new HostContractValidationError(path, "must be an HTTPS origin without credentials");
  }
  return url.origin;
}

export function parseEdaApiCredentials(value: unknown, path: string): EdaApiCredentialsInput {
  const credentials = record(value, path);
  exactKeys(credentials, ["baseUrl", "password", "username", "verifyTls"], path);
  const verifyTls = Object.hasOwn(credentials, "verifyTls") ? credentials.verifyTls : true;
  if (typeof verifyTls !== "boolean") {
    throw new HostContractValidationError(`${path}.verifyTls`, "must be a boolean");
  }
  return {
    baseUrl: edaApiUrl(credentials.baseUrl, `${path}.baseUrl`),
    password: text(
      credentials.password,
      `${path}.password`,
      EDA_CAPTURE_LIMITS.credentialCharacters,
    ),
    username: text(
      credentials.username,
      `${path}.username`,
      EDA_CAPTURE_LIMITS.credentialCharacters,
    ),
    verifyTls,
  };
}

export function parseEdaCaptureApplicationInput(
  value: unknown,
  path = "captureApplication",
): EdaCaptureApplicationInput {
  const input = record(value, path);
  exactKeys(input, ["authorization", "edaApi"], path);
  const administrator = input.authorization;
  let authorization: EdaCaptureApplicationInput["authorization"];
  if (administrator !== undefined) {
    const credentials = record(administrator, `${path}.authorization`);
    exactKeys(credentials, ["password", "username"], `${path}.authorization`);
    authorization = {
      password: text(
        credentials.password,
        `${path}.authorization.password`,
        EDA_CAPTURE_LIMITS.credentialCharacters,
      ),
      username: text(
        credentials.username,
        `${path}.authorization.username`,
        EDA_CAPTURE_LIMITS.credentialCharacters,
      ),
    };
  }
  return {
    edaApi: parseEdaApiCredentials(input.edaApi, `${path}.edaApi`),
    ...(authorization === undefined ? {} : { authorization }),
  };
}

export function parseEdaCaptureApplicationStatus(
  value: unknown,
  path = "captureApplication",
): EdaCaptureApplicationStatus {
  const status = record(value, path);
  exactKeys(status, ["appId", "publisher", "state", "version"], path);
  return {
    appId: declaredValue(status.appId, [EDA_CAPTURE_APPLICATION.appId] as const, `${path}.appId`),
    publisher: declaredValue(
      status.publisher,
      [EDA_CAPTURE_APPLICATION.publisher] as const,
      `${path}.publisher`,
    ),
    state: declaredValue(status.state, ["installed", "missing"] as const, `${path}.state`),
    version: declaredValue(
      status.version,
      [EDA_CAPTURE_APPLICATION.version] as const,
      `${path}.version`,
    ),
  };
}

function stringArray(
  value: unknown,
  path: string,
  maximumItems: number,
  maximumCharacters: number = EDA_CAPTURE_LIMITS.nameCharacters,
): readonly string[] {
  if (!Array.isArray(value) || value.length > maximumItems) {
    throw new HostContractValidationError(path, `must contain at most ${maximumItems} items`);
  }
  return value.map((entry, index) => text(entry, `${path}[${String(index)}]`, maximumCharacters));
}

export function parseEdaCaptureSourceIdentity(
  value: unknown,
  path: string,
): EdaCaptureSourceIdentity {
  const source = record(value, path);
  exactKeys(source, ["apiVersion", "kind", "name", "namespace"], path);
  return {
    apiVersion: declaredValue(source.apiVersion, PRODUCER_API_VERSIONS, `${path}.apiVersion`),
    kind: declaredValue<EdaCaptureProducerKind>(source.kind, PRODUCER_KINDS, `${path}.kind`),
    name: kubernetesName(source.name, `${path}.name`),
    namespace: kubernetesName(source.namespace, `${path}.namespace`, true),
  };
}

export function parseEdaCaptureInspectInput(
  value: unknown,
  path = "capture",
): EdaCaptureInspectInput {
  const input = record(value, path);
  exactKeys(input, ["edaApi"], path);
  return {
    edaApi: parseEdaApiCredentials(input.edaApi, `${path}.edaApi`),
  };
}

export function parseEdaCaptureDeployInput(
  value: unknown,
  path = "capture",
): EdaCaptureDeployInput {
  const input = record(value, path);
  exactKeys(
    input,
    [
      "context",
      "edaApi",
      "image",
      "imageDelivery",
      "imagePullSecret",
      "localPort",
      "registry",
      "sessionId",
      "source",
    ],
    path,
  );
  const imageDelivery = declaredValue(
    input.imageDelivery,
    ["cluster", "configured", "embedded"] as const,
    `${path}.imageDelivery`,
  );
  const localPort = positiveBoundedInteger(input.localPort, `${path}.localPort`, 65_535);
  if (localPort < 1_024) {
    throw new HostContractValidationError(`${path}.localPort`, "must be at least 1024");
  }
  const common = {
    context: contextName(input.context, `${path}.context`),
    edaApi: parseEdaApiCredentials(input.edaApi, `${path}.edaApi`),
    localPort,
    ...(input.sessionId === undefined
      ? {}
      : { sessionId: sessionId(input.sessionId, `${path}.sessionId`) }),
    source: parseEdaCaptureSourceIdentity(input.source, `${path}.source`),
  };
  if (imageDelivery === "configured") {
    for (const field of ["image", "imagePullSecret", "registry"] as const) {
      if (Object.hasOwn(input, field)) {
        throw new HostContractValidationError(
          `${path}.${field}`,
          "must be omitted when using the saved environment setup",
        );
      }
    }
    return { ...common, imageDelivery };
  }
  const image = imageReference(input.image, `${path}.image`);
  if (imageDelivery === "cluster") {
    if (Object.hasOwn(input, "registry")) {
      throw new HostContractValidationError(
        `${path}.registry`,
        "is available only for embedded image delivery",
      );
    }
    const imagePullSecret = optionalText(
      input,
      "imagePullSecret",
      path,
      EDA_CAPTURE_LIMITS.nameCharacters,
    );
    return {
      ...common,
      image,
      imageDelivery,
      ...(imagePullSecret === undefined
        ? {}
        : {
            imagePullSecret: kubernetesName(imagePullSecret, `${path}.imagePullSecret`),
          }),
    };
  }
  if (Object.hasOwn(input, "imagePullSecret")) {
    throw new HostContractValidationError(
      `${path}.imagePullSecret`,
      "is unavailable for the embedded registry",
    );
  }
  const registry = record(input.registry, `${path}.registry`);
  exactKeys(
    registry,
    ["advertisedHost", "certificatePem", "port", "privateKeyPem"],
    `${path}.registry`,
  );
  const registryPort = positiveBoundedInteger(registry.port, `${path}.registry.port`, 65_535);
  if (registryPort < 1_024) {
    throw new HostContractValidationError(`${path}.registry.port`, "must be at least 1024");
  }
  if (registryPort === localPort) {
    throw new HostContractValidationError(
      `${path}.registry.port`,
      "must differ from the local Kafka port",
    );
  }
  return {
    ...common,
    image,
    imageDelivery,
    registry: {
      advertisedHost: registryHost(registry.advertisedHost, `${path}.registry.advertisedHost`),
      certificatePem: text(
        registry.certificatePem,
        `${path}.registry.certificatePem`,
        EDA_CAPTURE_LIMITS.certificateCharacters,
      ),
      port: registryPort,
      privateKeyPem: text(
        registry.privateKeyPem,
        `${path}.registry.privateKeyPem`,
        EDA_CAPTURE_LIMITS.privateKeyCharacters,
      ),
    },
  };
}

function parseSource(value: unknown, path: string): EdaCaptureSource {
  const source = record(value, path);
  exactKeys(source, ["apiVersion", "kind", "name", "namespace", "topics", "brokers"], path);
  return {
    ...parseEdaCaptureSourceIdentity(
      {
        apiVersion: source.apiVersion,
        kind: source.kind,
        name: source.name,
        namespace: source.namespace,
      },
      path,
    ),
    topics: stringArray(source.topics, `${path}.topics`, EDA_CAPTURE_LIMITS.topics, 512),
    ...(source.brokers === undefined
      ? {}
      : { brokers: stringArray(source.brokers, `${path}.brokers`, 32, 512) }),
  };
}

function parseImageSetup(value: unknown, path: string): EdaCaptureInspection["imageSetup"] {
  const setup = record(value, path);
  const state = declaredValue(
    setup.state,
    ["configured", "unconfigured"] as const,
    `${path}.state`,
  );
  if (state === "unconfigured") {
    exactKeys(setup, ["state"], path);
    return { state };
  }
  exactKeys(setup, ["image", "imageDelivery", "registryHost", "registryPort", "state"], path);
  const imageDelivery = declaredValue(
    setup.imageDelivery,
    ["cluster", "embedded"] as const,
    `${path}.imageDelivery`,
  );
  const common = {
    image: imageReference(setup.image, `${path}.image`),
    imageDelivery,
    state,
  } as const;
  if (imageDelivery === "cluster") return common;
  return {
    ...common,
    registryHost: registryHost(setup.registryHost, `${path}.registryHost`),
    registryPort: positiveBoundedInteger(setup.registryPort, `${path}.registryPort`, 65_535),
  };
}

export function parseEdaCaptureInspection(
  value: unknown,
  path = "inspection",
): EdaCaptureInspection {
  const inspection = record(value, path);
  exactKeys(
    inspection,
    ["context", "contexts", "edaApiUrl", "imageSetup", "namespace", "sources"],
    path,
  );
  if (
    !Array.isArray(inspection.sources) ||
    inspection.sources.length > EDA_CAPTURE_LIMITS.sources
  ) {
    throw new HostContractValidationError(
      `${path}.sources`,
      `must contain at most ${EDA_CAPTURE_LIMITS.sources} sources`,
    );
  }
  return {
    ...(inspection.context === undefined
      ? {}
      : { context: contextName(inspection.context, `${path}.context`) }),
    contexts: stringArray(inspection.contexts, `${path}.contexts`, EDA_CAPTURE_LIMITS.contexts).map(
      (context, index) => contextName(context, `${path}.contexts[${String(index)}]`),
    ),
    ...(Object.hasOwn(inspection, "edaApiUrl")
      ? { edaApiUrl: edaApiUrl(inspection.edaApiUrl, `${path}.edaApiUrl`) }
      : {}),
    imageSetup: parseImageSetup(inspection.imageSetup, `${path}.imageSetup`),
    namespace: kubernetesName(inspection.namespace, `${path}.namespace`, true),
    sources: inspection.sources.map((source, index) =>
      parseSource(source, `${path}.sources[${String(index)}]`),
    ),
  };
}

export function parseEdaCaptureDeployment(
  value: unknown,
  path = "deployment",
): EdaCaptureDeployment {
  const deployment = record(value, path);
  exactKeys(
    deployment,
    [
      "broker",
      "clusterBroker",
      "context",
      "exporterName",
      "namespace",
      "profileName",
      "topics",
      "workloadName",
      "sessionId",
    ],
    path,
  );
  return {
    ...(deployment.sessionId === undefined
      ? {}
      : { sessionId: text(deployment.sessionId, `${path}.sessionId`, 128) }),
    broker: text(deployment.broker, `${path}.broker`, 512),
    clusterBroker: text(deployment.clusterBroker, `${path}.clusterBroker`, 512),
    context: contextName(deployment.context, `${path}.context`),
    exporterName: kubernetesName(deployment.exporterName, `${path}.exporterName`),
    namespace: kubernetesName(deployment.namespace, `${path}.namespace`, true),
    profileName: text(deployment.profileName, `${path}.profileName`, 256),
    topics: stringArray(deployment.topics, `${path}.topics`, EDA_CAPTURE_LIMITS.topics, 512),
    workloadName: kubernetesName(deployment.workloadName, `${path}.workloadName`),
  };
}

export function parseEdaCaptureProgress(value: unknown, path = "progress"): EdaCaptureProgress {
  const progress = record(value, path);
  exactKeys(progress, ["detail", "phase", "requestId"], path);
  return {
    detail: text(progress.detail, `${path}.detail`, 2_048),
    phase: declaredValue(progress.phase, EDA_CAPTURE_PROGRESS_PHASES, `${path}.phase`),
    requestId: text(progress.requestId, `${path}.requestId`, 128),
  };
}
