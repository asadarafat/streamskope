import type { HOST_PROTOCOL_VERSION } from "./types";

export interface ExternalUrlOpenRequest {
  readonly url: string;
  readonly version: typeof HOST_PROTOCOL_VERSION;
}

export interface ExternalUrlOpenResult {
  readonly state: "accepted";
  readonly version: typeof HOST_PROTOCOL_VERSION;
}
