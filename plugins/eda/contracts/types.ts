import type { HostError } from "../../../src/features/kafka/contracts";

import type {
  EdaCaptureApplicationInput,
  EdaCaptureDeployInput,
  EdaCaptureInspectInput,
  EdaCaptureProgress,
  EdaCaptureCommandResults,
} from "./eda-capture-types";
import type { ProfileEdaCaptureSource } from "./profile-types";
export const EDA_PROTOCOL_VERSION = 1 as const;
export const HOST_PROTOCOL_VERSION = EDA_PROTOCOL_VERSION;
export const EDA_PLUGIN_ID = "streamskope.eda" as const;
export const EDA_CAPTURE_COMMANDS = [
  "edaCapture.application.status",
  "edaCapture.application.install",
  "edaCapture.preflight",
  "edaCapture.status",
  "edaCapture.stop",
  "edaCapture.remove",
  "edaCapture.cancel",
  "edaCapture.inspect",
  "edaCapture.deploy",
] as const;
export type EdaCaptureCommandName = (typeof EDA_CAPTURE_COMMANDS)[number];
interface HostCommandBase {
  readonly id: string;
  readonly version: number;
}
type EdaCommandDefinition =
  | (HostCommandBase & {
      readonly command: "edaCapture.application.status" | "edaCapture.application.install";
      readonly payload: EdaCaptureApplicationInput;
    })
  | (HostCommandBase & {
      readonly command: "edaCapture.preflight" | "edaCapture.status";
      readonly payload: Record<string, never>;
    })
  | (HostCommandBase & {
      readonly command: "edaCapture.stop" | "edaCapture.remove";
      readonly payload: { readonly source: ProfileEdaCaptureSource };
    })
  | (HostCommandBase & {
      readonly command: "edaCapture.cancel";
      readonly payload: { readonly requestId: string };
    })
  | (HostCommandBase & {
      readonly command: "edaCapture.inspect";
      readonly payload: EdaCaptureInspectInput;
    })
  | (HostCommandBase & {
      readonly command: "edaCapture.deploy";
      readonly payload: EdaCaptureDeployInput;
    });

type Distribute<T> = T extends { readonly command: infer Name; readonly payload: infer Payload }
  ? Name extends string
    ? HostCommandBase & { readonly command: Name; readonly payload: Payload }
    : never
  : never;
export type EdaCaptureCommand = Distribute<EdaCommandDefinition>;
export type EdaCaptureCommandResponse<Name extends EdaCaptureCommandName = EdaCaptureCommandName> =
  Name extends EdaCaptureCommandName
    ? | {
          readonly command: Name;
          readonly id: string;
          readonly version: number;
          readonly ok: false;
          readonly error: HostError;
        }
      | {
          readonly command: Name;
          readonly id: string;
          readonly version: number;
          readonly ok: true;
          readonly result: Name extends keyof EdaCaptureCommandResults
            ? EdaCaptureCommandResults[Name]
            : { readonly correlationId: string };
        }
    : never;
export interface EdaCaptureHostEvent {
  readonly event: "edaCapture.progress";
  readonly payload: EdaCaptureProgress;
  readonly sequence: number;
  readonly version: number;
}
export type HostCommand = EdaCaptureCommand;
export type HostCommandName = EdaCaptureCommandName;
export type HostCommandResponse<Name extends EdaCaptureCommandName = EdaCaptureCommandName> =
  EdaCaptureCommandResponse<Name>;
export type HostEvent = EdaCaptureHostEvent;
export type HostEventName = EdaCaptureHostEvent["event"];
