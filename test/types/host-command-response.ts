import {
  HOST_PROTOCOL_VERSION,
  type HostCommandResponse,
  type StreamSkopeBackend,
} from "../../src/features/kafka/contracts";

// This fixture is compiled by the test TypeScript project; it is never executed.
export const missingEditor: HostCommandResponse<"trustAcquisition.editor.open"> = {
  command: "trustAcquisition.editor.open",
  id: "editor",
  ok: true,
  // @ts-expect-error An editor success must include its editor identity.
  result: { correlationId: "editor" },
  version: HOST_PROTOCOL_VERSION,
};

export const wrongRecipeResult: HostCommandResponse<"recipes.import.preview"> = {
  command: "recipes.import.preview",
  id: "preview",
  ok: true,
  result: {
    correlationId: "preview",
    // @ts-expect-error Import preview returns a draft, not recipe usage.
    usage: { profileIds: [] },
  },
  version: HOST_PROTOCOL_VERSION,
};

export async function assertExecuteInference(backend: StreamSkopeBackend): Promise<void> {
  const editor = await backend.execute({
    command: "trustAcquisition.editor.open",
    id: "editor",
    payload: {},
    version: HOST_PROTOCOL_VERSION,
  });
  if (editor.ok) {
    const identity: { readonly id: string; readonly generation: number } = editor.result.editor;
    void identity;
    // @ts-expect-error Editor success cannot expose acquisition material.
    void editor.result.acquisition;
  }
  const acknowledgement = await backend.execute({
    command: "profiles.list",
    id: "list",
    payload: {},
    version: HOST_PROTOCOL_VERSION,
  });
  if (acknowledgement.ok) {
    // @ts-expect-error Profile listing returns an acknowledgement; data is emitted as an event.
    void acknowledgement.result.editor;
  }
}

export function assertDiscriminatedResponse(response: HostCommandResponse): void {
  if (response.ok && response.command === "recipes.import.preview") {
    void response.result.draft;
    // @ts-expect-error Command narrowing excludes the export document result.
    void response.result.document;
  }
}
