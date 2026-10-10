import {
  parseReplayPatchJson,
  replayPatchSegments,
  type ReplayJsonPatch,
} from "../contracts/structured-replay";

/** Edits a detached projection; paths never invoke prototypes or create missing parents. */
export function applyReplayJsonPatches(json: string, patches: readonly ReplayJsonPatch[]): unknown {
  let value: unknown = JSON.parse(json);
  const index = (name: string, length: number): number => {
    if (!/^(?:0|[1-9]\d*)$/u.test(name) || name.length > 9 || Number(name) >= length)
      throw new Error("Patch array position is unavailable.");
    return Number(name);
  };
  for (const patch of patches) {
    const segments = replayPatchSegments(patch.path);
    if (!segments.length) {
      if (patch.op !== "set") throw new Error("Cannot remove the entire record.");
      value = parseReplayPatchJson(patch.json);
      continue;
    }
    let parent = value;
    for (const name of segments.slice(0, -1)) {
      if (!parent || typeof parent !== "object" || !Object.hasOwn(parent, name))
        throw new Error("Patch parent is unavailable.");
      parent = Array.isArray(parent)
        ? parent[index(name, parent.length)]
        : (parent as Record<string, unknown>)[name];
    }
    const name = segments.at(-1)!;
    if (!parent || typeof parent !== "object") throw new Error("Patch parent is unavailable.");
    if (Array.isArray(parent)) {
      const position = index(name, parent.length);
      if (patch.op === "remove") parent.splice(position, 1);
      else parent[position] = parseReplayPatchJson(patch.json);
    } else {
      const object = parent as Record<string, unknown>;
      if (patch.op === "remove") {
        if (!Object.hasOwn(object, name)) throw new Error("Patch field is unavailable.");
        delete object[name];
      } else
        Object.defineProperty(object, name, {
          value: parseReplayPatchJson(patch.json),
          enumerable: true,
          configurable: true,
          writable: true,
        });
    }
  }
  return value;
}
