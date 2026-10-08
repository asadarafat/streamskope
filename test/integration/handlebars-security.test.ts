import { createRequire } from "node:module";

import { afterEach, describe, expect, it, vi } from "vitest";

const rootRequire = createRequire(import.meta.url);
const consumers = ["eslint-plugin-boundaries", "@boundaries/elements"];

function consumerEngine(consumer: string): typeof import("handlebars") {
  const load = createRequire(rootRequire.resolve(consumer));
  const engine = load("handlebars") as typeof import("handlebars");
  return engine.create();
}

afterEach(() => vi.unstubAllGlobals());

describe.each(consumers)("Handlebars resolved by %s", (consumer) => {
  it("preserves ordinary escaped template rendering", () => {
    const engine = consumerEngine(consumer);
    expect(engine.compile("{{name}} / {{topic}}")({ name: "a&b", topic: "orders" })).toBe(
      "a&amp;b / orders",
    );
  });

  it.each(["compile", "precompile"] as const)(
    "rejects injected program metadata through %s before code can execute",
    (operation) => {
      const engine = consumerEngine(consumer);
      let executed = false;
      vi.stubGlobal("__streamSkopeHandlebarsProbe", () => {
        executed = true;
        return 0;
      });
      const ast = engine.parse("{{#probe}}ok{{/probe}}");
      // Malformed JSON-shaped AST input bypassed the previous type validation.
      const block = ast.body[0] as unknown as { program: { blockParams: unknown } };
      block.program.blockParams = { length: "globalThis.__streamSkopeHandlebarsProbe()" };
      expect(() => {
        if (operation === "compile") engine.compile(ast)({});
        else engine.precompile(ast);
      }).toThrow();
      expect(executed).toBe(false);
    },
  );

  it("keeps the forbidden constructor inaccessible on a function prototype", () => {
    const engine = consumerEngine(consumer);
    const render = engine.compile('{{lookup (lookup fn "__proto__") "constructor"}}');
    expect(render({ fn: () => undefined }, { allowProtoMethodsByDefault: true })).toBe("");
  });
});
