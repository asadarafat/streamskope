import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { fileURLToPath } from "node:url";

import ts from "typescript";
import { expect, it } from "vitest";

const root = fileURLToPath(new URL("../../", import.meta.url));

it("keeps decomposed facade dispatch exhaustive at the HostCommand boundary", () => {
  const source = ts.createSourceFile(
    "cluster-service-facades.ts",
    readFileSync(resolve(root, "src/features/kafka/facade/cluster-service-facades.ts"), "utf8"),
    ts.ScriptTarget.Latest,
    true,
    ts.ScriptKind.TS,
  );
  const facade = source.statements.find(
    (node): node is ts.ClassDeclaration =>
      ts.isClassDeclaration(node) && node.name?.text === "ClusterServiceFacadeController",
  );
  const execute = facade?.members.find(
    (member): member is ts.MethodDeclaration =>
      ts.isMethodDeclaration(member) &&
      member.name !== undefined &&
      member.name.getText(source) === "execute",
  );
  if (execute === undefined) throw new Error("KafkaBackendFacade.execute is missing.");
  const assertions: ts.SatisfiesExpression[] = [];
  const visit = (node: ts.Node): void => {
    if (
      ts.isSatisfiesExpression(node) &&
      node.expression.getText(source) === "command" &&
      node.type.kind === ts.SyntaxKind.NeverKeyword
    ) {
      assertions.push(node);
    }
    ts.forEachChild(node, visit);
  };
  visit(execute);

  expect(assertions).toHaveLength(1);
  expect(readFileSync(resolve(root, "src/features/kafka/facade/facade.ts"), "utf8")).toContain(
    "default:\n        return this.clusterServices.execute(command, correlationId);",
  );
});
