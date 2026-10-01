import { KAFKA_RULE_LIMITS } from "../contracts";

import type {
  CompiledKafkaRuleExpression,
  RuleCondition,
  RuleOperand,
  RuleOperator,
  RulePath,
  RulePathSegment,
} from "./rule-expression-types";

function boundedDiagnostic(position: number, detail: string): string {
  return `Position ${String(position)}: ${detail}`.slice(0, KAFKA_RULE_LIMITS.diagnosticCharacters);
}

export class KafkaRuleExpressionError extends Error {
  readonly position: number;

  constructor(position: number, detail: string) {
    super(boundedDiagnostic(position, detail));
    this.name = "KafkaRuleExpressionError";
    this.position = position;
  }
}

class RuleExpressionParser {
  private conditionCount = 0;
  private index = 0;

  constructor(private readonly source: string) {}

  parse(): CompiledKafkaRuleExpression {
    if (this.source.length === 0) {
      this.fail("Expression is required.");
    }
    if (this.source.length > KAFKA_RULE_LIMITS.expressionCharacters) {
      this.fail(`Expression exceeds ${String(KAFKA_RULE_LIMITS.expressionCharacters)} characters.`);
    }
    const expression = this.parseOr(0);
    this.skipWhitespace();
    if (!this.atEnd()) {
      this.fail(`Unexpected token ${JSON.stringify(this.source[this.index])}.`);
    }
    return expression;
  }

  private parseOr(depth: number): CompiledKafkaRuleExpression {
    let expression = this.parseAnd(depth);
    for (;;) {
      this.skipWhitespace();
      if (!this.consume("||")) {
        return expression;
      }
      expression = {
        kind: "or",
        left: expression,
        right: this.parseAnd(depth),
      };
    }
  }

  private parseAnd(depth: number): CompiledKafkaRuleExpression {
    let expression = this.parsePrimary(depth);
    for (;;) {
      this.skipWhitespace();
      if (!this.consume("&&")) {
        return expression;
      }
      expression = {
        kind: "and",
        left: expression,
        right: this.parsePrimary(depth),
      };
    }
  }

  private parsePrimary(depth: number): CompiledKafkaRuleExpression {
    this.skipWhitespace();
    if (!this.consume("(")) {
      return this.parseCondition();
    }
    if (depth + 1 > KAFKA_RULE_LIMITS.groupingDepth) {
      this.fail(`Grouping exceeds depth ${String(KAFKA_RULE_LIMITS.groupingDepth)}.`);
    }
    const expression = this.parseOr(depth + 1);
    this.skipWhitespace();
    this.expect(")", "Expected a closing parenthesis.");
    return expression;
  }

  private parseCondition(): RuleCondition {
    this.countCondition();
    const path = this.parsePath("root");
    this.skipWhitespace();
    if (this.conditionEnded()) {
      return { kind: "condition", operator: "exists", path };
    }
    const operator = this.parseOperator();
    if (operator === "exists") {
      return { kind: "condition", operator, path };
    }
    return {
      kind: "condition",
      operand: this.parseOperand(operator),
      operator,
      path,
    };
  }

  private parseFilterCondition(): RuleCondition {
    this.countCondition();
    const path = this.parsePath("current");
    this.skipWhitespace();
    if (this.peek() === ")") {
      return { kind: "condition", operator: "exists", path };
    }
    const operator = this.parseOperator();
    if (operator === "exists") {
      return { kind: "condition", operator, path };
    }
    return {
      kind: "condition",
      operand: this.parseOperand(operator),
      operator,
      path,
    };
  }

  private parsePath(expectedOrigin: RulePath["origin"]): RulePath {
    this.skipWhitespace();
    const originCharacter = expectedOrigin === "root" ? "$" : "@";
    this.expect(originCharacter, `Path must start with ${originCharacter}.`);
    const segments: RulePathSegment[] = [];
    for (;;) {
      if (this.startsWith("..")) {
        this.index += 2;
        const name = this.parseIdentifier("Expected a property after recursive descent.");
        segments.push({ kind: "recursive-property", name });
      } else if (this.consume(".")) {
        if (this.consume("*")) {
          segments.push({ kind: "wildcard" });
        } else {
          segments.push({
            kind: "property",
            name: this.parseIdentifier("Expected a property after '.'."),
          });
        }
      } else if (this.consume("[")) {
        segments.push(this.parseBracketSegment());
      } else {
        break;
      }
      if (segments.length > KAFKA_RULE_LIMITS.pathSegments) {
        this.fail(`Path exceeds ${String(KAFKA_RULE_LIMITS.pathSegments)} segments.`);
      }
    }
    return { origin: expectedOrigin, segments };
  }

  private parseBracketSegment(): RulePathSegment {
    this.skipWhitespace();
    if (this.consume("*")) {
      this.skipWhitespace();
      this.expect("]", "Expected ']' after wildcard.");
      return { kind: "wildcard" };
    }
    if (this.consume("?(")) {
      const condition = this.parseFilterCondition();
      this.skipWhitespace();
      this.expect(")", "Expected ')' after array filter.");
      this.skipWhitespace();
      this.expect("]", "Expected ']' after array filter.");
      return { condition, kind: "filter" };
    }
    const current = this.peek();
    if (current === '"' || current === "'") {
      const name = this.parseQuotedString();
      this.skipWhitespace();
      this.expect("]", "Expected ']' after property.");
      return { kind: "property", name };
    }
    const start = this.index;
    while (this.isDigit(this.peek())) {
      this.index += 1;
    }
    if (start !== this.index) {
      const index = Number(this.source.slice(start, this.index));
      if (!Number.isSafeInteger(index)) {
        this.fail("Array index must be a safe non-negative integer.", start);
      }
      this.skipWhitespace();
      this.expect("]", "Expected ']' after array index.");
      return { index, kind: "index" };
    }
    this.fail("Expected a quoted property, non-negative index, wildcard, or array filter.");
  }

  private parseOperator(): RuleOperator {
    this.skipWhitespace();
    const symbolic: ReadonlyArray<readonly [string, RuleOperator]> = [
      [">=", "greater-than-or-equal"],
      ["<=", "less-than-or-equal"],
      ["==", "equals"],
      ["!=", "not-equals"],
      [">", "greater-than"],
      ["<", "less-than"],
    ];
    for (const [token, operator] of symbolic) {
      if (this.consume(token)) {
        return operator;
      }
    }
    const named: ReadonlyArray<readonly [string, RuleOperator]> = [
      ["contains", "contains"],
      ["matches", "matches"],
      ["exists", "exists"],
    ];
    for (const [token, operator] of named) {
      if (this.consumeWord(token)) {
        return operator;
      }
    }
    this.fail("Expected one of ==, !=, >, >=, <, <=, contains, matches, or exists.");
  }

  private parseOperand(operator: RuleOperator): RuleOperand {
    this.skipWhitespace();
    if (this.peek() === "/") {
      if (operator !== "matches") {
        this.fail("Regular-expression literals are valid only with matches.");
      }
      return this.parseRegexLiteral();
    }
    if (this.peek() === '"' || this.peek() === "'") {
      const value = this.parseQuotedString();
      return operator === "matches" ? this.safeRegex(value, "") : { kind: "scalar", value };
    }
    for (const [token, value] of [
      ["true", true],
      ["false", false],
      ["null", null],
    ] as const) {
      if (this.consumeWord(token)) {
        if (operator === "matches") {
          this.fail("matches requires a string or regular-expression operand.");
        }
        return { kind: "scalar", value };
      }
    }
    const numeric = this.parseNumber();
    if (numeric !== undefined) {
      if (operator === "matches" || operator === "contains") {
        this.fail(`${operator} requires a string operand.`);
      }
      return { kind: "scalar", value: numeric };
    }
    this.fail("Expected a JSON scalar or safe regular-expression literal.");
  }

  private parseNumber(): number | undefined {
    const match = this.source
      .slice(this.index)
      .match(/^-?(?:0|[1-9]\d*)(?:\.\d+)?(?:[eE][+-]?\d+)?/);
    if (match === null) {
      return undefined;
    }
    const value = Number(match[0]);
    if (!Number.isFinite(value)) {
      this.fail("Numeric operand must be finite.");
    }
    this.index += match[0].length;
    return value;
  }

  private parseRegexLiteral(): RuleOperand {
    this.expect("/", "Expected a regular-expression literal.");
    const start = this.index;
    let escaped = false;
    let inClass = false;
    while (!this.atEnd()) {
      const character = this.source[this.index];
      if (escaped) {
        escaped = false;
        this.index += 1;
        continue;
      }
      if (character === "\\") {
        escaped = true;
        this.index += 1;
        continue;
      }
      if (character === "[") {
        inClass = true;
      } else if (character === "]") {
        inClass = false;
      } else if (character === "/" && !inClass) {
        const source = this.source.slice(start, this.index);
        this.index += 1;
        const flagsStart = this.index;
        while (/[A-Za-z]/.test(this.peek() ?? "")) {
          this.index += 1;
        }
        return this.safeRegex(source, this.source.slice(flagsStart, this.index));
      }
      this.index += 1;
    }
    this.fail("Unterminated regular-expression literal.");
  }

  private safeRegex(source: string, flags: string): RuleOperand {
    if (source.length > KAFKA_RULE_LIMITS.regexCharacters) {
      this.fail(
        `Regular expression exceeds ${String(KAFKA_RULE_LIMITS.regexCharacters)} characters.`,
      );
    }
    if (!/^(?!.*(.).*\1)[imsu]*$/.test(flags)) {
      this.fail("Regular-expression flags may contain i, m, s, or u at most once.");
    }
    let escaped = false;
    let inClass = false;
    for (let offset = 0; offset < source.length; offset += 1) {
      const character = source[offset];
      if (escaped) {
        if (/[1-9k]/.test(character ?? "")) {
          this.fail("Regular-expression backreferences are not supported.");
        }
        escaped = false;
        continue;
      }
      if (character === "\\") {
        escaped = true;
        continue;
      }
      if (character === "[") {
        inClass = true;
        continue;
      }
      if (character === "]") {
        inClass = false;
        continue;
      }
      if (inClass) {
        continue;
      }
      if (character === "*" || character === "+" || character === "{") {
        this.fail("Unbounded regular-expression repetition is not supported.");
      }
      if (character === "(" && source.slice(offset, offset + 3) !== "(?:") {
        this.fail("Only non-capturing regular-expression groups are supported.");
      }
      if (character === "?" && !(source[offset - 1] === "(" && source[offset + 1] === ":")) {
        this.fail("Regular-expression lookarounds and optional repetition are not supported.");
      }
    }
    try {
      void new RegExp(source, flags);
    } catch {
      this.fail("Regular expression is invalid.");
    }
    return { flags, kind: "regex", source };
  }

  private parseQuotedString(): string {
    const quote = this.peek();
    if (quote !== '"' && quote !== "'") {
      this.fail("Expected a quoted string.");
    }
    const start = this.index;
    this.index += 1;
    let value = "";
    while (!this.atEnd()) {
      const character = this.source[this.index];
      this.index += 1;
      if (character === quote) {
        if (quote === '"') {
          try {
            return JSON.parse(this.source.slice(start, this.index)) as string;
          } catch {
            this.fail("String literal contains an invalid escape.", start);
          }
        }
        return value;
      }
      if (character !== "\\") {
        value += character;
        continue;
      }
      if (this.atEnd()) {
        this.fail("Unterminated string escape.", start);
      }
      const escaped = this.source[this.index];
      this.index += 1;
      const escapes: Readonly<Record<string, string>> = {
        "'": "'",
        '"': '"',
        "\\": "\\",
        b: "\b",
        f: "\f",
        n: "\n",
        r: "\r",
        t: "\t",
      };
      const replacement = escapes[escaped ?? ""];
      if (replacement === undefined) {
        this.fail("String literal contains an invalid escape.", start);
      }
      value += replacement;
    }
    this.fail("Unterminated string literal.", start);
  }

  private parseIdentifier(message: string): string {
    const start = this.index;
    const first = this.peek();
    if (first === undefined || !/[A-Za-z_]/.test(first)) {
      this.fail(message);
    }
    this.index += 1;
    while (/[A-Za-z0-9_-]/.test(this.peek() ?? "")) {
      this.index += 1;
    }
    return this.source.slice(start, this.index);
  }

  private conditionEnded(): boolean {
    return this.atEnd() || this.peek() === ")" || this.startsWith("&&") || this.startsWith("||");
  }

  private consume(token: string): boolean {
    if (!this.startsWith(token)) {
      return false;
    }
    this.index += token.length;
    return true;
  }

  private countCondition(): void {
    this.conditionCount += 1;
    if (this.conditionCount > KAFKA_RULE_LIMITS.conditions) {
      this.fail(`Expression exceeds ${String(KAFKA_RULE_LIMITS.conditions)} conditions.`);
    }
  }

  private consumeWord(token: string): boolean {
    if (!this.startsWith(token)) {
      return false;
    }
    const next = this.source[this.index + token.length];
    if (next !== undefined && /[A-Za-z0-9_-]/.test(next)) {
      return false;
    }
    this.index += token.length;
    return true;
  }

  private expect(token: string, detail: string): void {
    if (!this.consume(token)) {
      this.fail(detail);
    }
  }

  private fail(detail: string, position = this.index): never {
    throw new KafkaRuleExpressionError(position, detail);
  }

  private isDigit(value: string | undefined): boolean {
    return value !== undefined && /\d/.test(value);
  }

  private peek(): string | undefined {
    return this.source[this.index];
  }

  private skipWhitespace(): void {
    while (/\s/.test(this.peek() ?? "")) {
      this.index += 1;
    }
  }

  private startsWith(token: string): boolean {
    return this.source.startsWith(token, this.index);
  }

  private atEnd(): boolean {
    return this.index >= this.source.length;
  }
}

export function compileKafkaRuleExpression(expression: string): CompiledKafkaRuleExpression {
  return new RuleExpressionParser(expression).parse();
}

export function validateKafkaRuleExpression(expression: string): {
  readonly diagnostic?: string;
  readonly valid: boolean;
} {
  try {
    compileKafkaRuleExpression(expression);
    return { valid: true };
  } catch (error) {
    const diagnostic =
      error instanceof KafkaRuleExpressionError
        ? error.message
        : boundedDiagnostic(0, "Expression validation failed.");
    return { diagnostic, valid: false };
  }
}
