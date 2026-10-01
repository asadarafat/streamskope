import type { HostCommand } from "../contracts";

export type RuleHostCommand = Extract<
  HostCommand,
  {
    readonly command:
      | "rules.create"
      | "rules.delete"
      | "rules.evaluate"
      | "rules.list"
      | "rules.update"
      | "rules.validate";
  }
>;

export function ruleOperation(command: RuleHostCommand): string {
  switch (command.command) {
    case "rules.create":
      return "Create rule";
    case "rules.delete":
      return "Delete rule";
    case "rules.evaluate":
      return command.payload.scope === "single" ? "Evaluate rule" : "Evaluate rules";
    case "rules.list":
      return "Load rules";
    case "rules.update":
      return "Update rule";
    case "rules.validate":
      return "Validate rule";
  }
}

export function ruleActivityObject(command: RuleHostCommand): string {
  switch (command.command) {
    case "rules.create":
    case "rules.validate":
      return command.payload.rule.name;
    case "rules.delete":
      return command.payload.name;
    case "rules.evaluate":
      return command.payload.scope === "single" ? command.payload.rule.name : "Kafka rule catalog";
    case "rules.list":
      return "Kafka rules";
    case "rules.update":
      return command.payload.rule.name;
  }
}
