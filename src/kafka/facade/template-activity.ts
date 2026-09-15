import type { HostCommand } from "../contracts";

export type TemplateHostCommand = Extract<
  HostCommand,
  {
    readonly command:
      | "templates.create"
      | "templates.delete"
      | "templates.list"
      | "templates.select"
      | "templates.update";
  }
>;

export function templateOperation(command: TemplateHostCommand): string {
  switch (command.command) {
    case "templates.create":
      return "Create template";
    case "templates.delete":
      return "Delete template";
    case "templates.list":
      return "Load templates";
    case "templates.select":
      return "Select template";
    case "templates.update":
      return "Update template";
  }
}

export function templateActivityObject(command: TemplateHostCommand): string {
  return command.command === "templates.list"
    ? "Connection templates"
    : `${command.payload.catalog} · ${command.payload.name}`;
}
