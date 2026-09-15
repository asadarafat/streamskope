export type NavigationView =
  "acls" | "consumer-groups" | "overview" | "profiles" | "schemas" | "topics" | "transforms";

export interface WorkbenchResourceDestination {
  readonly label: string;
  readonly value: NavigationView;
}

export interface WorkbenchResourceGroup {
  readonly items: readonly WorkbenchResourceDestination[];
  readonly label: string;
}

export const WORKBENCH_RESOURCE_GROUPS: readonly WorkbenchResourceGroup[] = Object.freeze([
  {
    items: [
      { label: "Connection Profiles", value: "profiles" },
      { label: "Overview", value: "overview" },
      { label: "Topics", value: "topics" },
      { label: "Consumer Groups", value: "consumer-groups" },
    ],
    label: "Explore",
  },
  {
    items: [
      { label: "Schema Registry", value: "schemas" },
      { label: "Transforms", value: "transforms" },
    ],
    label: "Governance",
  },
  {
    items: [{ label: "Access Control Lists", value: "acls" }],
    label: "Security",
  },
]);

const resourceLabels = new Map<NavigationView, string>(
  WORKBENCH_RESOURCE_GROUPS.flatMap((group) => group.items).map((item) => [item.value, item.label]),
);

export function navigationLabel(navigation: NavigationView): string {
  return resourceLabels.get(navigation) ?? navigation;
}

const resourceGroups = new Map<NavigationView, string>(
  WORKBENCH_RESOURCE_GROUPS.flatMap((group) =>
    group.items.map((item) => [item.value, group.label] as const),
  ),
);

export function navigationGroupLabel(navigation: NavigationView): string {
  return resourceGroups.get(navigation) ?? "Explore";
}

/**
 * Connection Profiles is the only useful resource before Kafka confirms a connection.
 * Keep this rule here so the sidebar, command palette, and workbench cannot drift.
 */
export function isNavigationAvailable(navigation: NavigationView, connected: boolean): boolean {
  return navigation === "profiles" || connected;
}
