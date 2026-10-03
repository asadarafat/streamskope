import { useId } from "react";
import { Box, useTheme } from "@mui/material";

import type { RelationshipGraph } from "../contracts/relationships";

export function RelationshipGraphView({
  graph,
  selected,
  onSelect,
}: {
  readonly graph: RelationshipGraph;
  readonly selected: string;
  readonly onSelect: (id: string) => void;
}): React.JSX.Element {
  const theme = useTheme(),
    marker = useId();
  const kinds = ["schema-id", "schema", "topic", "group", "connector"];
  const counts = new Map<string, number>();
  const positions = new Map(
    graph.nodes.map((node) => {
      const row = counts.get(node.kind) ?? 0;
      counts.set(node.kind, row + 1);
      return [node.id, { x: 20 + kinds.indexOf(node.kind) * 220, y: 64 + row * 72 }];
    }),
  );
  const height = 86 + Math.max(1, ...counts.values()) * 72;
  return (
    <Box
      sx={{
        overflow: "auto",
        maxHeight: 500,
        flexShrink: 0,
        border: 1,
        borderColor: "divider",
        borderRadius: 1,
      }}
    >
      <svg
        width="1120"
        height={height}
        role="group"
        aria-label="Observed relationships graph. Select a node to filter the evidence table."
      >
        <defs>
          <marker
            id={marker}
            viewBox="0 0 10 10"
            refX="9"
            refY="5"
            markerWidth="7"
            markerHeight="7"
            orient="auto-start-reverse"
          >
            <path d="M 0 0 L 10 5 L 0 10 z" fill={theme.palette.text.secondary} />
          </marker>
        </defs>
        {kinds.map((kind, i) => (
          <text
            key={kind}
            x={20 + i * 220}
            y={20}
            fill={theme.palette.text.secondary}
            fontSize="13"
          >
            {kind === "schema-id"
              ? "Framing IDs"
              : kind === "schema"
                ? "Subject versions"
                : kind === "topic"
                  ? "Topics"
                  : kind === "group"
                    ? "Consumer groups"
                    : "Connectors"}
          </text>
        ))}
        {graph.edges.map((edge, i) => {
          const a = positions.get(edge.from)!,
            b = positions.get(edge.to)!;
          return (
            <path
              key={i}
              d={edgePath(a, b, i)}
              stroke={theme.palette.text.secondary}
              opacity={!selected || [edge.from, edge.to].includes(selected) ? 0.8 : 0.15}
              strokeWidth="1.5"
              strokeDasharray={
                edge.evidence === "inferred"
                  ? "2 4"
                  : edge.evidence === "declared"
                    ? "8 4"
                    : undefined
              }
              fill="none"
              markerEnd={`url(#${marker})`}
            >
              <title>{`${edge.relation}: ${edge.evidence}; ${edge.source}; ${edge.detail}`}</title>
            </path>
          );
        })}
        {graph.nodes.map((node) => {
          const p = positions.get(node.id)!,
            label = node.label + (node.version === null ? "" : ` v${node.version}`);
          return (
            <g
              key={node.id}
              role="button"
              tabIndex={0}
              aria-label={`Show relationships for ${label}`}
              aria-pressed={selected === node.id}
              onClick={() => onSelect(node.id)}
              onKeyDown={(e) => {
                if (e.key === "Enter" || e.key === " ") {
                  e.preventDefault();
                  onSelect(node.id);
                }
              }}
              style={{ cursor: "pointer" }}
            >
              <rect
                x={p.x}
                y={p.y}
                width="180"
                height="46"
                rx="5"
                fill={
                  selected === node.id
                    ? theme.palette.action.selected
                    : theme.palette.background.paper
                }
                stroke={selected === node.id ? theme.palette.primary.main : theme.palette.divider}
                strokeWidth="2"
              />
              <text x={p.x + 8} y={p.y + 28} fill={theme.palette.text.primary} fontSize="12">
                {label.length > 23 ? label.slice(0, 22) + "…" : label}
              </text>
              <title>{label}</title>
            </g>
          );
        })}
      </svg>
    </Box>
  );
}

function edgePath(a: { x: number; y: number }, b: { x: number; y: number }, index: number): string {
  if (a.x === b.x && a.y === b.y)
    return `M ${a.x + 180} ${a.y + 15} C ${a.x + 215} ${a.y - 20}, ${a.x + 110} ${a.y - 25}, ${a.x + 110} ${a.y}`;
  // Route through column gaps, never through an unrelated node. Long edges use
  // the corridor above the first row so they cannot look like two shorter links.
  if (a.x === b.x) return `M ${a.x + 180} ${a.y + 23} H ${a.x + 194} V ${b.y + 23} H ${b.x + 180}`;
  const right = b.x > a.x,
    from = a.x + (right ? 180 : 0),
    to = b.x + (right ? 0 : 180),
    y1 = a.y + 23,
    y2 = b.y + 23;
  if (Math.abs(b.x - a.x) === 220) return `M ${from} ${y1} H ${(from + to) / 2} V ${y2} H ${to}`;
  const x1 = from + (right ? 12 : -12),
    x2 = to + (right ? -12 : 12),
    corridor = 38 + (index % 4) * 4;
  return `M ${from} ${y1} H ${x1} V ${corridor} H ${x2} V ${y2} H ${to}`;
}
