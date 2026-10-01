import { useMemo, type ReactNode } from "react";
import { Box, Typography } from "@mui/material";

type DocumentationContainerKind = "code" | "emphasis" | "paragraph" | "reference";

type DocumentationNode =
  | {
      readonly children: readonly DocumentationNode[];
      readonly kind: DocumentationContainerKind;
    }
  | {
      readonly kind: "line-break";
    }
  | {
      readonly kind: "text";
      readonly value: string;
    };

interface DocumentationFrame {
  readonly children: DocumentationNode[];
  readonly kind: DocumentationContainerKind | "root";
  readonly opening: string;
}

const documentationTagPattern = /<[^<>]*>/gu;

function openingDocumentationKind(value: string): DocumentationContainerKind | null {
  if (/^<code\s*>$/iu.test(value)) {
    return "code";
  }
  if (/^<i\s*>$/iu.test(value)) {
    return "emphasis";
  }
  if (/^<p\s*>$/iu.test(value)) {
    return "paragraph";
  }
  return /^<a\s+href="#[a-z0-9_-]+"\s*>$/iu.test(value) ? "reference" : null;
}

function closingDocumentationKind(value: string): DocumentationContainerKind | null {
  if (/^<\/code\s*>$/iu.test(value)) {
    return "code";
  }
  if (/^<\/i\s*>$/iu.test(value)) {
    return "emphasis";
  }
  if (/^<\/p\s*>$/iu.test(value)) {
    return "paragraph";
  }
  return /^<\/a\s*>$/iu.test(value) ? "reference" : null;
}

function appendDocumentationText(frame: DocumentationFrame, value: string): void {
  if (value.length > 0) {
    frame.children.push({ kind: "text", value });
  }
}

function parseConfigurationDocumentation(value: string): readonly DocumentationNode[] {
  const frames: DocumentationFrame[] = [{ children: [], kind: "root", opening: "" }];
  let cursor = 0;

  for (const match of value.matchAll(documentationTagPattern)) {
    const index = match.index;
    const token = match[0];
    const current = frames.at(-1)!;
    appendDocumentationText(current, value.slice(cursor, index));

    const openingKind = openingDocumentationKind(token);
    const closingKind = closingDocumentationKind(token);
    if (openingKind !== null) {
      frames.push({ children: [], kind: openingKind, opening: token });
    } else if (closingKind !== null && frames.length > 1 && frames.at(-1)!.kind === closingKind) {
      const completed = frames.pop()!;
      frames.at(-1)!.children.push({
        children: completed.children,
        kind: closingKind,
      });
    } else if (/^<br\s*\/?>$/iu.test(token)) {
      current.children.push({ kind: "line-break" });
    } else {
      appendDocumentationText(current, token);
    }
    cursor = index + token.length;
  }

  appendDocumentationText(frames.at(-1)!, value.slice(cursor));
  while (frames.length > 1) {
    const incomplete = frames.pop()!;
    const parent = frames.at(-1)!;
    appendDocumentationText(parent, incomplete.opening);
    parent.children.push(...incomplete.children);
  }
  return frames[0]!.children;
}

function renderDocumentationNode(node: DocumentationNode, key: string): ReactNode {
  switch (node.kind) {
    case "text":
      return node.value;
    case "line-break":
      return <br key={key} />;
    case "code":
      return (
        <Typography
          component="code"
          key={key}
          sx={{ bgcolor: "action.hover", color: "text.primary", px: 0.25 }}
          variant="body2"
        >
          {node.children.map((child, index) => renderDocumentationNode(child, `${key}-${index}`))}
        </Typography>
      );
    case "emphasis":
      return (
        <Box component="em" key={key}>
          {node.children.map((child, index) => renderDocumentationNode(child, `${key}-${index}`))}
        </Box>
      );
    case "paragraph":
      return (
        <Box component="p" key={key} sx={{ mb: 0, mt: 0.75 }}>
          {node.children.map((child, index) => renderDocumentationNode(child, `${key}-${index}`))}
        </Box>
      );
    case "reference":
      return (
        <Box component="span" key={key}>
          {node.children.map((child, index) => renderDocumentationNode(child, `${key}-${index}`))}
        </Box>
      );
  }
}

export function KafkaConfigurationDocumentation({
  value,
}: {
  readonly value: string | null;
}): React.JSX.Element {
  const documentation = value ?? "No broker documentation is available.";
  const nodes = useMemo(() => parseConfigurationDocumentation(documentation), [documentation]);
  return (
    <Typography
      color="text.secondary"
      component="div"
      sx={{ overflowWrap: "anywhere" }}
      variant="caption"
    >
      {nodes.map((node, index) => renderDocumentationNode(node, `documentation-${index}`))}
    </Typography>
  );
}
