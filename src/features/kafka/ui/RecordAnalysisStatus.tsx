import { Stack, Typography } from "@mui/material";

import { StudioButton as Button } from "../../../platform/ui/controls";

import { analysisCountLabel } from "./record-analysis-presentation";
import type { RecordAnalysisController } from "./use-record-analysis";

export function RecordAnalysisStatus({
  controller,
  onOpen,
}: {
  readonly controller: RecordAnalysisController;
  readonly onOpen: () => void;
}): React.JSX.Element | null {
  const operation = controller.snapshot?.operation;
  if (!operation && !controller.error && !controller.uncertainStart) return null;
  const active = operation && ["preparing", "reading", "stopping"].includes(operation.state);
  return (
    <Stack
      role="region"
      aria-label="Range analysis"
      direction="row"
      useFlexGap
      sx={{
        alignItems: "center",
        flexWrap: "wrap",
        px: 2,
        py: 0.5,
        gap: 1,
        borderBottom: 1,
        borderColor: "divider",
      }}
    >
      <Typography variant="body2" role="status">
        {operation
          ? `${operation.input.topic} · ${analysisCountLabel(operation)}`
          : "Analysis status needs attention."}
      </Typography>
      <Button onClick={onOpen}>View analysis</Button>
      {active && (
        <Button
          disabled={controller.busy || operation.state === "stopping"}
          onClick={() => void controller.cancel()}
        >
          Cancel analysis
        </Button>
      )}
      {controller.error && (
        <Typography color="error" variant="caption">
          {controller.error}
        </Typography>
      )}
    </Stack>
  );
}
