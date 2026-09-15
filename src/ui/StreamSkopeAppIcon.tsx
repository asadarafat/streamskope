import Box from "@mui/material/Box";

import applicationIcon from "./assets/streamskope.svg";
import { streamSkopeGeometry } from "./studioTokens";

export function StreamSkopeAppIcon({
  size = streamSkopeGeometry.brandMarkSize,
}: {
  readonly size?: number;
}): React.JSX.Element {
  return (
    <Box
      alt=""
      aria-hidden="true"
      component="img"
      draggable={false}
      src={applicationIcon}
      sx={{ display: "block", flexShrink: 0, height: size, width: size }}
    />
  );
}
