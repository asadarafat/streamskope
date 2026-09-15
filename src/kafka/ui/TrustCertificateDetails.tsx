import { Box, Stack, Typography } from "@mui/material";

import type { RemoteTrustAcquisitionSummary } from "../contracts";
import { StudioDetailRow } from "../../ui/StudioPropertyRow";
import { StudioAlert } from "../../ui/controls";

import { formatUtcTimestamp } from "./timestamp-presentation";

export function TrustCertificateDetails({
  acquisition,
}: {
  readonly acquisition: RemoteTrustAcquisitionSummary;
}): React.JSX.Element {
  const material = acquisition.material;
  const evidence = material?.evidence;
  return (
    <Stack spacing={1}>
      <Box component="dl" sx={{ m: 0 }}>
        <StudioDetailRow
          label="Source"
          value={
            acquisition.recipe === undefined
              ? "SSH legacy acquisition"
              : acquisition.recipe.source === "https"
                ? `HTTPS ${acquisition.target.origin ?? ""}`
                : `SSH ${acquisition.recipe.source}`
          }
        />
        <StudioDetailRow label="Template" value={material?.templateName ?? "Unavailable"} />
        {acquisition.recipe === undefined ? null : (
          <StudioDetailRow label="Pinned revision" value={String(acquisition.recipe.revision)} />
        )}
        {acquisition.createdAt === undefined ? null : (
          <StudioDetailRow label="Acquired at" value={formatUtcTimestamp(acquisition.createdAt)} />
        )}
        <StudioDetailRow label="Expires" value={formatUtcTimestamp(acquisition.expiresAt)} />
      </Box>
      <Typography variant="body2">
        Kafka connectivity has not been tested by this acquisition. Certificate evidence does not
        confirm broker hostname validation or authentication.
      </Typography>
      {evidence === undefined ? (
        <Typography color="text.secondary" variant="body2">
          Certificate evidence unavailable.
        </Typography>
      ) : (
        <>
          <Typography component="h4" variant="subtitle2">
            Certificate evidence
          </Typography>
          {material?.expiredCertificates ? (
            <StudioAlert severity="warning">
              Expired certificates are present in the inspected evidence.
            </StudioAlert>
          ) : null}
          {material?.notYetValidCertificates ? (
            <StudioAlert severity="warning">
              Certificates that are not yet valid are present in the inspected evidence.
            </StudioAlert>
          ) : null}
          {evidence.truncated ? (
            <StudioAlert severity="warning">
              Showing {evidence.certificates.length} of {evidence.count} certificates.{" "}
              {evidence.validity === undefined
                ? "The remaining certificate dates are not represented in this evidence."
                : "Validity warnings include every certificate in the bundle."}
            </StudioAlert>
          ) : null}
          {evidence.certificates.map((certificate, index) => (
            <Box key={`${certificate.fingerprint}-${index}`}>
              <Typography component="h5" variant="body2">
                Certificate {index + 1}
              </Typography>
              <Box component="dl" sx={{ m: 0 }}>
                <StudioDetailRow label="Subject" value={certificate.subject || "Not provided"} />
                <StudioDetailRow label="Issuer" value={certificate.issuer || "Not provided"} />
                <StudioDetailRow
                  label="Valid from"
                  value={formatUtcTimestamp(certificate.validFrom)}
                />
                <StudioDetailRow
                  label="Valid until"
                  value={formatUtcTimestamp(certificate.validTo)}
                />
                <StudioDetailRow label="SHA-256 fingerprint" value={certificate.fingerprint} />
              </Box>
              {certificate.truncated ? (
                <Typography variant="body2" color="text.secondary">
                  Certificate names are truncated.
                </Typography>
              ) : null}
            </Box>
          ))}
        </>
      )}
    </Stack>
  );
}
