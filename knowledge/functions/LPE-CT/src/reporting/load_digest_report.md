---
type: Rust Function
title: load_digest_report
resource: LPE-CT/src/reporting.rs#L420-L429
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/reporting/digest_report_dir
  - functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str
  called_by:
  - functions/LPE-CT/src/http_routes/digest_report_details
  - functions/LPE-CT/src/reporting/tests/digest_report_enriches_status_and_domain_counts_and_persists_artifact
---

# Signature

`pub(crate) fn load_digest_report( spool_dir: &Path, report_id: &str, ) -> Result<Option<DigestReportDetails>>`

# Calls

- [digest_report_dir](../../../../functions/LPE-CT/src/reporting/digest_report_dir.md)
- [from_str](../../../../functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str.md)

# Called by

- [digest_report_details](../../../../functions/LPE-CT/src/http_routes/digest_report_details.md)
- [digest_report_enriches_status_and_domain_counts_and_persists_artifact](../../../../functions/LPE-CT/src/reporting/tests/digest_report_enriches_status_and_domain_counts_and_persists_artifact.md)