---
type: Rust Function
title: build_digest_report
resource: LPE-CT/src/reporting.rs#L1003-L1057
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/reporting/ensure_digest_dir
  - functions/LPE-CT/src/reporting/render_digest_content
  - functions/LPE-CT/src/reporting/summarize_digest_counts
  - functions/LPE-CT/src/reporting/enrich_digest_detail
  - functions/LPE-CT/src/reporting/digest_report_dir
  called_by:
  - functions/LPE-CT/src/reporting/run_digest_generation
  - functions/LPE-CT/src/reporting/tests/digest_report_enriches_status_and_domain_counts_and_persists_artifact
---

# Signature

`fn build_digest_report( spool_dir: &Path, generated_at: &str, scope: &str, scope_label: &str, recipient: &str, items: Vec<QuarantineSummary>, ) -> Result<DigestReportDetails>`

# Calls

- [ensure_digest_dir](../../../../functions/LPE-CT/src/reporting/ensure_digest_dir.md)
- [render_digest_content](../../../../functions/LPE-CT/src/reporting/render_digest_content.md)
- [summarize_digest_counts](../../../../functions/LPE-CT/src/reporting/summarize_digest_counts.md)
- [enrich_digest_detail](../../../../functions/LPE-CT/src/reporting/enrich_digest_detail.md)
- [digest_report_dir](../../../../functions/LPE-CT/src/reporting/digest_report_dir.md)

# Called by

- [run_digest_generation](../../../../functions/LPE-CT/src/reporting/run_digest_generation.md)
- [digest_report_enriches_status_and_domain_counts_and_persists_artifact](../../../../functions/LPE-CT/src/reporting/tests/digest_report_enriches_status_and_domain_counts_and_persists_artifact.md)