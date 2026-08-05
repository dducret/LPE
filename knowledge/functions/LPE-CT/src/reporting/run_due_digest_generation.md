---
type: Rust Function
title: run_due_digest_generation
resource: LPE-CT/src/reporting.rs#L327-L337
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/reporting/normalize_reporting_settings
  - functions/LPE-CT/src/reporting/prune_digest_reports
  - functions/LPE-CT/src/reporting/digest_is_due
  - functions/LPE-CT/src/reporting/run_digest_generation
  called_by:
  - functions/LPE-CT/src/run_reporting_scheduler
---

# Signature

`pub(crate) fn run_due_digest_generation( spool_dir: &Path, settings: &mut ReportingSettings, ) -> Result<Vec<DigestReportSummary>>`

# Calls

- [normalize_reporting_settings](../../../../functions/LPE-CT/src/reporting/normalize_reporting_settings.md)
- [prune_digest_reports](../../../../functions/LPE-CT/src/reporting/prune_digest_reports.md)
- [digest_is_due](../../../../functions/LPE-CT/src/reporting/digest_is_due.md)
- [run_digest_generation](../../../../functions/LPE-CT/src/reporting/run_digest_generation.md)

# Called by

- [run_reporting_scheduler](../../../../functions/LPE-CT/src/run_reporting_scheduler.md)