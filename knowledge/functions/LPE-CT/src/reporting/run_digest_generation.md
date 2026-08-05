---
type: Rust Function
title: run_digest_generation
resource: LPE-CT/src/reporting.rs#L339-L395
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/reporting/normalize_reporting_settings
  - functions/LPE-CT/src/reporting/prune_digest_reports
  - functions/LPE-CT/src/smtp/quarantine/list_quarantine_items_from_spool
  - functions/LPE-CT/src/reporting/filter_quarantine_for_domain
  - functions/LPE-CT/src/reporting/build_digest_report
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/LPE-CT/src/reporting/filter_quarantine_for_mailbox
  - functions/LPE-CT/src/reporting/timestamp_from_now
  called_by:
  - functions/LPE-CT/src/http_routes/run_digest_reports
  - functions/LPE-CT/src/reporting/run_due_digest_generation
---

# Signature

`pub(crate) fn run_digest_generation( spool_dir: &Path, settings: &mut ReportingSettings, ) -> Result<Vec<DigestReportSummary>>`

# Calls

- [normalize_reporting_settings](../../../../functions/LPE-CT/src/reporting/normalize_reporting_settings.md)
- [prune_digest_reports](../../../../functions/LPE-CT/src/reporting/prune_digest_reports.md)
- [list_quarantine_items_from_spool](../../../../functions/LPE-CT/src/smtp/quarantine/list_quarantine_items_from_spool.md)
- [filter_quarantine_for_domain](../../../../functions/LPE-CT/src/reporting/filter_quarantine_for_domain.md)
- [build_digest_report](../../../../functions/LPE-CT/src/reporting/build_digest_report.md)
- [push](../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [filter_quarantine_for_mailbox](../../../../functions/LPE-CT/src/reporting/filter_quarantine_for_mailbox.md)
- [timestamp_from_now](../../../../functions/LPE-CT/src/reporting/timestamp_from_now.md)

# Called by

- [run_digest_reports](../../../../functions/LPE-CT/src/http_routes/run_digest_reports.md)
- [run_due_digest_generation](../../../../functions/LPE-CT/src/reporting/run_due_digest_generation.md)