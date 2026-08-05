---
type: Rust Function
title: enforce_retention
resource: LPE-CT/src/reporting.rs#L511-L520
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/reporting/prune_transport_audit_jsonl
  - functions/LPE-CT/src/reporting/prune_digest_reports
  - functions/LPE-CT/src/reporting/prune_retained_rows_from_db
  called_by:
  - functions/LPE-CT/src/main
  - functions/LPE-CT/src/run_reporting_scheduler
---

# Signature

`pub(crate) async fn enforce_retention( spool_dir: &Path, config: &RuntimeConfig, settings: &ReportingSettings, ) -> Result<()>`

# Calls

- [prune_transport_audit_jsonl](../../../../functions/LPE-CT/src/reporting/prune_transport_audit_jsonl.md)
- [prune_digest_reports](../../../../functions/LPE-CT/src/reporting/prune_digest_reports.md)
- [prune_retained_rows_from_db](../../../../functions/LPE-CT/src/reporting/prune_retained_rows_from_db.md)

# Called by

- [main](../../../../functions/LPE-CT/src/main.md)
- [run_reporting_scheduler](../../../../functions/LPE-CT/src/run_reporting_scheduler.md)