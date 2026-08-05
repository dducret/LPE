---
type: Rust Function
title: snapshot
resource: LPE-CT/src/reporting.rs#L317-L325
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/reporting/list_recent_digest_reports
  called_by:
  - functions/LPE-CT/src/http_routes/reporting_snapshot
  - functions/LPE-CT/src/http_routes/update_reporting
---

# Signature

`pub(crate) fn snapshot( spool_dir: &Path, settings: &ReportingSettings, ) -> Result<ReportingSnapshot>`

# Calls

- [list_recent_digest_reports](../../../../functions/LPE-CT/src/reporting/list_recent_digest_reports.md)

# Called by

- [reporting_snapshot](../../../../functions/LPE-CT/src/http_routes/reporting_snapshot.md)
- [update_reporting](../../../../functions/LPE-CT/src/http_routes/update_reporting.md)