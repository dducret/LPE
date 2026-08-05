---
type: Rust Function
title: list_recent_digest_reports
resource: LPE-CT/src/reporting.rs#L397-L418
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/reporting/digest_report_dir
  - functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/LPE-CT/src/http_routes/digest_reports
  - functions/LPE-CT/src/reporting/snapshot
---

# Signature

`pub(crate) fn list_recent_digest_reports( spool_dir: &Path, limit: usize, ) -> Result<Vec<DigestReportSummary>>`

# Calls

- [digest_report_dir](../../../../functions/LPE-CT/src/reporting/digest_report_dir.md)
- [from_str](../../../../functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str.md)
- [push](../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [digest_reports](../../../../functions/LPE-CT/src/http_routes/digest_reports.md)
- [snapshot](../../../../functions/LPE-CT/src/reporting/snapshot.md)