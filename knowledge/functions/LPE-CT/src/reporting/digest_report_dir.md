---
type: Rust Function
title: digest_report_dir
resource: LPE-CT/src/reporting.rs#L1277-L1279
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/LPE-CT/src/reporting/list_recent_digest_reports
  - functions/LPE-CT/src/reporting/load_digest_report
  - functions/LPE-CT/src/reporting/build_digest_report
  - functions/LPE-CT/src/reporting/ensure_digest_dir
  - functions/LPE-CT/src/reporting/prune_digest_reports
---

# Signature

`fn digest_report_dir(spool_dir: &Path) -> PathBuf`

# Called by

- [list_recent_digest_reports](../../../../functions/LPE-CT/src/reporting/list_recent_digest_reports.md)
- [load_digest_report](../../../../functions/LPE-CT/src/reporting/load_digest_report.md)
- [build_digest_report](../../../../functions/LPE-CT/src/reporting/build_digest_report.md)
- [ensure_digest_dir](../../../../functions/LPE-CT/src/reporting/ensure_digest_dir.md)
- [prune_digest_reports](../../../../functions/LPE-CT/src/reporting/prune_digest_reports.md)