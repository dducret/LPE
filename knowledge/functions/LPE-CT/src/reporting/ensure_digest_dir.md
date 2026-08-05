---
type: Rust Function
title: ensure_digest_dir
resource: LPE-CT/src/reporting.rs#L1272-L1275
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/reporting/digest_report_dir
  called_by:
  - functions/LPE-CT/src/reporting/build_digest_report
---

# Signature

`fn ensure_digest_dir(spool_dir: &Path) -> Result<()>`

# Calls

- [digest_report_dir](../../../../functions/LPE-CT/src/reporting/digest_report_dir.md)

# Called by

- [build_digest_report](../../../../functions/LPE-CT/src/reporting/build_digest_report.md)