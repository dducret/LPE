---
type: Rust Function
title: prune_digest_reports
resource: LPE-CT/src/reporting.rs#L1409-L1427
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/reporting/digest_report_dir
  - functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str
  called_by:
  - functions/LPE-CT/src/reporting/run_due_digest_generation
  - functions/LPE-CT/src/reporting/run_digest_generation
  - functions/LPE-CT/src/reporting/enforce_retention
---

# Signature

`fn prune_digest_reports(spool_dir: &Path, retention_days: u32) -> Result<()>`

# Calls

- [digest_report_dir](../../../../functions/LPE-CT/src/reporting/digest_report_dir.md)
- [from_str](../../../../functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str.md)

# Called by

- [run_due_digest_generation](../../../../functions/LPE-CT/src/reporting/run_due_digest_generation.md)
- [run_digest_generation](../../../../functions/LPE-CT/src/reporting/run_digest_generation.md)
- [enforce_retention](../../../../functions/LPE-CT/src/reporting/enforce_retention.md)