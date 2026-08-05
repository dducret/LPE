---
type: Rust Function
title: count_queue_files
resource: LPE-CT/src/readiness.rs#L359-L366
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/LPE-CT/src/readiness/check_spool_pressure
  - functions/LPE-CT/src/readiness/check_quarantine_backlog
---

# Signature

`fn count_queue_files(path: &Path, queue: &str) -> u32`

# Called by

- [check_spool_pressure](../../../../functions/LPE-CT/src/readiness/check_spool_pressure.md)
- [check_quarantine_backlog](../../../../functions/LPE-CT/src/readiness/check_quarantine_backlog.md)