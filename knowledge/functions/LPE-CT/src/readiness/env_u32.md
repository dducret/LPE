---
type: Rust Function
title: env_u32
resource: LPE-CT/src/readiness.rs#L368-L374
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/LPE-CT/src/readiness/check_spool_pressure
  - functions/LPE-CT/src/readiness/check_quarantine_backlog
---

# Signature

`fn env_u32(name: &str, default: u32) -> u32`

# Called by

- [check_spool_pressure](../../../../functions/LPE-CT/src/readiness/check_spool_pressure.md)
- [check_quarantine_backlog](../../../../functions/LPE-CT/src/readiness/check_quarantine_backlog.md)