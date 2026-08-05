---
type: Rust Function
title: check_quarantine_backlog
resource: LPE-CT/src/readiness.rs#L342-L357
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/readiness/env_u32
  - functions/LPE-CT/src/readiness/count_queue_files
  called_by:
  - functions/LPE-CT/src/http_routes/health_ready
---

# Signature

`pub(crate) fn check_quarantine_backlog(path: &Path) -> ReadinessCheck`

# Calls

- [env_u32](../../../../functions/LPE-CT/src/readiness/env_u32.md)
- [count_queue_files](../../../../functions/LPE-CT/src/readiness/count_queue_files.md)

# Called by

- [health_ready](../../../../functions/LPE-CT/src/http_routes/health_ready.md)