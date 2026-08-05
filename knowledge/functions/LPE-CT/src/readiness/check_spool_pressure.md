---
type: Rust Function
title: check_spool_pressure
resource: LPE-CT/src/readiness.rs#L318-L340
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

`pub(crate) fn check_spool_pressure(path: &Path) -> ReadinessCheck`

# Calls

- [env_u32](../../../../functions/LPE-CT/src/readiness/env_u32.md)
- [count_queue_files](../../../../functions/LPE-CT/src/readiness/count_queue_files.md)

# Called by

- [health_ready](../../../../functions/LPE-CT/src/http_routes/health_ready.md)