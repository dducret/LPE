---
type: Rust Function
title: check_spool_layout
resource: LPE-CT/src/readiness.rs#L139-L161
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/LPE-CT/src/http_routes/health_ready
---

# Signature

`pub(crate) fn check_spool_layout(path: &Path) -> ReadinessCheck`

# Called by

- [health_ready](../../../../functions/LPE-CT/src/http_routes/health_ready.md)