---
type: Rust Function
title: readiness_status
resource: LPE-CT/src/readiness.rs#L97-L106
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/LPE-CT/src/http_routes/health_ready
---

# Signature

`pub(crate) fn readiness_status(checks: &[ReadinessCheck]) -> &'static str`

# Called by

- [health_ready](../../../../functions/LPE-CT/src/http_routes/health_ready.md)