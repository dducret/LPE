---
type: Rust Function
title: check_non_empty_value
resource: LPE-CT/src/readiness.rs#L108-L120
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/LPE-CT/src/http_routes/health_ready
---

# Signature

`pub(crate) fn check_non_empty_value( name: &str, critical: bool, value: &str, ok_detail: &str, failed_detail: &str, ) -> ReadinessCheck`

# Called by

- [health_ready](../../../../functions/LPE-CT/src/http_routes/health_ready.md)