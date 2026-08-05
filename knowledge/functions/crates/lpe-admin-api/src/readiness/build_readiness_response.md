---
type: Rust Function
title: build_readiness_response
resource: crates/lpe-admin-api/src/readiness.rs#L99-L118
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-admin-api/src/health/health_ready
---

# Signature

`pub(crate) fn build_readiness_response( service: &str, checks: Vec<ReadinessCheck>, ) -> ReadinessResponse`

# Called by

- [health_ready](../../../../../functions/crates/lpe-admin-api/src/health/health_ready.md)