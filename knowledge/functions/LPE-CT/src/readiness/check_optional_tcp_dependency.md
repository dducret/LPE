---
type: Rust Function
title: check_optional_tcp_dependency
resource: LPE-CT/src/readiness.rs#L285-L316
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/readiness/smtp_target_socket_address
  - functions/crates/lpe-storage/src/core/Storage/connect
  called_by:
  - functions/LPE-CT/src/http_routes/health_ready
---

# Signature

`pub(crate) async fn check_optional_tcp_dependency( name: &str, target: &str, ok_detail: &str, warn_detail: &str, ) -> ReadinessCheck`

# Calls

- [smtp_target_socket_address](../../../../functions/LPE-CT/src/readiness/smtp_target_socket_address.md)
- [connect](../../../../functions/crates/lpe-storage/src/core/Storage/connect.md)

# Called by

- [health_ready](../../../../functions/LPE-CT/src/http_routes/health_ready.md)