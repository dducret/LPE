---
type: Rust Function
title: smtp_target_socket_address
resource: LPE-CT/src/readiness.rs#L376-L386
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/LPE-CT/src/readiness/check_optional_tcp_dependency
---

# Signature

`fn smtp_target_socket_address(target: &str) -> String`

# Called by

- [check_optional_tcp_dependency](../../../../functions/LPE-CT/src/readiness/check_optional_tcp_dependency.md)