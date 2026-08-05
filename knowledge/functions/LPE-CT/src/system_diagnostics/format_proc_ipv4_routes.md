---
type: Rust Function
title: format_proc_ipv4_routes
resource: LPE-CT/src/system_diagnostics.rs#L157-L168
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/LPE-CT/src/system_diagnostics/routing_table_from_proc
---

# Signature

`fn format_proc_ipv4_routes(content: &str) -> String`

# Called by

- [routing_table_from_proc](../../../../functions/LPE-CT/src/system_diagnostics/routing_table_from_proc.md)