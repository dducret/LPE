---
type: Rust Function
title: routing_table_from_proc
resource: LPE-CT/src/system_diagnostics.rs#L130-L155
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/system_diagnostics/format_proc_ipv4_routes
  called_by:
  - functions/LPE-CT/src/system_diagnostics/routing_table_report
---

# Signature

`async fn routing_table_from_proc(ip_error: Option<String>) -> Result<DiagnosticReport>`

# Calls

- [format_proc_ipv4_routes](../../../../functions/LPE-CT/src/system_diagnostics/format_proc_ipv4_routes.md)

# Called by

- [routing_table_report](../../../../functions/LPE-CT/src/system_diagnostics/routing_table_report.md)