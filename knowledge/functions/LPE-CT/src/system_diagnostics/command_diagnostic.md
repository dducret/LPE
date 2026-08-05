---
type: Rust Function
title: command_diagnostic
resource: LPE-CT/src/system_diagnostics.rs#L79-L114
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/system_diagnostics/command_report
  - functions/LPE-CT/src/system_diagnostics/routing_table_report
  called_by:
  - functions/LPE-CT/src/http_routes/system_diagnostic_report
---

# Signature

`pub(crate) async fn command_diagnostic(kind: &str) -> Result<DiagnosticReport>`

# Calls

- [command_report](../../../../functions/LPE-CT/src/system_diagnostics/command_report.md)
- [routing_table_report](../../../../functions/LPE-CT/src/system_diagnostics/routing_table_report.md)

# Called by

- [system_diagnostic_report](../../../../functions/LPE-CT/src/http_routes/system_diagnostic_report.md)