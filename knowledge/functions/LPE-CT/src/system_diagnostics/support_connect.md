---
type: Rust Function
title: support_connect
resource: LPE-CT/src/system_diagnostics.rs#L232-L250
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/system_diagnostics/configured_command
  - functions/LPE-CT/src/system_diagnostics/command_report
  called_by:
  - functions/LPE-CT/src/http_routes/connect_lpe_support
---

# Signature

`pub(crate) async fn support_connect() -> Result<DiagnosticReport>`

# Calls

- [configured_command](../../../../functions/LPE-CT/src/system_diagnostics/configured_command.md)
- [command_report](../../../../functions/LPE-CT/src/system_diagnostics/command_report.md)

# Called by

- [connect_lpe_support](../../../../functions/LPE-CT/src/http_routes/connect_lpe_support.md)