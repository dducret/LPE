---
type: Rust Function
title: run_tool
resource: LPE-CT/src/system_diagnostics.rs#L205-L230
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/system_diagnostics/validate_target
  - functions/LPE-CT/src/system_diagnostics/command_report
  called_by:
  - functions/LPE-CT/src/http_routes/run_system_tool
---

# Signature

`pub(crate) async fn run_tool(payload: ToolRunRequest) -> Result<DiagnosticReport>`

# Calls

- [validate_target](../../../../functions/LPE-CT/src/system_diagnostics/validate_target.md)
- [command_report](../../../../functions/LPE-CT/src/system_diagnostics/command_report.md)

# Called by

- [run_system_tool](../../../../functions/LPE-CT/src/http_routes/run_system_tool.md)