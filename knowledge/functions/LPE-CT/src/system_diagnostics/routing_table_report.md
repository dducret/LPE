---
type: Rust Function
title: routing_table_report
resource: LPE-CT/src/system_diagnostics.rs#L116-L128
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/system_diagnostics/run_command
  - functions/LPE-CT/src/system_diagnostics/routing_table_from_proc
  called_by:
  - functions/LPE-CT/src/system_diagnostics/command_diagnostic
---

# Signature

`async fn routing_table_report() -> Result<DiagnosticReport>`

# Calls

- [run_command](../../../../functions/LPE-CT/src/system_diagnostics/run_command.md)
- [routing_table_from_proc](../../../../functions/LPE-CT/src/system_diagnostics/routing_table_from_proc.md)

# Called by

- [command_diagnostic](../../../../functions/LPE-CT/src/system_diagnostics/command_diagnostic.md)