---
type: Rust Function
title: run_command
resource: LPE-CT/src/system_diagnostics.rs#L380-L388
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/LPE-CT/src/system_diagnostics/service_action
  - functions/LPE-CT/src/system_diagnostics/routing_table_report
  - functions/LPE-CT/src/system_diagnostics/service_status
  - functions/LPE-CT/src/system_diagnostics/command_report
---

# Signature

`async fn run_command(program: &str, args: &[&str]) -> Result<std::process::Output>`

# Called by

- [service_action](../../../../functions/LPE-CT/src/system_diagnostics/service_action.md)
- [routing_table_report](../../../../functions/LPE-CT/src/system_diagnostics/routing_table_report.md)
- [service_status](../../../../functions/LPE-CT/src/system_diagnostics/service_status.md)
- [command_report](../../../../functions/LPE-CT/src/system_diagnostics/command_report.md)