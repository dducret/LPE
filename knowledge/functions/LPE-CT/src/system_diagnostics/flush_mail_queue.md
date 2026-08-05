---
type: Rust Function
title: flush_mail_queue
resource: LPE-CT/src/system_diagnostics.rs#L297-L317
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/system_diagnostics/configured_command
  - functions/LPE-CT/src/system_diagnostics/command_report
---

# Signature

`pub(crate) async fn flush_mail_queue() -> Result<DiagnosticReport>`

# Calls

- [configured_command](../../../../functions/LPE-CT/src/system_diagnostics/configured_command.md)
- [command_report](../../../../functions/LPE-CT/src/system_diagnostics/command_report.md)