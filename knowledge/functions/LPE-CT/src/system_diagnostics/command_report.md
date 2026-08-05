---
type: Rust Function
title: command_report
resource: LPE-CT/src/system_diagnostics.rs#L360-L378
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/system_diagnostics/run_command
  called_by:
  - functions/LPE-CT/src/system_diagnostics/command_diagnostic
  - functions/LPE-CT/src/system_diagnostics/run_tool
  - functions/LPE-CT/src/system_diagnostics/support_connect
  - functions/LPE-CT/src/system_diagnostics/spam_test
  - functions/LPE-CT/src/system_diagnostics/flush_mail_queue
---

# Signature

`async fn command_report( title: &str, detail: &str, program: &str, args: &[&str], ) -> Result<DiagnosticReport>`

# Calls

- [run_command](../../../../functions/LPE-CT/src/system_diagnostics/run_command.md)

# Called by

- [command_diagnostic](../../../../functions/LPE-CT/src/system_diagnostics/command_diagnostic.md)
- [run_tool](../../../../functions/LPE-CT/src/system_diagnostics/run_tool.md)
- [support_connect](../../../../functions/LPE-CT/src/system_diagnostics/support_connect.md)
- [spam_test](../../../../functions/LPE-CT/src/system_diagnostics/spam_test.md)
- [flush_mail_queue](../../../../functions/LPE-CT/src/system_diagnostics/flush_mail_queue.md)