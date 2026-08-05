---
type: Rust Function
title: spam_test
resource: LPE-CT/src/system_diagnostics.rs#L252-L295
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/system_diagnostics/configured_command
  - functions/crates/lpe-core/src/sieve/context
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/LPE-CT/src/system_diagnostics/command_report
  called_by:
  - functions/LPE-CT/src/http_routes/run_spam_test
---

# Signature

`pub(crate) async fn spam_test(payload: SpamTestRequest) -> Result<DiagnosticReport>`

# Calls

- [configured_command](../../../../functions/LPE-CT/src/system_diagnostics/configured_command.md)
- [context](../../../../functions/crates/lpe-core/src/sieve/context.md)
- [push](../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [command_report](../../../../functions/LPE-CT/src/system_diagnostics/command_report.md)

# Called by

- [run_spam_test](../../../../functions/LPE-CT/src/http_routes/run_spam_test.md)