---
type: Rust Function
title: configured_command
resource: LPE-CT/src/system_diagnostics.rs#L428-L443
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/LPE-CT/src/system_diagnostics/support_connect
  - functions/LPE-CT/src/system_diagnostics/spam_test
  - functions/LPE-CT/src/system_diagnostics/flush_mail_queue
---

# Signature

`fn configured_command(bin_env: &str, args_env: &str) -> Result<ConfiguredCommand>`

# Called by

- [support_connect](../../../../functions/LPE-CT/src/system_diagnostics/support_connect.md)
- [spam_test](../../../../functions/LPE-CT/src/system_diagnostics/spam_test.md)
- [flush_mail_queue](../../../../functions/LPE-CT/src/system_diagnostics/flush_mail_queue.md)