---
type: Rust Function
title: service_status
resource: LPE-CT/src/system_diagnostics.rs#L319-L344
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/system_diagnostics/run_command
  - functions/crates/lpe-core/src/sieve/Parser/next
  called_by:
  - functions/LPE-CT/src/system_diagnostics/service_action
---

# Signature

`async fn service_status(id: &str, name: &str, unit: String) -> ServiceStatus`

# Calls

- [run_command](../../../../functions/LPE-CT/src/system_diagnostics/run_command.md)
- [next](../../../../functions/crates/lpe-core/src/sieve/Parser/next.md)

# Called by

- [service_action](../../../../functions/LPE-CT/src/system_diagnostics/service_action.md)