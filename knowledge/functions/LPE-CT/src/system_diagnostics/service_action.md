---
type: Rust Function
title: service_action
resource: LPE-CT/src/system_diagnostics.rs#L66-L77
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/system_diagnostics/service_definition
  - functions/LPE-CT/src/system_diagnostics/run_command
  - functions/LPE-CT/src/system_diagnostics/service_status
  called_by:
  - functions/LPE-CT/src/http_routes/system_diagnostic_service_action
---

# Signature

`pub(crate) async fn service_action(service_id: &str, action: &str) -> Result<ServiceStatus>`

# Calls

- [service_definition](../../../../functions/LPE-CT/src/system_diagnostics/service_definition.md)
- [run_command](../../../../functions/LPE-CT/src/system_diagnostics/run_command.md)
- [service_status](../../../../functions/LPE-CT/src/system_diagnostics/service_status.md)

# Called by

- [system_diagnostic_service_action](../../../../functions/LPE-CT/src/http_routes/system_diagnostic_service_action.md)