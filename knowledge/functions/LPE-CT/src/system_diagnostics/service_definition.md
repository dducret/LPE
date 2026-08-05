---
type: Rust Function
title: service_definition
resource: LPE-CT/src/system_diagnostics.rs#L346-L358
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/LPE-CT/src/system_diagnostics/service_action
---

# Signature

`fn service_definition(service_id: &str) -> Result<(&'static str, String)>`

# Called by

- [service_action](../../../../functions/LPE-CT/src/system_diagnostics/service_action.md)