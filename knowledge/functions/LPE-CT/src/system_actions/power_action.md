---
type: Rust Function
title: power_action
resource: LPE-CT/src/system_actions.rs#L86-L108
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/system_actions/run_host_action
  called_by:
  - functions/LPE-CT/src/http_routes/run_system_power_action
---

# Signature

`pub(crate) async fn power_action(action: &str) -> Result<SystemActionResponse>`

# Calls

- [run_host_action](../../../../functions/LPE-CT/src/system_actions/run_host_action.md)

# Called by

- [run_system_power_action](../../../../functions/LPE-CT/src/http_routes/run_system_power_action.md)