---
type: Rust Function
title: sync_ntp
resource: LPE-CT/src/system_actions.rs#L56-L72
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/system_actions/run_host_action
  called_by:
  - functions/LPE-CT/src/http_routes/sync_system_ntp
---

# Signature

`pub(crate) async fn sync_ntp() -> Result<SystemActionResponse>`

# Calls

- [run_host_action](../../../../functions/LPE-CT/src/system_actions/run_host_action.md)

# Called by

- [sync_system_ntp](../../../../functions/LPE-CT/src/http_routes/sync_system_ntp.md)