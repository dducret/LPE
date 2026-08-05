---
type: Rust Function
title: apt_update_upgrade
resource: LPE-CT/src/system_actions.rs#L74-L84
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/system_actions/run_host_action
  called_by:
  - functions/LPE-CT/src/http_routes/run_apt_update_upgrade
---

# Signature

`pub(crate) async fn apt_update_upgrade() -> Result<SystemActionResponse>`

# Calls

- [run_host_action](../../../../functions/LPE-CT/src/system_actions/run_host_action.md)

# Called by

- [run_apt_update_upgrade](../../../../functions/LPE-CT/src/http_routes/run_apt_update_upgrade.md)