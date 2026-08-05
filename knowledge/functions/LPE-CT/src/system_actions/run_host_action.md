---
type: Rust Function
title: run_host_action
resource: LPE-CT/src/system_actions.rs#L134-L171
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-core/src/sieve/context
  called_by:
  - functions/LPE-CT/src/system_actions/update_ntp
  - functions/LPE-CT/src/system_actions/sync_ntp
  - functions/LPE-CT/src/system_actions/apt_update_upgrade
  - functions/LPE-CT/src/system_actions/power_action
---

# Signature

`async fn run_host_action( action: &str, args: &[&str], stdin: Option<&str>, timeout: Duration, ) -> Result<Output>`

# Calls

- [context](../../../../functions/crates/lpe-core/src/sieve/context.md)

# Called by

- [update_ntp](../../../../functions/LPE-CT/src/system_actions/update_ntp.md)
- [sync_ntp](../../../../functions/LPE-CT/src/system_actions/sync_ntp.md)
- [apt_update_upgrade](../../../../functions/LPE-CT/src/system_actions/apt_update_upgrade.md)
- [power_action](../../../../functions/LPE-CT/src/system_actions/power_action.md)