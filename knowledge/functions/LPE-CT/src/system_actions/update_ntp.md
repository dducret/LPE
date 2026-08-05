---
type: Rust Function
title: update_ntp
resource: LPE-CT/src/system_actions.rs#L26-L54
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/system_actions/normalize_servers
  - functions/LPE-CT/src/system_actions/run_host_action
  called_by:
  - functions/LPE-CT/src/http_routes/update_system_ntp
---

# Signature

`pub(crate) async fn update_ntp(payload: NtpUpdateRequest) -> Result<SystemActionResponse>`

# Calls

- [normalize_servers](../../../../functions/LPE-CT/src/system_actions/normalize_servers.md)
- [run_host_action](../../../../functions/LPE-CT/src/system_actions/run_host_action.md)

# Called by

- [update_system_ntp](../../../../functions/LPE-CT/src/http_routes/update_system_ntp.md)