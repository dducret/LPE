---
type: Rust Function
title: normalize_servers
resource: LPE-CT/src/system_actions.rs#L110-L132
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/LPE-CT/src/system_actions/update_ntp
---

# Signature

`fn normalize_servers(servers: Vec<String>) -> Result<Vec<String>>`

# Calls

- [push](../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [update_ntp](../../../../functions/LPE-CT/src/system_actions/update_ntp.md)