---
type: Rust Function
title: policy_key
resource: crates/lpe-activesync/src/response.rs#L97-L104
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-activesync/src/service/provisioning/ActiveSyncService/handle_provision
---

# Signature

`pub(crate) fn policy_key(account_id: Uuid, device_id: &str) -> String`

# Called by

- [handle_provision](../../../../../functions/crates/lpe-activesync/src/service/provisioning/ActiveSyncService/handle_provision.md)