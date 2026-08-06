---
type: Rust Function
title: change_key_version
resource: crates/lpe-exchange/src/service/ews/sync_state.rs#L552-L561
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/sync_state/contact_change_keys
  - functions/crates/lpe-exchange/src/service/ews/sync_state/event_change_keys
  - functions/crates/lpe-exchange/src/service/ews/sync_state/task_change_keys
---

# Signature

`fn change_key_version<'a>( versions: &'a HashMap<Uuid, String>, item_id: Uuid, item_kind: &str, ) -> Result<&'a str>`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [contact_change_keys](../../../../../../../functions/crates/lpe-exchange/src/service/ews/sync_state/contact_change_keys.md)
- [event_change_keys](../../../../../../../functions/crates/lpe-exchange/src/service/ews/sync_state/event_change_keys.md)
- [task_change_keys](../../../../../../../functions/crates/lpe-exchange/src/service/ews/sync_state/task_change_keys.md)