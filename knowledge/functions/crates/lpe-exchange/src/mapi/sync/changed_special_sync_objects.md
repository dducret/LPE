---
type: Rust Function
title: changed_special_sync_objects
resource: crates/lpe-exchange/src/mapi/sync.rs#L563-L574
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_configure/append_synchronization_configure_response
---

# Signature

`pub(in crate::mapi) fn changed_special_sync_objects( objects: Vec<mapi_mailstore::SpecialMessageSyncFact>, changed_ids: &[Uuid], ) -> Vec<mapi_mailstore::SpecialMessageSyncFact>`

# Called by

- [append_synchronization_configure_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_configure/append_synchronization_configure_response.md)