---
type: Rust Function
title: mapi_hierarchy_old_parent_folder_id
resource: crates/lpe-exchange/src/store/storage_impl/address_helpers.rs#L821-L836
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/mapi_notification_folder_id
  called_by:
  - functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/mapi_notification_event_from_change_row
---

# Signature

`fn mapi_hierarchy_old_parent_folder_id( old_parent_id_present: bool, old_parent_is_root: bool, role: Option<&str>, durable_identity: Option<i64>, canonical_id: Option<Uuid>, allocated_ids: &std::collections::HashMap<Uuid, u64>, ) -> Option<u64>`

# Calls

- [mapi_notification_folder_id](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/mapi_notification_folder_id.md)

# Called by

- [mapi_notification_event_from_change_row](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/mapi_notification_event_from_change_row.md)