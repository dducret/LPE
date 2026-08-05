---
type: Rust Function
title: mapi_hierarchy_movement_source_ids
resource: crates/lpe-exchange/src/store/storage_impl/address_helpers.rs#L622-L634
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/mapi_notification_event_from_change_row
---

# Signature

`fn mapi_hierarchy_movement_source_ids( event_mask: u16, changed_folder_id: u64, source_folder_id: Option<u64>, old_parent_folder_id: Option<u64>, ) -> Option<(u64, u64)>`

# Called by

- [mapi_notification_event_from_change_row](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/mapi_notification_event_from_change_row.md)