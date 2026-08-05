---
type: Rust Function
title: mapi_notification_old_message_id
resource: crates/lpe-exchange/src/store/storage_impl/address_helpers.rs#L609-L618
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/mapi_notification_event_from_change_row
---

# Signature

`fn mapi_notification_old_message_id( event_mask: u16, captured_old_message_id: Option<i64>, move_identity_snapshot_complete: bool, ) -> Option<u64>`

# Called by

- [mapi_notification_event_from_change_row](../../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/mapi_notification_event_from_change_row.md)