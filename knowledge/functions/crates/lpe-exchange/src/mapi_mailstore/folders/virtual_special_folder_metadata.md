---
type: Rust Function
title: virtual_special_folder_metadata
resource: crates/lpe-exchange/src/mapi_mailstore/folders.rs#L317-L603
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi_mailstore/fast_transfer_email_matches_folder
  - functions/crates/lpe-exchange/src/mapi_mailstore/folders/mapi_folder_parent_id_for_mailbox
  - functions/crates/lpe-exchange/src/mapi_mailstore/folders/mapi_folder_message_class
  - functions/crates/lpe-exchange/src/mapi_mailstore/folders/mapi_folder_display_name
  - functions/crates/lpe-exchange/src/mapi_mailstore/folders/email_unread_in_manifest_folder
  - functions/crates/lpe-exchange/src/mapi_mailstore/folders/mapi_parent_folder_id_for_folder_id
  - functions/crates/lpe-exchange/src/mapi_mailstore/folders/hierarchy_folder_sort_order
  - functions/crates/lpe-exchange/src/mapi_mailstore/folders/virtual_special_mailbox_ids
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/virtual_special_mailbox
  - functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/mapi_notification_event_from_change_row
---

# Signature

`pub(crate) fn virtual_special_folder_metadata( folder_id: u64, ) -> Option<(&'static str, &'static str, i32, u64, &'static str)>`

# Called by

- [fast_transfer_email_matches_folder](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/fast_transfer_email_matches_folder.md)
- [mapi_folder_parent_id_for_mailbox](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/folders/mapi_folder_parent_id_for_mailbox.md)
- [mapi_folder_message_class](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/folders/mapi_folder_message_class.md)
- [mapi_folder_display_name](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/folders/mapi_folder_display_name.md)
- [email_unread_in_manifest_folder](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/folders/email_unread_in_manifest_folder.md)
- [mapi_parent_folder_id_for_folder_id](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/folders/mapi_parent_folder_id_for_folder_id.md)
- [hierarchy_folder_sort_order](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/folders/hierarchy_folder_sort_order.md)
- [virtual_special_mailbox_ids](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/folders/virtual_special_mailbox_ids.md)
- [virtual_special_mailbox](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/virtual_special_mailbox.md)
- [mapi_notification_event_from_change_row](../../../../../../functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/mapi_notification_event_from_change_row.md)