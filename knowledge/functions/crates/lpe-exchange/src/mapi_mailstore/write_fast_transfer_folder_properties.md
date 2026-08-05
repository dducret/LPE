---
type: Rust Function
title: write_fast_transfer_folder_properties
resource: crates/lpe-exchange/src/mapi_mailstore.rs#L920-L957
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_mailstore/write_utf16_property
  - functions/crates/lpe-exchange/src/mapi_mailstore/folders/mapi_folder_display_name
  - functions/crates/lpe-exchange/src/mapi_mailstore/folders/mapi_folder_parent_id_for_mailbox
  - functions/crates/lpe-exchange/src/mapi_mailstore/folders/folder_content_counts
  - functions/crates/lpe-exchange/src/mapi_mailstore/folders/mapi_folder_message_class
  - functions/crates/lpe-exchange/src/mapi_mailstore/write_i32_property
  - functions/crates/lpe-exchange/src/mapi_mailstore/write_bool_property
  - functions/crates/lpe-exchange/src/mapi_mailstore/folders/mapi_folder_has_subfolders
  called_by:
  - functions/crates/lpe-exchange/src/mapi_mailstore/write_fast_transfer_folder_content
---

# Signature

`fn write_fast_transfer_folder_properties( buffer: &mut Vec<u8>, folder_id: u64, mailbox: &JmapMailbox, mailboxes: &[JmapMailbox], emails: &[JmapEmail], top_folder: bool, )`

# Calls

- [write_utf16_property](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/write_utf16_property.md)
- [mapi_folder_display_name](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/folders/mapi_folder_display_name.md)
- [mapi_folder_parent_id_for_mailbox](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/folders/mapi_folder_parent_id_for_mailbox.md)
- [folder_content_counts](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/folders/folder_content_counts.md)
- [mapi_folder_message_class](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/folders/mapi_folder_message_class.md)
- [write_i32_property](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/write_i32_property.md)
- [write_bool_property](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/write_bool_property.md)
- [mapi_folder_has_subfolders](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/folders/mapi_folder_has_subfolders.md)

# Called by

- [write_fast_transfer_folder_content](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/write_fast_transfer_folder_content.md)