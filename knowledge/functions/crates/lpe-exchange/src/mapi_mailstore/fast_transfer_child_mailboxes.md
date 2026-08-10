---
type: Rust Function
title: fast_transfer_child_mailboxes
resource: crates/lpe-exchange/src/mapi_mailstore.rs#L962-L980
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/identity/mapped_mapi_object_id
  - functions/crates/lpe-exchange/src/mapi_mailstore/folders/mapi_folder_id_for_mailbox
  - functions/crates/lpe-exchange/src/mapi_mailstore/folders/mapi_folder_parent_id_for_mailbox
  - functions/crates/lpe-exchange/src/mapi_mailstore/folders/mapi_folder_display_name
  called_by:
  - functions/crates/lpe-exchange/src/mapi_mailstore/write_fast_transfer_folder_content
---

# Signature

`fn fast_transfer_child_mailboxes<'a>( folder_id: u64, mailboxes: &'a [JmapMailbox], ) -> Vec<&'a JmapMailbox>`

# Calls

- [mapped_mapi_object_id](../../../../../functions/crates/lpe-exchange/src/mapi/identity/mapped_mapi_object_id.md)
- [mapi_folder_id_for_mailbox](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/folders/mapi_folder_id_for_mailbox.md)
- [mapi_folder_parent_id_for_mailbox](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/folders/mapi_folder_parent_id_for_mailbox.md)
- [mapi_folder_display_name](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/folders/mapi_folder_display_name.md)

# Called by

- [write_fast_transfer_folder_content](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/write_fast_transfer_folder_content.md)