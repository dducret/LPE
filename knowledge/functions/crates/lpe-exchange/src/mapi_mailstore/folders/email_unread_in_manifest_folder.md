---
type: Rust Function
title: email_unread_in_manifest_folder
resource: crates/lpe-exchange/src/mapi_mailstore/folders.rs#L207-L241
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_mailstore/folders/virtual_special_folder_metadata
  - functions/crates/lpe-exchange/src/mapi/identity/mapped_mapi_object_id
  - functions/crates/lpe-exchange/src/mapi_mailstore/folders/mapi_folder_id_for_mailbox
  called_by:
  - functions/crates/lpe-exchange/src/mapi_mailstore/local_commit_time_max
  - functions/crates/lpe-exchange/src/mapi_mailstore/folders/folder_content_counts
---

# Signature

`pub(crate) fn email_unread_in_manifest_folder( email: &JmapEmail, folder_id: u64, mailboxes: &[JmapMailbox], ) -> Option<bool>`

# Calls

- [virtual_special_folder_metadata](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/folders/virtual_special_folder_metadata.md)
- [mapped_mapi_object_id](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/mapped_mapi_object_id.md)
- [mapi_folder_id_for_mailbox](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/folders/mapi_folder_id_for_mailbox.md)

# Called by

- [local_commit_time_max](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/local_commit_time_max.md)
- [folder_content_counts](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/folders/folder_content_counts.md)