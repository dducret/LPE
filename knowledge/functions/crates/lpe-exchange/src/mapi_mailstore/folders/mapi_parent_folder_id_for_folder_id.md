---
type: Rust Function
title: mapi_parent_folder_id_for_folder_id
resource: crates/lpe-exchange/src/mapi_mailstore/folders.rs#L277-L295
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_mailstore/folders/virtual_special_folder_metadata
  - functions/crates/lpe-exchange/src/mapi/identity/mapped_mapi_object_id
  - functions/crates/lpe-exchange/src/mapi_mailstore/folders/mapi_folder_id_for_mailbox
  - functions/crates/lpe-exchange/src/mapi_mailstore/folders/mapi_folder_parent_id_for_mailbox
  called_by:
  - functions/crates/lpe-exchange/src/mapi_mailstore/folders/hierarchy_sort_depth
---

# Signature

`pub(crate) fn mapi_parent_folder_id_for_folder_id( folder_id: u64, mailboxes: &[JmapMailbox], ) -> Option<u64>`

# Calls

- [virtual_special_folder_metadata](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/folders/virtual_special_folder_metadata.md)
- [mapped_mapi_object_id](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/mapped_mapi_object_id.md)
- [mapi_folder_id_for_mailbox](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/folders/mapi_folder_id_for_mailbox.md)
- [mapi_folder_parent_id_for_mailbox](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/folders/mapi_folder_parent_id_for_mailbox.md)

# Called by

- [hierarchy_sort_depth](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/folders/hierarchy_sort_depth.md)