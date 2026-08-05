---
type: Rust Function
title: mapi_folder_parent_id_for_mailbox
resource: crates/lpe-exchange/src/mapi_mailstore/folders.rs#L47-L98
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_mailstore/folders/virtual_special_folder_metadata
  - functions/crates/lpe-exchange/src/mapi_mailstore/folders/mapi_folder_id_for_mailbox
  - functions/crates/lpe-exchange/src/mapi/identity/mapped_mapi_object_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi_mailstore/write_fast_transfer_folder_properties
  - functions/crates/lpe-exchange/src/mapi_mailstore/fast_transfer_child_mailboxes
  - functions/crates/lpe-exchange/src/mapi_mailstore/folders/mapi_folder_has_subfolders
  - functions/crates/lpe-exchange/src/mapi_mailstore/folders/hierarchy_sort_depth
  - functions/crates/lpe-exchange/src/mapi_mailstore/folders/mapi_parent_folder_id_for_folder_id
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_manifest_buffer_with_special_objects_and_final_state_with_folder_versions_and_commit_times_and_normal_message_facts
  - functions/crates/lpe-exchange/src/mapi_store/folder_commit_time/MapiMailStoreSnapshot/folder_local_commit_time_max
---

# Signature

`pub(crate) fn mapi_folder_parent_id_for_mailbox( mailbox: &JmapMailbox, mailboxes: &[JmapMailbox], ) -> u64`

# Calls

- [virtual_special_folder_metadata](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/folders/virtual_special_folder_metadata.md)
- [mapi_folder_id_for_mailbox](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/folders/mapi_folder_id_for_mailbox.md)
- [mapped_mapi_object_id](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/mapped_mapi_object_id.md)

# Called by

- [write_fast_transfer_folder_properties](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/write_fast_transfer_folder_properties.md)
- [fast_transfer_child_mailboxes](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/fast_transfer_child_mailboxes.md)
- [mapi_folder_has_subfolders](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/folders/mapi_folder_has_subfolders.md)
- [hierarchy_sort_depth](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/folders/hierarchy_sort_depth.md)
- [mapi_parent_folder_id_for_folder_id](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/folders/mapi_parent_folder_id_for_folder_id.md)
- [sync_manifest_buffer_with_special_objects_and_final_state_with_folder_versions_and_commit_times_and_normal_message_facts](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_manifest_buffer_with_special_objects_and_final_state_with_folder_versions_and_commit_times_and_normal_message_facts.md)
- [folder_local_commit_time_max](../../../../../../functions/crates/lpe-exchange/src/mapi_store/folder_commit_time/MapiMailStoreSnapshot/folder_local_commit_time_max.md)