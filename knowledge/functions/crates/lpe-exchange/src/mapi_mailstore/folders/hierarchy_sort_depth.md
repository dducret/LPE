---
type: Rust Function
title: hierarchy_sort_depth
resource: crates/lpe-exchange/src/mapi_mailstore/folders.rs#L243-L275
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_mailstore/folders/mapi_folder_id_for_mailbox
  - functions/crates/lpe-exchange/src/mapi_mailstore/folders/mapi_folder_parent_id_for_mailbox
  - functions/crates/lpe-exchange/src/mapi_mailstore/folders/mapi_parent_folder_id_for_folder_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_manifest_buffer_with_special_objects_and_final_state_with_folder_versions_and_commit_times_and_normal_message_facts
---

# Signature

`pub(crate) fn hierarchy_sort_depth( sync_type: u8, sync_root_folder_id: u64, mailbox: &JmapMailbox, mailboxes: &[JmapMailbox], ) -> u8`

# Calls

- [mapi_folder_id_for_mailbox](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/folders/mapi_folder_id_for_mailbox.md)
- [mapi_folder_parent_id_for_mailbox](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/folders/mapi_folder_parent_id_for_mailbox.md)
- [mapi_parent_folder_id_for_folder_id](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/folders/mapi_parent_folder_id_for_folder_id.md)

# Called by

- [sync_manifest_buffer_with_special_objects_and_final_state_with_folder_versions_and_commit_times_and_normal_message_facts](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_manifest_buffer_with_special_objects_and_final_state_with_folder_versions_and_commit_times_and_normal_message_facts.md)