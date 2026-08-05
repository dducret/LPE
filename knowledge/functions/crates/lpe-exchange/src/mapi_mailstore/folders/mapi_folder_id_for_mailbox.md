---
type: Rust Function
title: mapi_folder_id_for_mailbox
resource: crates/lpe-exchange/src/mapi_mailstore/folders.rs#L3-L45
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/identity/mapped_mapi_object_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi_mailstore/sync_state_object_ids
  - functions/crates/lpe-exchange/src/mapi_mailstore/sync_state_change_numbers
  - functions/crates/lpe-exchange/src/mapi_mailstore/fast_transfer_top_folder_buffer_with_attachments
  - functions/crates/lpe-exchange/src/mapi_mailstore/write_fast_transfer_folder_content
  - functions/crates/lpe-exchange/src/mapi_mailstore/fast_transfer_child_mailboxes
  - functions/crates/lpe-exchange/src/mapi_mailstore/fast_transfer_email_matches_folder
  - functions/crates/lpe-exchange/src/mapi_mailstore/client_state/download_change_facts_with_normal_message_sync_facts
  - functions/crates/lpe-exchange/src/mapi_mailstore/folders/mapi_folder_parent_id_for_mailbox
  - functions/crates/lpe-exchange/src/mapi_mailstore/folders/mapi_folder_message_class
  - functions/crates/lpe-exchange/src/mapi_mailstore/folders/mapi_folder_display_name
  - functions/crates/lpe-exchange/src/mapi_mailstore/folders/mapi_folder_has_subfolders
  - functions/crates/lpe-exchange/src/mapi_mailstore/folders/email_unread_in_manifest_folder
  - functions/crates/lpe-exchange/src/mapi_mailstore/folders/hierarchy_sort_depth
  - functions/crates/lpe-exchange/src/mapi_mailstore/folders/mapi_parent_folder_id_for_folder_id
  - functions/crates/lpe-exchange/src/mapi_mailstore/folders/hierarchy_folder_sort_order
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/source_key_for_mailbox_folder
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_manifest_buffer_with_special_objects_and_final_state_with_folder_versions_and_commit_times_and_normal_message_facts
---

# Signature

`pub(crate) fn mapi_folder_id_for_mailbox(mailbox: &JmapMailbox, fallback: u64) -> u64`

# Calls

- [mapped_mapi_object_id](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/mapped_mapi_object_id.md)

# Called by

- [sync_state_object_ids](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/sync_state_object_ids.md)
- [sync_state_change_numbers](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/sync_state_change_numbers.md)
- [fast_transfer_top_folder_buffer_with_attachments](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/fast_transfer_top_folder_buffer_with_attachments.md)
- [write_fast_transfer_folder_content](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/write_fast_transfer_folder_content.md)
- [fast_transfer_child_mailboxes](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/fast_transfer_child_mailboxes.md)
- [fast_transfer_email_matches_folder](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/fast_transfer_email_matches_folder.md)
- [download_change_facts_with_normal_message_sync_facts](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/download_change_facts_with_normal_message_sync_facts.md)
- [mapi_folder_parent_id_for_mailbox](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/folders/mapi_folder_parent_id_for_mailbox.md)
- [mapi_folder_message_class](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/folders/mapi_folder_message_class.md)
- [mapi_folder_display_name](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/folders/mapi_folder_display_name.md)
- [mapi_folder_has_subfolders](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/folders/mapi_folder_has_subfolders.md)
- [email_unread_in_manifest_folder](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/folders/email_unread_in_manifest_folder.md)
- [hierarchy_sort_depth](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/folders/hierarchy_sort_depth.md)
- [mapi_parent_folder_id_for_folder_id](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/folders/mapi_parent_folder_id_for_folder_id.md)
- [hierarchy_folder_sort_order](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/folders/hierarchy_folder_sort_order.md)
- [source_key_for_mailbox_folder](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/source_key_for_mailbox_folder.md)
- [sync_manifest_buffer_with_special_objects_and_final_state_with_folder_versions_and_commit_times_and_normal_message_facts](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_manifest_buffer_with_special_objects_and_final_state_with_folder_versions_and_commit_times_and_normal_message_facts.md)