---
type: Rust Function
title: sync_manifest_buffer_with_special_objects_and_final_state_with_folder_versions
resource: crates/lpe-exchange/src/mapi_mailstore/manifest.rs#L665-L710
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_manifest_buffer_with_special_objects_and_final_state_with_folder_versions_and_commit_times
  called_by:
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_manifest_buffer_with_special_objects_and_final_state
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_download_keeps_imported_change_key_and_predecessor_lineage
---

# Signature

`pub(crate) fn sync_manifest_buffer_with_special_objects_and_final_state_with_folder_versions( mailbox_guid: Uuid, sync_type: u8, sync_flags: u16, sync_extra_flags: u32, sync_property_tags: &[u32], folder_id: u64, mailboxes: &[JmapMailbox], emails: &[JmapEmail], attachment_facts: &[MessageAttachmentSyncFacts], special_objects: &[SpecialMessageSyncFact], deleted_message_ids: &[u64], parent_context_mailboxes: &[JmapMailbox], state_mailboxes: &[JmapMailbox], state_emails: &[JmapEmail], state_attachment_facts: &[MessageAttachmentSyncFacts], state_special_objects: &[SpecialMessageSyncFact], aggregate_emails: &[JmapEmail], aggregate_attachment_facts: &[MessageAttachmentSyncFacts], folder_versions: &[crate::mapi_store::MapiFolderVersion], final_change_sequence: u64, ) -> Vec<u8>`

# Calls

- [sync_manifest_buffer_with_special_objects_and_final_state_with_folder_versions_and_commit_times](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_manifest_buffer_with_special_objects_and_final_state_with_folder_versions_and_commit_times.md)

# Called by

- [sync_manifest_buffer_with_special_objects_and_final_state](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_manifest_buffer_with_special_objects_and_final_state.md)
- [hierarchy_download_keeps_imported_change_key_and_predecessor_lineage](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_download_keeps_imported_change_key_and_predecessor_lineage.md)