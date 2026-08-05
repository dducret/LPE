---
type: Rust Function
title: folder_content_counts
resource: crates/lpe-exchange/src/mapi_mailstore/folders.rs#L161-L205
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_mailstore/folders/email_unread_in_manifest_folder
  called_by:
  - functions/crates/lpe-exchange/src/mapi_mailstore/write_fast_transfer_folder_properties
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_manifest_buffer_with_special_objects_and_final_state_with_folder_versions_and_commit_times_and_normal_message_facts
---

# Signature

`pub(crate) fn folder_content_counts( folder_id: u64, mailbox: &JmapMailbox, mailboxes: &[JmapMailbox], aggregate_emails: &[JmapEmail], ) -> (i32, i32, &'static str)`

# Calls

- [email_unread_in_manifest_folder](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/folders/email_unread_in_manifest_folder.md)

# Called by

- [write_fast_transfer_folder_properties](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/write_fast_transfer_folder_properties.md)
- [sync_manifest_buffer_with_special_objects_and_final_state_with_folder_versions_and_commit_times_and_normal_message_facts](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_manifest_buffer_with_special_objects_and_final_state_with_folder_versions_and_commit_times_and_normal_message_facts.md)