---
type: Rust Function
title: local_commit_time_max
resource: crates/lpe-exchange/src/mapi_mailstore.rs#L383-L411
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_mailstore/folders/email_unread_in_manifest_folder
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/filetime_from_change_number
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/canonical_message_change_number_with_attachments
  called_by:
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_manifest_buffer_with_special_objects_and_final_state_with_folder_versions_and_commit_times_and_normal_message_facts
---

# Signature

`fn local_commit_time_max( folder_id: u64, mailboxes: &[JmapMailbox], emails: &[JmapEmail], attachment_facts: &[MessageAttachmentSyncFacts], folder_commit_times: &[(u64, u64)], ) -> u64`

# Calls

- [email_unread_in_manifest_folder](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/folders/email_unread_in_manifest_folder.md)
- [filetime_from_change_number](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/filetime_from_change_number.md)
- [canonical_message_change_number_with_attachments](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/canonical_message_change_number_with_attachments.md)

# Called by

- [sync_manifest_buffer_with_special_objects_and_final_state_with_folder_versions_and_commit_times_and_normal_message_facts](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_manifest_buffer_with_special_objects_and_final_state_with_folder_versions_and_commit_times_and_normal_message_facts.md)