---
type: Rust Function
title: write_fast_transfer_message_children
resource: crates/lpe-exchange/src/mapi_mailstore.rs#L823-L851
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_mailstore/write_i32_property
  - functions/crates/lpe-exchange/src/mapi_mailstore/write_fast_transfer_visible_recipients
  - functions/crates/lpe-exchange/src/mapi_mailstore/write_fast_transfer_attachments
  called_by:
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/write_fast_transfer_message_content
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_manifest_buffer_with_special_objects_and_final_state_with_folder_versions_and_commit_times_and_normal_message_facts
---

# Signature

`fn write_fast_transfer_message_children( buffer: &mut Vec<u8>, message_children: FastTransferMessageChildren, email: Option<&JmapEmail>, attachments: &[AttachmentSyncFact], )`

# Calls

- [write_i32_property](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/write_i32_property.md)
- [write_fast_transfer_visible_recipients](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/write_fast_transfer_visible_recipients.md)
- [write_fast_transfer_attachments](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/write_fast_transfer_attachments.md)

# Called by

- [write_fast_transfer_message_content](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/write_fast_transfer_message_content.md)
- [sync_manifest_buffer_with_special_objects_and_final_state_with_folder_versions_and_commit_times_and_normal_message_facts](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_manifest_buffer_with_special_objects_and_final_state_with_folder_versions_and_commit_times_and_normal_message_facts.md)