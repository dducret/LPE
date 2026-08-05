---
type: Rust Function
title: write_bool_property
resource: crates/lpe-exchange/src/mapi_mailstore.rs#L1346-L1349
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi_mailstore/write_fast_transfer_folder_properties
  - functions/crates/lpe-exchange/src/mapi_mailstore/write_fast_transfer_attachments
  - functions/crates/lpe-exchange/src/mapi_mailstore/write_content_sync_progress_per_message
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_manifest_buffer_with_special_objects_and_final_state_with_folder_versions_and_commit_times_and_normal_message_facts
  - functions/crates/lpe-exchange/src/mapi_mailstore/special_message/write_fast_transfer_special_message_content
---

# Signature

`fn write_bool_property(buffer: &mut Vec<u8>, property_tag: u32, value: bool)`

# Called by

- [write_fast_transfer_folder_properties](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/write_fast_transfer_folder_properties.md)
- [write_fast_transfer_attachments](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/write_fast_transfer_attachments.md)
- [write_content_sync_progress_per_message](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/write_content_sync_progress_per_message.md)
- [sync_manifest_buffer_with_special_objects_and_final_state_with_folder_versions_and_commit_times_and_normal_message_facts](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_manifest_buffer_with_special_objects_and_final_state_with_folder_versions_and_commit_times_and_normal_message_facts.md)
- [write_fast_transfer_special_message_content](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/special_message/write_fast_transfer_special_message_content.md)