---
type: Rust Function
title: write_utf16_property
resource: crates/lpe-exchange/src/mapi_mailstore.rs#L1360-L1369
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi_mailstore/write_fast_transfer_folder_properties
  - functions/crates/lpe-exchange/src/mapi_mailstore/write_fast_transfer_visible_recipients
  - functions/crates/lpe-exchange/src/mapi_mailstore/write_fast_transfer_attachments
  - functions/crates/lpe-exchange/src/mapi_mailstore/write_fast_transfer_embedded_message
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/write_fast_transfer_message_content
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/write_normalized_subject_property
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_manifest_buffer_with_special_objects_and_final_state_with_folder_versions_and_commit_times_and_normal_message_facts
  - functions/crates/lpe-exchange/src/mapi_mailstore/special_message/write_fast_transfer_special_message_content
---

# Signature

`fn write_utf16_property(buffer: &mut Vec<u8>, property_tag: u32, value: &str)`

# Called by

- [write_fast_transfer_folder_properties](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/write_fast_transfer_folder_properties.md)
- [write_fast_transfer_visible_recipients](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/write_fast_transfer_visible_recipients.md)
- [write_fast_transfer_attachments](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/write_fast_transfer_attachments.md)
- [write_fast_transfer_embedded_message](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/write_fast_transfer_embedded_message.md)
- [write_fast_transfer_message_content](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/write_fast_transfer_message_content.md)
- [write_normalized_subject_property](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/write_normalized_subject_property.md)
- [sync_manifest_buffer_with_special_objects_and_final_state_with_folder_versions_and_commit_times_and_normal_message_facts](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_manifest_buffer_with_special_objects_and_final_state_with_folder_versions_and_commit_times_and_normal_message_facts.md)
- [write_fast_transfer_special_message_content](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/special_message/write_fast_transfer_special_message_content.md)