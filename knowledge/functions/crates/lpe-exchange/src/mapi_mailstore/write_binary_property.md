---
type: Rust Function
title: write_binary_property
resource: crates/lpe-exchange/src/mapi_mailstore.rs#L1354-L1358
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi_mailstore/sync_state_stream_from_raw_properties
  - functions/crates/lpe-exchange/src/mapi_mailstore/upload_sync_state_stream_from_raw_properties
  - functions/crates/lpe-exchange/src/mapi_mailstore/final_sync_state_stream_with_cnsets
  - functions/crates/lpe-exchange/src/mapi_mailstore/write_fast_transfer_attachments
  - functions/crates/lpe-exchange/src/mapi_mailstore/write_content_sync_progress_mode
  - functions/crates/lpe-exchange/src/mapi_mailstore/client_state/write_selected_progress_mode
  - functions/crates/lpe-exchange/src/mapi_mailstore/client_state/write_replid_idset_property
  - functions/crates/lpe-exchange/src/mapi_mailstore/client_state/write_state
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/write_fast_transfer_message_content
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_manifest_buffer_with_special_objects_and_final_state_with_folder_versions_and_commit_times_and_normal_message_facts
  - functions/crates/lpe-exchange/src/mapi_mailstore/special_message/write_fast_transfer_special_message_content
---

# Signature

`fn write_binary_property(buffer: &mut Vec<u8>, property_tag: u32, value: &[u8])`

# Called by

- [sync_state_stream_from_raw_properties](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/sync_state_stream_from_raw_properties.md)
- [upload_sync_state_stream_from_raw_properties](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/upload_sync_state_stream_from_raw_properties.md)
- [final_sync_state_stream_with_cnsets](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/final_sync_state_stream_with_cnsets.md)
- [write_fast_transfer_attachments](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/write_fast_transfer_attachments.md)
- [write_content_sync_progress_mode](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/write_content_sync_progress_mode.md)
- [write_selected_progress_mode](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/write_selected_progress_mode.md)
- [write_replid_idset_property](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/write_replid_idset_property.md)
- [write_state](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/write_state.md)
- [write_fast_transfer_message_content](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/write_fast_transfer_message_content.md)
- [sync_manifest_buffer_with_special_objects_and_final_state_with_folder_versions_and_commit_times_and_normal_message_facts](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_manifest_buffer_with_special_objects_and_final_state_with_folder_versions_and_commit_times_and_normal_message_facts.md)
- [write_fast_transfer_special_message_content](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/special_message/write_fast_transfer_special_message_content.md)