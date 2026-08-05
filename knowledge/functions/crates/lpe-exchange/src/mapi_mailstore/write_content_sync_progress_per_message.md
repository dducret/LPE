---
type: Rust Function
title: write_content_sync_progress_per_message
resource: crates/lpe-exchange/src/mapi_mailstore.rs#L1336-L1344
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_mailstore/write_i32_property
  - functions/crates/lpe-exchange/src/mapi_mailstore/write_bool_property
  called_by:
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_manifest_buffer_with_special_objects_and_final_state_with_folder_versions_and_commit_times_and_normal_message_facts
---

# Signature

`fn write_content_sync_progress_per_message( buffer: &mut Vec<u8>, message_size: i32, associated: bool, )`

# Calls

- [write_i32_property](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/write_i32_property.md)
- [write_bool_property](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/write_bool_property.md)

# Called by

- [sync_manifest_buffer_with_special_objects_and_final_state_with_folder_versions_and_commit_times_and_normal_message_facts](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_manifest_buffer_with_special_objects_and_final_state_with_folder_versions_and_commit_times_and_normal_message_facts.md)