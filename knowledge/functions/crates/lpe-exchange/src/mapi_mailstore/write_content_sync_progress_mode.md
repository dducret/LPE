---
type: Rust Function
title: write_content_sync_progress_mode
resource: crates/lpe-exchange/src/mapi_mailstore.rs#L1289-L1334
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_mailstore/write_binary_property
  called_by:
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_manifest_buffer_with_special_objects_and_final_state_with_folder_versions_and_commit_times_and_normal_message_facts
---

# Signature

`fn write_content_sync_progress_mode( buffer: &mut Vec<u8>, messages: &[&JmapEmail], special_objects: &[&SpecialMessageSyncFact], )`

# Calls

- [write_binary_property](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/write_binary_property.md)

# Called by

- [sync_manifest_buffer_with_special_objects_and_final_state_with_folder_versions_and_commit_times_and_normal_message_facts](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_manifest_buffer_with_special_objects_and_final_state_with_folder_versions_and_commit_times_and_normal_message_facts.md)