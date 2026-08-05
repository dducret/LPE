---
type: Rust Function
title: write_normalized_subject_property
resource: crates/lpe-exchange/src/mapi_mailstore/manifest.rs#L199-L207
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_mailstore/write_utf16_property
  - functions/crates/lpe-exchange/src/mapi_mailstore/write_string8_property
  called_by:
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_manifest_buffer_with_special_objects_and_final_state_with_folder_versions_and_commit_times_and_normal_message_facts
---

# Signature

`fn write_normalized_subject_property(buffer: &mut Vec<u8>, property_tag: u32, subject: &str)`

# Calls

- [write_utf16_property](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/write_utf16_property.md)
- [write_string8_property](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/write_string8_property.md)

# Called by

- [sync_manifest_buffer_with_special_objects_and_final_state_with_folder_versions_and_commit_times_and_normal_message_facts](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_manifest_buffer_with_special_objects_and_final_state_with_folder_versions_and_commit_times_and_normal_message_facts.md)