---
type: Rust Function
title: write_i32
resource: crates/lpe-exchange/src/mapi_mailstore.rs#L1264-L1266
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi_mailstore/write_i32_property
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_manifest_buffer_with_special_objects_and_final_state_with_folder_versions_and_commit_times_and_normal_message_facts
  - functions/crates/lpe-exchange/src/mapi_mailstore/special_message/write_special_message_property
---

# Signature

`fn write_i32(buffer: &mut Vec<u8>, value: i32)`

# Called by

- [write_i32_property](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/write_i32_property.md)
- [sync_manifest_buffer_with_special_objects_and_final_state_with_folder_versions_and_commit_times_and_normal_message_facts](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_manifest_buffer_with_special_objects_and_final_state_with_folder_versions_and_commit_times_and_normal_message_facts.md)
- [write_special_message_property](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/special_message/write_special_message_property.md)