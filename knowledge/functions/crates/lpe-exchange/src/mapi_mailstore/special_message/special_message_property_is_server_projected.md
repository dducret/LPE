---
type: Rust Function
title: special_message_property_is_server_projected
resource: crates/lpe-exchange/src/mapi_mailstore/special_message.rs#L198-L203
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_manifest_buffer_with_special_objects_and_final_state_with_folder_versions_and_commit_times_and_normal_message_facts
  - functions/crates/lpe-exchange/src/mapi_mailstore/special_message/write_fast_transfer_special_message_content
---

# Signature

`pub(super) fn special_message_property_is_server_projected(property_tag: u32) -> bool`

# Called by

- [sync_manifest_buffer_with_special_objects_and_final_state_with_folder_versions_and_commit_times_and_normal_message_facts](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_manifest_buffer_with_special_objects_and_final_state_with_folder_versions_and_commit_times_and_normal_message_facts.md)
- [write_fast_transfer_special_message_content](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/special_message/write_fast_transfer_special_message_content.md)