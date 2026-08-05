---
type: Rust Function
title: content_sync_message_children
resource: crates/lpe-exchange/src/mapi_mailstore.rs#L316-L339
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_mailstore/content_property_in_scope
  called_by:
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_manifest_buffer_with_special_objects_and_final_state_with_folder_versions_and_commit_times_and_normal_message_facts
---

# Signature

`fn content_sync_message_children( sync_type: u8, sync_flags: u16, sync_property_tags: &[u32], ) -> FastTransferMessageChildren`

# Calls

- [content_property_in_scope](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/content_property_in_scope.md)

# Called by

- [sync_manifest_buffer_with_special_objects_and_final_state_with_folder_versions_and_commit_times_and_normal_message_facts](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_manifest_buffer_with_special_objects_and_final_state_with_folder_versions_and_commit_times_and_normal_message_facts.md)