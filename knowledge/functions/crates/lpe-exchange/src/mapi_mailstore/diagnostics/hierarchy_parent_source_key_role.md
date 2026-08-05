---
type: Rust Function
title: hierarchy_parent_source_key_role
resource: crates/lpe-exchange/src/mapi_mailstore/diagnostics.rs#L15-L31
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_manifest_buffer_with_special_objects_and_final_state_with_folder_versions_and_commit_times_and_normal_message_facts
---

# Signature

`pub(crate) fn hierarchy_parent_source_key_role( parent_folder_id: u64, sync_root_folder_id: u64, parent_source_key_empty: bool, ) -> &'static str`

# Called by

- [sync_manifest_buffer_with_special_objects_and_final_state_with_folder_versions_and_commit_times_and_normal_message_facts](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_manifest_buffer_with_special_objects_and_final_state_with_folder_versions_and_commit_times_and_normal_message_facts.md)