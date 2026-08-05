---
type: Rust Function
title: property_tag_excluded
resource: crates/lpe-exchange/src/mapi_mailstore.rs#L276-L278
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_mailstore/property_tag_requested
  called_by:
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_manifest_buffer_with_special_objects_and_final_state_with_folder_versions_and_commit_times_and_normal_message_facts
---

# Signature

`fn property_tag_excluded(excluded_property_tags: &[u32], property_tag: u32) -> bool`

# Calls

- [property_tag_requested](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/property_tag_requested.md)

# Called by

- [sync_manifest_buffer_with_special_objects_and_final_state_with_folder_versions_and_commit_times_and_normal_message_facts](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_manifest_buffer_with_special_objects_and_final_state_with_folder_versions_and_commit_times_and_normal_message_facts.md)