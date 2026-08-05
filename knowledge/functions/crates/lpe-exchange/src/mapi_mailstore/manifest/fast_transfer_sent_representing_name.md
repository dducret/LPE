---
type: Rust Function
title: fast_transfer_sent_representing_name
resource: crates/lpe-exchange/src/mapi_mailstore/manifest.rs#L96-L98
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/write_fast_transfer_message_content
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_manifest_buffer_with_special_objects_and_final_state_with_folder_versions_and_commit_times_and_normal_message_facts
---

# Signature

`fn fast_transfer_sent_representing_name(email: &JmapEmail) -> &str`

# Called by

- [write_fast_transfer_message_content](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/write_fast_transfer_message_content.md)
- [sync_manifest_buffer_with_special_objects_and_final_state_with_folder_versions_and_commit_times_and_normal_message_facts](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_manifest_buffer_with_special_objects_and_final_state_with_folder_versions_and_commit_times_and_normal_message_facts.md)