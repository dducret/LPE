---
type: Rust Function
title: hierarchy_entry_id_mailbox_guid
resource: crates/lpe-exchange/src/mapi_mailstore/folders.rs#L100-L108
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_manifest_buffer_with_special_objects_and_final_state_with_folder_versions_and_commit_times_and_normal_message_facts
---

# Signature

`pub(crate) fn hierarchy_entry_id_mailbox_guid( mailbox: &JmapMailbox, fallback_mailbox_guid: Uuid, ) -> Uuid`

# Called by

- [sync_manifest_buffer_with_special_objects_and_final_state_with_folder_versions_and_commit_times_and_normal_message_facts](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_manifest_buffer_with_special_objects_and_final_state_with_folder_versions_and_commit_times_and_normal_message_facts.md)