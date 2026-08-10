---
type: Rust Function
title: special_message_sync_parent_source_key
resource: crates/lpe-exchange/src/mapi_mailstore/special_message.rs#L103-L112
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/source_key_for_store_id
  - functions/crates/lpe-exchange/src/mapi_mailstore/special_message/special_message_parent_source_key
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/special_folders/log_calendar_special_sync_objects
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_manifest_buffer_with_special_objects_and_final_state_with_folder_versions_and_commit_times_and_normal_message_facts
---

# Signature

`pub(crate) fn special_message_sync_parent_source_key( object: &SpecialMessageSyncFact, sync_flags: u16, ) -> Vec<u8>`

# Calls

- [source_key_for_store_id](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/source_key_for_store_id.md)
- [special_message_parent_source_key](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/special_message/special_message_parent_source_key.md)

# Called by

- [log_calendar_special_sync_objects](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/special_folders/log_calendar_special_sync_objects.md)
- [sync_manifest_buffer_with_special_objects_and_final_state_with_folder_versions_and_commit_times_and_normal_message_facts](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_manifest_buffer_with_special_objects_and_final_state_with_folder_versions_and_commit_times_and_normal_message_facts.md)