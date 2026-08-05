---
type: Rust Function
title: normal_message_sync_source_key_for_fact
resource: crates/lpe-exchange/src/mapi_mailstore/manifest.rs#L257-L266
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/source_key_for_store_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi_mailstore/client_state/download_change_facts_with_normal_message_sync_facts
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_state_token_with_special_objects_and_normal_message_facts
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_manifest_buffer_with_special_objects_and_final_state_with_folder_versions_and_commit_times_and_normal_message_facts
---

# Signature

`pub(crate) fn normal_message_sync_source_key_for_fact( fact: &NormalMessageSyncFact, sync_flags: u16, ) -> Vec<u8>`

# Calls

- [source_key_for_store_id](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/source_key_for_store_id.md)

# Called by

- [download_change_facts_with_normal_message_sync_facts](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/download_change_facts_with_normal_message_sync_facts.md)
- [sync_state_token_with_special_objects_and_normal_message_facts](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_state_token_with_special_objects_and_normal_message_facts.md)
- [sync_manifest_buffer_with_special_objects_and_final_state_with_folder_versions_and_commit_times_and_normal_message_facts](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_manifest_buffer_with_special_objects_and_final_state_with_folder_versions_and_commit_times_and_normal_message_facts.md)