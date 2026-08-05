---
type: Rust Function
title: canonical_hierarchy_change_number
resource: crates/lpe-exchange/src/mapi_mailstore.rs#L467-L472
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/canonical_folder_change_number
  called_by:
  - functions/crates/lpe-exchange/src/mapi_mailstore/sync_state_change_numbers
  - functions/crates/lpe-exchange/src/mapi_mailstore/client_state/download_change_facts_with_normal_message_sync_facts
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_manifest_buffer_with_special_objects_and_final_state_with_folder_versions_and_commit_times_and_normal_message_facts
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_download_selection_uses_uploaded_empty_client_state
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_download_selection_preserves_foreign_replica_and_uses_local_cnset
---

# Signature

`pub(crate) fn canonical_hierarchy_change_number( _sync_root_folder_id: u64, mailbox: &JmapMailbox, ) -> u64`

# Calls

- [canonical_folder_change_number](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/canonical_folder_change_number.md)

# Called by

- [sync_state_change_numbers](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/sync_state_change_numbers.md)
- [download_change_facts_with_normal_message_sync_facts](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/download_change_facts_with_normal_message_sync_facts.md)
- [sync_manifest_buffer_with_special_objects_and_final_state_with_folder_versions_and_commit_times_and_normal_message_facts](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_manifest_buffer_with_special_objects_and_final_state_with_folder_versions_and_commit_times_and_normal_message_facts.md)
- [hierarchy_download_selection_uses_uploaded_empty_client_state](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_download_selection_uses_uploaded_empty_client_state.md)
- [hierarchy_download_selection_preserves_foreign_replica_and_uses_local_cnset](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_download_selection_preserves_foreign_replica_and_uses_local_cnset.md)