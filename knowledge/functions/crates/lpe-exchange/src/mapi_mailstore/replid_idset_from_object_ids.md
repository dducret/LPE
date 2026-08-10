---
type: Rust Function
title: replid_idset_from_object_ids
resource: crates/lpe-exchange/src/mapi_mailstore.rs#L1444-L1457
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/identity/durable_object_id
  - functions/crates/lpe-exchange/src/mapi_mailstore/write_globset_ranges
  - functions/crates/lpe-exchange/src/mapi_mailstore/coalesced_ranges
  called_by:
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_manifest_buffer_with_special_objects_and_final_state_with_folder_versions_and_commit_times_and_normal_message_facts
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_download_emits_explicit_tombstone_absent_from_client_idset
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/deleted_idset_uses_replid_globset_ranges
---

# Signature

`fn replid_idset_from_object_ids(ids: &[u64]) -> Vec<u8>`

# Calls

- [durable_object_id](../../../../../functions/crates/lpe-exchange/src/mapi/identity/durable_object_id.md)
- [write_globset_ranges](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/write_globset_ranges.md)
- [coalesced_ranges](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/coalesced_ranges.md)

# Called by

- [sync_manifest_buffer_with_special_objects_and_final_state_with_folder_versions_and_commit_times_and_normal_message_facts](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_manifest_buffer_with_special_objects_and_final_state_with_folder_versions_and_commit_times_and_normal_message_facts.md)
- [hierarchy_download_emits_explicit_tombstone_absent_from_client_idset](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_download_emits_explicit_tombstone_absent_from_client_idset.md)
- [deleted_idset_uses_replid_globset_ranges](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/deleted_idset_uses_replid_globset_ranges.md)