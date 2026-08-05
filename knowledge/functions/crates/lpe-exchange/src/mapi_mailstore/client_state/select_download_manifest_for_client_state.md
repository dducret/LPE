---
type: Rust Function
title: select_download_manifest_for_client_state
resource: crates/lpe-exchange/src/mapi_mailstore/client_state.rs#L357-L509
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_mailstore/client_state/parse_standalone_state
  - functions/crates/lpe-exchange/src/mapi_mailstore/client_state/parse_manifest
  - functions/crates/lpe-exchange/src/mapi_mailstore/client_state/source_key_replica_counter
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi_mailstore/client_state/ReplicaCounterSets/local
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi_mailstore/client_state/write_selected_progress_mode
  - functions/crates/lpe-core/src/sieve/Parser/expect
  - functions/crates/lpe-exchange/src/mapi_mailstore/client_state/ReplicaCounterSets/local_mut
  - functions/crates/lpe-exchange/src/mapi_mailstore/client_state/CounterSet/difference
  - functions/crates/lpe-exchange/src/mapi_mailstore/client_state/CounterSet/intersection
  - functions/crates/lpe-exchange/src/mapi_mailstore/client_state/CounterSet/union_with
  - functions/crates/lpe-exchange/src/mapi_mailstore/client_state/write_deletion_section
  - functions/crates/lpe-exchange/src/mapi_mailstore/client_state/write_state
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_get_buffer/append_fast_transfer_source_get_buffer_response
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_download_keeps_imported_change_key_and_predecessor_lineage
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/sync_manifest_serializes_content_message_header_in_fixed_order
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/content_download_selection_emits_unseen_durable_inbox_change_after_completed_sync
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_download_selection_uses_uploaded_empty_client_state
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_download_selection_preserves_foreign_replica_and_uses_local_cnset
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_download_no_deletions_keeps_missing_id_without_tombstone
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_download_emits_explicit_tombstone_absent_from_client_idset
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_download_rejects_malformed_client_globset
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/fai_foreign_source_key_identity_is_used_by_selected_and_full_idset_given
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/normal_message_no_foreign_identifiers_uses_local_source_key_for_selection
---

# Signature

`pub(crate) fn select_download_manifest_for_client_state( sync_type: u8, sync_flags: u16, full_manifest: &[u8], client_state: &[u8], change_facts: &[DownloadChangeFact], resident_hierarchy_alias_counters: &[u64], ) -> Result<(Vec<u8>, Vec<u8>), String>`

# Calls

- [parse_standalone_state](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/parse_standalone_state.md)
- [parse_manifest](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/parse_manifest.md)
- [source_key_replica_counter](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/source_key_replica_counter.md)
- [get](../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [local](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/ReplicaCounterSets/local.md)
- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [write_selected_progress_mode](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/write_selected_progress_mode.md)
- [expect](../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)
- [local_mut](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/ReplicaCounterSets/local_mut.md)
- [difference](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/CounterSet/difference.md)
- [intersection](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/CounterSet/intersection.md)
- [union_with](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/CounterSet/union_with.md)
- [write_deletion_section](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/write_deletion_section.md)
- [write_state](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/write_state.md)

# Called by

- [append_fast_transfer_source_get_buffer_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_get_buffer/append_fast_transfer_source_get_buffer_response.md)
- [hierarchy_download_keeps_imported_change_key_and_predecessor_lineage](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_download_keeps_imported_change_key_and_predecessor_lineage.md)
- [sync_manifest_serializes_content_message_header_in_fixed_order](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/sync_manifest_serializes_content_message_header_in_fixed_order.md)
- [content_download_selection_emits_unseen_durable_inbox_change_after_completed_sync](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/content_download_selection_emits_unseen_durable_inbox_change_after_completed_sync.md)
- [hierarchy_download_selection_uses_uploaded_empty_client_state](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_download_selection_uses_uploaded_empty_client_state.md)
- [hierarchy_download_selection_preserves_foreign_replica_and_uses_local_cnset](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_download_selection_preserves_foreign_replica_and_uses_local_cnset.md)
- [hierarchy_download_no_deletions_keeps_missing_id_without_tombstone](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_download_no_deletions_keeps_missing_id_without_tombstone.md)
- [hierarchy_download_emits_explicit_tombstone_absent_from_client_idset](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_download_emits_explicit_tombstone_absent_from_client_idset.md)
- [hierarchy_download_rejects_malformed_client_globset](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_download_rejects_malformed_client_globset.md)
- [fai_foreign_source_key_identity_is_used_by_selected_and_full_idset_given](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/fai_foreign_source_key_identity_is_used_by_selected_and_full_idset_given.md)
- [normal_message_no_foreign_identifiers_uses_local_source_key_for_selection](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/normal_message_no_foreign_identifiers_uses_local_source_key_for_selection.md)