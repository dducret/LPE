---
type: Rust Function
title: append_rop_sync_import_deletes
resource: crates/lpe-exchange/src/tests/mapi_over_http/sync.rs#L3-L18
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_microsoft_oxcfxics_4_2_2_message_delete_returns_transfer_state
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_sync_import_delete_and_read_state_use_canonical_store
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_sync_import_delete_ignores_transient_trash_artifact
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_sync_import_deletes_removes_fai_by_outlook_source_key
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_sync_import_deletes_removes_common_views_wlink_by_source_key_and_reloads
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_import_deletes_retry_ignores_online_unreserved_common_views_wlink
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_import_deletes_tombstones_reserved_unknown_common_views_wlink
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_import_deletes_prevalidates_common_views_batch_in_postgresql
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_import_deletes_rejects_unknown_flags_before_mutation
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_import_deletes_deduplicates_source_keys_before_mutation
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_sync_import_hard_delete_returns_failure_when_retention_blocks_delete
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_sync_import_soft_delete_moves_to_trash
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_sync_import_delete_from_trash_child_hard_deletes
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_import_deletes_prevalidates_hierarchy_batch_before_mutation
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_microsoft_oxcfxics_4_1_2_hierarchy_delete_returns_transfer_state
---

# Signature

`fn append_rop_sync_import_deletes( rops: &mut Vec<u8>, input_handle_index: u8, import_delete_flags: u8, object_ids: &[u64], )`

# Called by

- [mapi_over_http_microsoft_oxcfxics_4_2_2_message_delete_returns_transfer_state](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_microsoft_oxcfxics_4_2_2_message_delete_returns_transfer_state.md)
- [mapi_over_http_sync_import_delete_and_read_state_use_canonical_store](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_sync_import_delete_and_read_state_use_canonical_store.md)
- [mapi_over_http_sync_import_delete_ignores_transient_trash_artifact](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_sync_import_delete_ignores_transient_trash_artifact.md)
- [mapi_over_http_sync_import_deletes_removes_fai_by_outlook_source_key](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_sync_import_deletes_removes_fai_by_outlook_source_key.md)
- [mapi_over_http_sync_import_deletes_removes_common_views_wlink_by_source_key_and_reloads](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_sync_import_deletes_removes_common_views_wlink_by_source_key_and_reloads.md)
- [mapi_over_http_import_deletes_retry_ignores_online_unreserved_common_views_wlink](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_import_deletes_retry_ignores_online_unreserved_common_views_wlink.md)
- [mapi_over_http_import_deletes_tombstones_reserved_unknown_common_views_wlink](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_import_deletes_tombstones_reserved_unknown_common_views_wlink.md)
- [mapi_over_http_import_deletes_prevalidates_common_views_batch_in_postgresql](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_import_deletes_prevalidates_common_views_batch_in_postgresql.md)
- [mapi_over_http_import_deletes_rejects_unknown_flags_before_mutation](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_import_deletes_rejects_unknown_flags_before_mutation.md)
- [mapi_over_http_import_deletes_deduplicates_source_keys_before_mutation](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_import_deletes_deduplicates_source_keys_before_mutation.md)
- [mapi_over_http_sync_import_hard_delete_returns_failure_when_retention_blocks_delete](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_sync_import_hard_delete_returns_failure_when_retention_blocks_delete.md)
- [mapi_over_http_sync_import_soft_delete_moves_to_trash](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_sync_import_soft_delete_moves_to_trash.md)
- [mapi_over_http_sync_import_delete_from_trash_child_hard_deletes](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_sync_import_delete_from_trash_child_hard_deletes.md)
- [mapi_over_http_import_deletes_prevalidates_hierarchy_batch_before_mutation](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_import_deletes_prevalidates_hierarchy_batch_before_mutation.md)
- [mapi_over_http_microsoft_oxcfxics_4_1_2_hierarchy_delete_returns_transfer_state](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_microsoft_oxcfxics_4_1_2_hierarchy_delete_returns_transfer_state.md)