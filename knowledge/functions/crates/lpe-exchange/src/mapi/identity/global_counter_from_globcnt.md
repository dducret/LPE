---
type: Rust Function
title: global_counter_from_globcnt
resource: crates/lpe-exchange/src/mapi/identity.rs#L614-L620
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/default_folders/default_view_entry_id_target_for_debug
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/import_message_change_conflicts_with_current_pcl
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/parse_predecessor_change_list_entries
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/source_key_global_counter
  - functions/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec/object_id_from_source_key
  - functions/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec/object_id_from_long_term_id
  - functions/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec/object_id_from_folder_entry_id
  - functions/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec/object_ids_from_message_entry_id
  - functions/crates/lpe-exchange/src/mapi/identity/raw_object_id_from_wire_id
  - functions/crates/lpe-exchange/src/mapi/identity/raw_object_id_from_trailing_replid_wire_id
  - functions/crates/lpe-exchange/src/mapi/identity/object_id_from_long_term_id_with_replica_guids
  - functions/crates/lpe-exchange/src/mapi/identity/raw_object_id_from_folder_entry_id
  - functions/crates/lpe-exchange/src/mapi/identity/stale_special_folder_object_id_from_long_term_id
  - functions/crates/lpe-exchange/src/mapi/identity/raw_object_ids_from_message_entry_id
  - functions/crates/lpe-exchange/src/mapi/identity/raw_object_id_from_source_key
  - functions/crates/lpe-exchange/src/mapi/rop/debug/default_view_message_entry_id_target
  - functions/crates/lpe-exchange/src/mapi/rop/object_ids/stale_special_folder_object_id_from_long_term_id
  - functions/crates/lpe-exchange/src/mapi/rop/object_ids/stale_special_folder_object_id_from_short_id
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/local_replica_deleted_ranges
  - functions/crates/lpe-exchange/src/mapi_mailstore/client_state/source_key_replica_counter
  - functions/crates/lpe-exchange/src/mapi_mailstore/client_state/decode_globset_range_prefix
  - functions/crates/lpe-exchange/src/mapi_mailstore/client_state/globcnt_suffix_range
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/globcnt_slice_to_u64
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/counter_from_xid
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/change_number_for_store_id
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/local_replica_ids/mapi_over_http_get_local_replica_ids_reserves_full_outlook_range_in_postgresql
  - functions/crates/lpe-exchange/src/tests/strict_decode_content_sync_stream
---

# Signature

`pub(crate) fn global_counter_from_globcnt(bytes: &[u8]) -> Option<u64>`

# Called by

- [default_view_entry_id_target_for_debug](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/default_folders/default_view_entry_id_target_for_debug.md)
- [import_message_change_conflicts_with_current_pcl](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/import_message_change_conflicts_with_current_pcl.md)
- [parse_predecessor_change_list_entries](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/parse_predecessor_change_list_entries.md)
- [source_key_global_counter](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/source_key_global_counter.md)
- [object_id_from_source_key](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec/object_id_from_source_key.md)
- [object_id_from_long_term_id](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec/object_id_from_long_term_id.md)
- [object_id_from_folder_entry_id](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec/object_id_from_folder_entry_id.md)
- [object_ids_from_message_entry_id](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec/object_ids_from_message_entry_id.md)
- [raw_object_id_from_wire_id](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/raw_object_id_from_wire_id.md)
- [raw_object_id_from_trailing_replid_wire_id](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/raw_object_id_from_trailing_replid_wire_id.md)
- [object_id_from_long_term_id_with_replica_guids](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/object_id_from_long_term_id_with_replica_guids.md)
- [raw_object_id_from_folder_entry_id](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/raw_object_id_from_folder_entry_id.md)
- [stale_special_folder_object_id_from_long_term_id](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/stale_special_folder_object_id_from_long_term_id.md)
- [raw_object_ids_from_message_entry_id](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/raw_object_ids_from_message_entry_id.md)
- [raw_object_id_from_source_key](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/raw_object_id_from_source_key.md)
- [default_view_message_entry_id_target](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/default_view_message_entry_id_target.md)
- [stale_special_folder_object_id_from_long_term_id](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/object_ids/stale_special_folder_object_id_from_long_term_id.md)
- [stale_special_folder_object_id_from_short_id](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/object_ids/stale_special_folder_object_id_from_short_id.md)
- [local_replica_deleted_ranges](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/local_replica_deleted_ranges.md)
- [source_key_replica_counter](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/source_key_replica_counter.md)
- [decode_globset_range_prefix](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/decode_globset_range_prefix.md)
- [globcnt_suffix_range](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/globcnt_suffix_range.md)
- [globcnt_slice_to_u64](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/globcnt_slice_to_u64.md)
- [counter_from_xid](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/counter_from_xid.md)
- [change_number_for_store_id](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/change_number_for_store_id.md)
- [mapi_over_http_get_local_replica_ids_reserves_full_outlook_range_in_postgresql](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/local_replica_ids/mapi_over_http_get_local_replica_ids_reserves_full_outlook_range_in_postgresql.md)
- [strict_decode_content_sync_stream](../../../../../../functions/crates/lpe-exchange/src/tests/strict_decode_content_sync_stream.md)