---
type: Rust Function
title: strict_decode_hierarchy_sync_stream
resource: crates/lpe-exchange/src/tests/mod.rs#L12888-L13062
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/read_strict_u32
  - functions/crates/lpe-exchange/src/tests/strict_hierarchy_marker
  - functions/crates/lpe-exchange/src/tests/strict_finish_folder_change
  - functions/crates/lpe-exchange/src/tests/strict_parse_fast_transfer_property
  - functions/crates/lpe-exchange/src/tests/strict_record_folder_property
  - functions/crates/lpe-exchange/src/tests/strict_validate_replguid_globset
  - functions/crates/lpe-exchange/src/tests/strict_validate_replid_globset
  - functions/crates/lpe-exchange/src/tests/strict_validate_store_xid
  - functions/crates/lpe-exchange/src/tests/strict_replguid_globset_contains_counter
  called_by:
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_hierarchy_sync_keeps_direct_reminders_projection_out_of_normal_hierarchy
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_hierarchy_sync_projects_outlook_special_folder_display_names
  - functions/crates/lpe-exchange/src/tests/strict_hierarchy_sync_transfer_from_response
  - functions/crates/lpe-exchange/src/tests/strict_hierarchy_decoder_rejects_child_before_parent
  - functions/crates/lpe-exchange/src/tests/strict_hierarchy_decoder_accepts_deletion_only_delta
  - functions/crates/lpe-exchange/src/tests/strict_hierarchy_decoder_rejects_empty_deletions_section
  - functions/crates/lpe-exchange/src/tests/strict_hierarchy_decoder_rejects_missing_final_cnset
  - functions/crates/lpe-exchange/src/tests/strict_hierarchy_decoder_rejects_folder_change_after_final_state
  - functions/crates/lpe-exchange/src/tests/strict_hierarchy_decoder_rejects_duplicate_folder_property
  - functions/crates/lpe-exchange/src/tests/strict_hierarchy_decoder_rejects_message_change_in_hierarchy_stream
  - functions/crates/lpe-exchange/src/tests/strict_hierarchy_decoder_rejects_final_state_missing_folder_id
  - functions/crates/lpe-exchange/src/tests/strict_hierarchy_decoder_rejects_non_replguid_final_state
---

# Signature

`fn strict_decode_hierarchy_sync_stream(bytes: &[u8]) -> Result<StrictHierarchySyncStream, String>`

# Calls

- [read_strict_u32](../../../../../functions/crates/lpe-exchange/src/tests/read_strict_u32.md)
- [strict_hierarchy_marker](../../../../../functions/crates/lpe-exchange/src/tests/strict_hierarchy_marker.md)
- [strict_finish_folder_change](../../../../../functions/crates/lpe-exchange/src/tests/strict_finish_folder_change.md)
- [strict_parse_fast_transfer_property](../../../../../functions/crates/lpe-exchange/src/tests/strict_parse_fast_transfer_property.md)
- [strict_record_folder_property](../../../../../functions/crates/lpe-exchange/src/tests/strict_record_folder_property.md)
- [strict_validate_replguid_globset](../../../../../functions/crates/lpe-exchange/src/tests/strict_validate_replguid_globset.md)
- [strict_validate_replid_globset](../../../../../functions/crates/lpe-exchange/src/tests/strict_validate_replid_globset.md)
- [strict_validate_store_xid](../../../../../functions/crates/lpe-exchange/src/tests/strict_validate_store_xid.md)
- [strict_replguid_globset_contains_counter](../../../../../functions/crates/lpe-exchange/src/tests/strict_replguid_globset_contains_counter.md)

# Called by

- [mapi_hierarchy_sync_keeps_direct_reminders_projection_out_of_normal_hierarchy](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_hierarchy_sync_keeps_direct_reminders_projection_out_of_normal_hierarchy.md)
- [mapi_hierarchy_sync_projects_outlook_special_folder_display_names](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_hierarchy_sync_projects_outlook_special_folder_display_names.md)
- [strict_hierarchy_sync_transfer_from_response](../../../../../functions/crates/lpe-exchange/src/tests/strict_hierarchy_sync_transfer_from_response.md)
- [strict_hierarchy_decoder_rejects_child_before_parent](../../../../../functions/crates/lpe-exchange/src/tests/strict_hierarchy_decoder_rejects_child_before_parent.md)
- [strict_hierarchy_decoder_accepts_deletion_only_delta](../../../../../functions/crates/lpe-exchange/src/tests/strict_hierarchy_decoder_accepts_deletion_only_delta.md)
- [strict_hierarchy_decoder_rejects_empty_deletions_section](../../../../../functions/crates/lpe-exchange/src/tests/strict_hierarchy_decoder_rejects_empty_deletions_section.md)
- [strict_hierarchy_decoder_rejects_missing_final_cnset](../../../../../functions/crates/lpe-exchange/src/tests/strict_hierarchy_decoder_rejects_missing_final_cnset.md)
- [strict_hierarchy_decoder_rejects_folder_change_after_final_state](../../../../../functions/crates/lpe-exchange/src/tests/strict_hierarchy_decoder_rejects_folder_change_after_final_state.md)
- [strict_hierarchy_decoder_rejects_duplicate_folder_property](../../../../../functions/crates/lpe-exchange/src/tests/strict_hierarchy_decoder_rejects_duplicate_folder_property.md)
- [strict_hierarchy_decoder_rejects_message_change_in_hierarchy_stream](../../../../../functions/crates/lpe-exchange/src/tests/strict_hierarchy_decoder_rejects_message_change_in_hierarchy_stream.md)
- [strict_hierarchy_decoder_rejects_final_state_missing_folder_id](../../../../../functions/crates/lpe-exchange/src/tests/strict_hierarchy_decoder_rejects_final_state_missing_folder_id.md)
- [strict_hierarchy_decoder_rejects_non_replguid_final_state](../../../../../functions/crates/lpe-exchange/src/tests/strict_hierarchy_decoder_rejects_non_replguid_final_state.md)